// Package mongo provides MongoDB repository implementation for domain-driven design aggregates.
package mongo

import (
	"context"
	"errors"
	"fmt"

	core "github.com/aqaliarept/go-ddd-kit/pkg/core"
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
	"go.mongodb.org/mongo-driver/v2/mongo/readconcern"
	"go.mongodb.org/mongo-driver/v2/mongo/writeconcern"
)

var _ core.Repository = (*repository)(nil)
var _ core.Transactional = (*repository)(nil)

type CollectionName string

type collectionNameEntity struct {
	collectionName CollectionName
}

func WithCollectionName(collectionName CollectionName) core.StorageOption {
	if collectionName == "" {
		panic("collection name must be non-empty")
	}
	return collectionNameEntity{collectionName: collectionName}
}

// Tx holds a MongoDB session + session context to run operations in a transaction.
type tx struct {
	session *mongo.Session
	ctx     context.Context
}

type repository struct {
	db *mongo.Database
	tx *tx
}

// Begin implements Transactional.
func (r *repository) Begin(ctx context.Context) (context.Context, error) {
	if r.tx != nil {
		panic("transaction in progress")
	}
	// Start a session
	sess, err := r.db.Client().StartSession()
	if err != nil {
		return ctx, err
	}

	// Start the transaction (set concerns as needed)
	txnOpts := options.Transaction().
		SetReadConcern(readconcern.Snapshot()).
		SetWriteConcern(writeconcern.Majority())

	// Create session context
	sctx := mongo.NewSessionContext(ctx, sess)

	// Start transaction
	if err := sess.StartTransaction(txnOpts); err != nil {
		sess.EndSession(ctx)
		return ctx, err
	}
	r.tx = &tx{session: sess, ctx: sctx}
	return sctx, nil
}

// Commit implements Transactional.
func (r *repository) Commit(ctx context.Context) error {
	err := r.tx.session.CommitTransaction(r.tx.ctx)
	var mongoErr mongo.CommandError
	if errors.As(err, &mongoErr) {
		switch mongoErr.Code {
		case 112: // WriteConflict error code
			err = fmt.Errorf("%w: %w", core.ErrConcurrentModification, err)
		case 251: // NoSuchTransaction error code
			err = fmt.Errorf("%w: %w", core.ErrTransactionNotFound, err)
		}
	}

	r.tx.session.EndSession(ctx)
	r.tx = nil
	return err
}

// Rollback implements Transactional.
func (r *repository) Rollback(ctx context.Context) error {
	err := r.tx.session.AbortTransaction(r.tx.ctx)
	r.tx.session.EndSession(ctx)
	r.tx = nil

	// If the transaction was already aborted (e.g., due to WriteConflict),
	// that's not an error - it's expected behavior
	var mongoErr mongo.CommandError
	if errors.As(err, &mongoErr) && mongoErr.Code == 251 { // NoSuchTransaction error code
		return nil // Transaction was already aborted, which is fine
	}

	return err
}

type AggregateDocument[T any] struct {
	State         T      `bson:"state"`
	ID            string `bson:"_id"`
	Version       uint64 `bson:"version"`
	SchemaVersion uint64 `bson:"schema"`
}

func getCollectionName(storageOptions []core.StorageOption) string {
	var collectionName CollectionName
	if len(storageOptions) > 0 {
		for _, opt := range storageOptions {
			if opt, ok := opt.(collectionNameEntity); ok {
				collectionName = opt.collectionName
				break
			}
		}
	}
	if collectionName == "" {
		panic("Collection name is required. Provide WithCollectionName storage option.")
	}
	return string(collectionName)
}

// Load implements Repository.
func (r *repository) Load(ctx context.Context, id core.ID, target core.Restorer, options ...core.LoadOption) error {
	operationCtx := ctx
	if r.tx != nil {
		operationCtx = r.tx.ctx
	}

	collectionName := getCollectionName(target.StorageOptions())
	var doc AggregateDocument[bson.RawValue]
	err := r.db.Collection(collectionName).FindOne(operationCtx, bson.M{"_id": string(id)}).Decode(&doc)
	if err != nil {
		if errors.Is(err, mongo.ErrNoDocuments) {
			return core.ErrAggregateNotFound
		}
		return fmt.Errorf("retrieval failed: %w", err)
	}

	return target.Restore(id, core.Version(doc.Version), core.SchemaVersion(doc.SchemaVersion), func(statePtr core.StatePtr) error {
		if err := doc.State.Unmarshal(statePtr); err != nil {
			return fmt.Errorf("state deserialization error: %w", err)
		}
		return nil
	})
}

// Save implements Repository.
func (r *repository) Save(ctx context.Context, storer core.Storer, options ...core.SaveOption) error {
	return storer.Store(func(identifier core.ID, aggregate core.AggregatePtr, storageState core.StatePtr, events core.EventPack, currentVersion core.Version, schemaVersion core.SchemaVersion) error {
		operationCtx := ctx
		if r.tx != nil {
			operationCtx = r.tx.ctx
		}
		collectionName := getCollectionName(storer.StorageOptions())
		collection := r.db.Collection(collectionName)

		deletionEvents := core.EventsOfType[core.Tombstone](events)
		if len(deletionEvents) > 0 {
			if currentVersion == 0 {
				return nil
			}

			return r.removeWithVersionCheck(operationCtx, collection, identifier, currentVersion)
		}

		stateBytes, err := bson.Marshal(storageState)
		if err != nil {
			return fmt.Errorf("state encoding failed: %w", err)
		}

		var statePayload bson.M
		if err = bson.Unmarshal(stateBytes, &statePayload); err != nil {
			return fmt.Errorf("state parsing failed: %w", err)
		}

		doc := AggregateDocument[any]{
			ID:            string(identifier),
			Version:       uint64(currentVersion + 1),
			State:         statePayload,
			SchemaVersion: uint64(schemaVersion),
		}

		if currentVersion == 0 {
			return r.insertNew(operationCtx, collection, doc)
		}

		return r.updateExisting(operationCtx, collection, identifier, doc, currentVersion)
	})
}

func (r *repository) removeWithVersionCheck(ctx context.Context, collection *mongo.Collection, id core.ID, expectedVersion core.Version) error {
	filter := bson.M{"_id": string(id), "version": uint64(expectedVersion)}
	result, err := collection.DeleteOne(ctx, filter)
	if err != nil {
		var mongoErr mongo.CommandError
		if errors.As(err, &mongoErr) && mongoErr.Code == 112 {
			return core.ErrConcurrentModification
		}
		return fmt.Errorf("removal failed: %w", err)
	}

	if result.DeletedCount == 0 {
		var existingDoc AggregateDocument[any]
		err = collection.FindOne(ctx, bson.M{"_id": string(id)}).Decode(&existingDoc)
		if err == nil {
			return core.ErrConcurrentModification
		}
		return core.ErrAggregateNotFound
	}

	return nil
}

func (r *repository) insertNew(ctx context.Context, collection *mongo.Collection, doc AggregateDocument[any]) error {
	_, err := collection.InsertOne(ctx, doc)
	if err != nil {
		var writeErr mongo.WriteException
		if errors.As(err, &writeErr) {
			for _, we := range writeErr.WriteErrors {
				if we.Code == 11000 {
					return core.ErrAggregateExists
				}
			}
		}
		return fmt.Errorf("insertion failed: %w", err)
	}
	return nil
}

func (r *repository) updateExisting(ctx context.Context, collection *mongo.Collection, id core.ID, doc AggregateDocument[any], expectedRev core.Version) error {
	filter := bson.M{"_id": string(id), "version": uint64(expectedRev)}
	result, err := collection.UpdateOne(ctx, filter, bson.M{"$set": doc})
	if err != nil {
		var mongoErr mongo.CommandError
		if errors.As(err, &mongoErr) && mongoErr.Code == 112 {
			return core.ErrConcurrentModification
		}
		return fmt.Errorf("update failed: %w", err)
	}

	if result.MatchedCount == 0 {
		var existingDoc AggregateDocument[any]
		err = collection.FindOne(ctx, bson.M{"_id": string(id)}).Decode(&existingDoc)
		if err == nil {
			return core.ErrConcurrentModification
		}
		return core.ErrAggregateNotFound
	}

	return nil
}

func newRepository(db *mongo.Database) core.Repository {
	return &repository{
		db: db,
	}
}

type Factory struct {
	db *mongo.Database
}

func NewRepositoryFactory(db *mongo.Database) core.RepositoryFactory {
	return &Factory{db: db}
}

func (f *Factory) Create(ctx context.Context) core.Repository {
	return newRepository(f.db)
}
