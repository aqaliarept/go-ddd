// Package postgres provides PostgreSQL repository implementation for domain-driven design aggregates.
package postgres

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	core "github.com/aqaliarept/go-ddd-kit/pkg/core"
	"github.com/jackc/pgconn"
	"github.com/jackc/pgx/v4"
	"github.com/jackc/pgx/v4/pgxpool"
)

var _ core.Repository = (*repository)(nil)
var _ core.Transactional = (*repository)(nil)

type TableName string

type tableNameEntity struct {
	tableName TableName
}

func WithTableName(tableName TableName) core.StorageOption {
	if tableName == "" {
		panic("table name must be non-empty")
	}
	return tableNameEntity{tableName: tableName}
}

// tx holds a PostgreSQL transaction
type tx struct {
	tx pgx.Tx
}

type repository struct {
	db *pgxpool.Pool
	tx *tx
}

func (r *repository) Begin(ctx context.Context) (context.Context, error) {
	if r.tx != nil {
		panic("transaction in progress")
	}

	sqlTx, err := r.db.Begin(ctx)
	if err != nil {
		return ctx, err
	}

	_, err = sqlTx.Exec(ctx, "SET TRANSACTION ISOLATION LEVEL SERIALIZABLE")
	if err != nil {
		return ctx, errors.Join(err, sqlTx.Rollback(ctx))
	}

	r.tx = &tx{tx: sqlTx}
	return ctx, nil
}

func (r *repository) Commit(ctx context.Context) error {
	if r.tx == nil {
		return fmt.Errorf("%w: no transaction to commit", core.ErrTransactionNotFound)
	}

	err := r.tx.tx.Commit(ctx)
	if err != nil {
		var pgErr *pgconn.PgError
		if errors.As(err, &pgErr) {
			// PostgreSQL serialization failure (40001) indicates concurrent modification
			if pgErr.Code == "40001" {
				err = fmt.Errorf("%w: %w", core.ErrConcurrentModification, err)
			}
		}
	}

	r.tx = nil
	return err
}

func (r *repository) Rollback(ctx context.Context) error {
	if r.tx == nil {
		return nil
	}

	err := r.tx.tx.Rollback(ctx)
	r.tx = nil
	return err
}

type AggregateDocument struct {
	ID            string          `json:"id"`
	State         json.RawMessage `json:"state"`
	Version       uint64          `json:"version"`
	SchemaVersion uint64          `json:"schema"`
}

func getTableName(storageOptions []core.StorageOption) string {
	var tableName TableName
	if len(storageOptions) > 0 {
		for _, opt := range storageOptions {
			if opt, ok := opt.(tableNameEntity); ok {
				tableName = opt.tableName
				break
			}
		}
	}
	if tableName == "" {
		panic("Table name is required. Provide WithTableName storage option.")
	}
	return string(tableName)
}

func (r *repository) Load(ctx context.Context, id core.ID, target core.Restorer, options ...core.LoadOption) error {
	tableName := getTableName(target.StorageOptions())
	// Use FOR UPDATE when in a transaction to ensure proper locking
	query := fmt.Sprintf(`SELECT id, version, data, COALESCE(schema_version, 1) FROM %s WHERE id = $1`, tableName)
	if r.tx != nil {
		query = fmt.Sprintf(`SELECT id, version, data, COALESCE(schema_version, 1) FROM %s WHERE id = $1 FOR UPDATE`, tableName)
	}

	var doc AggregateDocument
	var err error
	if r.tx != nil {
		err = r.tx.tx.QueryRow(ctx, query, string(id)).Scan(&doc.ID, &doc.Version, &doc.State, &doc.SchemaVersion)
	} else {
		err = r.db.QueryRow(ctx, query, string(id)).Scan(&doc.ID, &doc.Version, &doc.State, &doc.SchemaVersion)
	}

	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return core.ErrAggregateNotFound
		}
		return fmt.Errorf("retrieval failed: %w", err)
	}

	// Restore the aggregate state
	return target.Restore(id, core.Version(doc.Version), core.SchemaVersion(doc.SchemaVersion), func(statePtr core.StatePtr) error {
		// Unmarshal JSON state directly into state pointer
		if err := json.Unmarshal(doc.State, statePtr); err != nil {
			return fmt.Errorf("state deserialization error: %w", err)
		}
		return nil
	})
}

// Save implements Repository.
func (r *repository) Save(ctx context.Context, source core.Storer, options ...core.SaveOption) error {
	return source.Store(func(identifier core.ID, aggregate core.AggregatePtr, storageState core.StatePtr, events core.EventPack, currentVersion core.Version, schemaVersion core.SchemaVersion) error {
		tableName := getTableName(source.StorageOptions())

		deletionEvents := core.EventsOfType[core.Tombstone](events)
		if len(deletionEvents) > 0 {
			if currentVersion == 0 {
				return nil
			}

			return r.removeWithVersionCheck(ctx, tableName, identifier, currentVersion)
		}

		stateBytes, err := json.Marshal(storageState)
		if err != nil {
			return fmt.Errorf("state encoding failed: %w", err)
		}

		nextVersion := currentVersion.Next()

		if currentVersion == 0 {
			return r.insertNew(ctx, tableName, identifier, stateBytes, nextVersion, schemaVersion)
		}

		return r.updateExisting(ctx, tableName, identifier, stateBytes, currentVersion, nextVersion, schemaVersion)
	})
}

func (r *repository) removeWithVersionCheck(ctx context.Context, tableName string, id core.ID, expectedVersion core.Version) error {
	query := fmt.Sprintf(`DELETE FROM %s WHERE id = $1 AND version = $2`, tableName)
	var tag pgconn.CommandTag
	var err error
	if r.tx != nil {
		tag, err = r.tx.tx.Exec(ctx, query, string(id), uint64(expectedVersion))
	} else {
		tag, err = r.db.Exec(ctx, query, string(id), uint64(expectedVersion))
	}
	if err != nil {
		var pgErr *pgconn.PgError
		if errors.As(err, &pgErr) {
			// PostgreSQL serialization failure (40001) indicates concurrent modification
			if pgErr.Code == "40001" {
				return core.ErrConcurrentModification
			}
		}
		return fmt.Errorf("removal failed: %w", err)
	}

	rowsAffected := tag.RowsAffected()
	if rowsAffected == 0 {
		// Check if aggregate exists with different version
		var existingVersion uint64
		checkQuery := fmt.Sprintf(`SELECT version FROM %s WHERE id = $1`, tableName)
		var scanErr error
		if r.tx != nil {
			scanErr = r.tx.tx.QueryRow(ctx, checkQuery, string(id)).Scan(&existingVersion)
		} else {
			scanErr = r.db.QueryRow(ctx, checkQuery, string(id)).Scan(&existingVersion)
		}
		if scanErr == nil {
			return core.ErrConcurrentModification
		}
		return core.ErrAggregateNotFound
	}

	return nil
}

func (r *repository) insertNew(ctx context.Context, tableName string, id core.ID, stateBytes []byte, version core.Version, schemaVersion core.SchemaVersion) error {
	query := fmt.Sprintf(`INSERT INTO %s (id, version, data, schema_version) VALUES ($1, $2, $3, $4)`, tableName)
	var err error
	if r.tx != nil {
		_, err = r.tx.tx.Exec(ctx, query, string(id), uint64(version), stateBytes, uint64(schemaVersion))
	} else {
		_, err = r.db.Exec(ctx, query, string(id), uint64(version), stateBytes, uint64(schemaVersion))
	}
	if err != nil {
		var pgErr *pgconn.PgError
		if errors.As(err, &pgErr) {
			// PostgreSQL unique violation (23505) indicates aggregate already exists
			if pgErr.Code == "23505" {
				return core.ErrAggregateExists
			}
			// PostgreSQL serialization failure (40001) indicates concurrent modification
			if pgErr.Code == "40001" {
				return core.ErrConcurrentModification
			}
		}
		return fmt.Errorf("insertion failed: %w", err)
	}
	return nil
}

func (r *repository) updateExisting(ctx context.Context, tableName string, id core.ID, stateBytes []byte, expectedVersion core.Version, nextVersion core.Version, schemaVersion core.SchemaVersion) error {
	query := fmt.Sprintf(`UPDATE %s SET version = $1, data = $2, schema_version = $3 WHERE id = $4 AND version = $5`, tableName)
	var tag pgconn.CommandTag
	var err error
	if r.tx != nil {
		tag, err = r.tx.tx.Exec(ctx, query, uint64(nextVersion), stateBytes, uint64(schemaVersion), string(id), uint64(expectedVersion))
	} else {
		tag, err = r.db.Exec(ctx, query, uint64(nextVersion), stateBytes, uint64(schemaVersion), string(id), uint64(expectedVersion))
	}
	if err != nil {
		var pgErr *pgconn.PgError
		if errors.As(err, &pgErr) {
			// PostgreSQL serialization failure (40001) indicates concurrent modification
			if pgErr.Code == "40001" {
				return core.ErrConcurrentModification
			}
		}
		return fmt.Errorf("update failed: %w", err)
	}

	rowsAffected := tag.RowsAffected()
	if rowsAffected == 0 {
		// Check if aggregate exists with different version
		var existingVersion uint64
		checkQuery := fmt.Sprintf(`SELECT version FROM %s WHERE id = $1`, tableName)
		var scanErr error
		if r.tx != nil {
			scanErr = r.tx.tx.QueryRow(ctx, checkQuery, string(id)).Scan(&existingVersion)
		} else {
			scanErr = r.db.QueryRow(ctx, checkQuery, string(id)).Scan(&existingVersion)
		}
		if scanErr == nil {
			return core.ErrConcurrentModification
		}
		return core.ErrAggregateNotFound
	}

	return nil
}

func newRepository(db *pgxpool.Pool) core.Repository {
	return &repository{
		db: db,
		tx: nil,
	}
}

type Factory struct {
	db *pgxpool.Pool
}

func NewRepositoryFactory(db *pgxpool.Pool) core.RepositoryFactory {
	return &Factory{db: db}
}

func (f *Factory) Create(ctx context.Context) core.Repository {
	return newRepository(f.db)
}
