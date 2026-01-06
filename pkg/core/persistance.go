package core

import (
	"context"
	"errors"
)

var (
	// ErrConcurrentModification is returned when an aggregate has been modified by another operation
	// since it was loaded. This indicates an optimistic concurrency conflict.
	// ConcurrentScope automatically retries operations that return this error.
	//
	// Example:
	//   err := repo.Save(ctx, session)
	//   if err == core.ErrConcurrentModification {
	//       // Aggregate was modified concurrently, retry the operation
	//   }
	ErrConcurrentModification = errors.New("aggregate modified concurrently")

	// ErrAggregateExists is returned when attempting to save an aggregate with an ID that already exists
	// and the aggregate's version is 0 (indicating it should be new).
	//
	// Example:
	//   newSession := domain.NewSession(...)
	//   err := repo.Save(ctx, newSession)
	//   if err == core.ErrAggregateExists {
	//       // An aggregate with this ID already exists
	//   }
	ErrAggregateExists = errors.New("aggregate already exists")

	// ErrAggregateNotFound is returned when attempting to load an aggregate that doesn't exist in storage.
	//
	// Example:
	//   session := &domain.Session{}
	//   err := repo.Load(ctx, sessionID, session)
	//   if err == core.ErrAggregateNotFound {
	//       // Aggregate doesn't exist, create a new one
	//   }
	ErrAggregateNotFound = errors.New("aggregate not found")

	// ErrTransient indicates a temporary error that may succeed on retry.
	// ConcurrentScope automatically retries operations that return this error.
	//
	// Example:
	//   err := repo.Load(ctx, id, aggregate)
	//   if err == core.ErrTransient {
	//       // Temporary error, will be retried automatically by ConcurrentScope
	//   }
	ErrTransient = errors.New("transient error")

	// ErrTransactionNotFound is returned when a transaction operation is attempted
	// but no active transaction is found in the context.
	ErrTransactionNotFound = errors.New("transaction not found")
)

type (
	// LoadOption is a type for repository-specific options that can be passed to Load operations.
	// Each repository implementation defines its own option types.
	LoadOption any

	// SaveOption is a type for repository-specific options that can be passed to Save operations.
	// Each repository implementation defines its own option types.
	//
	// Example (Redis):
	//   err := repo.Save(ctx, session, redis.WithExpiration(24*time.Hour))
	//
	// Example (MongoDB):
	//   err := repo.Save(ctx, session, mongo.WithCollectionName("sessions"))
	SaveOption any

	// SchemaVersion represents the version of the aggregate's state schema.
	// It is used for schema migration when loading aggregates with different state structures.
	SchemaVersion uint64
)

// DefaultSchemaVersion is the default schema version used when an aggregate's state
// doesn't implement StateStorer or StateRestorer.
const DefaultSchemaVersion = SchemaVersion(0)

// Storer is implemented by aggregates to persist their state and events to storage.
// The Aggregate type implements this interface automatically.
//
// Example (implemented by Aggregate):
//   type Session struct {
//       core.Aggregate[SessionState]
//   }
//
//   // Aggregate automatically implements Storer
//   err := repo.Save(ctx, session)
type Storer interface {
	// Store is called by repository implementations to persist the aggregate.
	// The storeFunc should persist the provided state, events, version, and schema version.
	Store(storeFunc func(id ID, aggregate AggregatePtr, storageState StatePtr, events EventPack, version Version, schemaVersion SchemaVersion) error) error

	// StorageOptions returns repository-specific options for this aggregate.
	// These options are used by repository implementations to configure storage behavior.
	//
	// Example:
	//   func (s *Session) StorageOptions() []core.StorageOption {
	//       return []core.StorageOption{redis.WithNamespace("sessions")}
	//   }
	StorageOptions() []StorageOption
}

// StorageOption is a type for repository-specific storage configuration options.
// Each repository implementation defines its own option types.
type StorageOption any

// Restorer is implemented by aggregates to restore their state from storage.
// The Aggregate type implements this interface automatically.
//
// Example (implemented by Aggregate):
//   type Session struct {
//       core.Aggregate[SessionState]
//   }
//
//   // Aggregate automatically implements Restorer
//   session := &Session{}
//   err := repo.Load(ctx, sessionID, session)
type Restorer interface {
	// Restore is called by repository implementations to load the aggregate's state.
	// The restoreFunc should load the state from storage and populate the provided state pointer.
	Restore(id ID, version Version, schemaVersion SchemaVersion, restoreFunc func(state StatePtr) error) error

	// StorageOptions returns repository-specific options for this aggregate.
	// These options are used by repository implementations to configure load behavior.
	StorageOptions() []StorageOption
}

// StateRestorer is optionally implemented by aggregate state types to provide custom
// restoration logic with schema version awareness. This enables schema migration support.
//
// When a state implements StateRestorer, it can handle loading aggregates stored with
// different schema versions and migrate them to the current schema.
//
// Example:
//   type SessionStateV2 struct {
//       AccessToken string
//   }
//
//   func (s *SessionStateV2) Restore(schemaVersion SchemaVersion, restoreFunc func(state StatePtr) error) error {
//       if schemaVersion == 1 {
//           // Migrate from V1 to V2
//           var v1State SessionStateV1
//           if err := restoreFunc(&v1State); err != nil {
//               return err
//           }
//           s.AccessToken = v1State.Token
//           return nil
//       }
//       return restoreFunc(s)
//   }
type StateRestorer interface {
	Restore(schemaVersion SchemaVersion, restoreFunc func(state StatePtr) error) error
}

// StateStorer is optionally implemented by aggregate state types to provide custom
// storage logic with schema version information. This enables schema versioning support.
//
// When a state implements StateStorer, it can specify which schema version to use
// when storing the aggregate, enabling backward compatibility and migration support.
//
// Example:
//   type SessionStateV2 struct {
//       AccessToken string
//   }
//
//   func (s *SessionStateV2) Store(storeFunc func(state StatePtr, schemaVersion SchemaVersion) error) error {
//       return storeFunc(s, SchemaVersion(2))
//   }
type StateStorer interface {
	Store(storeFunc func(state StatePtr, schemaVersion SchemaVersion) error) error
}

// Repository provides persistence operations for aggregates.
// Repository implementations are provided by storage-specific packages (redis, mongo, postgres).
//
// IMPORTANT: Repositories should never be used directly. They must only be accessed through
// ConcurrentScope.Run() to ensure proper transaction handling, retries, and change tracking.
//
// Example (within ConcurrentScope):
//   changes, err := scope.Run(ctx, func(ctx context.Context, repo core.Repository) error {
//       session := &domain.Session{}
//       err := repo.Load(ctx, sessionID, session)
//       if err != nil {
//           return err
//       }
//
//       events, err := session.ProcessRequest(now)
//       if err != nil {
//           return err
//       }
//
//       return repo.Save(ctx, session, redis.WithExpiration(24*time.Hour))
//   })
type Repository interface {
	// Load retrieves an aggregate by its ID from storage.
	// The aggregate must implement Restorer (Aggregate does this automatically).
	// Returns ErrAggregateNotFound if the aggregate doesn't exist.
	//
	// Example:
	//   session := &domain.Session{}
	//   err := repo.Load(ctx, sessionID, session)
	//   if err == core.ErrAggregateNotFound {
	//       // Create new session
	//   }
	Load(ctx context.Context, id ID, aggregate Restorer, options ...LoadOption) error

	// Save persists an aggregate to storage.
	// The aggregate must implement Storer (Aggregate does this automatically).
	// Returns ErrConcurrentModification if the aggregate version doesn't match.
	// Returns ErrAggregateExists if saving a new aggregate (version 0) with an existing ID.
	//
	// Example:
	//   err := repo.Save(ctx, session, redis.WithExpiration(24*time.Hour))
	//   if err == core.ErrConcurrentModification {
	//       // Retry the operation
	//   }
	Save(ctx context.Context, aggregate Storer, options ...SaveOption) error
}

// RepositoryFactory creates Repository instances for use within ConcurrentScope.
// Each storage backend provides its own factory implementation.
//
// Example:
//   repoFactory := redispkg.NewRepositoryFactory(redisClient)
//   scope := core.NewConcurrentScope(repoFactory)
//
//   // Factory is used internally by ConcurrentScope
//   changes, err := scope.Run(ctx, func(ctx context.Context, repo core.Repository) error {
//       // Use repo here
//   })
type RepositoryFactory interface {
	// Create returns a new Repository instance for the given context.
	// The context may contain transaction information for transactional repositories.
	Create(ctx context.Context) Repository
}

// Transactional is optionally implemented by Repository implementations to provide
// transaction support. When implemented, ConcurrentScope will automatically manage
// transactions (Begin, Commit, Rollback) for operations within Run().
//
// Example (repository implementation):
//   type TransactionalRepository struct {
//       // Repository implementation
//   }
//
//   func (r *TransactionalRepository) Begin(ctx context.Context) (context.Context, error) {
//       tx, err := r.db.Begin()
//       if err != nil {
//           return nil, err
//       }
//       return context.WithValue(ctx, txKey, tx), nil
//   }
//
//   func (r *TransactionalRepository) Commit(ctx context.Context) error {
//       tx := ctx.Value(txKey).(*sql.Tx)
//       return tx.Commit()
//   }
//
//   func (r *TransactionalRepository) Rollback(ctx context.Context) error {
//       tx := ctx.Value(txKey).(*sql.Tx)
//       return tx.Rollback()
//   }
type Transactional interface {
	// Begin starts a new transaction and returns a context enriched with transaction information.
	// The returned context should be used for all subsequent operations within the transaction.
	Begin(ctx context.Context) (context.Context, error)

	// Commit commits the transaction associated with the context.
	Commit(ctx context.Context) error

	// Rollback rolls back the transaction associated with the context.
	Rollback(ctx context.Context) error
}
