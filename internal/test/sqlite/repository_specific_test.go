package sqlite_test

import (
	"context"
	"testing"

	testpkg "github.com/aqaliarept/go-ddd-kit/internal/test"
	core "github.com/aqaliarept/go-ddd-kit/pkg/core"
	sqlitepkg "github.com/aqaliarept/go-ddd-kit/pkg/sqlite"
	"github.com/stretchr/testify/require"
)

func newSQLiteTestAgg(id core.ID) *testpkg.TestAgg {
	return testpkg.NewTestAgg(id)
}

func newSQLiteTestAggForLoad() *testpkg.TestAgg {
	return &testpkg.TestAgg{}
}

type noTableNameAgg struct {
	testpkg.TestAgg
}

func (n *noTableNameAgg) StorageOptions() []core.StorageOption {
	return []core.StorageOption{}
}

type invalidState struct {
	InvalidField chan int
}

func (s *invalidState) Apply(event core.Event) {
}

type invalidSQLiteAgg struct {
	core.Aggregate[invalidState]
}

func (i *invalidSQLiteAgg) StorageOptions() []core.StorageOption {
	return []core.StorageOption{sqlitepkg.WithTableName("test_agg")}
}

func TestSQLiteRepositorySpecific(t *testing.T) {
	testDB := SetupSQLiteTestDB(t)
	ctx := context.Background()
	factory := sqlitepkg.NewRepositoryFactory(testDB.Database())

	t.Run(`Scenario: Begin transaction with existing transaction panics
  Given a repository with an active transaction
  When an attempt is made to begin another transaction
  Then the operation should panic`, func(t *testing.T) {
		t.Parallel()
		repo := factory.Create(ctx)
		tx, ok := repo.(core.Transactional)
		require.True(t, ok)

		txCtx, err := tx.Begin(ctx)
		require.NoError(t, err)

		require.Panics(t, func() {
			_, beginErr := tx.Begin(txCtx)
			require.NoError(t, beginErr)
		})

		err = tx.Rollback(txCtx)
		require.NoError(t, err)
	})

	t.Run(`Scenario: Begin transaction with cancelled context returns error
  Given a repository
  When Begin is called with a cancelled context
  Then the operation should return an error`, func(t *testing.T) {
		t.Parallel()
		repo := factory.Create(ctx)
		tx, ok := repo.(core.Transactional)
		require.True(t, ok)

		errorCtx, cancel := context.WithCancel(context.Background())
		cancel()

		_, err := tx.Begin(errorCtx)
		require.Error(t, err)
	})

	t.Run(`Scenario: Begin transaction execution error with cancelled context
  Given a repository
  When Begin is called with a cancelled context
  Then the operation should return an error`, func(t *testing.T) {
		t.Parallel()
		repo := factory.Create(ctx)
		tx, ok := repo.(core.Transactional)
		require.True(t, ok)

		errorCtx, cancel := context.WithCancel(context.Background())
		cancel()

		_, err := tx.Begin(errorCtx)
		require.Error(t, err)
	})

	t.Run(`Scenario: Commit without active transaction returns error
  Given a repository without an active transaction
  When Commit is called
  Then the operation should return ErrTransactionNotFound`, func(t *testing.T) {
		t.Parallel()
		repo := factory.Create(ctx)
		tx, ok := repo.(core.Transactional)
		require.True(t, ok)

		err := tx.Commit(ctx)
		require.Error(t, err)
		require.ErrorIs(t, err, core.ErrTransactionNotFound)
	})

	t.Run(`Scenario: Rollback without active transaction succeeds
  Given a repository without an active transaction
  When Rollback is called
  Then the operation should succeed`, func(t *testing.T) {
		t.Parallel()
		repo := factory.Create(ctx)
		tx, ok := repo.(core.Transactional)
		require.True(t, ok)

		err := tx.Rollback(ctx)
		require.NoError(t, err)
	})

	t.Run(`Scenario: Load with cancelled context returns retrieval error
  Given a repository
  When Load is called with a cancelled context
  Then the operation should return a retrieval error`, func(t *testing.T) {
		t.Parallel()
		repo := factory.Create(ctx)
		errorCtx, cancel := context.WithCancel(ctx)
		cancel()

		agg := newSQLiteTestAggForLoad()
		err := repo.Load(errorCtx, "non-existent-id", agg)
		require.Error(t, err)
		require.Contains(t, err.Error(), "retrieval failed")
	})

	t.Run(`Scenario: Load aggregate within transaction
  Given an aggregate that has been saved
  When a transaction is started
  And the aggregate is loaded within the transaction
  Then the aggregate should be loaded successfully
  And the aggregate state should match the saved state
  When the transaction is rolled back
  Then the operation should succeed`, func(t *testing.T) {
		t.Parallel()
		repo := factory.Create(ctx)
		tx, ok := repo.(core.Transactional)
		require.True(t, ok)

		agg := newSQLiteTestAgg("tx-load-id")
		_, err := agg.SingleEventCommand("tx-load-value")
		require.NoError(t, err)

		err = repo.Save(ctx, agg)
		require.NoError(t, err)

		txCtx, err := tx.Begin(ctx)
		require.NoError(t, err)

		loadedAgg := newSQLiteTestAggForLoad()
		err = repo.Load(txCtx, "tx-load-id", loadedAgg)
		require.NoError(t, err)

		state := loadedAgg.State()
		require.Equal(t, "tx-load-value", state.String)

		err = tx.Rollback(txCtx)
		require.NoError(t, err)
	})

	t.Run(`Scenario: Load aggregate without transaction
  Given an aggregate that has been saved
  When the aggregate is loaded without a transaction
  Then the aggregate should be loaded successfully
  And the aggregate state should match the saved state`, func(t *testing.T) {
		t.Parallel()
		repo := factory.Create(ctx)
		agg := newSQLiteTestAgg("non-tx-load-id")
		_, err := agg.SingleEventCommand("non-tx-load-value")
		require.NoError(t, err)

		err = repo.Save(ctx, agg)
		require.NoError(t, err)

		loadedAgg := newSQLiteTestAggForLoad()
		err = repo.Load(ctx, "non-tx-load-id", loadedAgg)
		require.NoError(t, err)

		state := loadedAgg.State()
		require.Equal(t, "non-tx-load-value", state.String)
	})

	t.Run(`Scenario: Save aggregate with invalid state encoding returns error
  Given an aggregate with a state that cannot be encoded
  When Save is called
  Then the operation should return a state encoding error`, func(t *testing.T) {
		t.Parallel()
		repo := factory.Create(ctx)

		agg := &invalidSQLiteAgg{}
		agg.Initialize("encoding-error-id", testpkg.Created{})

		err := repo.Save(ctx, agg)
		require.Error(t, err)
		require.Contains(t, err.Error(), "state encoding failed")
	})

	t.Run(`Scenario: Save aggregate removal within transaction
  Given an aggregate that has been saved
  When a transaction is started
  And the aggregate is loaded within the transaction
  And the aggregate is removed
  And the aggregate is saved within the transaction
  And the transaction is committed
  Then the aggregate should be deleted
  When an attempt is made to load the aggregate
  Then the load operation should fail with ErrAggregateNotFound`, func(t *testing.T) {
		t.Parallel()
		repo := factory.Create(ctx)
		tx, ok := repo.(core.Transactional)
		require.True(t, ok)

		agg := newSQLiteTestAgg("tx-removal-id")
		_, err := agg.SingleEventCommand("tx-removal-value")
		require.NoError(t, err)

		err = repo.Save(ctx, agg)
		require.NoError(t, err)

		txCtx, err := tx.Begin(ctx)
		require.NoError(t, err)

		loadedAgg := newSQLiteTestAggForLoad()
		err = repo.Load(txCtx, "tx-removal-id", loadedAgg)
		require.NoError(t, err)

		_, err = loadedAgg.Remove()
		require.NoError(t, err)

		err = repo.Save(txCtx, loadedAgg)
		require.NoError(t, err)

		err = tx.Commit(txCtx)
		require.NoError(t, err)

		err = repo.Load(ctx, "tx-removal-id", newSQLiteTestAggForLoad())
		require.Error(t, err)
		require.ErrorIs(t, err, core.ErrAggregateNotFound)
	})

	t.Run(`Scenario: Save aggregate removal without transaction
  Given an aggregate that has been saved
  When the aggregate is loaded
  And the aggregate is removed
  And the aggregate is saved
  Then the aggregate should be deleted
  When an attempt is made to load the aggregate
  Then the load operation should fail with ErrAggregateNotFound`, func(t *testing.T) {
		t.Parallel()
		repo := factory.Create(ctx)
		agg := newSQLiteTestAgg("non-tx-removal-id")
		_, err := agg.SingleEventCommand("non-tx-removal-value")
		require.NoError(t, err)

		err = repo.Save(ctx, agg)
		require.NoError(t, err)

		loadedAgg := newSQLiteTestAggForLoad()
		err = repo.Load(ctx, "non-tx-removal-id", loadedAgg)
		require.NoError(t, err)

		_, err = loadedAgg.Remove()
		require.NoError(t, err)

		err = repo.Save(ctx, loadedAgg)
		require.NoError(t, err)

		err = repo.Load(ctx, "non-tx-removal-id", newSQLiteTestAggForLoad())
		require.Error(t, err)
		require.ErrorIs(t, err, core.ErrAggregateNotFound)
	})

	t.Run(`Scenario: Insert new aggregate within transaction
  Given a new aggregate
  When a transaction is started
  And the aggregate is saved within the transaction
  And the transaction is committed
  Then the aggregate should be persisted
  When the aggregate is loaded
  Then the aggregate state should match the saved state`, func(t *testing.T) {
		t.Parallel()
		repo := factory.Create(ctx)
		tx, ok := repo.(core.Transactional)
		require.True(t, ok)

		txCtx, err := tx.Begin(ctx)
		require.NoError(t, err)

		agg := newSQLiteTestAgg("tx-insert-id")
		_, err = agg.SingleEventCommand("tx-insert-value")
		require.NoError(t, err)

		err = repo.Save(txCtx, agg)
		require.NoError(t, err)

		err = tx.Commit(txCtx)
		require.NoError(t, err)

		loadedAgg := newSQLiteTestAggForLoad()
		err = repo.Load(ctx, "tx-insert-id", loadedAgg)
		require.NoError(t, err)

		state := loadedAgg.State()
		require.Equal(t, "tx-insert-value", state.String)
	})

	t.Run(`Scenario: Insert new aggregate without transaction
  Given a new aggregate
  When the aggregate is saved without a transaction
  Then the aggregate should be persisted
  When the aggregate is loaded
  Then the aggregate state should match the saved state`, func(t *testing.T) {
		t.Parallel()
		repo := factory.Create(ctx)
		agg := newSQLiteTestAgg("non-tx-insert-id")
		_, err := agg.SingleEventCommand("non-tx-insert-value")
		require.NoError(t, err)

		err = repo.Save(ctx, agg)
		require.NoError(t, err)

		loadedAgg := newSQLiteTestAggForLoad()
		err = repo.Load(ctx, "non-tx-insert-id", loadedAgg)
		require.NoError(t, err)

		state := loadedAgg.State()
		require.Equal(t, "non-tx-insert-value", state.String)
	})

	t.Run(`Scenario: Update existing aggregate within transaction
  Given an aggregate that has been saved
  When a transaction is started
  And the aggregate is loaded within the transaction
  And the aggregate is modified
  And the aggregate is saved within the transaction
  And the transaction is committed
  Then the aggregate should be updated
  When the aggregate is loaded
  Then the aggregate state should reflect the update`, func(t *testing.T) {
		t.Parallel()
		repo := factory.Create(ctx)
		tx, ok := repo.(core.Transactional)
		require.True(t, ok)

		agg := newSQLiteTestAgg("tx-update-id")
		_, err := agg.SingleEventCommand("initial-value")
		require.NoError(t, err)

		err = repo.Save(ctx, agg)
		require.NoError(t, err)

		txCtx, err := tx.Begin(ctx)
		require.NoError(t, err)

		loadedAgg := newSQLiteTestAggForLoad()
		err = repo.Load(txCtx, "tx-update-id", loadedAgg)
		require.NoError(t, err)

		_, err = loadedAgg.SingleEventCommand("updated-value")
		require.NoError(t, err)

		err = repo.Save(txCtx, loadedAgg)
		require.NoError(t, err)

		err = tx.Commit(txCtx)
		require.NoError(t, err)

		finalAgg := newSQLiteTestAggForLoad()
		err = repo.Load(ctx, "tx-update-id", finalAgg)
		require.NoError(t, err)

		state := finalAgg.State()
		require.Equal(t, "updated-value", state.String)
	})

	t.Run(`Scenario: Update existing aggregate without transaction
  Given an aggregate that has been saved
  When the aggregate is loaded
  And the aggregate is modified
  And the aggregate is saved
  Then the aggregate should be updated
  When the aggregate is loaded again
  Then the aggregate state should reflect the update`, func(t *testing.T) {
		t.Parallel()
		repo := factory.Create(ctx)
		agg := newSQLiteTestAgg("non-tx-update-id")
		_, err := agg.SingleEventCommand("initial-value")
		require.NoError(t, err)

		err = repo.Save(ctx, agg)
		require.NoError(t, err)

		loadedAgg := newSQLiteTestAggForLoad()
		err = repo.Load(ctx, "non-tx-update-id", loadedAgg)
		require.NoError(t, err)

		_, err = loadedAgg.SingleEventCommand("updated-value")
		require.NoError(t, err)

		err = repo.Save(ctx, loadedAgg)
		require.NoError(t, err)

		finalAgg := newSQLiteTestAggForLoad()
		err = repo.Load(ctx, "non-tx-update-id", finalAgg)
		require.NoError(t, err)

		state := finalAgg.State()
		require.Equal(t, "updated-value", state.String)
	})

	t.Run(`Scenario: WithTableName with empty string panics
  Given the WithTableName function
  When it is called with an empty string
  Then the operation should panic`, func(t *testing.T) {
		t.Parallel()
		require.Panics(t, func() {
			sqlitepkg.WithTableName("")
		})
	})

	t.Run(`Scenario: GetTableName without table name panics
  Given a repository
  When Load is called with an aggregate that has no table name
  Then the operation should panic`, func(t *testing.T) {
		t.Parallel()
		repo := factory.Create(ctx)

		baseAgg := testpkg.NewTestAgg("no-table-name-id")
		agg := &noTableNameAgg{
			TestAgg: *baseAgg,
		}

		require.Panics(t, func() {
			err := repo.Load(ctx, "no-table-name-id", agg)
			require.NoError(t, err)
		}, "Load should panic when aggregate has no table name in StorageOptions")
	})

	t.Run(`Scenario: Load aggregate version check within transaction
  Given an aggregate that has been saved
  When a transaction is started
  And the aggregate is loaded within the transaction
  Then the aggregate version should be correct
  When the transaction is committed
  Then the operation should succeed`, func(t *testing.T) {
		t.Parallel()
		repo := factory.Create(ctx)
		tx, ok := repo.(core.Transactional)
		require.True(t, ok)

		agg := newSQLiteTestAgg("tx-version-check-id")
		_, err := agg.SingleEventCommand("initial-value")
		require.NoError(t, err)

		err = repo.Save(ctx, agg)
		require.NoError(t, err)

		txCtx, err := tx.Begin(ctx)
		require.NoError(t, err)

		loadedAgg := newSQLiteTestAggForLoad()
		err = repo.Load(txCtx, "tx-version-check-id", loadedAgg)
		require.NoError(t, err)
		require.Equal(t, core.Version(1), loadedAgg.Version())

		err = tx.Commit(txCtx)
		require.NoError(t, err)
	})

	t.Run(`Scenario: Remove aggregate with version check within transaction
  Given an aggregate that has been saved
  When a transaction is started
  And the aggregate is loaded within the transaction
  And the aggregate is removed
  And the aggregate is saved within the transaction
  And the transaction is committed
  Then the operation should succeed`, func(t *testing.T) {
		t.Parallel()
		repo := factory.Create(ctx)
		tx, ok := repo.(core.Transactional)
		require.True(t, ok)

		agg := newSQLiteTestAgg("tx-remove-version-id")
		_, err := agg.SingleEventCommand("initial-value")
		require.NoError(t, err)

		err = repo.Save(ctx, agg)
		require.NoError(t, err)

		txCtx, err := tx.Begin(ctx)
		require.NoError(t, err)

		loadedAgg := newSQLiteTestAggForLoad()
		err = repo.Load(txCtx, "tx-remove-version-id", loadedAgg)
		require.NoError(t, err)

		_, err = loadedAgg.Remove()
		require.NoError(t, err)

		err = repo.Save(txCtx, loadedAgg)
		require.NoError(t, err)

		err = tx.Commit(txCtx)
		require.NoError(t, err)
	})

	t.Run(`Scenario: Remove aggregate with version check without transaction
  Given an aggregate that has been saved
  When the aggregate is loaded
  And the aggregate is removed
  And the aggregate is saved
  Then the operation should succeed`, func(t *testing.T) {
		t.Parallel()
		repo := factory.Create(ctx)
		agg := newSQLiteTestAgg("non-tx-remove-version-id")
		_, err := agg.SingleEventCommand("initial-value")
		require.NoError(t, err)

		err = repo.Save(ctx, agg)
		require.NoError(t, err)

		loadedAgg := newSQLiteTestAggForLoad()
		err = repo.Load(ctx, "non-tx-remove-version-id", loadedAgg)
		require.NoError(t, err)

		_, err = loadedAgg.Remove()
		require.NoError(t, err)

		err = repo.Save(ctx, loadedAgg)
		require.NoError(t, err)
	})

	t.Run(`Scenario: Update existing aggregate with version check within transaction
  Given an aggregate that has been saved
  When a transaction is started
  And the aggregate is loaded within the transaction
  And the aggregate is modified
  And the aggregate is saved within the transaction
  And the transaction is committed
  Then the operation should succeed`, func(t *testing.T) {
		t.Parallel()
		repo := factory.Create(ctx)
		tx, ok := repo.(core.Transactional)
		require.True(t, ok)

		agg := newSQLiteTestAgg("tx-update-version-id")
		_, err := agg.SingleEventCommand("initial-value")
		require.NoError(t, err)

		err = repo.Save(ctx, agg)
		require.NoError(t, err)

		txCtx, err := tx.Begin(ctx)
		require.NoError(t, err)

		loadedAgg := newSQLiteTestAggForLoad()
		err = repo.Load(txCtx, "tx-update-version-id", loadedAgg)
		require.NoError(t, err)

		_, err = loadedAgg.SingleEventCommand("updated-value")
		require.NoError(t, err)

		err = repo.Save(txCtx, loadedAgg)
		require.NoError(t, err)

		err = tx.Commit(txCtx)
		require.NoError(t, err)
	})

	t.Run(`Scenario: Update existing aggregate with version check without transaction
  Given an aggregate that has been saved
  When the aggregate is loaded
  And the aggregate is modified
  And the aggregate is saved
  Then the operation should succeed`, func(t *testing.T) {
		t.Parallel()
		repo := factory.Create(ctx)
		agg := newSQLiteTestAgg("non-tx-update-version-id")
		_, err := agg.SingleEventCommand("initial-value")
		require.NoError(t, err)

		err = repo.Save(ctx, agg)
		require.NoError(t, err)

		loadedAgg := newSQLiteTestAggForLoad()
		err = repo.Load(ctx, "non-tx-update-version-id", loadedAgg)
		require.NoError(t, err)

		_, err = loadedAgg.SingleEventCommand("updated-value")
		require.NoError(t, err)

		err = repo.Save(ctx, loadedAgg)
		require.NoError(t, err)
	})

	t.Run(`Scenario: Remove aggregate with concurrent modification error
  Given an aggregate that has been saved
  And two instances of the same aggregate loaded
  When the first aggregate is modified and saved
  And an attempt is made to remove and save the second aggregate
  Then the removal save operation should fail
  And the error should be ErrConcurrentModification`, func(t *testing.T) {
		t.Parallel()
		repo := factory.Create(ctx)
		agg := newSQLiteTestAgg("remove-concurrent-id")
		_, err := agg.SingleEventCommand("initial-value")
		require.NoError(t, err)

		err = repo.Save(ctx, agg)
		require.NoError(t, err)

		agg1 := newSQLiteTestAggForLoad()
		err = repo.Load(ctx, "remove-concurrent-id", agg1)
		require.NoError(t, err)

		agg2 := newSQLiteTestAggForLoad()
		err = repo.Load(ctx, "remove-concurrent-id", agg2)
		require.NoError(t, err)

		_, err = agg1.SingleEventCommand("updated-value")
		require.NoError(t, err)

		err = repo.Save(ctx, agg1)
		require.NoError(t, err)

		_, err = agg2.Remove()
		require.NoError(t, err)

		err = repo.Save(ctx, agg2)
		require.Error(t, err)
		require.ErrorIs(t, err, core.ErrConcurrentModification)
	})

	t.Run(`Scenario: Update existing aggregate with concurrent modification error
  Given an aggregate that has been saved
  And two instances of the same aggregate loaded
  When the first aggregate is modified and saved
  And an attempt is made to modify and save the second aggregate
  Then the save operation should fail
  And the error should be ErrConcurrentModification`, func(t *testing.T) {
		t.Parallel()
		repo := factory.Create(ctx)
		agg := newSQLiteTestAgg("update-concurrent-id")
		_, err := agg.SingleEventCommand("initial-value")
		require.NoError(t, err)

		err = repo.Save(ctx, agg)
		require.NoError(t, err)

		agg1 := newSQLiteTestAggForLoad()
		err = repo.Load(ctx, "update-concurrent-id", agg1)
		require.NoError(t, err)

		agg2 := newSQLiteTestAggForLoad()
		err = repo.Load(ctx, "update-concurrent-id", agg2)
		require.NoError(t, err)

		_, err = agg1.SingleEventCommand("value-1")
		require.NoError(t, err)

		err = repo.Save(ctx, agg1)
		require.NoError(t, err)

		_, err = agg2.SingleEventCommand("value-2")
		require.NoError(t, err)

		err = repo.Save(ctx, agg2)
		require.Error(t, err)
		require.ErrorIs(t, err, core.ErrConcurrentModification)
	})

	t.Run(`Scenario: Load aggregate with corrupted JSON data returns deserialization error
  Given an aggregate with corrupted JSON data in the database
  When Load is called
  Then the operation should return a state deserialization error`, func(t *testing.T) {
		t.Parallel()
		repo := factory.Create(ctx)

		_, err := testDB.Database().ExecContext(ctx, `INSERT INTO test_agg (id, version, data, schema_version) VALUES (?, ?, ?, ?)`, "corrupted-json-id", 1, "invalid json data{", 1)
		require.NoError(t, err)

		agg := newSQLiteTestAggForLoad()
		err = repo.Load(ctx, "corrupted-json-id", agg)
		require.Error(t, err)
		require.Contains(t, err.Error(), "state deserialization error")
	})

	t.Run(`Scenario: Remove aggregate version check within transaction detects concurrent modification
  Given an aggregate that has been saved
  When a transaction is started
  And the aggregate is loaded within the transaction
  And the aggregate is modified externally (outside transaction)
  And an attempt is made to remove the aggregate within the transaction
  Then the removal should detect version mismatch using transaction path
  And the error should be ErrConcurrentModification`, func(t *testing.T) {
		t.Parallel()
		repo := factory.Create(ctx)
		tx, ok := repo.(core.Transactional)
		require.True(t, ok)

		agg := newSQLiteTestAgg("tx-remove-version-check-id")
		_, err := agg.SingleEventCommand("initial-value")
		require.NoError(t, err)

		err = repo.Save(ctx, agg)
		require.NoError(t, err)

		txCtx, err := tx.Begin(ctx)
		require.NoError(t, err)

		loadedAgg := newSQLiteTestAggForLoad()
		err = repo.Load(txCtx, "tx-remove-version-check-id", loadedAgg)
		require.NoError(t, err)

		externalAgg := newSQLiteTestAggForLoad()
		err = repo.Load(ctx, "tx-remove-version-check-id", externalAgg)
		require.NoError(t, err)

		_, err = externalAgg.SingleEventCommand("external-update")
		require.NoError(t, err)
		err = repo.Save(ctx, externalAgg)
		require.NoError(t, err)

		_, err = loadedAgg.Remove()
		require.NoError(t, err)

		err = repo.Save(txCtx, loadedAgg)
		require.Error(t, err)
		require.ErrorIs(t, err, core.ErrConcurrentModification)

		err = tx.Rollback(txCtx)
		require.NoError(t, err)
	})

	t.Run(`Scenario: Update aggregate version check within transaction detects concurrent modification
  Given an aggregate that has been saved
  When a transaction is started
  And the aggregate is loaded within the transaction
  And the aggregate is modified externally (outside transaction)
  And an attempt is made to update the aggregate within the transaction
  Then the update should detect version mismatch using transaction path
  And the error should be ErrConcurrentModification`, func(t *testing.T) {
		t.Parallel()
		repo := factory.Create(ctx)
		tx, ok := repo.(core.Transactional)
		require.True(t, ok)

		agg := newSQLiteTestAgg("tx-update-version-check-id")
		_, err := agg.SingleEventCommand("initial-value")
		require.NoError(t, err)

		err = repo.Save(ctx, agg)
		require.NoError(t, err)

		txCtx, err := tx.Begin(ctx)
		require.NoError(t, err)

		loadedAgg := newSQLiteTestAggForLoad()
		err = repo.Load(txCtx, "tx-update-version-check-id", loadedAgg)
		require.NoError(t, err)

		externalAgg := newSQLiteTestAggForLoad()
		err = repo.Load(ctx, "tx-update-version-check-id", externalAgg)
		require.NoError(t, err)

		_, err = externalAgg.SingleEventCommand("external-update")
		require.NoError(t, err)
		err = repo.Save(ctx, externalAgg)
		require.NoError(t, err)

		_, err = loadedAgg.SingleEventCommand("tx-update")
		require.NoError(t, err)

		err = repo.Save(txCtx, loadedAgg)
		require.Error(t, err)
		require.ErrorIs(t, err, core.ErrConcurrentModification)

		err = tx.Rollback(txCtx)
		require.NoError(t, err)
	})

	t.Run(`Scenario: Insert new aggregate with duplicate ID returns ErrAggregateExists
  Given a repository
  When an insert operation attempts to insert an aggregate with an existing ID
  Then the operation should return ErrAggregateExists`, func(t *testing.T) {
		t.Parallel()
		repo := factory.Create(ctx)

		agg1 := newSQLiteTestAgg("duplicate-busy-test-id")
		_, err := agg1.SingleEventCommand("value-1")
		require.NoError(t, err)

		err = repo.Save(ctx, agg1)
		require.NoError(t, err)

		agg2 := newSQLiteTestAgg("duplicate-busy-test-id")
		_, err = agg2.SingleEventCommand("value-2")
		require.NoError(t, err)

		err = repo.Save(ctx, agg2)
		require.Error(t, err)
		require.ErrorIs(t, err, core.ErrAggregateExists)
	})

	t.Run(`Scenario: Remove aggregate with error during execution
  Given an aggregate that has been saved
  When a removal operation encounters a database error
  Then the operation should return a removal error`, func(t *testing.T) {
		t.Parallel()
		repo := factory.Create(ctx)

		agg := newSQLiteTestAgg("error-remove-id")
		_, err := agg.SingleEventCommand("initial-value")
		require.NoError(t, err)

		err = repo.Save(ctx, agg)
		require.NoError(t, err)

		loadedAgg := newSQLiteTestAggForLoad()
		err = repo.Load(ctx, "error-remove-id", loadedAgg)
		require.NoError(t, err)

		_, err = loadedAgg.Remove()
		require.NoError(t, err)

		err = repo.Save(ctx, loadedAgg)
		require.NoError(t, err)

		err = repo.Load(ctx, "error-remove-id", newSQLiteTestAggForLoad())
		require.Error(t, err)
		require.ErrorIs(t, err, core.ErrAggregateNotFound)
	})
}
