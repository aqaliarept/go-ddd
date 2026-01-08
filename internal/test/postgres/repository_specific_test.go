package postgres_test

import (
	"context"
	"testing"

	testpkg "github.com/aqaliarept/go-ddd-kit/internal/test"
	core "github.com/aqaliarept/go-ddd-kit/pkg/core"
	postgrespkg "github.com/aqaliarept/go-ddd-kit/pkg/postgres"
	"github.com/stretchr/testify/require"
)

func newPostgresTestAgg(id core.ID) *testpkg.TestAgg {
	return testpkg.NewTestAgg(id)
}

func newPostgresTestAggForLoad() *testpkg.TestAgg {
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

type invalidPostgresAgg struct {
	core.Aggregate[invalidState]
}

func (i *invalidPostgresAgg) StorageOptions() []core.StorageOption {
	return []core.StorageOption{postgrespkg.WithTableName("test_agg")}
}

func TestPostgresRepositorySpecific(t *testing.T) {
	container := SetupPostgresTestContainer(t)
	ctx := context.Background()
	factory := postgrespkg.NewRepositoryFactory(container.Database())

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

		agg := newPostgresTestAggForLoad()
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

		agg := newPostgresTestAgg("tx-load-id")
		_, err := agg.SingleEventCommand("tx-load-value")
		require.NoError(t, err)

		err = repo.Save(ctx, agg)
		require.NoError(t, err)

		txCtx, err := tx.Begin(ctx)
		require.NoError(t, err)

		loadedAgg := newPostgresTestAggForLoad()
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
		agg := newPostgresTestAgg("non-tx-load-id")
		_, err := agg.SingleEventCommand("non-tx-load-value")
		require.NoError(t, err)

		err = repo.Save(ctx, agg)
		require.NoError(t, err)

		loadedAgg := newPostgresTestAggForLoad()
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

		agg := &invalidPostgresAgg{}
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

		agg := newPostgresTestAgg("tx-removal-id")
		_, err := agg.SingleEventCommand("tx-removal-value")
		require.NoError(t, err)

		err = repo.Save(ctx, agg)
		require.NoError(t, err)

		txCtx, err := tx.Begin(ctx)
		require.NoError(t, err)

		loadedAgg := newPostgresTestAggForLoad()
		err = repo.Load(txCtx, "tx-removal-id", loadedAgg)
		require.NoError(t, err)

		_, err = loadedAgg.Remove()
		require.NoError(t, err)

		err = repo.Save(txCtx, loadedAgg)
		require.NoError(t, err)

		err = tx.Commit(txCtx)
		require.NoError(t, err)

		err = repo.Load(ctx, "tx-removal-id", newPostgresTestAggForLoad())
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
		agg := newPostgresTestAgg("non-tx-removal-id")
		_, err := agg.SingleEventCommand("non-tx-removal-value")
		require.NoError(t, err)

		err = repo.Save(ctx, agg)
		require.NoError(t, err)

		loadedAgg := newPostgresTestAggForLoad()
		err = repo.Load(ctx, "non-tx-removal-id", loadedAgg)
		require.NoError(t, err)

		_, err = loadedAgg.Remove()
		require.NoError(t, err)

		err = repo.Save(ctx, loadedAgg)
		require.NoError(t, err)

		err = repo.Load(ctx, "non-tx-removal-id", newPostgresTestAggForLoad())
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

		agg := newPostgresTestAgg("tx-insert-id")
		_, err = agg.SingleEventCommand("tx-insert-value")
		require.NoError(t, err)

		err = repo.Save(txCtx, agg)
		require.NoError(t, err)

		err = tx.Commit(txCtx)
		require.NoError(t, err)

		loadedAgg := newPostgresTestAggForLoad()
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
		agg := newPostgresTestAgg("non-tx-insert-id")
		_, err := agg.SingleEventCommand("non-tx-insert-value")
		require.NoError(t, err)

		err = repo.Save(ctx, agg)
		require.NoError(t, err)

		loadedAgg := newPostgresTestAggForLoad()
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

		agg := newPostgresTestAgg("tx-update-id")
		_, err := agg.SingleEventCommand("initial-value")
		require.NoError(t, err)

		err = repo.Save(ctx, agg)
		require.NoError(t, err)

		txCtx, err := tx.Begin(ctx)
		require.NoError(t, err)

		loadedAgg := newPostgresTestAggForLoad()
		err = repo.Load(txCtx, "tx-update-id", loadedAgg)
		require.NoError(t, err)

		_, err = loadedAgg.SingleEventCommand("updated-value")
		require.NoError(t, err)

		err = repo.Save(txCtx, loadedAgg)
		require.NoError(t, err)

		err = tx.Commit(txCtx)
		require.NoError(t, err)

		finalAgg := newPostgresTestAggForLoad()
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
		agg := newPostgresTestAgg("non-tx-update-id")
		_, err := agg.SingleEventCommand("initial-value")
		require.NoError(t, err)

		err = repo.Save(ctx, agg)
		require.NoError(t, err)

		loadedAgg := newPostgresTestAggForLoad()
		err = repo.Load(ctx, "non-tx-update-id", loadedAgg)
		require.NoError(t, err)

		_, err = loadedAgg.SingleEventCommand("updated-value")
		require.NoError(t, err)

		err = repo.Save(ctx, loadedAgg)
		require.NoError(t, err)

		finalAgg := newPostgresTestAggForLoad()
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
			postgrespkg.WithTableName("")
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

		agg := newPostgresTestAgg("tx-version-check-id")
		_, err := agg.SingleEventCommand("initial-value")
		require.NoError(t, err)

		err = repo.Save(ctx, agg)
		require.NoError(t, err)

		txCtx, err := tx.Begin(ctx)
		require.NoError(t, err)

		loadedAgg := newPostgresTestAggForLoad()
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

		agg := newPostgresTestAgg("tx-remove-version-id")
		_, err := agg.SingleEventCommand("initial-value")
		require.NoError(t, err)

		err = repo.Save(ctx, agg)
		require.NoError(t, err)

		txCtx, err := tx.Begin(ctx)
		require.NoError(t, err)

		loadedAgg := newPostgresTestAggForLoad()
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
		agg := newPostgresTestAgg("non-tx-remove-version-id")
		_, err := agg.SingleEventCommand("initial-value")
		require.NoError(t, err)

		err = repo.Save(ctx, agg)
		require.NoError(t, err)

		loadedAgg := newPostgresTestAggForLoad()
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

		agg := newPostgresTestAgg("tx-update-version-id")
		_, err := agg.SingleEventCommand("initial-value")
		require.NoError(t, err)

		err = repo.Save(ctx, agg)
		require.NoError(t, err)

		txCtx, err := tx.Begin(ctx)
		require.NoError(t, err)

		loadedAgg := newPostgresTestAggForLoad()
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
		agg := newPostgresTestAgg("non-tx-update-version-id")
		_, err := agg.SingleEventCommand("initial-value")
		require.NoError(t, err)

		err = repo.Save(ctx, agg)
		require.NoError(t, err)

		loadedAgg := newPostgresTestAggForLoad()
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
		agg := newPostgresTestAgg("remove-concurrent-id")
		_, err := agg.SingleEventCommand("initial-value")
		require.NoError(t, err)

		err = repo.Save(ctx, agg)
		require.NoError(t, err)

		agg1 := newPostgresTestAggForLoad()
		err = repo.Load(ctx, "remove-concurrent-id", agg1)
		require.NoError(t, err)

		agg2 := newPostgresTestAggForLoad()
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
		agg := newPostgresTestAgg("update-concurrent-id")
		_, err := agg.SingleEventCommand("initial-value")
		require.NoError(t, err)

		err = repo.Save(ctx, agg)
		require.NoError(t, err)

		agg1 := newPostgresTestAggForLoad()
		err = repo.Load(ctx, "update-concurrent-id", agg1)
		require.NoError(t, err)

		agg2 := newPostgresTestAggForLoad()
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
}

