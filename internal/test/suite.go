package test

import (
	"context"
	"errors"
	"fmt"
	"testing"

	core "github.com/aqaliarept/go-ddd-kit/pkg/core"
	"github.com/stretchr/testify/require"
)

var (
	errIntentionalRollback        = errors.New("intentional rollback error")
	errSimulatedFailureThirdSave  = errors.New("simulated failure during third aggregate save")
)

// TestRunner defines the interface for running repository tests
type TestRunner interface {
	// SetupRepository creates and returns a repository instance
	SetupRepository(t *testing.T) core.Repository
	// SetupContext returns the context to use for repository operations
	SetupContext(t *testing.T) context.Context
}

// ConcurrentTestRunner defines the interface for running concurrent scope tests
type ConcurrentTestRunner interface {
	TestRunner
	// SetupConcurrentScope creates and returns a ConcurrentScope for concurrent testing
	SetupConcurrentScope(t *testing.T) *core.ConcurrentScope
	// SetupRepositoryFactory creates and returns a RepositoryFactory
	SetupRepositoryFactory(t *testing.T) core.RepositoryFactory
}

// RunBaseTests runs all base repository tests
func RunBaseTests(t *testing.T, runner TestRunner) {
	repo := runner.SetupRepository(t)
	ctx := runner.SetupContext(t)

	t.Run(`Scenario: Save and Load aggregate with single event
  Given a new aggregate
  When a single event command is executed
  And the aggregate is saved to the repository
  Then the aggregate version should be correct
  And the aggregate should have no pending events
  And the aggregate state should reflect the event
  When the aggregate is loaded from the repository
  Then the loaded aggregate should have the correct ID
  And the loaded aggregate should have the correct version
  And the loaded aggregate state should match the saved state`, func(t *testing.T) {
		t.Parallel()
		agg := NewTestAgg("test-id-1")
		pack, err := agg.SingleEventCommand("test-value")
		require.NoError(t, err)
		require.Len(t, pack, 1)

		err = repo.Save(ctx, agg)
		require.NoError(t, err)

		require.Equal(t, core.Version(1), agg.Version())
		require.Empty(t, agg.Events())
		state := agg.State()
		require.Equal(t, "test-value", state.String)

		loadedAgg := &TestAgg{}
		err = repo.Load(ctx, "test-id-1", loadedAgg)
		require.NoError(t, err)

		require.Equal(t, core.ID("test-id-1"), loadedAgg.ID())
		require.Equal(t, core.Version(1), loadedAgg.Version())
		loadedState := loadedAgg.State()
		require.Equal(t, "test-value", loadedState.String)
		require.Empty(t, loadedAgg.Events())
		require.NoError(t, loadedAgg.Error())
	})

	t.Run(`Scenario: Save aggregate with multiple events
  Given a new aggregate
  When multiple event commands are executed
  And the aggregate is saved to the repository
  Then the aggregate version should be correct
  And the aggregate should have no pending events
  And the aggregate state should reflect the last event
  When the aggregate is loaded from the repository
  Then the loaded aggregate state should match the final state`, func(t *testing.T) {
		t.Parallel()
		agg := NewTestAgg("test-id-2")
		pack1, err := agg.SingleEventCommand("first-value")
		require.NoError(t, err)
		require.Len(t, pack1, 1)

		pack2, err := agg.SingleEventCommand("second-value")
		require.NoError(t, err)
		require.Len(t, pack2, 1)

		err = repo.Save(ctx, agg)
		require.NoError(t, err)

		require.Equal(t, core.Version(1), agg.Version())
		require.Empty(t, agg.Events())
		state := agg.State()
		require.Equal(t, "second-value", state.String)

		loadedAgg := &TestAgg{}
		err = repo.Load(ctx, "test-id-2", loadedAgg)
		require.NoError(t, err)

		require.Equal(t, core.ID("test-id-2"), loadedAgg.ID())
		require.Equal(t, core.Version(1), loadedAgg.Version())
		loadedState := loadedAgg.State()
		require.Equal(t, "second-value", loadedState.String)
	})

	t.Run(`Scenario: Save and Load aggregate preserves complex state
  Given a new aggregate
  When an event command is executed
  And the aggregate is saved to the repository
  When the aggregate is loaded from the repository
  Then the loaded aggregate state should preserve complex nested structures
  And the entity slice should be initialized
  And the map structure should be preserved if present`, func(t *testing.T) {
		t.Parallel()
		agg := NewTestAgg("test-id-3")
		_, err := agg.SingleEventCommand("complex-value")
		require.NoError(t, err)

		err = repo.Save(ctx, agg)
		require.NoError(t, err)

		loadedAgg := &TestAgg{}
		err = repo.Load(ctx, "test-id-3", loadedAgg)
		require.NoError(t, err)

		state := loadedAgg.State()
		require.Equal(t, "complex-value", state.String)
		require.NotNil(t, state.EntitySlice)
		if state.Map != nil {
			require.IsType(t, map[string]NestedEntity{}, state.Map)
		}
	})

	t.Run(`Scenario: Save duplicate aggregate returns ErrAggregateExists
  Given an aggregate that has been saved
  When an attempt is made to save another aggregate with the same ID
  Then the save operation should fail
  And the error should be ErrAggregateExists`, func(t *testing.T) {
		t.Parallel()
		// Create and save first aggregate
		agg1 := NewTestAgg("test-id-duplicate")
		_, err := agg1.SingleEventCommand("first-value")
		require.NoError(t, err)
		err = repo.Save(ctx, agg1)
		require.NoError(t, err)

		// Try to save another aggregate with the same ID
		agg2 := NewTestAgg("test-id-duplicate")
		_, err = agg2.SingleEventCommand("second-value")
		require.NoError(t, err)
		err = repo.Save(ctx, agg2)
		require.ErrorIs(t, err, core.ErrAggregateExists)
	})

	t.Run(`Scenario: Load non-existent aggregate
  Given an aggregate ID that does not exist in the repository
  When an attempt is made to load the aggregate
  Then the load operation should fail
  And the error should be ErrAggregateNotFound`, func(t *testing.T) {
		t.Parallel()
		loadedAgg := &TestAgg{}
		err := repo.Load(ctx, "non-existent-id", loadedAgg)
		require.Error(t, err)
		require.ErrorIs(t, err, core.ErrAggregateNotFound)
	})

	t.Run(`Scenario: Save aggregate without events
  Given a new aggregate that has no pending events
  When the aggregate is saved to the repository
  Then the save operation should succeed
  And the aggregate version should be correct
  When the aggregate is loaded from the repository
  Then the loaded aggregate should have the correct version`, func(t *testing.T) {
		t.Parallel()
		agg := NewTestAgg("test-id-4")
		err := repo.Save(ctx, agg)
		require.NoError(t, err)
		require.Equal(t, core.Version(1), agg.Version())

		// Verify it was saved
		loadedAgg := &TestAgg{}
		err = repo.Load(ctx, "test-id-4", loadedAgg)
		require.NoError(t, err)
		require.Equal(t, core.Version(1), loadedAgg.Version())
	})

	t.Run(`Scenario: Update existing aggregate
  Given an aggregate that has been saved
  When another event command is executed
  And the aggregate is saved again
  Then the aggregate version should increment
  When the aggregate is loaded from the repository
  Then the loaded aggregate should have the incremented version
  And the loaded aggregate state should reflect the latest event`, func(t *testing.T) {
		t.Parallel()
		// Create and save initial aggregate
		agg := NewTestAgg("test-id-5")
		_, err := agg.SingleEventCommand("initial-value")
		require.NoError(t, err)

		err = repo.Save(ctx, agg)
		require.NoError(t, err)
		require.Equal(t, core.Version(1), agg.Version())

		// Execute another command and save again
		_, err = agg.SingleEventCommand("updated-value")
		require.NoError(t, err)

		err = repo.Save(ctx, agg)
		require.NoError(t, err)
		require.Equal(t, core.Version(2), agg.Version())

		// Load and verify the updated aggregate
		loadedAgg := &TestAgg{}
		err = repo.Load(ctx, "test-id-5", loadedAgg)
		require.NoError(t, err)
		require.Equal(t, core.Version(2), loadedAgg.Version())
		loadedState := loadedAgg.State()
		require.Equal(t, "updated-value", loadedState.String)
	})

	t.Run(`Scenario: Save new aggregate with tombstone event
  Given a new aggregate
  When the aggregate is removed
  And the aggregate is saved to the repository
  Then the save operation should succeed
  When an attempt is made to load the aggregate from the repository
  Then the load operation should fail
  And the error should be ErrAggregateNotFound
  And the document should not have been created`, func(t *testing.T) {
		t.Parallel()
		agg := NewTestAgg("test-id-6")
		pack, err := agg.Remove()
		require.NoError(t, err)
		require.Len(t, pack, 1)
		state := agg.State()
		require.True(t, state.Removed)

		err = repo.Save(ctx, agg)
		require.NoError(t, err)

		// Verify document was not created
		loadedAgg := &TestAgg{}
		err = repo.Load(ctx, "test-id-6", loadedAgg)
		require.Error(t, err)
		require.ErrorIs(t, err, core.ErrAggregateNotFound)
	})

	t.Run(`Scenario: Save existing aggregate with tombstone event
  Given an aggregate that has been saved
  When the aggregate is removed
  And the aggregate is saved to the repository
  Then the save operation should succeed
  When an attempt is made to load the aggregate from the repository
  Then the load operation should fail
  And the error should be ErrAggregateNotFound
  And the document should have been deleted`, func(t *testing.T) {
		t.Parallel()
		agg := NewTestAgg("test-id-7")
		_, err := agg.SingleEventCommand("initial-value")
		require.NoError(t, err)
		err = repo.Save(ctx, agg)
		require.NoError(t, err)
		require.Equal(t, core.Version(1), agg.Version())

		pack, err := agg.Remove()
		require.NoError(t, err)
		require.Len(t, pack, 1)
		state := agg.State()
		require.True(t, state.Removed)

		err = repo.Save(ctx, agg)
		require.NoError(t, err)

		// Verify document was deleted
		loadedAgg := &TestAgg{}
		err = repo.Load(ctx, "test-id-7", loadedAgg)
		require.Error(t, err)
		require.ErrorIs(t, err, core.ErrAggregateNotFound)
	})

	t.Run(`Scenario: Atomic concurrency control - ErrConcurrentModification
  Given an aggregate that has been saved
  And a second instance of the same aggregate loaded
  When the first aggregate is modified and saved
  And an attempt is made to save the second aggregate with stale version
  Then the save operation should fail
  And the error should be ErrConcurrentModification`, func(t *testing.T) {
		t.Parallel()
		// Create and save an aggregate
		agg := NewTestAgg("concurrency-test-id")
		_, err := agg.SingleEventCommand("initial-value")
		require.NoError(t, err)

		err = repo.Save(ctx, agg)
		require.NoError(t, err)
		require.Equal(t, core.Version(1), agg.Version())

		// Create a second instance of the same aggregate (simulating concurrent access)
		agg2 := &TestAgg{}
		err = repo.Load(ctx, "concurrency-test-id", agg2)
		require.NoError(t, err)
		require.Equal(t, core.Version(1), agg2.Version())

		// Modify and save the first aggregate (increments version to 2)
		_, err = agg.SingleEventCommand("updated-value")
		require.NoError(t, err)
		err = repo.Save(ctx, agg)
		require.NoError(t, err)
		require.Equal(t, core.Version(2), agg.Version())

		// Try to save the second aggregate with stale version (should fail)
		_, err = agg2.SingleEventCommand("stale-value")
		require.NoError(t, err)
		err = repo.Save(ctx, agg2)
		require.Error(t, err)
		require.ErrorIs(t, err, core.ErrConcurrentModification)
	})

	t.Run(`Scenario: Atomic concurrency control - version conflict on stale aggregate
  Given an aggregate that has been saved
  And two instances of the same aggregate loaded
  When the second aggregate is modified and saved first
  And an attempt is made to save the first aggregate with stale version
  Then the save operation should fail
  And the error should be ErrConcurrentModification`, func(t *testing.T) {
		t.Parallel()
		// Create and save an aggregate
		agg := NewTestAgg("successful-concurrency-test-id")
		_, err := agg.SingleEventCommand("initial-value")
		require.NoError(t, err)

		err = repo.Save(ctx, agg)
		require.NoError(t, err)
		require.Equal(t, core.Version(1), agg.Version())

		// Create a second instance and modify it
		agg2 := &TestAgg{}
		err = repo.Load(ctx, "successful-concurrency-test-id", agg2)
		require.NoError(t, err)
		require.Equal(t, core.Version(1), agg2.Version())

		// Modify and save the second aggregate first
		_, err = agg2.SingleEventCommand("second-value")
		require.NoError(t, err)
		err = repo.Save(ctx, agg2)
		require.NoError(t, err)
		require.Equal(t, core.Version(2), agg2.Version())

		// Now modify and save the first aggregate (should fail due to version mismatch)
		_, err = agg.SingleEventCommand("first-value")
		require.NoError(t, err)
		err = repo.Save(ctx, agg)
		require.Error(t, err)
		require.ErrorIs(t, err, core.ErrConcurrentModification)
	})

	t.Run(`Scenario: Concurrent modification wins over removal
  Given an aggregate that has been saved
  And two instances of the same aggregate loaded concurrently
  When the first instance is modified and saved
  And an attempt is made to remove and save the second instance
  Then the removal save operation should fail
  And the error should be ErrConcurrentModification
  When the aggregate is loaded from the repository
  Then the aggregate should exist with the modified state
  And the aggregate should not be removed`, func(t *testing.T) {
		t.Parallel()
		agg := NewTestAgg("concurrent-modify-remove-1")
		_, err := agg.SingleEventCommand("initial-value")
		require.NoError(t, err)
		err = repo.Save(ctx, agg)
		require.NoError(t, err)

		modifyAgg := &TestAgg{}
		err = repo.Load(ctx, "concurrent-modify-remove-1", modifyAgg)
		require.NoError(t, err)

		removeAgg := &TestAgg{}
		err = repo.Load(ctx, "concurrent-modify-remove-1", removeAgg)
		require.NoError(t, err)

		_, err = modifyAgg.SingleEventCommand("modified-value")
		require.NoError(t, err)
		err = repo.Save(ctx, modifyAgg)
		require.NoError(t, err)

		_, err = removeAgg.Remove()
		require.NoError(t, err)
		err = repo.Save(ctx, removeAgg)
		require.Error(t, err)
		require.ErrorIs(t, err, core.ErrConcurrentModification)

		loadedAgg := &TestAgg{}
		err = repo.Load(ctx, "concurrent-modify-remove-1", loadedAgg)
		require.NoError(t, err)
		state := loadedAgg.State()
		require.Equal(t, "modified-value", state.String)
		require.False(t, state.Removed)
	})

	t.Run(`Scenario: Concurrent removal wins over modification
  Given an aggregate that has been saved
  And two instances of the same aggregate loaded concurrently
  When the first instance is removed and saved
  And an attempt is made to modify and save the second instance
  Then the modification save operation should fail
  And the error should be ErrAggregateNotFound
  When an attempt is made to load the aggregate from the repository
  Then the load operation should fail
  And the error should be ErrAggregateNotFound
  And the aggregate should have been deleted`, func(t *testing.T) {
		t.Parallel()
		agg := NewTestAgg("concurrent-modify-remove-2")
		_, err := agg.SingleEventCommand("initial-value")
		require.NoError(t, err)
		err = repo.Save(ctx, agg)
		require.NoError(t, err)

		removeAgg := &TestAgg{}
		err = repo.Load(ctx, "concurrent-modify-remove-2", removeAgg)
		require.NoError(t, err)

		modifyAgg := &TestAgg{}
		err = repo.Load(ctx, "concurrent-modify-remove-2", modifyAgg)
		require.NoError(t, err)

		_, err = removeAgg.Remove()
		require.NoError(t, err)
		err = repo.Save(ctx, removeAgg)
		require.NoError(t, err)

		_, err = modifyAgg.SingleEventCommand("modified-value")
		require.NoError(t, err)
		err = repo.Save(ctx, modifyAgg)
		require.Error(t, err)
		require.ErrorIs(t, err, core.ErrAggregateNotFound)

		loadedAgg := &TestAgg{}
		err = repo.Load(ctx, "concurrent-modify-remove-2", loadedAgg)
		require.Error(t, err)
		require.ErrorIs(t, err, core.ErrAggregateNotFound)
	})

	t.Run(`Scenario: Concurrent removals - first wins
  Given an aggregate that has been saved
  And two instances of the same aggregate loaded concurrently
  When the first instance is removed and saved
  And an attempt is made to remove and save the second instance
  Then the second removal save operation should fail
  And the error should be ErrAggregateNotFound
  When an attempt is made to load the aggregate from the repository
  Then the load operation should fail
  And the error should be ErrAggregateNotFound
  And the aggregate should have been deleted by the first removal`, func(t *testing.T) {
		t.Parallel()
		agg := NewTestAgg("concurrent-remove-remove-1")
		_, err := agg.SingleEventCommand("initial-value")
		require.NoError(t, err)
		err = repo.Save(ctx, agg)
		require.NoError(t, err)

		firstRemoveAgg := &TestAgg{}
		err = repo.Load(ctx, "concurrent-remove-remove-1", firstRemoveAgg)
		require.NoError(t, err)

		secondRemoveAgg := &TestAgg{}
		err = repo.Load(ctx, "concurrent-remove-remove-1", secondRemoveAgg)
		require.NoError(t, err)

		// First removal succeeds
		_, err = firstRemoveAgg.Remove()
		require.NoError(t, err)
		err = repo.Save(ctx, firstRemoveAgg)
		require.NoError(t, err)

		// Second removal fails because aggregate was already deleted
		_, err = secondRemoveAgg.Remove()
		require.NoError(t, err)
		err = repo.Save(ctx, secondRemoveAgg)
		require.Error(t, err)
		require.ErrorIs(t, err, core.ErrAggregateNotFound)

		// Verify the first removal was successful
		loadedAgg := &TestAgg{}
		err = repo.Load(ctx, "concurrent-remove-remove-1", loadedAgg)
		require.Error(t, err)
		require.ErrorIs(t, err, core.ErrAggregateNotFound)
	})

	t.Run(`Scenario: V1 aggregate lifecycle - create, save, load
	  Given a new V1 aggregate
	  When the aggregate is saved to the repository
	  And the aggregate is loaded from the repository
	  Then the loaded aggregate should have correct V1 state
	  And the aggregate version should be correct`, func(t *testing.T) {
		t.Parallel()
		agg := NewStateTestAggV1("migration-v1-1")
		err := repo.Save(ctx, agg)
		require.NoError(t, err)
		require.Equal(t, core.Version(1), agg.Version())

		loadedAgg := &StateTestAggV1{}
		err = repo.Load(ctx, "migration-v1-1", loadedAgg)
		require.NoError(t, err)
		require.Equal(t, core.ID("migration-v1-1"), loadedAgg.ID())
		require.Equal(t, core.Version(1), loadedAgg.Version())
		state := loadedAgg.State()
		require.Equal(t, "version 1", state.ValueV1)
	})

	t.Run(`Scenario: V1 aggregate - save again without changes
	  Given a V1 aggregate that has been saved and loaded
	  When the aggregate is saved again without changes
	  And the aggregate is loaded from the repository
	  Then the aggregate state should persist correctly`, func(t *testing.T) {
		t.Parallel()
		agg := NewStateTestAggV1("migration-v1-2")
		err := repo.Save(ctx, agg)
		require.NoError(t, err)

		loadedAgg := &StateTestAggV1{}
		err = repo.Load(ctx, "migration-v1-2", loadedAgg)
		require.NoError(t, err)
		state := loadedAgg.State()
		require.Equal(t, "version 1", state.ValueV1)

		err = repo.Save(ctx, loadedAgg)
		require.NoError(t, err)

		loadedAgg2 := &StateTestAggV1{}
		err = repo.Load(ctx, "migration-v1-2", loadedAgg2)
		require.NoError(t, err)
		state2 := loadedAgg2.State()
		require.Equal(t, "version 1", state2.ValueV1)
		require.Equal(t, core.Version(1), loadedAgg2.Version())
	})

	t.Run(`Scenario: V2 aggregate lifecycle - create, save, load
	  Given a new V2 aggregate
	  When the aggregate is saved to the repository
	  And the aggregate is loaded from the repository
	  Then the loaded aggregate should have correct V2 state
	  And the aggregate version should be correct`, func(t *testing.T) {
		t.Parallel()
		agg := NewStateTestAggV2("migration-v2-1")
		err := repo.Save(ctx, agg)
		require.NoError(t, err)
		require.Equal(t, core.Version(1), agg.Version())

		loadedAgg := &StateTestAggV2{}
		err = repo.Load(ctx, "migration-v2-1", loadedAgg)
		require.NoError(t, err)
		require.Equal(t, core.ID("migration-v2-1"), loadedAgg.ID())
		require.Equal(t, core.Version(1), loadedAgg.Version())
		state := loadedAgg.State()
		require.Equal(t, "version 2", state.ValueV2)
	})

	t.Run(`Scenario: V2 aggregate - update, save, load
	  Given a V2 aggregate that has been saved
	  When the aggregate value is updated
	  And the aggregate is saved to the repository
	  And the aggregate is loaded from the repository
	  Then the loaded aggregate should have the updated value
	  And the aggregate version should be incremented`, func(t *testing.T) {
		t.Parallel()
		agg := NewStateTestAggV2("migration-v2-2")
		err := repo.Save(ctx, agg)
		require.NoError(t, err)

		_, err = agg.SingleEventCommand("updated-value")
		require.NoError(t, err)
		err = repo.Save(ctx, agg)
		require.NoError(t, err)
		require.Equal(t, core.Version(2), agg.Version())

		loadedAgg := &StateTestAggV2{}
		err = repo.Load(ctx, "migration-v2-2", loadedAgg)
		require.NoError(t, err)
		require.Equal(t, core.Version(2), loadedAgg.Version())
		state := loadedAgg.State()
		require.Equal(t, "updated-value", state.ValueV2)
	})

	t.Run(`Scenario: V1 to V2 migration - load V1 state as V2
	  Given a V1 aggregate that has been saved
	  When the aggregate is loaded as V2
	  Then the V2 aggregate should have migrated state from V1
	  And the migration should succeed`, func(t *testing.T) {
		t.Parallel()
		aggV1 := NewStateTestAggV1("migration-forward-1")
		err := repo.Save(ctx, aggV1)
		require.NoError(t, err)

		aggV2 := &StateTestAggV2{}
		err = repo.Load(ctx, "migration-forward-1", aggV2)
		require.NoError(t, err)
		require.Equal(t, core.ID("migration-forward-1"), aggV2.ID())
		require.Equal(t, core.Version(1), aggV2.Version())
		state := aggV2.State()
		require.Equal(t, "version 1", state.ValueV2)
	})

	t.Run(`Scenario: V1 to V2 migration then update
	  Given a V1 aggregate that has been saved
	  When it is loaded as V2 and the value is updated
	  And the V2 aggregate is saved
	  And the V2 aggregate is loaded again
	  Then the updated value should be persisted
	  And the aggregate version should be incremented`, func(t *testing.T) {
		t.Parallel()
		aggV1 := NewStateTestAggV1("migration-forward-2")
		err := repo.Save(ctx, aggV1)
		require.NoError(t, err)

		aggV2 := &StateTestAggV2{}
		err = repo.Load(ctx, "migration-forward-2", aggV2)
		require.NoError(t, err)
		state := aggV2.State()
		require.Equal(t, "version 1", state.ValueV2)

		_, err = aggV2.SingleEventCommand("migrated-and-updated")
		require.NoError(t, err)
		err = repo.Save(ctx, aggV2)
		require.NoError(t, err)
		require.Equal(t, core.Version(2), aggV2.Version())

		loadedAggV2 := &StateTestAggV2{}
		err = repo.Load(ctx, "migration-forward-2", loadedAggV2)
		require.NoError(t, err)
		require.Equal(t, core.Version(2), loadedAggV2.Version())
		state2 := loadedAggV2.State()
		require.Equal(t, "migrated-and-updated", state2.ValueV2)
	})

	t.Run(`Scenario: V1 to V2 migration, update, save, load
	  Given a V1 aggregate that has been saved
	  When it is loaded as V2, updated, and saved
	  And the V2 aggregate is loaded again
	  Then the state should be persisted with schema version 2
	  And the value should be correct`, func(t *testing.T) {
		t.Parallel()
		aggV1 := NewStateTestAggV1("migration-forward-3")
		err := repo.Save(ctx, aggV1)
		require.NoError(t, err)

		aggV2 := &StateTestAggV2{}
		err = repo.Load(ctx, "migration-forward-3", aggV2)
		require.NoError(t, err)

		_, err = aggV2.SingleEventCommand("final-value")
		require.NoError(t, err)
		err = repo.Save(ctx, aggV2)
		require.NoError(t, err)

		loadedAggV2 := &StateTestAggV2{}
		err = repo.Load(ctx, "migration-forward-3", loadedAggV2)
		require.NoError(t, err)
		state := loadedAggV2.State()
		require.Equal(t, "final-value", state.ValueV2)
		require.Equal(t, core.Version(2), loadedAggV2.Version())
	})

	t.Run(`Scenario: V2 to V1 backward compatibility - V1 cannot load schema version 2
	  Given a V2 aggregate that has been saved
	  When an attempt is made to load it as V1
	  Then the load operation should fail
	  And the error should indicate unsupported schema version`, func(t *testing.T) {
		t.Parallel()
		aggV2 := NewStateTestAggV2("migration-backward-1")
		err := repo.Save(ctx, aggV2)
		require.NoError(t, err)

		aggV1 := &StateTestAggV1{}
		err = repo.Load(ctx, "migration-backward-1", aggV1)
		require.Error(t, err)
		require.Contains(t, err.Error(), "unsupported schema version")
	})

	t.Run(`Scenario: V2 to V1 backward compatibility - updated V2 cannot be loaded as V1
	  Given a V2 aggregate that has been updated and saved
	  When an attempt is made to load it as V1
	  Then the load operation should fail
	  And the error should indicate unsupported schema version`, func(t *testing.T) {
		t.Parallel()
		aggV2 := NewStateTestAggV2("migration-backward-2")
		err := repo.Save(ctx, aggV2)
		require.NoError(t, err)

		_, err = aggV2.SingleEventCommand("updated-value")
		require.NoError(t, err)
		err = repo.Save(ctx, aggV2)
		require.NoError(t, err)

		aggV1 := &StateTestAggV1{}
		err = repo.Load(ctx, "migration-backward-2", aggV1)
		require.Error(t, err)
		require.Contains(t, err.Error(), "unsupported schema version")
	})

	t.Run(`Scenario: V1 can load schema 1 even after V2 has read it
	  Given a V1 aggregate that has been saved
	  When it is loaded as V2
	  And an attempt is made to load the same ID as V1
	  Then V1 should still be able to load the aggregate
	  And the state should be correct`, func(t *testing.T) {
		t.Parallel()
		aggV1 := NewStateTestAggV1("migration-mixed-1")
		err := repo.Save(ctx, aggV1)
		require.NoError(t, err)

		aggV2 := &StateTestAggV2{}
		err = repo.Load(ctx, "migration-mixed-1", aggV2)
		require.NoError(t, err)
		state := aggV2.State()
		require.Equal(t, "version 1", state.ValueV2)

		aggV1Loaded := &StateTestAggV1{}
		err = repo.Load(ctx, "migration-mixed-1", aggV1Loaded)
		require.NoError(t, err)
		stateV1 := aggV1Loaded.State()
		require.Equal(t, "version 1", stateV1.ValueV1)
		require.Equal(t, core.Version(1), aggV1Loaded.Version())
	})

	t.Run(`Scenario: V1 cannot load after schema upgrade to version 2
	  Given a V1 aggregate that has been saved
	  When it is loaded as V2, updated, and saved
	  And an attempt is made to load it as V1
	  Then the load operation should fail
	  And the error should indicate unsupported schema version`, func(t *testing.T) {
		t.Parallel()
		aggV1 := NewStateTestAggV1("migration-mixed-2")
		err := repo.Save(ctx, aggV1)
		require.NoError(t, err)

		aggV2 := &StateTestAggV2{}
		err = repo.Load(ctx, "migration-mixed-2", aggV2)
		require.NoError(t, err)

		_, err = aggV2.SingleEventCommand("upgraded-value")
		require.NoError(t, err)
		err = repo.Save(ctx, aggV2)
		require.NoError(t, err)

		aggV1Loaded := &StateTestAggV1{}
		err = repo.Load(ctx, "migration-mixed-2", aggV1Loaded)
		require.Error(t, err)
		require.Contains(t, err.Error(), "unsupported schema version")
	})
}

// RunBaseConcurrentTests runs all base concurrent scope tests
func RunBaseConcurrentTests(t *testing.T, runner ConcurrentTestRunner) {
	ctx := runner.SetupContext(t)
	concurrentScope := runner.SetupConcurrentScope(t)
	factory := runner.SetupRepositoryFactory(t)

	t.Run(`Scenario: Save and Load aggregate within concurrent scope - successful commit
  Given a concurrent scope
  When an aggregate is created and saved within the concurrent scope
  And the aggregate is loaded within the concurrent scope
  Then the aggregate should be visible within the concurrent scope
  When the transaction commits successfully
  Then the aggregate should be visible after transaction commit
  And the aggregate state should match the saved state`, func(t *testing.T) {
		t.Parallel()
		_, err := concurrentScope.Run(ctx, func(ctx context.Context, repo core.Repository) error {
			agg := NewTestAgg("tx-scope-success-id")
			_, err := agg.SingleEventCommand("tx-scope-success-value")
			require.NoError(t, err)

			err = repo.Save(ctx, agg)
			require.NoError(t, err)

			loadedAgg := &TestAgg{}
			err = repo.Load(ctx, "tx-scope-success-id", loadedAgg)
			require.NoError(t, err)
			require.Equal(t, "tx-scope-success-value", loadedAgg.State().String)

			return nil
		})
		require.NoError(t, err)

		repo := factory.Create(ctx)
		loadedAgg := &TestAgg{}
		err = repo.Load(ctx, "tx-scope-success-id", loadedAgg)
		require.NoError(t, err)
		require.Equal(t, "tx-scope-success-value", loadedAgg.State().String)
	})

	t.Run(`Scenario: Save aggregate within concurrent scope - rollback on error
  Given a concurrent scope
  When an aggregate is created and saved within the concurrent scope
  And the aggregate is verified to be visible within the concurrent scope
  And an error is returned to trigger rollback
  Then the transaction should rollback
  And the error should be returned
  When an attempt is made to load the aggregate after rollback
  Then the load operation should fail
  And the error should be ErrAggregateNotFound
  And the aggregate should not have been persisted`, func(t *testing.T) {
		t.Parallel()
		_, err := concurrentScope.Run(ctx, func(ctx context.Context, repo core.Repository) error {
			agg := NewTestAgg("tx-scope-rollback-id")
			_, err := agg.SingleEventCommand("tx-scope-rollback-value")
			require.NoError(t, err)

			err = repo.Save(ctx, agg)
			require.NoError(t, err)

			loadedAgg := &TestAgg{}
			err = repo.Load(ctx, "tx-scope-rollback-id", loadedAgg)
			require.NoError(t, err)
			require.Equal(t, "tx-scope-rollback-value", loadedAgg.State().String)

			return errIntentionalRollback
		})
		require.Error(t, err)
		require.Contains(t, err.Error(), "intentional rollback error")

		repo := factory.Create(ctx)
		loadedAgg := &TestAgg{}
		err = repo.Load(ctx, "tx-scope-rollback-id", loadedAgg)
		require.Error(t, err)
		require.ErrorIs(t, err, core.ErrAggregateNotFound)
	})

	t.Run(`Scenario: Multiple aggregates in single concurrent scope
  Given a concurrent scope
  When multiple aggregates are created and saved within the concurrent scope
  And all aggregates are verified to be visible within the concurrent scope
  Then the transaction should commit successfully
  When all aggregates are loaded after transaction commit
  Then all aggregates should be visible
  And all aggregate states should match the saved states`, func(t *testing.T) {
		t.Parallel()
		_, err := concurrentScope.Run(ctx, func(ctx context.Context, repo core.Repository) error {
			for i := 0; i < 3; i++ {
				agg := NewTestAgg(core.ID(fmt.Sprintf("tx-scope-multi-id-%d", i)))
				_, err := agg.SingleEventCommand(fmt.Sprintf("tx-scope-multi-value-%d", i))
				require.NoError(t, err)

				err = repo.Save(ctx, agg)
				require.NoError(t, err)
			}

			for i := 0; i < 3; i++ {
				loadedAgg := &TestAgg{}
				err := repo.Load(ctx, core.ID(fmt.Sprintf("tx-scope-multi-id-%d", i)), loadedAgg)
				require.NoError(t, err)
				require.Equal(t, fmt.Sprintf("tx-scope-multi-value-%d", i), loadedAgg.State().String)
			}

			return nil
		})
		require.NoError(t, err)

		repo := factory.Create(ctx)
		for i := 0; i < 3; i++ {
			loadedAgg := &TestAgg{}
			err := repo.Load(ctx, core.ID(fmt.Sprintf("tx-scope-multi-id-%d", i)), loadedAgg)
			require.NoError(t, err)
			require.Equal(t, fmt.Sprintf("tx-scope-multi-value-%d", i), loadedAgg.State().String)
		}
	})

	t.Run(`Scenario: Concurrent scope with read-only operations
  Given an aggregate that has been saved outside concurrent scope
  When a read-only transaction is executed using concurrent scope
  And the aggregate is loaded within the concurrent scope
  Then the aggregate should be visible within the concurrent scope
  And the aggregate state should match the saved state
  When an attempt is made to load a non-existent aggregate within the concurrent scope
  Then the load operation should fail
  And the error should be ErrAggregateNotFound
  When the transaction commits
  Then the original aggregate should be unchanged`, func(t *testing.T) {
		t.Parallel()
		repo := factory.Create(ctx)
		agg := NewTestAgg("tx-scope-readonly-id")
		_, err := agg.SingleEventCommand("readonly-value")
		require.NoError(t, err)

		err = repo.Save(ctx, agg)
		require.NoError(t, err)

		_, err = concurrentScope.Run(ctx, func(ctx context.Context, repo core.Repository) error {
			loadedAgg := &TestAgg{}
			err = repo.Load(ctx, "tx-scope-readonly-id", loadedAgg)
			require.NoError(t, err)
			require.Equal(t, "readonly-value", loadedAgg.State().String)

			nonExistentAgg := &TestAgg{}
			err = repo.Load(ctx, "non-existent-id", nonExistentAgg)
			require.Error(t, err)
			require.ErrorIs(t, err, core.ErrAggregateNotFound)

			return nil
		})
		require.NoError(t, err)

		loadedAgg := &TestAgg{}
		err = repo.Load(ctx, "tx-scope-readonly-id", loadedAgg)
		require.NoError(t, err)
		require.Equal(t, "readonly-value", loadedAgg.State().String)
	})

	t.Run(`Scenario: Concurrent scope with error handling and cleanup
  Given a concurrent scope
  When multiple aggregates are created and saved within the concurrent scope
  And a failure is simulated during the transaction
  Then the transaction should rollback
  And the error should be returned
  When attempts are made to load all aggregates after rollback
  Then all load operations should fail
  And all errors should be ErrAggregateNotFound
  And no aggregates should have been persisted`, func(t *testing.T) {
		t.Parallel()
		_, err := concurrentScope.Run(ctx, func(ctx context.Context, repo core.Repository) error {
			agg1 := NewTestAgg("tx-scope-cleanup-1")
			_, err := agg1.SingleEventCommand("value-1")
			require.NoError(t, err)

			err = repo.Save(ctx, agg1)
			require.NoError(t, err)

			agg2 := NewTestAgg("tx-scope-cleanup-2")
			_, err = agg2.SingleEventCommand("value-2")
			require.NoError(t, err)

			err = repo.Save(ctx, agg2)
			require.NoError(t, err)

			agg3 := NewTestAgg("tx-scope-cleanup-3")
			_, err = agg3.SingleEventCommand("value-3")
			require.NoError(t, err)

			return errSimulatedFailureThirdSave
		})
		require.Error(t, err)
		require.Contains(t, err.Error(), "simulated failure during third aggregate save")

		repo := factory.Create(ctx)
		loadedAgg1 := &TestAgg{}
		err = repo.Load(ctx, "tx-scope-cleanup-1", loadedAgg1)
		require.Error(t, err)
		require.ErrorIs(t, err, core.ErrAggregateNotFound)

		loadedAgg2 := &TestAgg{}
		err = repo.Load(ctx, "tx-scope-cleanup-2", loadedAgg2)
		require.Error(t, err)
		require.ErrorIs(t, err, core.ErrAggregateNotFound)

		loadedAgg3 := &TestAgg{}
		err = repo.Load(ctx, "tx-scope-cleanup-3", loadedAgg3)
		require.Error(t, err)
		require.ErrorIs(t, err, core.ErrAggregateNotFound)
	})
}
