package mongo_test

import (
	"context"
	"testing"
	"time"

	testpkg "github.com/aqaliarept/go-ddd-kit/internal/test"
	core "github.com/aqaliarept/go-ddd-kit/pkg/core"
	mongopkg "github.com/aqaliarept/go-ddd-kit/pkg/mongo"
	"github.com/avast/retry-go/v4"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
)

type ConcurrentScopeConfig struct {
	Retry RetryConfig
}

type RetryConfig struct {
	Attempts  uint
	Delay     time.Duration
	MaxJitter time.Duration
}

type MongoConcurrentScope struct {
	*core.ConcurrentScope
}

func NewMongoConcurrentScope(factory core.RepositoryFactory, config ConcurrentScopeConfig) *MongoConcurrentScope {
	retryOpts := []retry.Option{
		retry.Attempts(config.Retry.Attempts),
		retry.Delay(config.Retry.Delay),
		retry.MaxJitter(config.Retry.MaxJitter),
	}
	return &MongoConcurrentScope{
		ConcurrentScope: core.NewConcurrentScope(factory, core.WithRetryOptions(retryOpts...)),
	}
}

func TestMongoRepository_ConcurrentScope(t *testing.T) {
	container := SetupMongoTestContainer(t)
	defer container.Cleanup()

	database := container.Client().Database("user_management")
	factory := mongopkg.NewRepositoryFactory(database)
	config := ConcurrentScopeConfig{
		Retry: RetryConfig{
			Attempts:  DefaultRetryAttempts,
			Delay:     DefaultRetryDelay,
			MaxJitter: DefaultRetryMaxJitter,
		},
	}
	concurrentScope := NewMongoConcurrentScope(factory, config)

	t.Run("Overlapping commits - first commit succeeds, second fails", func(t *testing.T) {
		ctx := context.Background()

		id := core.ID(uuid.NewString())

		func() {
			repo := factory.Create(ctx)

			tx, ok := repo.(core.Transactional)
			require.True(t, ok)

			tctx, err := tx.Begin(ctx)
			require.NoError(t, err)

			agg := testpkg.NewTestAgg(id)
			_, err = agg.SingleEventCommand("test-agg-value-0")
			require.NoError(t, err)

			err = repo.Save(tctx, agg)
			require.NoError(t, err)

			err = tx.Commit(tctx)
			require.NoError(t, err)
		}()

		repo0 := factory.Create(ctx)

		tx0, ok := repo0.(core.Transactional)
		require.True(t, ok)

		tx0ctx, err := tx0.Begin(ctx)
		require.NoError(t, err)

		agg0 := &testpkg.TestAgg{}
		err = repo0.Load(tx0ctx, id, agg0)
		require.NoError(t, err)

		_, err = agg0.SingleEventCommand("test-agg-value-0")
		require.NoError(t, err)

		repo1 := factory.Create(ctx)

		tx1, ok := repo1.(core.Transactional)
		require.True(t, ok)

		tx1ctx, err := tx1.Begin(ctx)
		require.NoError(t, err)

		agg1 := &testpkg.TestAgg{}
		err = repo1.Load(tx1ctx, id, agg1)
		require.NoError(t, err)

		_, err = agg1.SingleEventCommand("test-agg-value-1")
		require.NoError(t, err)

		err = repo0.Save(tx0ctx, agg0)
		require.NoError(t, err)

		err = repo1.Save(tx1ctx, agg1)
		require.Error(t, err)
		require.ErrorIs(t, err, core.ErrConcurrentModification)

		err = tx0.Commit(tx0ctx)
		require.NoError(t, err)

		err = tx1.Commit(tx1ctx)
		require.Error(t, err)
		require.ErrorIs(t, err, core.ErrTransactionNotFound)
	})

	t.Run("Save aggregate within concurrent scope - retry success", func(t *testing.T) {
		ctx := context.Background()
		attempts := DefaultRetryAttempts - 1

		_, err := concurrentScope.Run(ctx,
			func(ctx context.Context, repo core.Repository) error {
				agg := testpkg.NewTestAgg("concurrent-scope-retry-success-id")
				_, err := agg.SingleEventCommand("concurrent-scope-retry-success-value")
				require.NoError(t, err)

				err = repo.Save(ctx, agg)
				require.NoError(t, err)

				loadedAgg := &testpkg.TestAgg{}
				err = repo.Load(ctx, "concurrent-scope-retry-success-id", loadedAgg)
				require.NoError(t, err)
				state := loadedAgg.State()
				require.Equal(t, "concurrent-scope-retry-success-value", state.String)

				attempts--
				if attempts >= 0 {
					return core.ErrConcurrentModification
				}

				return nil
			})
		require.NoError(t, err)

		repo := factory.Create(ctx)
		loadedAgg := &testpkg.TestAgg{}
		err = repo.Load(ctx, "concurrent-scope-retry-success-id", loadedAgg)
		require.NoError(t, err)
		state := loadedAgg.State()
		require.Equal(t, "concurrent-scope-retry-success-value", state.String)
	})

	t.Run("Save aggregate within concurrent scope - retry failure", func(t *testing.T) {
		ctx := context.Background()

		_, err := concurrentScope.Run(ctx, func(ctx context.Context, repo core.Repository) error {
			agg := testpkg.NewTestAgg("concurrent-scope-retry-id")
			_, err := agg.SingleEventCommand("concurrent-scope-retry-value")
			require.NoError(t, err)

			err = repo.Save(ctx, agg)
			require.NoError(t, err)

			loadedAgg := &testpkg.TestAgg{}
			err = repo.Load(ctx, "concurrent-scope-retry-id", loadedAgg)
			require.NoError(t, err)
			state := loadedAgg.State()
			require.Equal(t, "concurrent-scope-retry-value", state.String)

			return core.ErrConcurrentModification
		})
		require.Error(t, err)
		require.ErrorIs(t, err, core.ErrConcurrentModification, "expected error to be ErrConcurrentModification")
		retryError := &retry.Error{}
		require.ErrorAs(t, err, retryError)
		require.ErrorIs(t, retryError, core.ErrConcurrentModification, "expected retry error to be ErrConcurrentModification")

		repo := factory.Create(ctx)
		loadedAgg := &testpkg.TestAgg{}
		err = repo.Load(ctx, "concurrent-scope-retry-id", loadedAgg)
		require.Error(t, err)
		require.Equal(t, core.ErrAggregateNotFound, err)
	})

	t.Run("Concurrent scope with aggregate updates and events", func(t *testing.T) {
		ctx := context.Background()

		_, err := concurrentScope.Run(ctx, func(ctx context.Context, repo core.Repository) error {
			agg := testpkg.NewTestAgg("concurrent-scope-updates-id")
			_, err := agg.SingleEventCommand("initial-value")
			require.NoError(t, err)

			err = repo.Save(ctx, agg)
			require.NoError(t, err)

			loadedAgg := &testpkg.TestAgg{}
			err = repo.Load(ctx, "concurrent-scope-updates-id", loadedAgg)
			require.NoError(t, err)
			state := loadedAgg.State()
			require.Equal(t, "initial-value", state.String)

			_, err = loadedAgg.SingleEventCommand("updated-value")
			require.NoError(t, err)
			_, err = loadedAgg.SingleEventCommand("final-value")
			require.NoError(t, err)

			err = repo.Save(ctx, loadedAgg)
			require.NoError(t, err)

			finalAgg := &testpkg.TestAgg{}
			err = repo.Load(ctx, "concurrent-scope-updates-id", finalAgg)
			require.NoError(t, err)
			finalState := finalAgg.State()
			require.Equal(t, "final-value", finalState.String)

			return nil
		})
		require.NoError(t, err)

		repo := factory.Create(ctx)
		finalAgg := &testpkg.TestAgg{}
		err = repo.Load(ctx, "concurrent-scope-updates-id", finalAgg)
		require.NoError(t, err)
		state := finalAgg.State()
		require.Equal(t, "final-value", state.String)
	})

	t.Run("Concurrent scope with concurrent access simulation", func(t *testing.T) {
		ctx := context.Background()
		outsideRepo := factory.Create(ctx)
		outsideCtx := ctx
		agg := testpkg.NewTestAgg("concurrent-scope-concurrent-id")
		_, err := agg.SingleEventCommand("initial-value")
		require.NoError(t, err)

		err = outsideRepo.Save(outsideCtx, agg)
		require.NoError(t, err)

		_, err = concurrentScope.Run(ctx,
			func(ctx context.Context, repo core.Repository) error {
				txAgg := &testpkg.TestAgg{}
				err = repo.Load(ctx, "concurrent-scope-concurrent-id", txAgg)
				require.NoError(t, err)

				outsideAgg := &testpkg.TestAgg{}
				err = outsideRepo.Load(outsideCtx, "concurrent-scope-concurrent-id", outsideAgg)
				require.NoError(t, err)

				if outsideAgg.State().String == "initial-value" {
					_, err = outsideAgg.SingleEventCommand("outside-value")
					require.NoError(t, err)

					err = outsideRepo.Save(outsideCtx, outsideAgg)
					require.NoError(t, err)
				}
				_, err = txAgg.SingleEventCommand("tx-value")
				require.NoError(t, err)

				return repo.Save(ctx, txAgg)
			})

		require.NoError(t, err)
		loadedAgg := &testpkg.TestAgg{}
		err = outsideRepo.Load(outsideCtx, "concurrent-scope-concurrent-id", loadedAgg)
		require.NoError(t, err)
		state := loadedAgg.State()
		require.NotEqual(t, "outside-value", state.String)
		require.Equal(t, "tx-value", state.String)
	})
}

