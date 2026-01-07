package redis_test

import (
	"context"
	"errors"
	"testing"
	"time"

	testpkg "github.com/aqaliarept/go-ddd-kit/internal/test"
	core "github.com/aqaliarept/go-ddd-kit/pkg/core"
	redispkg "github.com/aqaliarept/go-ddd-kit/pkg/redis"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	rediscontainer "github.com/testcontainers/testcontainers-go/modules/redis"
	"github.com/testcontainers/testcontainers-go/wait"
)

type redisTestContainer struct {
	container testcontainers.Container
	client    *redis.Client
}

func (r *redisTestContainer) Client() *redis.Client {
	return r.client
}

func (r *redisTestContainer) Cleanup() {
	ctx := context.Background()
	if r.client != nil {
		_ = r.client.Close() //nolint:errcheck
	}
	if r.container != nil {
		_ = r.container.Terminate(ctx) //nolint:errcheck
	}
}

func SetupRedisTestContainer(t *testing.T) *redisTestContainer {
	ctx := context.Background()

	redisContainer, err := rediscontainer.Run(ctx,
		"redis:7-alpine",
		testcontainers.WithWaitStrategy(
			wait.ForLog("Ready to accept connections").
				WithOccurrence(1).
				WithStartupTimeout(60*time.Second).
				WithPollInterval(1*time.Second),
		),
	)
	if err != nil {
		t.Fatalf("failed to start Redis container: %v", err)
	}

	connStr, err := redisContainer.ConnectionString(ctx)
	if err != nil {
		termErr := redisContainer.Terminate(ctx)
		t.Fatalf("failed to get Redis connection string: %v", errors.Join(err, termErr))
	}

	opt, err := redis.ParseURL(connStr)
	if err != nil {
		termErr := redisContainer.Terminate(ctx)
		t.Fatalf("failed to parse Redis connection string: %v", errors.Join(err, termErr))
	}

	client := redis.NewClient(opt)

	if err := client.Ping(ctx).Err(); err != nil {
		closeErr := client.Close()
		termErr := redisContainer.Terminate(ctx)
		t.Fatalf("failed to ping Redis: %v", errors.Join(err, closeErr, termErr))
	}

	t.Cleanup(func() {
		if err := client.Close(); err != nil {
			t.Logf("failed to close Redis client: %v", err)
		}
		if err := redisContainer.Terminate(ctx); err != nil {
			t.Logf("failed to terminate Redis container: %v", err)
		}
	})

	return &redisTestContainer{
		container: redisContainer,
		client:    client,
	}
}

type redisTestRunner struct {
	container *redisTestContainer
	repo      core.Repository
}

func (r *redisTestRunner) SetupRepository(t *testing.T) core.Repository {
	return r.repo
}

func (r *redisTestRunner) SetupContext(t *testing.T) context.Context {
	return context.Background()
}

func TestRedisRepository(t *testing.T) {
	container := SetupRedisTestContainer(t)

	factory := redispkg.NewRepositoryFactory(container.Client())
	runner := &redisTestRunner{
		container: container,
		repo:      factory.Create(context.Background()),
	}

	testpkg.RunBaseTests(t, runner)
}

func TestRedisRepository_Expiration(t *testing.T) {
	container := SetupRedisTestContainer(t)

	factory := redispkg.NewRepositoryFactory(container.Client())
	repo := factory.Create(context.Background())
	ctx := context.Background()

	t.Run("Save new aggregate with expiration", func(t *testing.T) {
		agg := testpkg.NewTestAgg("expire-test-id-1")
		_, err := agg.SingleEventCommand("expire-value")
		require.NoError(t, err)

		err = repo.Save(ctx, agg, redispkg.WithExpiration(1*time.Second))
		require.NoError(t, err)

		loadedAgg := &testpkg.TestAgg{}
		err = repo.Load(ctx, "expire-test-id-1", loadedAgg)
		require.NoError(t, err)
		state := loadedAgg.State()
		require.Equal(t, "expire-value", state.String)

		time.Sleep(1100 * time.Millisecond)

		err = repo.Load(ctx, "expire-test-id-1", loadedAgg)
		require.Error(t, err)
		require.ErrorIs(t, err, core.ErrAggregateNotFound)
	})

	t.Run("Save existing aggregate with expiration", func(t *testing.T) {
		agg := testpkg.NewTestAgg("expire-test-id-2")
		_, err := agg.SingleEventCommand("initial-value")
		require.NoError(t, err)
		err = repo.Save(ctx, agg)
		require.NoError(t, err)

		_, err = agg.SingleEventCommand("updated-value")
		require.NoError(t, err)
		err = repo.Save(ctx, agg, redispkg.WithExpiration(1*time.Second))
		require.NoError(t, err)

		loadedAgg := &testpkg.TestAgg{}
		err = repo.Load(ctx, "expire-test-id-2", loadedAgg)
		require.NoError(t, err)
		state := loadedAgg.State()
		require.Equal(t, "updated-value", state.String)

		time.Sleep(1100 * time.Millisecond)

		err = repo.Load(ctx, "expire-test-id-2", loadedAgg)
		require.Error(t, err)
		require.ErrorIs(t, err, core.ErrAggregateNotFound)
	})

	t.Run("Save aggregate without expiration persists", func(t *testing.T) {
		agg := testpkg.NewTestAgg("no-expire-test-id")
		_, err := agg.SingleEventCommand("persistent-value")
		require.NoError(t, err)
		err = repo.Save(ctx, agg)
		require.NoError(t, err)

		time.Sleep(500 * time.Millisecond)

		loadedAgg := &testpkg.TestAgg{}
		err = repo.Load(ctx, "no-expire-test-id", loadedAgg)
		require.NoError(t, err)
		state := loadedAgg.State()
		require.Equal(t, "persistent-value", state.String)
	})

	t.Run("WithExpiration panics on zero duration", func(t *testing.T) {
		require.Panics(t, func() {
			redispkg.WithExpiration(0)
		}, "WithExpiration should panic when zero duration is passed")

		require.Panics(t, func() {
			redispkg.WithExpiration(-1 * time.Second)
		}, "WithExpiration should panic when negative duration is passed")
	})

	t.Run("Update expiration on existing aggregate", func(t *testing.T) {
		agg := testpkg.NewTestAgg("update-expire-test-id")
		_, err := agg.SingleEventCommand("initial-value")
		require.NoError(t, err)
		err = repo.Save(ctx, agg)
		require.NoError(t, err)

		loadedAgg := &testpkg.TestAgg{}
		err = repo.Load(ctx, "update-expire-test-id", loadedAgg)
		require.NoError(t, err)

		_, err = agg.SingleEventCommand("updated-with-expire")
		require.NoError(t, err)
		err = repo.Save(ctx, agg, redispkg.WithExpiration(1*time.Second))
		require.NoError(t, err)

		err = repo.Load(ctx, "update-expire-test-id", loadedAgg)
		require.NoError(t, err)
		state := loadedAgg.State()
		require.Equal(t, "updated-with-expire", state.String)

		time.Sleep(1100 * time.Millisecond)

		err = repo.Load(ctx, "update-expire-test-id", loadedAgg)
		require.Error(t, err)
		require.ErrorIs(t, err, core.ErrAggregateNotFound)
	})

	t.Run("Second save prolongs existing expiration", func(t *testing.T) {
		agg := testpkg.NewTestAgg("prolong-expire-test-id")
		_, err := agg.SingleEventCommand("initial-value")
		require.NoError(t, err)

		err = repo.Save(ctx, agg, redispkg.WithExpiration(1*time.Second))
		require.NoError(t, err)

		time.Sleep(600 * time.Millisecond)

		loadedAgg := &testpkg.TestAgg{}
		err = repo.Load(ctx, "prolong-expire-test-id", loadedAgg)
		require.NoError(t, err)
		state := loadedAgg.State()
		require.Equal(t, "initial-value", state.String)

		_, err = agg.SingleEventCommand("updated-value")
		require.NoError(t, err)
		err = repo.Save(ctx, agg, redispkg.WithExpiration(2*time.Second))
		require.NoError(t, err)

		time.Sleep(600 * time.Millisecond)

		err = repo.Load(ctx, "prolong-expire-test-id", loadedAgg)
		require.NoError(t, err)
		updatedState := loadedAgg.State()
		require.Equal(t, "updated-value", updatedState.String)

		time.Sleep(1500 * time.Millisecond)

		err = repo.Load(ctx, "prolong-expire-test-id", loadedAgg)
		require.Error(t, err)
		require.ErrorIs(t, err, core.ErrAggregateNotFound)
	})
}
