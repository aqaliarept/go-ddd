package mongo_test

import (
	"context"
	"errors"
	"testing"
	"time"

	testpkg "github.com/aqaliarept/go-ddd-kit/internal/test"
	core "github.com/aqaliarept/go-ddd-kit/pkg/core"
	mongopkg "github.com/aqaliarept/go-ddd-kit/pkg/mongo"
	"github.com/avast/retry-go/v4"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/mongodb"
	"github.com/testcontainers/testcontainers-go/wait"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
)

type (
	ID         = core.ID
	Version    = core.Version
	Repository = core.Repository
)

type mongoTestContainer struct {
	container testcontainers.Container
	client    *mongo.Client
	database  *mongo.Database
	connStr   string
}

func (m *mongoTestContainer) ConnectionString() string {
	return m.connStr
}

func (m *mongoTestContainer) Database() *mongo.Database {
	return m.database
}

func (m *mongoTestContainer) Client() *mongo.Client {
	return m.client
}

func (m *mongoTestContainer) Cleanup() {
	ctx := context.Background()
	if m.client != nil {
		_ = m.client.Disconnect(ctx) //nolint:errcheck
	}
	if m.container != nil {
		_ = m.container.Terminate(ctx) //nolint:errcheck
	}
}

func SetupMongoTestContainer(t *testing.T) *mongoTestContainer {
	ctx := context.Background()

	mongoContainer, err := mongodb.Run(ctx,
		"mongo:7.0",
		testcontainers.WithWaitStrategy(
			wait.ForLog("Waiting for connections").
				WithOccurrence(1).
				WithStartupTimeout(60*time.Second),
		),
		mongodb.WithReplicaSet("rs0"),
	)
	if err != nil {
		t.Fatalf("failed to start MongoDB container: %v", err)
	}

	connStr, err := mongoContainer.ConnectionString(ctx)

	if err != nil {
		termErr := mongoContainer.Terminate(ctx)
		t.Fatalf("failed to get MongoDB connection string: %v", errors.Join(err, termErr))
	}

	clientOptions := options.Client().ApplyURI(connStr)
	client, err := mongo.Connect(clientOptions)
	if err != nil {
		termErr := mongoContainer.Terminate(ctx)
		t.Fatalf("failed to connect to MongoDB: %v", errors.Join(err, termErr))
	}

	if err := client.Ping(ctx, nil); err != nil {
		discErr := client.Disconnect(ctx)
		termErr := mongoContainer.Terminate(ctx)
		t.Fatalf("failed to ping MongoDB: %v", errors.Join(err, discErr, termErr))
	}

	database := client.Database("test_db")

	t.Cleanup(func() {
		if err := client.Disconnect(ctx); err != nil {
			t.Logf("failed to disconnect MongoDB client: %v", err)
		}
		if err := mongoContainer.Terminate(ctx); err != nil {
			t.Logf("failed to terminate MongoDB container: %v", err)
		}
	})

	return &mongoTestContainer{
		container: mongoContainer,
		client:    client,
		database:  database,
		connStr:   connStr,
	}
}

type mongoTestRunner struct {
	container *mongoTestContainer
	repo      core.Repository
}

func (m *mongoTestRunner) SetupRepository(t *testing.T) core.Repository {
	return m.repo
}

func (m *mongoTestRunner) SetupContext(t *testing.T) context.Context {
	return context.Background()
}

func (m *mongoTestRunner) SetupConcurrentScope(t *testing.T) *core.ConcurrentScope {
	factory := mongopkg.NewRepositoryFactory(m.container.Database())
	return core.NewConcurrentScope(factory,
		retry.Attempts(DefaultRetryAttempts),
		retry.Delay(DefaultRetryDelay),
		retry.MaxJitter(DefaultRetryMaxJitter),
	)
}

func (m *mongoTestRunner) SetupRepositoryFactory(t *testing.T) core.RepositoryFactory {
	return mongopkg.NewRepositoryFactory(m.container.Database())
}

const (
	DefaultRetryAttempts  = 3
	DefaultRetryDelay     = 100 * time.Millisecond
	DefaultRetryMaxJitter = 50 * time.Millisecond
)

func TestMongoRepository(t *testing.T) {
	container := SetupMongoTestContainer(t)

	factory := mongopkg.NewRepositoryFactory(container.Database())
	runner := &mongoTestRunner{
		container: container,
		repo:      factory.Create(context.Background()),
	}

	testpkg.RunBaseTests(t, runner)
	testpkg.RunBaseConcurrentTests(t, runner)
}
