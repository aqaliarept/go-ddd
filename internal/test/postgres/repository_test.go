package postgres_test

import (
	"context"
	"errors"
	"log"
	"testing"
	"time"

	testpkg "github.com/aqaliarept/go-ddd-kit/internal/test"
	core "github.com/aqaliarept/go-ddd-kit/pkg/core"
	postgrespkg "github.com/aqaliarept/go-ddd-kit/pkg/postgres"
	"github.com/avast/retry-go/v4"
	"github.com/jackc/pgx/v4/pgxpool"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/postgres"
	"github.com/testcontainers/testcontainers-go/wait"
)

type (
	ID         = core.ID
	Version    = core.Version
	Repository = core.Repository
)

const (
	DefaultRetryAttempts  = 5
	DefaultRetryDelay     = 100 * time.Millisecond
	DefaultRetryMaxJitter = 50 * time.Millisecond
)

type postgresTestContainer struct {
	container testcontainers.Container
	db        *pgxpool.Pool
	connStr   string
}

func (p *postgresTestContainer) ConnectionString() string {
	return p.connStr
}

func (p *postgresTestContainer) Database() *pgxpool.Pool {
	return p.db
}

func (p *postgresTestContainer) Cleanup() {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	if p.db != nil {
		p.db.Close()
	}
	if p.container != nil {
		err := p.container.Terminate(ctx)
		log.Println(err)
	}
}

func SetupPostgresTestContainer(t *testing.T) *postgresTestContainer {
	ctx := context.Background()

	postgresContainer, err := postgres.Run(ctx,
		"postgres:15-alpine",
		testcontainers.WithWaitStrategy(
			wait.ForLog("database system is ready to accept connections").
				WithOccurrence(2).
				WithStartupTimeout(60*time.Second),
		),
		postgres.WithDatabase("test_db"),
		postgres.WithUsername("test_user"),
		postgres.WithPassword("test_password"),
	)
	if err != nil {
		t.Fatalf("failed to start PostgreSQL container: %v", err)
	}

	connStr, err := postgresContainer.ConnectionString(ctx, "sslmode=disable")
	if err != nil {
		termErr := postgresContainer.Terminate(ctx)
		t.Fatalf("failed to get PostgreSQL connection string: %v", errors.Join(err, termErr))
	}

	config, err := pgxpool.ParseConfig(connStr)
	if err != nil {
		termErr := postgresContainer.Terminate(ctx)
		t.Fatalf("failed to parse connection string: %v", errors.Join(err, termErr))
	}

	db, err := pgxpool.ConnectConfig(ctx, config)
	if err != nil {
		termErr := postgresContainer.Terminate(ctx)
		t.Fatalf("failed to connect to PostgreSQL: %v", errors.Join(err, termErr))
	}

	if err := db.Ping(ctx); err != nil {
		db.Close()
		termErr := postgresContainer.Terminate(ctx)
		t.Fatalf("failed to ping PostgreSQL: %v", errors.Join(err, termErr))
	}

	if err := createTestTable(ctx, db); err != nil {
		db.Close()
		termErr := postgresContainer.Terminate(ctx)
		t.Fatalf("failed to create test table: %v", errors.Join(err, termErr))
	}

	t.Cleanup(func() {
		db.Close()
		if err := postgresContainer.Terminate(ctx); err != nil {
			t.Logf("failed to terminate PostgreSQL container: %v", err)
		}
	})

	return &postgresTestContainer{
		container: postgresContainer,
		db:        db,
		connStr:   connStr,
	}
}

func createTestTable(ctx context.Context, db *pgxpool.Pool) error {
	query := `
		CREATE TABLE IF NOT EXISTS test_agg (
			id VARCHAR(255) PRIMARY KEY,
			version BIGINT NOT NULL,
			data JSONB NOT NULL,
			schema_version BIGINT,
			CONSTRAINT test_agg_id_unique UNIQUE (id)
		)
	`
	_, err := db.Exec(ctx, query)
	return err
}

type postgresTestRunner struct {
	container *postgresTestContainer
	repo      core.Repository
}

func (p *postgresTestRunner) SetupRepository(t *testing.T) core.Repository {
	return p.repo
}

func (p *postgresTestRunner) SetupContext(t *testing.T) context.Context {
	return context.Background()
}

func (p *postgresTestRunner) SetupConcurrentScope(t *testing.T) *core.ConcurrentScope {
	factory := postgrespkg.NewRepositoryFactory(p.container.Database())
	return core.NewConcurrentScope(factory,
		retry.Attempts(DefaultRetryAttempts),
		retry.Delay(DefaultRetryDelay),
		retry.MaxJitter(DefaultRetryMaxJitter),
	)
}

func (p *postgresTestRunner) SetupRepositoryFactory(t *testing.T) core.RepositoryFactory {
	return postgrespkg.NewRepositoryFactory(p.container.Database())
}

func TestPostgresRepository(t *testing.T) {
	container := SetupPostgresTestContainer(t)

	factory := postgrespkg.NewRepositoryFactory(container.Database())
	runner := &postgresTestRunner{
		container: container,
		repo:      factory.Create(context.Background()),
	}

	testpkg.RunBaseTests(t, runner)
	testpkg.RunBaseTransactionalTests(t, runner)
}
