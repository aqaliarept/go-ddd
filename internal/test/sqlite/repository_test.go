package sqlite_test

import (
	"context"
	"database/sql"
	"testing"
	"time"

	testpkg "github.com/aqaliarept/go-ddd-kit/internal/test"
	core "github.com/aqaliarept/go-ddd-kit/pkg/core"
	sqlitepkg "github.com/aqaliarept/go-ddd-kit/pkg/sqlite"
	"github.com/avast/retry-go/v4"
	_ "modernc.org/sqlite"
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

type sqliteTestDB struct {
	db *sql.DB
}

func (s *sqliteTestDB) Database() *sql.DB {
	return s.db
}

func (s *sqliteTestDB) Cleanup() {
	if s.db != nil {
		s.db.Close()
	}
}

func SetupSQLiteTestDB(t *testing.T) *sqliteTestDB {
	db, err := sql.Open("sqlite", "file::memory:?cache=shared")
	if err != nil {
		t.Fatalf("failed to open SQLite database: %v", err)
	}

	db.SetMaxOpenConns(10)
	db.SetMaxIdleConns(5)
	db.SetConnMaxLifetime(5 * time.Minute)
	db.SetConnMaxIdleTime(1 * time.Minute)

	if err := db.Ping(); err != nil {
		db.Close()
		t.Fatalf("failed to ping SQLite database: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if _, err := db.ExecContext(ctx, "PRAGMA journal_mode=WAL"); err != nil {
		db.Close()
		t.Fatalf("failed to set WAL mode: %v", err)
	}

	if _, err := db.ExecContext(ctx, "PRAGMA busy_timeout=5000"); err != nil {
		db.Close()
		t.Fatalf("failed to set busy timeout: %v", err)
	}

	if err := sqlitepkg.CreateTable(ctx, db, "test_agg"); err != nil {
		db.Close()
		t.Fatalf("failed to create test table: %v", err)
	}

	t.Cleanup(func() {
		if err := db.Close(); err != nil {
			t.Logf("failed to close SQLite database: %v", err)
		}
	})

	return &sqliteTestDB{
		db: db,
	}
}

type sqliteTestRunner struct {
	testDB *sqliteTestDB
	repo   core.Repository
}

func (s *sqliteTestRunner) SetupRepository(t *testing.T) core.Repository {
	return s.repo
}

func (s *sqliteTestRunner) SetupContext(t *testing.T) context.Context {
	return context.Background()
}

func (s *sqliteTestRunner) SetupConcurrentScope(t *testing.T) *core.ConcurrentScope {
	factory := sqlitepkg.NewRepositoryFactory(s.testDB.Database())
	return core.NewConcurrentScope(factory,
		retry.Attempts(DefaultRetryAttempts),
		retry.Delay(DefaultRetryDelay),
		retry.MaxJitter(DefaultRetryMaxJitter),
	)
}

func (s *sqliteTestRunner) SetupRepositoryFactory(t *testing.T) core.RepositoryFactory {
	return sqlitepkg.NewRepositoryFactory(s.testDB.Database())
}

func TestSQLiteRepository(t *testing.T) {
	testDB := SetupSQLiteTestDB(t)

	factory := sqlitepkg.NewRepositoryFactory(testDB.Database())
	runner := &sqliteTestRunner{
		testDB: testDB,
		repo:    factory.Create(context.Background()),
	}

	testpkg.RunBaseTests(t, runner)
	testpkg.RunBaseTransactionalTests(t, runner)
}
