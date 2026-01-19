package sqlite

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"

	core "github.com/aqaliarept/go-ddd-kit/pkg/core"
	"modernc.org/sqlite"
	_ "modernc.org/sqlite"
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

type tx struct {
	tx *sql.Tx
}

type repository struct {
	db *sql.DB
	tx *tx
}

func (r *repository) Begin(ctx context.Context) (context.Context, error) {
	if r.tx != nil {
		panic("transaction in progress")
	}

	sqlTx, err := r.db.BeginTx(ctx, &sql.TxOptions{
		Isolation: sql.LevelSerializable,
	})
	if err != nil {
		return ctx, err
	}

	r.tx = &tx{tx: sqlTx}
	return ctx, nil
}

func (r *repository) Commit(ctx context.Context) error {
	if r.tx == nil {
		return fmt.Errorf("%w: no transaction to commit", core.ErrTransactionNotFound)
	}

	err := r.tx.tx.Commit()
	r.tx = nil
	return err
}

func (r *repository) Rollback(ctx context.Context) error {
	if r.tx == nil {
		return nil
	}

	err := r.tx.tx.Rollback()
	r.tx = nil
	return err
}

type AggregateDocument struct {
	ID            string
	State         []byte
	Version       uint64
	SchemaVersion uint64
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
	query := fmt.Sprintf(`SELECT id, version, data, COALESCE(schema_version, 1) FROM %s WHERE id = ?`, tableName)

	var doc AggregateDocument
	var err error
	if r.tx != nil {
		err = r.tx.tx.QueryRowContext(ctx, query, string(id)).Scan(&doc.ID, &doc.Version, &doc.State, &doc.SchemaVersion)
	} else {
		err = r.db.QueryRowContext(ctx, query, string(id)).Scan(&doc.ID, &doc.Version, &doc.State, &doc.SchemaVersion)
	}

	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return core.ErrAggregateNotFound
		}
		return fmt.Errorf("retrieval failed: %w", err)
	}

	return target.Restore(id, core.Version(doc.Version), core.SchemaVersion(doc.SchemaVersion), func(statePtr core.StatePtr) error {
		if err := json.Unmarshal(doc.State, statePtr); err != nil {
			return fmt.Errorf("state deserialization error: %w", err)
		}
		return nil
	})
}

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
	query := fmt.Sprintf(`DELETE FROM %s WHERE id = ? AND version = ?`, tableName)
	var result sql.Result
	var err error
	if r.tx != nil {
		result, err = r.tx.tx.ExecContext(ctx, query, string(id), uint64(expectedVersion))
	} else {
		result, err = r.db.ExecContext(ctx, query, string(id), uint64(expectedVersion))
	}
	if err != nil {
		return fmt.Errorf("removal failed: %w", err)
	}

	rowsAffected, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("removal failed: %w", err)
	}
	if rowsAffected == 0 {
		var existingVersion uint64
		checkQuery := fmt.Sprintf(`SELECT version FROM %s WHERE id = ?`, tableName)
		var scanErr error
		if r.tx != nil {
			scanErr = r.tx.tx.QueryRowContext(ctx, checkQuery, string(id)).Scan(&existingVersion)
		} else {
			scanErr = r.db.QueryRowContext(ctx, checkQuery, string(id)).Scan(&existingVersion)
		}
		if scanErr == nil {
			return core.ErrConcurrentModification
		}
		return core.ErrAggregateNotFound
	}

	return nil
}

func (r *repository) insertNew(ctx context.Context, tableName string, id core.ID, stateBytes []byte, version core.Version, schemaVersion core.SchemaVersion) error {
	query := fmt.Sprintf(`INSERT INTO %s (id, version, data, schema_version) VALUES (?, ?, ?, ?)`, tableName)
	var err error
	if r.tx != nil {
		_, err = r.tx.tx.ExecContext(ctx, query, string(id), uint64(version), stateBytes, uint64(schemaVersion))
	} else {
		_, err = r.db.ExecContext(ctx, query, string(id), uint64(version), stateBytes, uint64(schemaVersion))
	}
	if err != nil {
		var sqliteErr *sqlite.Error
		if errors.As(err, &sqliteErr) {
			code := sqliteErr.Code()
			if code == 2067 || code == 1555 || code == 19 {
				return core.ErrAggregateExists
			}
		}
		return fmt.Errorf("insertion failed: %w", err)
	}
	return nil
}

func (r *repository) updateExisting(ctx context.Context, tableName string, id core.ID, stateBytes []byte, expectedVersion core.Version, nextVersion core.Version, schemaVersion core.SchemaVersion) error {
	query := fmt.Sprintf(`UPDATE %s SET version = ?, data = ?, schema_version = ? WHERE id = ? AND version = ?`, tableName)
	var result sql.Result
	var err error
	if r.tx != nil {
		result, err = r.tx.tx.ExecContext(ctx, query, uint64(nextVersion), stateBytes, uint64(schemaVersion), string(id), uint64(expectedVersion))
	} else {
		result, err = r.db.ExecContext(ctx, query, uint64(nextVersion), stateBytes, uint64(schemaVersion), string(id), uint64(expectedVersion))
	}
	if err != nil {
		return fmt.Errorf("update failed: %w", err)
	}

	rowsAffected, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("update failed: %w", err)
	}
	if rowsAffected == 0 {
		var existingVersion uint64
		checkQuery := fmt.Sprintf(`SELECT version FROM %s WHERE id = ?`, tableName)
		var scanErr error
		if r.tx != nil {
			scanErr = r.tx.tx.QueryRowContext(ctx, checkQuery, string(id)).Scan(&existingVersion)
		} else {
			scanErr = r.db.QueryRowContext(ctx, checkQuery, string(id)).Scan(&existingVersion)
		}
		if scanErr == nil {
			return core.ErrConcurrentModification
		}
		return core.ErrAggregateNotFound
	}

	return nil
}

func newRepository(db *sql.DB) core.Repository {
	return &repository{
		db: db,
		tx: nil,
	}
}

type Factory struct {
	db *sql.DB
}

func NewRepositoryFactory(db *sql.DB) core.RepositoryFactory {
	return &Factory{db: db}
}

func (f *Factory) Create(ctx context.Context) core.Repository {
	return newRepository(f.db)
}

func CreateTable(ctx context.Context, db *sql.DB, aggregateName string) error {
	if aggregateName == "" {
		return fmt.Errorf("aggregate name must be non-empty")
	}
	query := fmt.Sprintf(`
		CREATE TABLE IF NOT EXISTS %s (
			id TEXT PRIMARY KEY,
			version INTEGER NOT NULL,
			data TEXT NOT NULL,
			schema_version INTEGER,
			CONSTRAINT %s_id_unique UNIQUE (id)
		)
	`, aggregateName, aggregateName)
	_, err := db.ExecContext(ctx, query)
	return err
}
