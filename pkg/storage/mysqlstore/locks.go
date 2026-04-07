// locks.go implements LockStore for database-level deployment locks.
// Locks prevent concurrent schema changes to the same database.
package mysqlstore

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"

	"github.com/block/schemabot/pkg/storage"
	"github.com/block/spirit/pkg/utils"
)

// lockColumns lists all columns for SELECT queries.
const lockColumns = `id, database_name, database_type, repository, pull_request, owner,
	created_at, updated_at`

// lockStore implements storage.LockStore using MySQL.
type lockStore struct {
	db *sql.DB
}

// Acquire attempts to acquire a lock. Returns ErrLockHeld if already held by another owner.
// If the same owner already holds the lock, this is a no-op (idempotent).
func (s *lockStore) Acquire(ctx context.Context, lock *storage.Lock) error {
	// Check if lock already exists
	existing, err := s.Get(ctx, lock.DatabaseName, lock.DatabaseType)
	if err != nil {
		return err
	}

	if existing != nil {
		// Lock exists - check if same owner (idempotent)
		if existing.Owner == lock.Owner {
			return nil // Same owner, no-op
		}
		return storage.ErrLockHeld
	}

	// Try to insert - will fail if lock exists due to UNIQUE constraint (race condition)
	_, err = s.db.ExecContext(ctx, `
		INSERT INTO locks (database_name, database_type, repository, pull_request, owner)
		VALUES (?, ?, ?, ?, ?)
	`, lock.DatabaseName, lock.DatabaseType, lock.Repository, lock.PullRequest, lock.Owner)

	if err != nil {
		// Check if it's a duplicate key error (lock already held - race condition)
		if isDuplicateKeyError(err) {
			return storage.ErrLockHeld
		}
		return err
	}
	return nil
}

// Release releases a lock. Only succeeds if caller is the owner.
func (s *lockStore) Release(ctx context.Context, database, dbType, owner string) error {
	result, err := s.db.ExecContext(ctx, `
		DELETE FROM locks
		WHERE database_name = ? AND database_type = ? AND owner = ?
	`, database, dbType, owner)
	if err != nil {
		return err
	}

	rowsAffected, _ := result.RowsAffected()
	if rowsAffected == 0 {
		// Check if lock exists at all
		existing, err := s.Get(ctx, database, dbType)
		if err != nil {
			return err
		}
		if existing == nil {
			return storage.ErrLockNotFound
		}
		// Lock exists but not owned by caller
		return storage.ErrLockNotOwned
	}
	return nil
}

// ForceRelease releases a lock regardless of owner (admin override).
func (s *lockStore) ForceRelease(ctx context.Context, database, dbType string) error {
	result, err := s.db.ExecContext(ctx, `
		DELETE FROM locks
		WHERE database_name = ? AND database_type = ?
	`, database, dbType)
	if err != nil {
		return err
	}

	return checkRowsAffected(result, storage.ErrLockNotFound)
}

// Get returns a lock by database name and type, or nil if not found.
func (s *lockStore) Get(ctx context.Context, database, dbType string) (*storage.Lock, error) {
	row := s.db.QueryRowContext(ctx, `
		SELECT `+lockColumns+`
		FROM locks
		WHERE database_name = ? AND database_type = ?
	`, database, dbType)

	return scanLock(row)
}

// List returns all active locks.
func (s *lockStore) List(ctx context.Context) ([]*storage.Lock, error) {
	rows, err := s.db.QueryContext(ctx, `
		SELECT `+lockColumns+`
		FROM locks
		ORDER BY created_at DESC
	`)
	if err != nil {
		return nil, err
	}
	defer utils.CloseAndLog(rows)

	return scanLocks(rows)
}

// Update updates lock metadata (touches updated_at).
func (s *lockStore) Update(ctx context.Context, lock *storage.Lock) error {
	result, err := s.db.ExecContext(ctx, `
		UPDATE locks
		SET updated_at = NOW()
		WHERE database_name = ? AND database_type = ?
	`, lock.DatabaseName, lock.DatabaseType)
	if err != nil {
		return err
	}

	return checkRowsAffected(result, storage.ErrLockNotFound)
}

// GetByPR returns all locks associated with a PR.
func (s *lockStore) GetByPR(ctx context.Context, repo string, pr int) ([]*storage.Lock, error) {
	rows, err := s.db.QueryContext(ctx, `
		SELECT `+lockColumns+`
		FROM locks
		WHERE repository = ? AND pull_request = ?
	`, repo, pr)
	if err != nil {
		return nil, fmt.Errorf("query locks for %s#%d: %w", repo, pr, err)
	}
	defer utils.CloseAndLog(rows)

	return scanLocks(rows)
}

// scanLock scans a single lock row, returning nil if not found.
func scanLock(row *sql.Row) (*storage.Lock, error) {
	lock, err := scanLockInto(row)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	return lock, err
}

// scanLocks scans multiple lock rows.
func scanLocks(rows *sql.Rows) ([]*storage.Lock, error) {
	var locks []*storage.Lock
	for rows.Next() {
		lock, err := scanLockInto(rows)
		if err != nil {
			return nil, err
		}
		locks = append(locks, lock)
	}
	return locks, rows.Err()
}

// scanLockInto scans lock data from any scanner (Row or Rows).
func scanLockInto(s scanner) (*storage.Lock, error) {
	var lock storage.Lock

	err := s.Scan(
		&lock.ID, &lock.DatabaseName, &lock.DatabaseType,
		&lock.Repository, &lock.PullRequest, &lock.Owner,
		&lock.CreatedAt, &lock.UpdatedAt,
	)
	if err != nil {
		return nil, err
	}

	return &lock, nil
}

// isDuplicateKeyError checks if the error is a MySQL duplicate key error (code 1062).
func isDuplicateKeyError(err error) bool {
	return err != nil && strings.Contains(err.Error(), "Duplicate entry")
}
