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
	gomysql "github.com/go-sql-driver/mysql"
)

// lockColumns lists all columns for SELECT queries.
const lockColumns = `id, database_name, database_type, repository, pull_request, owner,
	pending_plan_id, created_at, updated_at`

// lockStore implements storage.LockStore using MySQL.
type lockStore struct {
	db *sql.DB
}

// Acquire attempts to acquire a lock. Returns ErrLockHeld if held by another owner.
// Acquiring a lock the same owner already holds is a success (idempotent): two
// concurrent applies for the same PR and database (e.g. staging and production
// apply-confirms) share the same owner and lock key, so both must succeed. A
// non-empty lock.PendingPlanID then overwrites the stored one — the latest apply
// attempt's confirmation plan must be the one apply-confirm loads. A re-acquire
// that passes an empty PendingPlanID (rollback, CLI) leaves the existing value
// intact.
func (s *lockStore) Acquire(ctx context.Context, lock *storage.Lock) error {
	existing, err := s.Get(ctx, lock.DatabaseName, lock.DatabaseType)
	if err != nil {
		return fmt.Errorf("read existing lock for %s/%s: %w", lock.DatabaseName, lock.DatabaseType, err)
	}

	if existing != nil {
		if existing.Owner != lock.Owner {
			return storage.ErrLockHeld
		}
		return s.refreshPendingPlanID(ctx, lock, existing)
	}

	// No lock yet — try to claim it. The UNIQUE(database_name, database_type)
	// constraint makes the INSERT the arbiter when two callers race past the
	// Get above. The INSERT loser sees a duplicate-key error, not a held lock:
	// re-read and treat a same-owner winner as success.
	_, err = s.db.ExecContext(ctx, `
		INSERT INTO locks (database_name, database_type, repository, pull_request, owner, pending_plan_id)
		VALUES (?, ?, ?, ?, ?, ?)
	`, lock.DatabaseName, lock.DatabaseType, lock.Repository, lock.PullRequest, lock.Owner, lock.PendingPlanID)
	if err == nil {
		return nil
	}
	if !isDuplicateKeyError(err) {
		return fmt.Errorf("insert lock for %s/%s owner=%s: %w",
			lock.DatabaseName, lock.DatabaseType, lock.Owner, err)
	}

	winner, getErr := s.Get(ctx, lock.DatabaseName, lock.DatabaseType)
	if getErr != nil {
		return fmt.Errorf("read lock after insert race for %s/%s: %w",
			lock.DatabaseName, lock.DatabaseType, getErr)
	}
	if winner == nil {
		// Released between the duplicate-key error and this read: another owner
		// holds it logically, so report ErrLockHeld rather than retry-loop.
		return storage.ErrLockHeld
	}
	if winner.Owner != lock.Owner {
		return storage.ErrLockHeld
	}
	return s.refreshPendingPlanID(ctx, lock, winner)
}

// refreshPendingPlanID overwrites the stored confirmation plan reference when the
// caller supplied a new one (an apply re-run posts a new confirmation plan).
// The UPDATE is owner-scoped: it only changes the row while this owner still holds
// the lock.
//
// RowsAffected==0 is ambiguous and must not be read as "the owner predicate no
// longer matched". Under MySQL's default changed-rows semantics, a matched row
// reports zero affected rows when the stored value already equals the new value —
// which happens when a concurrent same-owner caller set the same pending_plan_id
// between this caller's read and its write. The owner still holds the lock in that
// case, so the refresh has succeeded. To distinguish that from a genuine ownership
// change, re-read the lock and branch on its actual state.
func (s *lockStore) refreshPendingPlanID(ctx context.Context, lock, existing *storage.Lock) error {
	if lock.PendingPlanID == "" || lock.PendingPlanID == existing.PendingPlanID {
		return nil
	}
	result, err := s.db.ExecContext(ctx, `
		UPDATE locks
		SET pending_plan_id = ?, updated_at = NOW()
		WHERE database_name = ? AND database_type = ? AND owner = ?
	`, lock.PendingPlanID, lock.DatabaseName, lock.DatabaseType, lock.Owner)
	if err != nil {
		return fmt.Errorf("refresh pending_plan_id for %s/%s owner=%s: %w",
			lock.DatabaseName, lock.DatabaseType, lock.Owner, err)
	}
	rowsAffected, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("read rows affected refreshing pending_plan_id for %s/%s owner=%s: %w",
			lock.DatabaseName, lock.DatabaseType, lock.Owner, err)
	}
	if rowsAffected > 0 {
		return nil
	}

	current, getErr := s.Get(ctx, lock.DatabaseName, lock.DatabaseType)
	if getErr != nil {
		return fmt.Errorf("read lock after pending_plan_id refresh affected no rows for %s/%s owner=%s: %w",
			lock.DatabaseName, lock.DatabaseType, lock.Owner, getErr)
	}
	if current == nil {
		return storage.ErrLockNotFound
	}
	if current.Owner == lock.Owner {
		// The caller still owns the lock; the UPDATE affected no rows only because
		// the stored value already matched, so the refresh is satisfied.
		return nil
	}
	return storage.ErrLockHeld
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
		&lock.PendingPlanID, &lock.CreatedAt, &lock.UpdatedAt,
	)
	if err != nil {
		return nil, err
	}

	return &lock, nil
}

// isDuplicateKeyError checks if the error is a MySQL duplicate key error (code 1062).
func isDuplicateKeyError(err error) bool {
	var mysqlErr *gomysql.MySQLError
	if errors.As(err, &mysqlErr) && mysqlErr.Number == 1062 {
		return true
	}
	return err != nil && strings.Contains(err.Error(), "Duplicate entry")
}
