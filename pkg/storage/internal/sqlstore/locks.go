// locks.go implements LockStore for database-level deployment locks.
// Locks prevent concurrent schema changes to the same database.
package sqlstore

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/block/schemabot/pkg/storage"
	"github.com/block/spirit/pkg/utils"
)

// lockColumns lists all columns for SELECT queries.
const lockColumns = `id, database_name, database_type, repository, pull_request, owner,
	pending_plan_id, disclosed_copy_discard, acquired_by, acquired_by_operator_groups, created_at, updated_at`

// lockStore implements storage.LockStore using MySQL.
type lockStore struct {
	db         *rebindDB
	dialect    Dialect
	classifier ErrorClassifier
}

func canonicalizeLock(lock *storage.Lock) {
	if lock == nil {
		return
	}
	lock.DatabaseName = storage.CanonicalKey(lock.DatabaseName)
	lock.DatabaseType = storage.CanonicalKey(lock.DatabaseType)
	lock.Repository = storage.CanonicalKey(lock.Repository)
}

// Acquire attempts to acquire a lock. Returns ErrLockHeld if held by another owner.
// Acquiring a lock the same owner already holds is a success (idempotent): two
// concurrent applies for the same PR and database (e.g. staging and production
// apply-confirms) share the same owner and lock key, so both must succeed. A
// non-empty lock.PendingPlanID then overwrites the stored one — the latest apply
// or rollback attempt's confirmation plan must be the one the corresponding
// confirm command loads, and its disclosure record travels with it. A re-acquire
// that passes an empty PendingPlanID (CLI) leaves the existing values intact.
// A re-acquire never changes the recorded acquirer: only the insert that
// creates the row writes it.
func (s *lockStore) Acquire(ctx context.Context, lock *storage.Lock) error {
	return s.acquire(ctx, lock, nil)
}

// AcquireIfPendingPlanID acquires like Acquire, but only while the lock is
// still in the state the caller observed: free when observedPendingPlanID is
// empty, or held by the same owner with observedPendingPlanID as its pending
// plan. Any other same-owner state is a newer intent that a concurrent command
// pinned since the caller looked, so the acquire returns ErrLockIntentChanged
// and leaves that pin alone rather than overwriting it.
func (s *lockStore) AcquireIfPendingPlanID(ctx context.Context, lock *storage.Lock, observedPendingPlanID string) error {
	return s.acquire(ctx, lock, &observedPendingPlanID)
}

// acquire claims the lock, retrying transient conflicts. A nil observed pin
// acquires unconditionally; a non-nil one is the pending plan the caller
// observed and the claim must find the lock still in that state.
func (s *lockStore) acquire(ctx context.Context, lock *storage.Lock, observedPendingPlanID *string) error {
	canonicalizeLock(lock)
	acquirer, err := encodeLockAcquirer(lock)
	if err != nil {
		return err
	}
	op := fmt.Sprintf("acquire lock for %s/%s owner=%s", lock.DatabaseName, lock.DatabaseType, lock.Owner)
	return withLockRetry(ctx, s.classifier, op, func() error {
		return s.acquireOnce(ctx, lock, acquirer, observedPendingPlanID)
	})
}

// acquireOnce performs a single claim attempt. Concurrent same-owner callers
// racing to claim the same key can hit a transient InnoDB lock conflict on the
// INSERT below; acquire retries those. acquire canonicalizes the lock and
// encodes its acquirer first.
func (s *lockStore) acquireOnce(ctx context.Context, lock *storage.Lock, acquirer lockAcquirerColumns, observedPendingPlanID *string) error {
	existing, err := s.Get(ctx, lock.DatabaseName, lock.DatabaseType)
	if err != nil {
		return fmt.Errorf("read existing lock for %s/%s: %w", lock.DatabaseName, lock.DatabaseType, err)
	}

	if existing != nil {
		if existing.Owner != lock.Owner {
			return storage.ErrLockHeld
		}
		return s.refreshPendingConfirmation(ctx, lock, existing, observedPendingPlanID)
	}

	// The caller observed a pin it still expects to hold; the lock is gone
	// instead, so its intent was released or replaced since.
	if observedPendingPlanID != nil && *observedPendingPlanID != "" {
		return storage.ErrLockIntentChanged
	}

	// No lock yet — try to claim it. The UNIQUE(database_name, database_type)
	// constraint makes the INSERT the arbiter when two callers race past the
	// Get above. The INSERT loser sees a duplicate-key error, not a held lock:
	// re-read and treat a same-owner winner as success.
	_, err = s.db.ExecContext(ctx, `
		INSERT INTO locks (database_name, database_type, repository, pull_request, owner, pending_plan_id, disclosed_copy_discard,
			acquired_by, acquired_by_operator_groups)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
	`, lock.DatabaseName, lock.DatabaseType, lock.Repository, lock.PullRequest, lock.Owner, lock.PendingPlanID, lock.DisclosedCopyDiscard,
		acquirer.subject, acquirer.operatorGroups)
	if err == nil {
		return nil
	}
	if !s.classifier.IsDuplicateKey(err) {
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
	return s.refreshPendingConfirmation(ctx, lock, winner, observedPendingPlanID)
}

// lockAcquirerColumns holds the insert values of the acquired_by and
// acquired_by_operator_groups columns; a nil value is written as NULL.
type lockAcquirerColumns struct {
	subject        any
	operatorGroups any
}

// encodeLockAcquirer renders the acquirer columns for a lock insert: both NULL
// when no verified caller was behind the acquire, else the subject and the
// operator groups as a JSON array — an empty array, never NULL, when the
// caller held none — so a recorded acquirer is always distinguishable from an
// unrecorded one.
func encodeLockAcquirer(lock *storage.Lock) (lockAcquirerColumns, error) {
	if lock.Acquirer == nil {
		return lockAcquirerColumns{}, nil
	}
	if lock.Acquirer.Subject == "" {
		return lockAcquirerColumns{}, fmt.Errorf("lock acquirer for %s/%s owner=%s names no subject",
			lock.DatabaseName, lock.DatabaseType, lock.Owner)
	}
	groups := lock.Acquirer.OperatorGroups
	if groups == nil {
		groups = []string{}
	}
	encoded, err := json.Marshal(groups)
	if err != nil {
		return lockAcquirerColumns{}, fmt.Errorf("marshal lock acquirer operator groups for %s/%s owner=%s: %w",
			lock.DatabaseName, lock.DatabaseType, lock.Owner, err)
	}
	return lockAcquirerColumns{subject: lock.Acquirer.Subject, operatorGroups: string(encoded)}, nil
}

// refreshPendingConfirmation overwrites the stored confirmation plan reference,
// and the disclosure record that describes it, when the caller supplied a new
// plan (an apply re-run posts a new confirmation plan). Both move in one
// statement so the flag can never describe a plan other than the one the confirm
// command will load. A caller supplying the same plan has nothing to change:
// DisclosedCopyDiscard is a property of that plan's comment, so re-deriving it
// cannot produce a different answer.
//
// The UPDATE is owner-scoped: it only changes the row while this owner still
// holds the lock. A conditional acquire also scopes it to the pending plan the
// caller observed, so the write itself, not only the read before it, refuses
// to replace an intent pinned in between.
//
// RowsAffected==0 is ambiguous and must not be read as "the owner predicate no
// longer matched". Under MySQL's default changed-rows semantics, a matched row
// reports zero affected rows when the stored value already equals the new value —
// which happens when a concurrent same-owner caller set the same pending_plan_id
// between this caller's read and its write. The owner still holds the lock in that
// case, so the refresh has succeeded. To distinguish that from a genuine ownership
// or intent change, re-read the lock and branch on its actual state.
// acquire canonicalizes the lock before reaching this helper.
func (s *lockStore) refreshPendingConfirmation(ctx context.Context, lock, existing *storage.Lock, observedPendingPlanID *string) error {
	if observedPendingPlanID != nil && existing.PendingPlanID != *observedPendingPlanID {
		return storage.ErrLockIntentChanged
	}
	if lock.PendingPlanID == "" || lock.PendingPlanID == existing.PendingPlanID {
		return nil
	}
	query := `
		UPDATE locks
		SET pending_plan_id = ?, disclosed_copy_discard = ?, updated_at = NOW()
		WHERE database_name = ? AND database_type = ? AND ` + s.dialect.BinaryEquals("owner")
	args := []any{lock.PendingPlanID, lock.DisclosedCopyDiscard, lock.DatabaseName, lock.DatabaseType, lock.Owner}
	if observedPendingPlanID != nil {
		query += ` AND pending_plan_id = ?`
		args = append(args, *observedPendingPlanID)
	}
	result, err := s.db.ExecContext(ctx, query, args...)
	if err != nil {
		return fmt.Errorf("refresh pending confirmation for %s/%s owner=%s: %w",
			lock.DatabaseName, lock.DatabaseType, lock.Owner, err)
	}
	rowsAffected, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("read rows affected refreshing pending confirmation for %s/%s owner=%s: %w",
			lock.DatabaseName, lock.DatabaseType, lock.Owner, err)
	}
	if rowsAffected > 0 {
		return nil
	}

	current, getErr := s.Get(ctx, lock.DatabaseName, lock.DatabaseType)
	if getErr != nil {
		return fmt.Errorf("read lock after pending confirmation refresh affected no rows for %s/%s owner=%s: %w",
			lock.DatabaseName, lock.DatabaseType, lock.Owner, getErr)
	}
	if current == nil {
		return storage.ErrLockNotFound
	}
	if current.Owner != lock.Owner {
		return storage.ErrLockHeld
	}
	if current.PendingPlanID == lock.PendingPlanID {
		// The caller still owns the lock; the UPDATE affected no rows only because
		// the stored value already matched, so the refresh is satisfied.
		return nil
	}
	if observedPendingPlanID != nil {
		// The owner predicate matched but the observed pin did not: a concurrent
		// same-owner command replaced the intent this caller planned against.
		return storage.ErrLockIntentChanged
	}
	return nil
}

// Release releases a lock. Only succeeds if caller is the owner.
func (s *lockStore) Release(ctx context.Context, database, dbType, owner string) error {
	database = storage.CanonicalKey(database)
	dbType = storage.CanonicalKey(dbType)
	result, err := s.db.ExecContext(ctx, `
		DELETE FROM locks
		WHERE database_name = ? AND database_type = ? AND `+s.dialect.BinaryEquals("owner")+`
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

// ReleaseByID deletes the lock only while the row the caller read still holds
// it for the pending plan the caller read. The row ID pins everything only the
// insert writes, the acquirer included; the pending plan pins the one thing a
// same-owner acquire rewrites in place. A caller that authorized against the
// row it read therefore cannot delete a lock acquired, or acquired again for a
// new plan, after that read. When nothing is deleted the lock is re-read, so
// the caller learns whether it is gone, held by a new row, held under another
// owner, or held for another plan.
func (s *lockStore) ReleaseByID(ctx context.Context, id int64, database, dbType, owner, pendingPlanID string) error {
	database = storage.CanonicalKey(database)
	dbType = storage.CanonicalKey(dbType)
	result, err := s.db.ExecContext(ctx, `
		DELETE FROM locks
		WHERE id = ? AND database_name = ? AND database_type = ? AND `+s.dialect.BinaryEquals("owner")+` AND `+s.dialect.BinaryEquals("pending_plan_id")+`
	`, id, database, dbType, owner, pendingPlanID)
	if err != nil {
		return fmt.Errorf("release lock row %d for %s/%s owner=%s: %w", id, database, dbType, owner, err)
	}
	rowsAffected, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("read rows affected releasing lock row %d for %s/%s owner=%s: %w",
			id, database, dbType, owner, err)
	}
	if rowsAffected > 0 {
		return nil
	}

	current, err := s.Get(ctx, database, dbType)
	if err != nil {
		return fmt.Errorf("read lock after release of row %d affected no rows for %s/%s owner=%s: %w",
			id, database, dbType, owner, err)
	}
	if current == nil {
		return storage.ErrLockNotFound
	}
	if current.ID != id {
		return storage.ErrLockReplaced
	}
	if current.Owner != owner {
		return storage.ErrLockNotOwned
	}
	return storage.ErrLockIntentChanged
}

// ReleaseIfPendingPlanID atomically releases only the lock intent the caller
// observed. Same-owner commands can replace pending_plan_id (for example, an
// apply lock can become a rollback lock), so owner-only release is insufficient
// after a network call or other long-running operation.
func (s *lockStore) ReleaseIfPendingPlanID(ctx context.Context, database, dbType, owner, pendingPlanID string) (bool, error) {
	database = storage.CanonicalKey(database)
	dbType = storage.CanonicalKey(dbType)
	result, err := s.db.ExecContext(ctx, `
		DELETE FROM locks
		WHERE database_name = ? AND database_type = ? AND `+s.dialect.BinaryEquals("owner")+` AND pending_plan_id = ?
	`, database, dbType, owner, pendingPlanID)
	if err != nil {
		return false, err
	}

	rowsAffected, err := result.RowsAffected()
	if err != nil {
		return false, err
	}
	return rowsAffected > 0, nil
}

// ForceRelease releases a lock regardless of owner (admin override).
func (s *lockStore) ForceRelease(ctx context.Context, database, dbType string) error {
	database = storage.CanonicalKey(database)
	dbType = storage.CanonicalKey(dbType)
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
	database = storage.CanonicalKey(database)
	dbType = storage.CanonicalKey(dbType)
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

// Update touches updated_at to mark lock liveness. The touch is owner-scoped,
// like every other lock mutator: a process whose lock was force-released and
// re-acquired by another owner must not keep the new owner's row looking
// alive, so it gets ErrLockNotOwned instead of a silent success.
//
// RowsAffected==0 is ambiguous and must not be read as "the lock does not
// exist". Under MySQL's default changed-rows semantics, a matched row reports
// zero affected rows when updated_at already equals NOW() — which happens when
// Update runs twice within the same one-second DATETIME tick. The lock still
// exists in that case, so the touch has succeeded. To distinguish that from a
// genuinely missing lock or an ownership change, re-read the row and return
// ErrLockNotFound when it is gone or ErrLockNotOwned when another owner holds
// it.
func (s *lockStore) Update(ctx context.Context, lock *storage.Lock) error {
	canonicalizeLock(lock)
	result, err := s.db.ExecContext(ctx, `
		UPDATE locks
		SET updated_at = NOW()
		WHERE database_name = ? AND database_type = ? AND `+s.dialect.BinaryEquals("owner")+`
	`, lock.DatabaseName, lock.DatabaseType, lock.Owner)
	if err != nil {
		return fmt.Errorf("touch lock for %s/%s: %w", lock.DatabaseName, lock.DatabaseType, err)
	}
	rowsAffected, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("read rows affected touching lock for %s/%s: %w",
			lock.DatabaseName, lock.DatabaseType, err)
	}
	if rowsAffected > 0 {
		return nil
	}

	current, getErr := s.Get(ctx, lock.DatabaseName, lock.DatabaseType)
	if getErr != nil {
		return fmt.Errorf("read lock after touch affected no rows for %s/%s: %w",
			lock.DatabaseName, lock.DatabaseType, getErr)
	}
	if current == nil {
		return storage.ErrLockNotFound
	}
	if current.Owner != lock.Owner {
		return storage.ErrLockNotOwned
	}
	return nil
}

// GetByPR returns all locks associated with a PR.
func (s *lockStore) GetByPR(ctx context.Context, repo string, pr int) ([]*storage.Lock, error) {
	repo = storage.CanonicalKey(repo)
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
	var acquiredBy sql.NullString
	var acquiredByGroups []byte

	err := s.Scan(
		&lock.ID, &lock.DatabaseName, &lock.DatabaseType,
		&lock.Repository, &lock.PullRequest, &lock.Owner,
		&lock.PendingPlanID, &lock.DisclosedCopyDiscard,
		&acquiredBy, &acquiredByGroups,
		&lock.CreatedAt, &lock.UpdatedAt,
	)
	if err != nil {
		return nil, err
	}

	acquirer, err := lockAcquirerFromColumns(&lock, acquiredBy, acquiredByGroups)
	if err != nil {
		return nil, err
	}
	lock.Acquirer = acquirer
	return &lock, nil
}

// lockAcquirerFromColumns rebuilds the acquirer from its stored columns. A row
// carrying operator groups but no subject was not written by
// encodeLockAcquirer, so it is reported rather than read as either answer.
func lockAcquirerFromColumns(lock *storage.Lock, acquiredBy sql.NullString, acquiredByGroups []byte) (*storage.LockAcquirer, error) {
	if !acquiredBy.Valid {
		if len(acquiredByGroups) > 0 {
			return nil, fmt.Errorf("lock %s/%s owner=%s records acquirer operator groups without an acquirer",
				lock.DatabaseName, lock.DatabaseType, lock.Owner)
		}
		return nil, nil
	}
	acquirer := &storage.LockAcquirer{Subject: acquiredBy.String}
	if len(acquiredByGroups) > 0 {
		if err := json.Unmarshal(acquiredByGroups, &acquirer.OperatorGroups); err != nil {
			return nil, fmt.Errorf("unmarshal acquired_by_operator_groups for lock %s/%s owner=%s: %w",
				lock.DatabaseName, lock.DatabaseType, lock.Owner, err)
		}
	}
	return acquirer, nil
}
