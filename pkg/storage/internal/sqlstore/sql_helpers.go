// sql_helpers.go provides shared utilities for MySQL store implementations.
package sqlstore

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log/slog"
	"time"
)

// rollbackTx rolls back tx, logging a warning if the rollback fails for a
// reason other than the transaction already being finished. operation is
// included in the log to identify the originating call site.
func rollbackTx(ctx context.Context, tx *rebindTx, operation string) {
	if err := tx.Rollback(); err != nil && !errors.Is(err, sql.ErrTxDone) {
		slog.WarnContext(ctx, "failed to roll back transaction", "operation", operation, "error", err)
	}
}

// scanner is implemented by both *sql.Row and *sql.Rows.
// Used by scan helpers to work with both single-row and multi-row queries.
type scanner interface {
	Scan(dest ...any) error
}

// nullString returns a sql.NullString for empty strings.
func nullString(s string) sql.NullString {
	if s == "" {
		return sql.NullString{}
	}
	return sql.NullString{String: s, Valid: true}
}

// nullInt64 returns a sql.NullInt64 for a row reference held as an int64, where
// zero means "no reference" and must be stored as NULL rather than as a row ID
// of 0 that no row can have.
func nullInt64(v int64) sql.NullInt64 {
	if v == 0 {
		return sql.NullInt64{}
	}
	return sql.NullInt64{Int64: v, Valid: true}
}

// nullInt64Ptr returns a sql.NullInt64 for a *int64 value.
func nullInt64Ptr(v *int64) sql.NullInt64 {
	if v == nil {
		return sql.NullInt64{}
	}
	return sql.NullInt64{Int64: *v, Valid: true}
}

// nullTimePtr returns a sql.NullTime for a *time.Time value.
func nullTimePtr(v *time.Time) sql.NullTime {
	if v == nil {
		return sql.NullTime{}
	}
	return sql.NullTime{Time: *v, Valid: true}
}

// nullJSON returns valid JSON from []byte, defaulting to "{}" if nil/empty.
func nullJSON(b []byte) string {
	if len(b) == 0 {
		return "{}"
	}
	return string(b)
}

// requestRematchGuardSQL renders the guard a request-gated claim needs when the
// claim does not move the row out of the state it matched on.
//
// A claim keyed on "state X and a pending control request" normally terminates
// itself: the claim transitions the row out of X, or it settles the request, and
// either way the predicate stops matching. A claim that leaves both in place
// keeps matching, so every poll re-claims the same row and occupies a driver
// that has nothing new to do. The guard admits such a claim only when the
// request is newer than the row's last lease rotation, or when the row's own
// heartbeat has gone stale — one attempt per request, then one per staleness
// window until the request settles.
//
// cmp decides whether a request whose stored timestamp equals the lease's is
// admitted, which matters because these columns are second-precision on some
// dialects: a command issued in the same second as the last lease rotation is
// indistinguishable from one the lease holder is already driving. Pick it per
// arm from leaseRematchComparison — the choice is a safety property, not a
// stylistic one.
//
// rowAlias is the table or alias holding lease_acquired_at and updated_at; the
// guard is written for embedding inside an EXISTS over apply_control_requests
// aliased cr, and staleCutoff is the dialect's rendered lease-staleness cutoff.
func requestRematchGuardSQL(rowAlias string, cmp leaseRematchComparison, staleCutoff string) string {
	return fmt.Sprintf(`(
							%[1]s.lease_acquired_at IS NULL
							OR %[1]s.lease_acquired_at %[2]s cr.updated_at
							OR %[1]s.updated_at < %[3]s
						)`, rowAlias, cmp, staleCutoff)
}

// leaseRematchComparison is the lease-vs-request comparison a rematch guard
// uses. Which one an arm needs follows from whether anything downstream of the
// claim makes it exclusive.
type leaseRematchComparison string

const (
	// rematchOnlyAfterLease refuses an equal timestamp. Use it when the claim
	// leaves the row in the state it matched on and nothing else prevents a
	// second driver from matching too: admitting the equal case would let a peer
	// polling in the same second rotate the lease out from under an in-flight
	// drive. The cost is that a request issued in that same second waits out one
	// staleness window before its first attempt.
	rematchOnlyAfterLease leaseRematchComparison = "<"

	// rematchAtOrAfterLease admits an equal timestamp. Use it only where a
	// downstream exclusive claim already rules out a same-second peer — the
	// operation-level claim moves the row stopped → resuming atomically with its
	// lease rotation, so the arm stops matching for everyone else. Without it, an
	// operator who stops an apply and immediately cancels it would wait out the
	// staleness window before the cancel is even attempted.
	rematchAtOrAfterLease leaseRematchComparison = "<="
)

// checkRowsAffected checks that at least one row was affected by the result.
// Returns notFoundErr if no rows were affected.
func checkRowsAffected(result sql.Result, notFoundErr error) error {
	rowsAffected, _ := result.RowsAffected()
	if rowsAffected == 0 {
		return notFoundErr
	}
	return nil
}
