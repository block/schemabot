// sql_helpers.go provides shared utilities for MySQL store implementations.
package mysqlstore

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log/slog"

	"github.com/block/schemabot/pkg/storage"
)

// applyLeaseStalenessSQL is the SQL INTERVAL after which a claimed applies or
// apply_operations row's lease heartbeat (updated_at) is stale and the row may
// be re-claimed by another driver. Derived from storage.ApplyLeaseStaleAfter so
// the reclaim window and the driver-side presumed-lost bound cannot drift
// apart; microsecond units keep the two windows exactly equal for any
// duration, so the reclaim window can never round below the presumed-lost
// bound.
var applyLeaseStalenessSQL = fmt.Sprintf("%d MICROSECOND", storage.ApplyLeaseStaleAfter.Microseconds())

// rollbackTx rolls back tx, logging a warning if the rollback fails for a
// reason other than the transaction already being finished. operation is
// included in the log to identify the originating call site.
func rollbackTx(ctx context.Context, tx *sql.Tx, operation string) {
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

// nullInt64Ptr returns a sql.NullInt64 for a *int64 value.
func nullInt64Ptr(v *int64) sql.NullInt64 {
	if v == nil {
		return sql.NullInt64{}
	}
	return sql.NullInt64{Int64: *v, Valid: true}
}

// nullJSON returns valid JSON from []byte, defaulting to "{}" if nil/empty.
func nullJSON(b []byte) string {
	if len(b) == 0 {
		return "{}"
	}
	return string(b)
}

// checkRowsAffected checks that at least one row was affected by the result.
// Returns notFoundErr if no rows were affected.
func checkRowsAffected(result sql.Result, notFoundErr error) error {
	rowsAffected, _ := result.RowsAffected()
	if rowsAffected == 0 {
		return notFoundErr
	}
	return nil
}
