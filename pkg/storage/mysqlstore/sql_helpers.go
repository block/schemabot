// sql_helpers.go provides shared utilities for MySQL store implementations.
package mysqlstore

import "database/sql"

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
