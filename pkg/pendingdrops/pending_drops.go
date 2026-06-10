// Package pendingdrops implements the pending drops quarantine for dropped
// MySQL tables.
//
// Instead of executing DROP TABLE, the Spirit engine renames the table into a
// _pending_drops database with a timestamp prefix. The table keeps its data and
// can be recovered with a manual RENAME TABLE until the cleaner removes it
// after the retention period.
package pendingdrops

import (
	"fmt"
	"strconv"
	"time"
)

const (
	// Database is the MySQL database (schema) that holds quarantined tables.
	Database = "_pending_drops"

	// DefaultRetention is how long quarantined tables are kept before the
	// cleaner drops them permanently.
	DefaultRetention = 7 * 24 * time.Hour

	// tableNameLengthLimit is MySQL's maximum table name length.
	tableNameLengthLimit = 64

	// timestampLen is the length of the timestamp prefix in quarantined table
	// names: "YYYYMMDDHHmmSSmmm_" = 18 characters.
	timestampLen = 18

	// timestampBaseFormat is the Go time format for the date+time portion of
	// the prefix (14 digits: YYYYMMDDHHmmSS). The trailing 3 digits are
	// milliseconds, parsed separately since Go's time.Parse cannot parse
	// concatenated millisecond digits without a separator.
	timestampBaseFormat = "20060102150405"
)

// TableName returns the quarantine table name for a dropped table, capped at
// MySQL's 64-character limit. The timestamp prefix is UTC and records when the
// table was quarantined so the cleaner can compute its age from the name alone.
// ParseTimestamp reads the prefix back as UTC, so both sides agree on the
// instant regardless of the server's local time zone.
func TableName(_ string, tableName string, now time.Time) string {
	now = now.UTC()
	prefix := fmt.Sprintf("%s%03d_", now.Format(timestampBaseFormat), now.Nanosecond()/int(time.Millisecond))
	maxTableLen := tableNameLengthLimit - len(prefix)
	if len(tableName) > maxTableLen {
		tableName = tableName[:maxTableLen]
	}
	return prefix + tableName
}

// ParseTimestamp extracts the quarantine time from a table name produced by
// TableName. It returns false for names that do not carry a valid timestamp
// prefix; the cleaner must never drop those tables because their age is
// unknown.
func ParseTimestamp(name string) (time.Time, bool) {
	if len(name) < timestampLen {
		return time.Time{}, false
	}
	if name[timestampLen-1] != '_' {
		return time.Time{}, false
	}
	t, err := time.Parse(timestampBaseFormat, name[:14])
	if err != nil {
		return time.Time{}, false
	}
	ms, err := strconv.Atoi(name[14 : timestampLen-1])
	if err != nil || ms < 0 || ms > 999 {
		return time.Time{}, false
	}
	return t.Add(time.Duration(ms) * time.Millisecond), true
}
