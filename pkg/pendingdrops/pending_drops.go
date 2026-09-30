// Package pendingdrops implements the pending drops quarantine for dropped
// MySQL tables.
//
// Instead of executing DROP TABLE, the Spirit engine renames the table into a
// _pending_drops database with a timestamp prefix. The table keeps its data and
// can be recovered with a manual RENAME TABLE until the cleaner removes it
// after the retention period.
package pendingdrops

import (
	"crypto/sha256"
	"encoding/hex"
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

	// disambiguatorHashLen is the number of hex characters of the source
	// table's hash appended to a quarantine name that would otherwise be
	// truncated or collide with another name in the same move.
	disambiguatorHashLen = 8
)

// TableName returns the quarantine table name for a dropped table, capped at
// MySQL's 64-character limit. The timestamp prefix is UTC and records when the
// table was quarantined so the cleaner can compute its age from the name alone.
// ParseTimestamp reads the prefix back as UTC, so both sides agree on the
// instant regardless of the server's local time zone.
//
// A table name too long to fit after the prefix is shortened and ends in a
// hash of the source schema and full table name, so two long names that share
// their leading characters still get different quarantine names.
func TableName(schemaName, tableName string, now time.Time) string {
	prefix := timestampPrefix(now)
	if len(prefix)+len(tableName) > tableNameLengthLimit {
		return disambiguatedTableName(prefix, schemaName, tableName)
	}
	return prefix + tableName
}

// timestampPrefix is the "YYYYMMDDHHmmSSmmm_" prefix for now, in UTC.
func timestampPrefix(now time.Time) string {
	now = now.UTC()
	return fmt.Sprintf("%s%03d_", now.Format(timestampBaseFormat), now.Nanosecond()/int(time.Millisecond))
}

// disambiguatedTableName returns prefix, then as much of tableName as fits,
// then "_" and a short hash of the source schema and full table name, capped
// at MySQL's 64-character limit. The hash depends only on the source table, so
// the same table and prefix always produce the same name, and the name still
// starts with the timestamp prefix ParseTimestamp reads.
func disambiguatedTableName(prefix, schemaName, tableName string) string {
	sum := sha256.Sum256([]byte(schemaName + "\x00" + tableName))
	suffix := "_" + hex.EncodeToString(sum[:])[:disambiguatorHashLen]
	maxTableLen := tableNameLengthLimit - len(prefix) - len(suffix)
	if len(tableName) > maxTableLen {
		tableName = tableName[:maxTableLen]
	}
	return prefix + tableName + suffix
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
