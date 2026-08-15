package schema

import (
	"database/sql"
	"regexp"
	"strings"
	"testing"

	"github.com/block/spirit/pkg/statement"
	"github.com/stretchr/testify/assert"
)

// columnDefault is a dialect-normalized column default: present is false when
// the column has no default (or an explicit NULL default, which is
// equivalent), and value holds the canonical form used for cross-dialect
// comparison.
type columnDefault struct {
	value   string
	present bool
}

// postgresDefaultCast matches a trailing PostgreSQL cast on a column_default
// expression (e.g. ::character varying, ::bigint). The type name is matched
// generically so a quoted literal of any castable type is unquoted rather
// than false-failing on an unlisted cast; quotes already protect string
// contents from a false match.
var postgresDefaultCast = regexp.MustCompile(`(?i)::[a-z_][a-z_ ]*$`)

// isTinyintBool reports whether the mysql column is a tinyint(1), which the
// postgres translation maps to boolean.
func isTinyintBool(col statement.Column) bool {
	return col.Type == "tinyint" && col.Length != nil && *col.Length == 1
}

// isStringColumn reports whether the mysql column holds string values, so a
// quoted default is a genuine string literal rather than MySQL's canonical
// quoting of a typed value (SHOW CREATE TABLE quotes numeric defaults too,
// e.g. bigint DEFAULT '0').
func isStringColumn(col statement.Column) bool {
	switch col.Type {
	case "varchar", "text":
		return true
	}
	return false
}

// normalizedMySQLDefault canonicalizes the default of a TiDB-parsed mysql
// column. A quoted literal on a string-typed column is compared verbatim:
// trimming or keyword-folding it would conflate genuinely different defaults
// such as 'NULL' vs a dropped default, or a blank ' ' vs an empty string. On
// non-string columns the quotes are only MySQL's canonical spelling of a
// typed value, so the value is trimmed and folded normally.
func normalizedMySQLDefault(col statement.Column) columnDefault {
	if col.Default == nil {
		return columnDefault{}
	}

	value := *col.Default
	if !col.DefaultIsString || !isStringColumn(col) {
		value = strings.TrimSpace(value)
		if strings.EqualFold(value, "NULL") {
			return columnDefault{}
		}
		if isTinyintBool(col) {
			switch value {
			case "0":
				value = "false"
			case "1":
				value = "true"
			}
		}
	}
	return columnDefault{value: normalizeDefaultValue(col, value), present: true}
}

// normalizedPostgresDefault canonicalizes an information_schema
// column_default read back from PostgreSQL: trailing casts are stripped, a
// bare NULL default folds to absent, and quoted literals are unquoted with
// ” unescaped to '. The order matters: the NULL fold runs before unquoting
// so a quoted 'NULL' literal stays a present string value.
func normalizedPostgresDefault(col statement.Column, value sql.NullString) columnDefault {
	if !value.Valid {
		return columnDefault{}
	}

	normalized := strings.TrimSpace(value.String)
	for postgresDefaultCast.MatchString(normalized) {
		normalized = postgresDefaultCast.ReplaceAllString(normalized, "")
	}
	if strings.EqualFold(normalized, "NULL") {
		return columnDefault{}
	}
	if len(normalized) >= 2 && normalized[0] == '\'' && normalized[len(normalized)-1] == '\'' {
		normalized = strings.ReplaceAll(normalized[1:len(normalized)-1], "''", "'")
	}
	return columnDefault{value: normalizeDefaultValue(col, normalized), present: true}
}

// normalizeDefaultValue folds dialect-specific spellings of the same default
// into one canonical form. Folding is gated on the column type so
// string-typed defaults that merely look like keywords (e.g. a varchar
// DEFAULT 'now()' or 'TRUE') compare verbatim.
func normalizeDefaultValue(col statement.Column, value string) string {
	switch {
	case col.Type == "datetime":
		switch strings.ToLower(value) {
		case "current_timestamp", "current_timestamp()", "now()":
			return "current_timestamp"
		}
	case isTinyintBool(col):
		switch lower := strings.ToLower(value); lower {
		case "true", "false":
			return lower
		}
	}
	return value
}

func TestNormalizedMySQLDefault(t *testing.T) {
	strPtr := func(s string) *string { return &s }
	intPtr := func(i int) *int { return &i }

	tests := []struct {
		name string
		col  statement.Column
		want columnDefault
	}{
		{
			name: "no default is absent",
			col:  statement.Column{Type: "varchar", Length: intPtr(64)},
			want: columnDefault{},
		},
		{
			name: "keyword NULL default is absent",
			col:  statement.Column{Type: "varchar", Length: intPtr(64), Default: strPtr("NULL")},
			want: columnDefault{},
		},
		{
			name: "string literal 'NULL' stays a present value",
			col:  statement.Column{Type: "varchar", Length: intPtr(64), Default: strPtr("NULL"), DefaultIsString: true},
			want: columnDefault{value: "NULL", present: true},
		},
		{
			name: "string literal whitespace is preserved",
			col:  statement.Column{Type: "varchar", Length: intPtr(64), Default: strPtr(" "), DefaultIsString: true},
			want: columnDefault{value: " ", present: true},
		},
		{
			name: "non-string default is trimmed",
			col:  statement.Column{Type: "int", Default: strPtr(" 0 ")},
			want: columnDefault{value: "0", present: true},
		},
		{
			name: "tinyint(1) 0 folds to false",
			col:  statement.Column{Type: "tinyint", Length: intPtr(1), Default: strPtr("0")},
			want: columnDefault{value: "false", present: true},
		},
		{
			name: "tinyint(1) quoted '0' folds to false",
			col:  statement.Column{Type: "tinyint", Length: intPtr(1), Default: strPtr("0"), DefaultIsString: true},
			want: columnDefault{value: "false", present: true},
		},
		{
			name: "bigint quoted '0' is a typed value not a string",
			col:  statement.Column{Type: "bigint", Default: strPtr("0"), DefaultIsString: true},
			want: columnDefault{value: "0", present: true},
		},
		{
			name: "tinyint(1) 1 folds to true",
			col:  statement.Column{Type: "tinyint", Length: intPtr(1), Default: strPtr("1")},
			want: columnDefault{value: "true", present: true},
		},
		{
			name: "datetime current_timestamp() folds",
			col:  statement.Column{Type: "datetime", Default: strPtr("CURRENT_TIMESTAMP()")},
			want: columnDefault{value: "current_timestamp", present: true},
		},
		{
			name: "varchar 'now()' literal does not fold",
			col:  statement.Column{Type: "varchar", Length: intPtr(64), Default: strPtr("now()"), DefaultIsString: true},
			want: columnDefault{value: "now()", present: true},
		},
		{
			name: "varchar 'TRUE' literal does not fold",
			col:  statement.Column{Type: "varchar", Length: intPtr(64), Default: strPtr("TRUE"), DefaultIsString: true},
			want: columnDefault{value: "TRUE", present: true},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, normalizedMySQLDefault(tt.col))
		})
	}
}

func TestNormalizedPostgresDefault(t *testing.T) {
	intPtr := func(i int) *int { return &i }
	varcharCol := statement.Column{Type: "varchar", Length: intPtr(64)}

	tests := []struct {
		name  string
		col   statement.Column
		value sql.NullString
		want  columnDefault
	}{
		{
			name:  "sql NULL is absent",
			col:   varcharCol,
			value: sql.NullString{},
			want:  columnDefault{},
		},
		{
			name:  "bare NULL folds to absent",
			col:   varcharCol,
			value: sql.NullString{String: "NULL", Valid: true},
			want:  columnDefault{},
		},
		{
			name:  "cast NULL folds to absent",
			col:   varcharCol,
			value: sql.NullString{String: "NULL::character varying", Valid: true},
			want:  columnDefault{},
		},
		{
			name:  "quoted 'NULL' literal stays a present value",
			col:   varcharCol,
			value: sql.NullString{String: "'NULL'::character varying", Valid: true},
			want:  columnDefault{value: "NULL", present: true},
		},
		{
			name:  "doubled quotes unescape",
			col:   statement.Column{Type: "text"},
			value: sql.NullString{String: "'It''s'::text", Valid: true},
			want:  columnDefault{value: "It's", present: true},
		},
		{
			name:  "chained casts strip iteratively",
			col:   statement.Column{Type: "text"},
			value: sql.NullString{String: "'x'::character varying::text", Valid: true},
			want:  columnDefault{value: "x", present: true},
		},
		{
			name:  "unlisted cast type strips generically",
			col:   statement.Column{Type: "int"},
			value: sql.NullString{String: "'0'::smallint", Valid: true},
			want:  columnDefault{value: "0", present: true},
		},
		{
			name:  "quoted whitespace literal is preserved",
			col:   varcharCol,
			value: sql.NullString{String: "' '::character varying", Valid: true},
			want:  columnDefault{value: " ", present: true},
		},
		{
			name:  "datetime now() folds to current_timestamp",
			col:   statement.Column{Type: "datetime"},
			value: sql.NullString{String: "now()", Valid: true},
			want:  columnDefault{value: "current_timestamp", present: true},
		},
		{
			name:  "datetime CURRENT_TIMESTAMP folds",
			col:   statement.Column{Type: "datetime"},
			value: sql.NullString{String: "CURRENT_TIMESTAMP", Valid: true},
			want:  columnDefault{value: "current_timestamp", present: true},
		},
		{
			name:  "boolean true stays true for tinyint(1)",
			col:   statement.Column{Type: "tinyint", Length: intPtr(1)},
			value: sql.NullString{String: "true", Valid: true},
			want:  columnDefault{value: "true", present: true},
		},
		{
			name:  "varchar 'now()' literal does not fold",
			col:   varcharCol,
			value: sql.NullString{String: "'now()'::character varying", Valid: true},
			want:  columnDefault{value: "now()", present: true},
		},
		{
			name:  "plain numeric default passes through",
			col:   statement.Column{Type: "bigint"},
			value: sql.NullString{String: "42", Valid: true},
			want:  columnDefault{value: "42", present: true},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, normalizedPostgresDefault(tt.col, tt.value))
		})
	}
}
