package serve

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/block/schemabot/pkg/schema"
)

// The target a DSN names is the server and the database on it, and nothing
// else: two DSNs that differ only in their credentials name the same database,
// because that is what a rotation does.
func TestStorageTargetForIgnoresCredentials(t *testing.T) {
	tests := []struct {
		name     string
		dialect  schema.Dialect
		first    string
		second   string
		want     storageTarget
		wantSame bool
	}{
		{
			name:     "mysql password rotated",
			dialect:  schema.DialectMySQL,
			first:    "schemabot:old@tcp(db.example:3306)/schemabot",
			second:   "schemabot:new@tcp(db.example:3306)/schemabot",
			want:     storageTarget{address: "db.example:3306", database: "schemabot"},
			wantSame: true,
		},
		{
			name:     "mysql user changed",
			dialect:  schema.DialectMySQL,
			first:    "schemabot:pw@tcp(db.example:3306)/schemabot",
			second:   "schemabot_rw:pw@tcp(db.example:3306)/schemabot",
			want:     storageTarget{address: "db.example:3306", database: "schemabot"},
			wantSame: true,
		},
		{
			name:     "postgres password rotated",
			dialect:  schema.DialectPostgres,
			first:    "postgres://schemabot:old@db.example:5432/schemabot",
			second:   "postgres://schemabot:new@db.example:5432/schemabot",
			want:     storageTarget{address: "db.example:5432", database: "schemabot"},
			wantSame: true,
		},
		{
			name:     "postgres keyword form names the same target as the URL form",
			dialect:  schema.DialectPostgres,
			first:    "postgres://schemabot@db.example:5432/schemabot",
			second:   "host=db.example port=5432 user=schemabot dbname=schemabot",
			want:     storageTarget{address: "db.example:5432", database: "schemabot"},
			wantSame: true,
		},
		{
			name:     "mysql database renamed",
			dialect:  schema.DialectMySQL,
			first:    "schemabot:pw@tcp(db.example:3306)/schemabot",
			second:   "schemabot:pw@tcp(db.example:3306)/schemabot_staging",
			want:     storageTarget{address: "db.example:3306", database: "schemabot"},
			wantSame: false,
		},
		{
			name:     "postgres server moved",
			dialect:  schema.DialectPostgres,
			first:    "postgres://schemabot@db.example:5432/schemabot",
			second:   "postgres://schemabot@other.example:5432/schemabot",
			want:     storageTarget{address: "db.example:5432", database: "schemabot"},
			wantSame: false,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			first, err := storageTargetFor(tc.dialect, tc.first)
			require.NoError(t, err)
			assert.Equal(t, tc.want, first)

			second, err := storageTargetFor(tc.dialect, tc.second)
			require.NoError(t, err)
			assert.Equal(t, tc.wantSame, first == second)
		})
	}
}

// A dialect with no DSN grammar of ours fails closed rather than being parsed
// by whichever driver happens to accept the string.
func TestStorageTargetForRefusesAnUnknownDialect(t *testing.T) {
	_, err := storageTargetFor(schema.Dialect("cockroach"), "postgres://schemabot@db.example:5432/schemabot")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "no storage DSN parser")
}

// An unparseable DSN is an error naming the family it was being read as, not a
// zero target that would compare equal to another zero target and let a request
// through.
func TestStorageTargetForRefusesAnUnparseableDSN(t *testing.T) {
	_, err := storageTargetFor(schema.DialectMySQL, "not a dsn")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "parse MySQL storage DSN")
}
