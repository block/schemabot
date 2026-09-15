package serve

import (
	"log/slog"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/block/schemabot/pkg/api"
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

// The pool's reload callback exists so a rotated credential is picked up
// without a restart. It must not also pick up a different database: the schema
// this server bootstrapped is on the one it booted against, and nothing
// bootstraps the new one, so a reload that followed the move would leave the
// server running against storage it never converged.
func TestPinnedStorageDSN(t *testing.T) {
	const booted = "schemabot:hunter2@tcp(db-1.example:3306)/schemabot"

	t.Run("a rotated credential is adopted", func(t *testing.T) {
		cfg := &api.ServerConfig{}
		cfg.Storage.DSN = "schemabot:rotated@tcp(db-1.example:3306)/schemabot"
		reload := pinnedStorageDSN(schema.DialectMySQL, booted, cfg, slog.New(slog.DiscardHandler))

		next, err := reload()
		require.NoError(t, err)
		assert.Equal(t, cfg.Storage.DSN, next)
	})

	t.Run("a moved database is refused", func(t *testing.T) {
		cfg := &api.ServerConfig{}
		cfg.Storage.DSN = "schemabot:hunter2@tcp(db-2.example:3306)/schemabot"
		reload := pinnedStorageDSN(schema.DialectMySQL, booted, cfg, slog.New(slog.DiscardHandler))

		_, err := reload()
		require.Error(t, err)
		assert.Contains(t, err.Error(), "db-2.example:3306/schemabot")
		assert.Contains(t, err.Error(), "db-1.example:3306/schemabot")
		assert.Contains(t, err.Error(), "restart it to adopt the new storage")
	})

	t.Run("a renamed database on the same server is refused", func(t *testing.T) {
		cfg := &api.ServerConfig{}
		cfg.Storage.DSN = "schemabot:hunter2@tcp(db-1.example:3306)/schemabot_next"
		reload := pinnedStorageDSN(schema.DialectMySQL, booted, cfg, slog.New(slog.DiscardHandler))

		_, err := reload()
		require.Error(t, err)
		assert.Contains(t, err.Error(), "schemabot_next")
	})

	t.Run("an unreadable boot DSN never resolves to a pass", func(t *testing.T) {
		cfg := &api.ServerConfig{}
		cfg.Storage.DSN = booted
		reload := pinnedStorageDSN(schema.DialectMySQL, "not a dsn", cfg, slog.New(slog.DiscardHandler))

		_, err := reload()
		require.Error(t, err)
		assert.Contains(t, err.Error(), "booted against")
	})
}
