package api

import (
	"io"
	"log/slog"
	"maps"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/block/schemabot/pkg/storage"
)

func newPendingDropsTestService(t *testing.T, databases map[string]DatabaseConfig) *Service {
	t.Helper()
	return New(nil, &ServerConfig{Databases: databases},
		nil, slog.New(slog.NewTextHandler(io.Discard, nil)))
}

// The pending-drops quarantine is a MySQL-only mechanism: the cleaner connects
// with the Go MySQL driver and issues MySQL-specific discovery, locking, and
// DROP TABLE statements. Target resolution must therefore offer the cleaner
// only mysql-typed databases with a local DSN — a target of any other database
// family, or one executed by a remote deployment, must never reach the
// cleaner's connection path.
func TestPendingDropsTargetsIncludeOnlyLocalMySQLDatabases(t *testing.T) {
	t.Parallel()
	svc := newPendingDropsTestService(t, map[string]DatabaseConfig{
		"mysql_local": {
			Type: storage.DatabaseTypeMySQL,
			Environments: map[string]EnvironmentConfig{
				"staging": {DSN: "user:pass@tcp(127.0.0.1:3306)/mysql_local"},
			},
		},
		"mysql_remote": {
			Type: storage.DatabaseTypeMySQL,
			Environments: map[string]EnvironmentConfig{
				"staging": {},
			},
		},
		"postgres_local": {
			Type: storage.DatabaseTypePostgres,
			Environments: map[string]EnvironmentConfig{
				"staging": {DSN: "postgres://user:pass@127.0.0.1:5432/postgres_local"},
			},
		},
		"vitess_local": {
			Type: storage.DatabaseTypeVitess,
			Environments: map[string]EnvironmentConfig{
				"staging": {DSN: "user:pass@tcp(127.0.0.1:3306)/vitess_local"},
			},
		},
		"strata_local": {
			Type: storage.DatabaseTypeStrata,
			Environments: map[string]EnvironmentConfig{
				"staging": {DSN: "user:pass@tcp(127.0.0.1:3306)/strata_local"},
			},
		},
	})

	targets, unresolved := svc.pendingDropsTargets(t.Context())

	require.Len(t, targets, 1,
		"only the mysql-typed database with a local DSN should be a cleanup target")
	assert.Equal(t, "mysql_local", targets[0].Database)
	assert.Equal(t, "staging", targets[0].Environment)
	assert.Zero(t, unresolved,
		"non-MySQL databases must be skipped by policy, not counted as resolution failures")
}

// hasPendingDropsLocalTargets gates whether the cleaner loop starts at all: a
// deployment whose local databases are all non-MySQL has no quarantine to
// clean, so the loop must not start for them.
func TestHasPendingDropsLocalTargetsIgnoresNonMySQLDatabases(t *testing.T) {
	t.Parallel()
	nonMySQL := map[string]DatabaseConfig{
		"postgres_local": {
			Type: storage.DatabaseTypePostgres,
			Environments: map[string]EnvironmentConfig{
				"staging": {DSN: "postgres://user:pass@127.0.0.1:5432/postgres_local"},
			},
		},
		"vitess_local": {
			Type: storage.DatabaseTypeVitess,
			Environments: map[string]EnvironmentConfig{
				"staging": {DSN: "user:pass@tcp(127.0.0.1:3306)/vitess_local"},
			},
		},
	}
	assert.False(t, newPendingDropsTestService(t, nonMySQL).hasPendingDropsLocalTargets(),
		"non-MySQL local databases must not enable the pending drops cleaner")

	withMySQL := maps.Clone(nonMySQL)
	withMySQL["mysql_local"] = DatabaseConfig{
		Type: storage.DatabaseTypeMySQL,
		Environments: map[string]EnvironmentConfig{
			"staging": {DSN: "user:pass@tcp(127.0.0.1:3306)/mysql_local"},
		},
	}
	assert.True(t, newPendingDropsTestService(t, withMySQL).hasPendingDropsLocalTargets(),
		"a mysql-typed local database enables the pending drops cleaner")
}
