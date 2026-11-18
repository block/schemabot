package api

import (
	"bytes"
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

// The local count gates whether the cleaner loop starts at all, and the routed
// count tells an operator whether a process with no local targets is a control
// plane whose deployments reap their own targets. Only MySQL databases are
// counted: no other family has a quarantine to reap.
func TestPendingDropsTargetCountsIgnoresNonMySQLDatabases(t *testing.T) {
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
	local, routed := newPendingDropsTestService(t, nonMySQL).pendingDropsTargetCounts()
	assert.Zero(t, local, "non-MySQL local databases must not enable the pending drops cleaner")
	assert.Zero(t, routed, "non-MySQL databases must not be counted as routed MySQL targets")

	withMySQL := maps.Clone(nonMySQL)
	withMySQL["mysql_local"] = DatabaseConfig{
		Type: storage.DatabaseTypeMySQL,
		Environments: map[string]EnvironmentConfig{
			"staging": {DSN: "user:pass@tcp(127.0.0.1:3306)/mysql_local"},
		},
	}
	local, routed = newPendingDropsTestService(t, withMySQL).pendingDropsTargetCounts()
	assert.Equal(t, 1, local, "a mysql-typed local database enables the pending drops cleaner")
	assert.Zero(t, routed)
}

// A control plane routes every MySQL target to the deployment that executes
// against it, so it has no local target to reap and its routed count is what
// tells an operator the targets are covered elsewhere.
func TestPendingDropsTargetCountsSeparatesRoutedTargets(t *testing.T) {
	t.Parallel()
	routed := map[string]DatabaseConfig{
		"mysql_routed": {
			Type: storage.DatabaseTypeMySQL,
			Environments: map[string]EnvironmentConfig{
				"staging":    {Target: "shard-1", Deployment: "executor"},
				"production": {Target: "shard-2", Deployment: "executor"},
			},
		},
		"mysql_local": {
			Type: storage.DatabaseTypeMySQL,
			Environments: map[string]EnvironmentConfig{
				"staging": {DSN: "user:pass@tcp(127.0.0.1:3306)/mysql_local"},
			},
		},
	}
	local, routedCount := newPendingDropsTestService(t, routed).pendingDropsTargetCounts()
	assert.Equal(t, 1, local, "only the database with a local DSN is reapable from this process")
	assert.Equal(t, 2, routedCount, "each routed environment is reaped by the deployment that executes it")
}

// The routed count is of execution targets, not environments: a
// multi-deployment environment is executed against once per deployment, and
// each of those targets has to be reaped by the deployment that executes it.
func TestPendingDropsTargetCountsCountExecutionTargets(t *testing.T) {
	t.Parallel()
	databases := map[string]DatabaseConfig{
		"mysql_multi": {
			Type: storage.DatabaseTypeMySQL,
			Environments: map[string]EnvironmentConfig{
				"staging": {Deployments: map[string]DeploymentTarget{
					"executor-a": {Target: "shard-1"},
					"executor-b": {Target: "shard-2"},
				}},
			},
		},
	}
	local, routed := newPendingDropsTestService(t, databases).pendingDropsTargetCounts()
	assert.Zero(t, local, "a routed environment has no local DSN")
	assert.Equal(t, 2, routed, "a multi-deployment environment routes one target per deployment")
}

// An environment that configures neither a local DSN nor any routing executes
// nowhere, so it counts as neither local nor routed. Counting it as routed
// would tell an operator that a deployment reaps a target no deployment
// executes against, which is the one claim the routed count exists to make
// trustworthy. Validate() rejects this shape before startup, so it is only
// reachable for an embedder that skips validation.
func TestPendingDropsTargetCountsIgnoreEnvironmentsThatRouteNowhere(t *testing.T) {
	t.Parallel()
	databases := map[string]DatabaseConfig{
		"mysql_unrouted": {
			Type: storage.DatabaseTypeMySQL,
			Environments: map[string]EnvironmentConfig{
				"staging": {},
			},
		},
	}
	local, routed := newPendingDropsTestService(t, databases).pendingDropsTargetCounts()
	assert.Zero(t, local, "an environment with no local DSN is not reapable from this process")
	assert.Zero(t, routed, "an environment that routes nowhere has no deployment to reap it")
}

// The cleaner declines to start for reasons that mean different things to an
// operator, and the message must say which one applies: a process with the
// quarantine off drops directly and reaps nothing, including anything
// quarantined before it was turned off, while a process that quarantines but
// leaves reaping to another deployment accumulates tables on its targets until
// that deployment runs.
func TestStartPendingDropsCleanerReportsWhyItDeclined(t *testing.T) {
	localMySQL := map[string]DatabaseConfig{
		"mysql_local": {
			Type: storage.DatabaseTypeMySQL,
			Environments: map[string]EnvironmentConfig{
				"staging": {DSN: "user:pass@tcp(127.0.0.1:3306)/mysql_local"},
			},
		},
	}
	enabled, disabled := true, false

	tests := []struct {
		name      string
		config    PendingDropsConfig
		databases map[string]DatabaseConfig
		wantLog   string
	}{
		{
			name:      "quarantine disabled",
			config:    PendingDropsConfig{},
			databases: localMySQL,
			wantLog:   "any tables quarantined before it was disabled are not reaped",
		},
		{
			name:      "cleanup disabled for this process",
			config:    PendingDropsConfig{Enabled: &enabled, CleanupEnabled: &disabled},
			databases: localMySQL,
			wantLog:   "another deployment must reap",
		},
		{
			name:      "retention invalid",
			config:    PendingDropsConfig{Enabled: &enabled, Retention: "not-a-duration"},
			databases: localMySQL,
			wantLog:   "retention is invalid",
		},
		{
			name:      "no local targets",
			config:    PendingDropsConfig{Enabled: &enabled},
			databases: map[string]DatabaseConfig{},
			wantLog:   "no local MySQL database targets are configured",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var logs bytes.Buffer
			svc := New(nil, &ServerConfig{Databases: tt.databases, PendingDrops: tt.config},
				nil, slog.New(slog.NewTextHandler(&logs, nil)))

			svc.StartPendingDropsCleaner(t.Context())
			t.Cleanup(svc.StopPendingDropsCleaner)

			svc.pendingDropsMu.Lock()
			started := svc.pendingDropsCancel != nil
			svc.pendingDropsMu.Unlock()
			assert.False(t, started, "the cleaner loop must not run")
			assert.Contains(t, logs.String(), tt.wantLog)
		})
	}
}
