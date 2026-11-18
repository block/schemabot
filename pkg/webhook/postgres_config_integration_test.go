//go:build integration

package webhook

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gopkg.in/yaml.v3"

	"github.com/block/schemabot/pkg/api"
	"github.com/block/schemabot/pkg/engine"
	ghconfig "github.com/block/schemabot/pkg/github"
	ternv1 "github.com/block/schemabot/pkg/proto/ternv1"
	"github.com/block/schemabot/pkg/state"
	"github.com/block/schemabot/pkg/storage"
	"github.com/block/schemabot/pkg/testutil"
)

// postgresConfigFixtureDeadline bounds a real containerized PostgreSQL apply
// (plan, queue, operator drive, target verification), which needs more
// headroom than the in-process waits webhookIntegrationCheckRunDeadline is
// sized for.
const postgresConfigFixtureDeadline = 30 * time.Second

// A repository config selecting PostgreSQL routes the declarative schema
// through the API and local engine registry, then the operator applies the
// native-safe plan to the configured target.
func TestPostgresConfigFixturePlansAndAppliesNativeSafeChange(t *testing.T) {
	fixture := loadPostgresConfigFixture(t, "postgres")
	dsn, db := testutil.StartPostgres(t, fixture.config.Database)
	createFixtureUsersTable(t, db)

	svc := setupE2EServiceOpts(t, fixture.config.Database, e2eServiceOpts{
		databaseType: string(fixture.config.Type),
		targetDSN:    dsn,
	})
	plan, err := svc.ExecutePlan(t.Context(), fixture.planRequest())
	require.NoError(t, err)
	require.Len(t, plan.Changes, 1)
	require.Len(t, plan.Changes[0].TableChanges, 1)
	change := plan.Changes[0].TableChanges[0]
	assert.Equal(t, "public", plan.Changes[0].Namespace)
	assert.Equal(t, "users", change.TableName)
	assert.False(t, change.EngineBlocked())
	assert.Empty(t, change.ExecutionMode)

	_, _, err = svc.ExecuteApply(t.Context(), api.ApplyRequest{
		PlanID:      plan.PlanID,
		Environment: "staging",
		Caller:      "integration-test",
	})
	require.NoError(t, err)

	require.Eventually(t, func() bool {
		applies, getErr := svc.Storage().Applies().GetByPR(t.Context(), "octocat/hello-world", 1)
		if getErr != nil {
			return false
		}
		for _, apply := range applies {
			if apply.Database == fixture.config.Database {
				return state.IsState(apply.State, state.Apply.Completed)
			}
		}
		return false
	}, postgresConfigFixtureDeadline, 100*time.Millisecond)

	assert.True(t, postgresColumnExists(t, db, "users", "email"))
}

// A PostgreSQL plan whose desired state adds a column and an index to one
// table is an ordered multi-statement sequence for that table: every
// statement must survive into both the plan response the operator reviews
// and the stored plan the apply executes — a table appearing more than once
// is legal, and dropping its later statements would silently under-apply the
// reviewed change.
func TestPostgresConfigFixtureKeepsAllStatementsForOneTable(t *testing.T) {
	fixture := loadPostgresConfigFixture(t, "postgres")
	fixture.schema = "CREATE TABLE users (\n    id bigint PRIMARY KEY,\n    email text\n);\nCREATE INDEX idx_users_email ON users (email);\n"
	dsn, db := testutil.StartPostgres(t, fixture.config.Database)
	createFixtureUsersTable(t, db)

	svc := setupE2EServiceOpts(t, fixture.config.Database, e2eServiceOpts{
		databaseType: string(fixture.config.Type),
		targetDSN:    dsn,
	})
	plan, err := svc.ExecutePlan(t.Context(), fixture.planRequest())
	require.NoError(t, err)
	require.Len(t, plan.Changes, 1)
	require.Len(t, plan.Changes[0].TableChanges, 2)
	alter := plan.Changes[0].TableChanges[0]
	index := plan.Changes[0].TableChanges[1]
	assert.Equal(t, "users", alter.TableName)
	assert.Contains(t, alter.DDL, "ADD COLUMN")
	assert.Equal(t, "users", index.TableName)
	assert.Contains(t, index.DDL, "CREATE")
	assert.Contains(t, index.DDL, "INDEX")
	assert.Contains(t, index.DDL, "idx_users_email")

	stored, err := svc.Storage().Plans().Get(t.Context(), plan.PlanID)
	require.NoError(t, err)
	require.NotNil(t, stored)
	storedChanges := stored.FlatDDLChanges()
	require.Len(t, storedChanges, 2)
	assert.Contains(t, storedChanges[0].DDL, "ADD COLUMN")
	assert.Contains(t, storedChanges[1].DDL, "idx_users_email")
	for _, tc := range storedChanges {
		assert.Equal(t, "users", tc.Table)
	}
}

// A PostgreSQL apply whose plan carries two statements for one table runs one
// task per statement: each task settles on its own outcome rather than
// inheriting the last-reported state of a sibling on the same table, and
// every effect lands on the target.
func TestPostgresConfigFixtureAppliesEveryStatementForOneTable(t *testing.T) {
	fixture := loadPostgresConfigFixture(t, "postgres")
	fixture.schema = "CREATE TABLE users (\n    id bigint PRIMARY KEY,\n    email text,\n    name text\n);\n"
	dsn, db := testutil.StartPostgres(t, fixture.config.Database)
	createFixtureUsersTable(t, db)

	svc := setupE2EServiceOpts(t, fixture.config.Database, e2eServiceOpts{
		databaseType: string(fixture.config.Type),
		targetDSN:    dsn,
	})
	plan, err := svc.ExecutePlan(t.Context(), fixture.planRequest())
	require.NoError(t, err)
	require.Len(t, plan.Changes, 1)
	require.Len(t, plan.Changes[0].TableChanges, 2)
	for _, tc := range plan.Changes[0].TableChanges {
		assert.Equal(t, "users", tc.TableName)
		assert.Contains(t, tc.DDL, "ADD COLUMN")
	}

	_, _, err = svc.ExecuteApply(t.Context(), api.ApplyRequest{
		PlanID:      plan.PlanID,
		Environment: "staging",
		Caller:      "integration-test",
	})
	require.NoError(t, err)

	var settled *storage.Apply
	require.EventuallyWithT(t, func(c *assert.CollectT) {
		apply, err := findFixtureApply(t, svc, fixture.config.Database)
		if !assert.NoError(c, err) || !assert.NotNil(c, apply, "no apply stored for the fixture PR") {
			return
		}
		if !assert.True(c, applyIsSettled(apply.State),
			"apply not settled yet, state=%s", apply.State) {
			return
		}
		settled = apply
	}, postgresConfigFixtureDeadline, 100*time.Millisecond)
	require.True(t, state.IsState(settled.State, state.Apply.Completed),
		"both statements must land, state=%s", settled.State)

	tasks, err := svc.Storage().Tasks().GetByApplyID(t.Context(), settled.ID)
	require.NoError(t, err)
	require.Len(t, tasks, 2, "one task per statement, not one per table")
	for _, task := range tasks {
		assert.Equal(t, "users", task.TableName)
		assert.True(t, state.IsState(task.State, state.Task.Completed),
			"task %s (%s) state=%s", task.TaskIdentifier, task.DDL, task.State)
		assert.NotNil(t, task.CompletedAt, "task %s (%s) must record its own completion", task.TaskIdentifier, task.DDL)
	}

	assert.True(t, postgresColumnExists(t, db, "users", "email"))
	assert.True(t, postgresColumnExists(t, db, "users", "name"))
}

// A repository config selecting PostgreSQL surfaces a statement outside the
// optimistic native-safe slice as a typed blocked plan, and the apply gate
// refuses to queue it when an apply is attempted anyway.
func TestPostgresConfigFixtureSurfacesBlockedPlan(t *testing.T) {
	fixture := loadPostgresConfigFixture(t, "postgres-blocked")
	dsn, db := testutil.StartPostgres(t, fixture.config.Database)
	_, err := db.ExecContext(t.Context(), "CREATE TABLE public.users (id bigint PRIMARY KEY, email text)")
	require.NoError(t, err)

	svc := setupE2EServiceOpts(t, fixture.config.Database, e2eServiceOpts{
		databaseType: string(fixture.config.Type),
		targetDSN:    dsn,
	})
	plan, err := svc.ExecutePlan(t.Context(), fixture.planRequest())
	require.NoError(t, err)
	assert.True(t, plan.HasBlockedChanges())
	require.Len(t, plan.Changes, 1)
	require.Len(t, plan.Changes[0].TableChanges, 1)
	assert.True(t, plan.Changes[0].TableChanges[0].EngineBlocked())
	assert.Equal(t, engine.ExecutionModeBlocked, plan.Changes[0].TableChanges[0].ExecutionMode)
	// The fixture's column type change routes to copy-and-swap; pin that cause
	// so the test keeps proving the intended blocking reason, not just the
	// fact of being blocked.
	assert.Contains(t, plan.Changes[0].TableChanges[0].ModeReason, "copy-and-swap")

	resp, applyID, err := svc.ExecuteApply(t.Context(), api.ApplyRequest{
		PlanID:      plan.PlanID,
		Environment: "staging",
		Caller:      "integration-test",
	})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "blocked change")
	assert.Nil(t, resp)
	assert.Zero(t, applyID)

	applies, err := svc.Storage().Applies().GetByPR(t.Context(), "octocat/hello-world", 1)
	require.NoError(t, err)
	assert.Empty(t, applies)
}

type postgresConfigFixture struct {
	config ghconfig.SchemabotConfig
	schema string
}

func loadPostgresConfigFixture(t *testing.T, name string) postgresConfigFixture {
	t.Helper()
	dir := filepath.Join("testdata", "myapp", name, "schema")
	configBytes, err := os.ReadFile(filepath.Join(dir, "schemabot.yaml"))
	require.NoError(t, err)
	var config ghconfig.SchemabotConfig
	require.NoError(t, yaml.Unmarshal(configBytes, &config))
	require.Equal(t, storage.DatabaseTypePostgres, string(config.Type))

	schemaBytes, err := os.ReadFile(filepath.Join(dir, "users.sql"))
	require.NoError(t, err)
	return postgresConfigFixture{config: config, schema: string(schemaBytes)}
}

func (f postgresConfigFixture) planRequest() api.PlanRequest {
	pullRequest := int32(1)
	headSHA := "abc123"
	return api.PlanRequest{
		Database:    f.config.Database,
		Environment: "staging",
		Type:        string(f.config.Type),
		SchemaFiles: map[string]*ternv1.SchemaFiles{
			"public": {Files: map[string]string{"users.sql": f.schema}},
		},
		Repository:    "octocat/hello-world",
		PullRequest:   &pullRequest,
		HeadSHA:       &headSHA,
		SchemaPath:    "schema",
		SourceTrusted: true,
	}
}
