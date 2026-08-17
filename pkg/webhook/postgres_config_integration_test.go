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

const postgresConfigFixtureDeadline = 30 * time.Second

// A repository config selecting PostgreSQL routes the declarative schema
// through the API and local engine registry, then the operator applies the
// native-safe plan to the configured target.
func TestPostgresConfigFixturePlansAndAppliesNativeSafeChange(t *testing.T) {
	fixture := loadPostgresConfigFixture(t, "postgres")
	dsn, db := testutil.StartPostgres(t, fixture.config.Database)
	_, err := db.ExecContext(t.Context(), "CREATE TABLE public.users (id bigint PRIMARY KEY)")
	require.NoError(t, err)

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
	}, postgresConfigFixtureDeadline, 20*time.Millisecond)

	var exists bool
	err = db.QueryRowContext(t.Context(), `SELECT EXISTS (
		SELECT 1 FROM information_schema.columns
		WHERE table_schema = 'public' AND table_name = 'users' AND column_name = 'email')`).Scan(&exists)
	require.NoError(t, err)
	assert.True(t, exists)
}

// A repository config selecting PostgreSQL surfaces a statement outside the
// optimistic native-safe slice as a typed blocked plan and never queues apply.
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
	dir := filepath.Join("..", "..", "integration", "testdata", "myapp", name, "schema")
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
