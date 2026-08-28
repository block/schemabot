package api

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	ternv1 "github.com/block/schemabot/pkg/proto/ternv1"
)

func rollupAlterUsers(ddl string) *ternv1.SchemaChange {
	return &ternv1.SchemaChange{
		Namespace: "testapp",
		TableChanges: []*ternv1.TableChange{{
			TableName:  "users",
			Ddl:        ddl,
			ChangeType: ternv1.ChangeType_CHANGE_TYPE_ALTER,
			Namespace:  "testapp",
		}},
	}
}

func rollupDeployment(name string, changes ...*ternv1.SchemaChange) DeploymentPlanDiff {
	return DeploymentPlanDiff{
		DatabaseType: "vitess",
		Deployment:   name,
		Target:       name,
		Changes:      changes,
	}
}

// rollupNames returns the deployment names of diffs in order, the expected
// deployment contract PlanDeploymentDiffs would produce for them.
func rollupNames(diffs []DeploymentPlanDiff) []string {
	names := make([]string, len(diffs))
	for i, d := range diffs {
		names[i] = d.Deployment
	}
	return names
}

// When every deployment would plan exactly the reviewed changes, the rollup is
// clean and every entry classifies as a match.
func TestRollupDeploymentDiffs_AllMatchIsClean(t *testing.T) {
	change := "ALTER TABLE `users` ADD COLUMN `email` varchar(255)"
	diffs := []DeploymentPlanDiff{
		rollupDeployment("eu", rollupAlterUsers(change)),
		rollupDeployment("au", rollupAlterUsers(change)),
		rollupDeployment("us", rollupAlterUsers(change)),
	}
	rollup, err := RollupDeploymentDiffs(diffs, rollupNames(diffs))
	require.NoError(t, err)
	assert.True(t, rollup.Clean)
	require.Len(t, rollup.Entries, 3)
	for _, e := range rollup.Entries {
		assert.Equal(t, DeploymentMatch, e.Class, "deployment %q", e.Deployment)
	}
}

// A deployment that would plan different DDL than was reviewed is diverged, and
// the rollup fails closed.
func TestRollupDeploymentDiffs_DivergenceBlocks(t *testing.T) {
	diffs := []DeploymentPlanDiff{
		rollupDeployment("eu", rollupAlterUsers("ALTER TABLE `users` ADD COLUMN `email` varchar(255)")),
		rollupDeployment("au", rollupAlterUsers("ALTER TABLE `users` ADD COLUMN `phone` varchar(255)")),
	}
	rollup, err := RollupDeploymentDiffs(diffs, rollupNames(diffs))
	require.NoError(t, err)
	assert.False(t, rollup.Clean)
	assert.Equal(t, DeploymentMatch, rollup.Entries[0].Class)
	assert.Equal(t, DeploymentDiverged, rollup.Entries[1].Class)
	assert.False(t, rollup.Entries[1].Diff.Empty())
}

// A deployment whose diff could not be computed is errored, and the rollup fails
// closed without hiding the healthy deployments.
func TestRollupDeploymentDiffs_ProducerErrorBlocks(t *testing.T) {
	change := "ALTER TABLE `users` ADD COLUMN `email` varchar(255)"
	errored := rollupDeployment("au")
	errored.Err = fmt.Errorf("deployment unreachable")
	diffs := []DeploymentPlanDiff{
		rollupDeployment("eu", rollupAlterUsers(change)),
		errored,
	}
	rollup, err := RollupDeploymentDiffs(diffs, rollupNames(diffs))
	require.NoError(t, err)
	assert.False(t, rollup.Clean)
	assert.Equal(t, DeploymentMatch, rollup.Entries[0].Class)
	assert.Equal(t, DeploymentErrored, rollup.Entries[1].Class)
	require.Error(t, rollup.Entries[1].Err)
}

// Malformed comparison input (unparseable DDL) classifies the deployment as
// errored rather than silently matching.
func TestRollupDeploymentDiffs_ComparisonErrorBlocks(t *testing.T) {
	diffs := []DeploymentPlanDiff{
		rollupDeployment("eu", rollupAlterUsers("ALTER TABLE `users` ADD COLUMN `email` varchar(255)")),
		rollupDeployment("au", rollupAlterUsers("not valid sql")),
	}
	rollup, err := RollupDeploymentDiffs(diffs, rollupNames(diffs))
	require.NoError(t, err)
	assert.False(t, rollup.Clean)
	assert.Equal(t, DeploymentErrored, rollup.Entries[1].Class)
	require.Error(t, rollup.Entries[1].Err)
}

// If the reviewed primary baseline itself errored, no deployment can be
// confirmed to match, so all non-primary deployments block.
func TestRollupDeploymentDiffs_UnusablePrimaryBlocksAll(t *testing.T) {
	primary := rollupDeployment("eu")
	primary.Err = fmt.Errorf("reviewed primary plan reported errors")
	diffs := []DeploymentPlanDiff{
		primary,
		rollupDeployment("au", rollupAlterUsers("ALTER TABLE `users` ADD COLUMN `email` varchar(255)")),
	}
	rollup, err := RollupDeploymentDiffs(diffs, rollupNames(diffs))
	require.NoError(t, err)
	assert.False(t, rollup.Clean)
	assert.Equal(t, DeploymentErrored, rollup.Entries[0].Class)
	assert.Equal(t, DeploymentErrored, rollup.Entries[1].Class)
	require.Error(t, rollup.Entries[1].Err)
}

// An empty result set is a fail-closed error: there is nothing to prove the
// deployments agree.
func TestRollupDeploymentDiffs_EmptyErrors(t *testing.T) {
	_, err := RollupDeploymentDiffs(nil, nil)
	require.Error(t, err)
}

// A single-deployment database rolls up clean: the primary matches itself.
func TestRollupDeploymentDiffs_SingleDeploymentClean(t *testing.T) {
	diffs := []DeploymentPlanDiff{
		rollupDeployment("eu", rollupAlterUsers("ALTER TABLE `users` ADD COLUMN `email` varchar(255)")),
	}
	rollup, err := RollupDeploymentDiffs(diffs, rollupNames(diffs))
	require.NoError(t, err)
	assert.True(t, rollup.Clean)
	require.Len(t, rollup.Entries, 1)
	assert.Equal(t, DeploymentMatch, rollup.Entries[0].Class)
}

// A single-deployment rollup whose primary baseline carries unparseable DDL
// fails closed: the primary is errored and the rollup blocks, rather than
// matching itself without a trustworthy comparison ever running.
func TestRollupDeploymentDiffs_MalformedSingleDeploymentBaselineBlocks(t *testing.T) {
	diffs := []DeploymentPlanDiff{
		rollupDeployment("eu", rollupAlterUsers("not valid sql")),
	}
	rollup, err := RollupDeploymentDiffs(diffs, rollupNames(diffs))
	require.NoError(t, err)
	assert.False(t, rollup.Clean)
	require.Len(t, rollup.Entries, 1)
	assert.Equal(t, DeploymentErrored, rollup.Entries[0].Class)
	require.Error(t, rollup.Entries[0].Err)
}

// rollupPostgresDeployment is rollupDeployment for a PostgreSQL deployment.
func rollupPostgresDeployment(name string, changes ...*ternv1.SchemaChange) DeploymentPlanDiff {
	d := rollupDeployment(name, changes...)
	d.DatabaseType = "postgres"
	return d
}

// A PostgreSQL database's deployments are compared under the PostgreSQL
// grammar: DDL the MySQL parser cannot read still rolls up clean when every
// deployment would plan the same changes.
func TestRollupDeploymentDiffs_PostgresDialectClean(t *testing.T) {
	change := func() *ternv1.SchemaChange {
		return &ternv1.SchemaChange{
			Namespace: "testapp",
			TableChanges: []*ternv1.TableChange{{
				TableName:  "users",
				Ddl:        "CREATE TABLE users (id uuid PRIMARY KEY, created_at timestamptz NOT NULL DEFAULT now())",
				ChangeType: ternv1.ChangeType_CHANGE_TYPE_CREATE,
				Namespace:  "testapp",
			}},
		}
	}
	diffs := []DeploymentPlanDiff{
		rollupPostgresDeployment("eu", change()),
		rollupPostgresDeployment("us", change()),
	}
	rollup, err := RollupDeploymentDiffs(diffs, rollupNames(diffs))
	require.NoError(t, err)
	assert.True(t, rollup.Clean)
	require.Len(t, rollup.Entries, 2)
	for _, e := range rollup.Entries {
		assert.Equal(t, DeploymentMatch, e.Class, "deployment %q", e.Deployment)
	}
}

// A deployment whose dialect differs from the reviewed primary's cannot be
// compared meaningfully, so it classifies as errored and the rollup fails
// closed rather than judging one dialect's DDL under another's grammar.
func TestRollupDeploymentDiffs_MixedDialectBlocks(t *testing.T) {
	diffs := []DeploymentPlanDiff{
		rollupDeployment("eu", rollupAlterUsers("ALTER TABLE `users` ADD COLUMN `email` varchar(255)")),
		rollupPostgresDeployment("us", rollupAlterUsers("ALTER TABLE users ADD COLUMN email varchar(255)")),
	}
	rollup, err := RollupDeploymentDiffs(diffs, rollupNames(diffs))
	require.NoError(t, err)
	assert.False(t, rollup.Clean)
	assert.Equal(t, DeploymentMatch, rollup.Entries[0].Class)
	assert.Equal(t, DeploymentErrored, rollup.Entries[1].Class)
	require.Error(t, rollup.Entries[1].Err)
	assert.Contains(t, rollup.Entries[1].Err.Error(), "cannot compare change sets across dialects")
}

// The rollup enforces the producer contract positionally: a result whose
// deployments do not match the expected rollout order (wrong primary, wrong
// order, or a missing deployment) is rejected rather than risking a false clean.
func TestRollupDeploymentDiffs_ContractMismatchErrors(t *testing.T) {
	change := "ALTER TABLE `users` ADD COLUMN `email` varchar(255)"
	diffs := []DeploymentPlanDiff{
		rollupDeployment("eu", rollupAlterUsers(change)),
		rollupDeployment("au", rollupAlterUsers(change)),
	}

	t.Run("wrong primary", func(t *testing.T) {
		_, err := RollupDeploymentDiffs(diffs, []string{"au", "eu"})
		require.Error(t, err)
	})
	t.Run("missing deployment", func(t *testing.T) {
		_, err := RollupDeploymentDiffs(diffs, []string{"eu", "au", "us"})
		require.Error(t, err)
	})
	t.Run("extra diff", func(t *testing.T) {
		_, err := RollupDeploymentDiffs(diffs, []string{"eu"})
		require.Error(t, err)
	})
}
