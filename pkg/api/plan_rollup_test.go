package api

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	ternv1 "github.com/block/schemabot/pkg/proto/ternv1"
	"github.com/block/schemabot/pkg/routing"
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
	return rollupMember(name, name, changes...)
}

// rollupMember builds a diff for one rollout member, for cases where the
// deployment and target differ.
func rollupMember(deployment, target string, changes ...*ternv1.SchemaChange) DeploymentPlanDiff {
	return DeploymentPlanDiff{
		DatabaseType: "vitess",
		Deployment:   deployment,
		Target:       target,
		Changes:      changes,
	}
}

// rollupMembers returns the rollout members of diffs in order, the expected
// member contract PlanDeploymentDiffs would produce for them.
func rollupMembers(diffs []DeploymentPlanDiff) []routing.ExecutionTarget {
	members := make([]routing.ExecutionTarget, len(diffs))
	for i, d := range diffs {
		members[i] = routing.ExecutionTarget{Deployment: d.Deployment, Target: d.Target}
	}
	return members
}

// rollupMemberList builds an expected member set from "deployment/target"
// pairs, for contract cases that deliberately disagree with the diffs.
func rollupMemberList(pairs ...[2]string) []routing.ExecutionTarget {
	members := make([]routing.ExecutionTarget, len(pairs))
	for i, p := range pairs {
		members[i] = routing.ExecutionTarget{Deployment: p[0], Target: p[1]}
	}
	return members
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
	rollup, err := RollupDeploymentDiffs(diffs, rollupMembers(diffs), PlanMirrored)
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
	rollup, err := RollupDeploymentDiffs(diffs, rollupMembers(diffs), PlanMirrored)
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
	rollup, err := RollupDeploymentDiffs(diffs, rollupMembers(diffs), PlanMirrored)
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
	rollup, err := RollupDeploymentDiffs(diffs, rollupMembers(diffs), PlanMirrored)
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
	rollup, err := RollupDeploymentDiffs(diffs, rollupMembers(diffs), PlanMirrored)
	require.NoError(t, err)
	assert.False(t, rollup.Clean)
	assert.Equal(t, DeploymentErrored, rollup.Entries[0].Class)
	assert.Equal(t, DeploymentErrored, rollup.Entries[1].Class)
	require.Error(t, rollup.Entries[1].Err)
}

// An empty result set is a fail-closed error: there is nothing to prove the
// deployments agree.
func TestRollupDeploymentDiffs_EmptyErrors(t *testing.T) {
	_, err := RollupDeploymentDiffs(nil, nil, PlanMirrored)
	require.Error(t, err)
}

// A single-deployment database rolls up clean: the primary matches itself.
func TestRollupDeploymentDiffs_SingleDeploymentClean(t *testing.T) {
	diffs := []DeploymentPlanDiff{
		rollupDeployment("eu", rollupAlterUsers("ALTER TABLE `users` ADD COLUMN `email` varchar(255)")),
	}
	rollup, err := RollupDeploymentDiffs(diffs, rollupMembers(diffs), PlanMirrored)
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
	rollup, err := RollupDeploymentDiffs(diffs, rollupMembers(diffs), PlanMirrored)
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
	rollup, err := RollupDeploymentDiffs(diffs, rollupMembers(diffs), PlanMirrored)
	require.NoError(t, err)
	assert.True(t, rollup.Clean)
	require.Len(t, rollup.Entries, 2)
	for _, e := range rollup.Entries {
		assert.Equal(t, DeploymentMatch, e.Class, "deployment %q", e.Deployment)
	}
}

// A primary whose database type maps to no registered grammar cannot anchor
// any comparison: the baseline self-comparison fails, so the rollup fails
// closed rather than parsing the reviewed plan by a guess.
func TestRollupDeploymentDiffs_UnregisteredPrimaryDialectBlocks(t *testing.T) {
	primary := rollupDeployment("eu", rollupAlterUsers("ALTER TABLE `users` ADD COLUMN `email` varchar(255)"))
	primary.DatabaseType = "oracle"
	diffs := []DeploymentPlanDiff{primary}
	rollup, err := RollupDeploymentDiffs(diffs, rollupMembers(diffs), PlanMirrored)
	require.NoError(t, err)
	assert.False(t, rollup.Clean)
	require.Len(t, rollup.Entries, 1)
	assert.Equal(t, DeploymentErrored, rollup.Entries[0].Class)
	require.Error(t, rollup.Entries[0].Err)
	assert.Contains(t, rollup.Entries[0].Err.Error(), `no statement parser registered for dialect "oracle"`)
}

// Deployments whose database types differ but share a grammar — vitess and
// mysql are both the MySQL dialect — compare normally: the dialect guard
// compares dialects, not raw type strings.
func TestRollupDeploymentDiffs_MySQLFamilyTypesShareDialect(t *testing.T) {
	change := "ALTER TABLE `users` ADD COLUMN `email` varchar(255)"
	mysqlDeployment := rollupDeployment("us", rollupAlterUsers(change))
	mysqlDeployment.DatabaseType = "mysql"
	diffs := []DeploymentPlanDiff{
		rollupDeployment("eu", rollupAlterUsers(change)),
		mysqlDeployment,
	}
	rollup, err := RollupDeploymentDiffs(diffs, rollupMembers(diffs), PlanMirrored)
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
	rollup, err := RollupDeploymentDiffs(diffs, rollupMembers(diffs), PlanMirrored)
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
		_, err := RollupDeploymentDiffs(diffs, rollupMemberList([2]string{"au", "au"}, [2]string{"eu", "eu"}), PlanMirrored)
		require.Error(t, err)
	})
	t.Run("missing member", func(t *testing.T) {
		_, err := RollupDeploymentDiffs(diffs, rollupMemberList([2]string{"eu", "eu"}, [2]string{"au", "au"}, [2]string{"us", "us"}), PlanMirrored)
		require.Error(t, err)
	})
	t.Run("extra diff", func(t *testing.T) {
		_, err := RollupDeploymentDiffs(diffs, rollupMemberList([2]string{"eu", "eu"}), PlanMirrored)
		require.Error(t, err)
	})
}

// Members of one deployment are distinguished by their target, so a result
// carrying the right deployments against the wrong targets is a contract
// mismatch rather than a silent pass. Without the target half of the member
// identity these two entries would be indistinguishable.
func TestRollupDeploymentDiffs_SameDeploymentDifferentTargets(t *testing.T) {
	change := "ALTER TABLE `users` ADD COLUMN `email` varchar(255)"
	diffs := []DeploymentPlanDiff{
		rollupMember("cake", "orders-001", rollupAlterUsers(change)),
		rollupMember("cake", "orders-002", rollupAlterUsers(change)),
	}

	t.Run("matching members roll up clean", func(t *testing.T) {
		rollup, err := RollupDeploymentDiffs(diffs, rollupMembers(diffs), PlanMirrored)
		require.NoError(t, err)
		assert.True(t, rollup.Clean)
		require.Len(t, rollup.Entries, 2)
		assert.Equal(t, "orders-001", rollup.Entries[0].Target)
		assert.Equal(t, "orders-002", rollup.Entries[1].Target)
	})

	t.Run("swapped targets are a contract mismatch", func(t *testing.T) {
		_, err := RollupDeploymentDiffs(diffs, rollupMemberList([2]string{"cake", "orders-002"}, [2]string{"cake", "orders-001"}), PlanMirrored)
		require.Error(t, err)
		assert.ErrorContains(t, err, "cake/orders-001")
	})
}

// An environment whose members are distinct targets plans each one against its
// own live schema, so members that would run different changes are ordinary
// rather than drift. The rollup stays clean and every member classifies as
// planned — the same change sets under mirrored planning would block.
func TestRollupDeploymentDiffs_IndependentMembersDoNotBlockOnDifference(t *testing.T) {
	diffs := []DeploymentPlanDiff{
		rollupMember("cake", "orders-001", rollupAlterUsers("ALTER TABLE users ADD COLUMN email VARCHAR(255)")),
		rollupMember("cake", "orders-002", rollupAlterUsers("ALTER TABLE users ADD COLUMN phone VARCHAR(32)")),
		rollupMember("cake", "orders-003"),
	}
	members := rollupMembers(diffs)

	independent, err := RollupDeploymentDiffs(diffs, members, PlanIndependent)
	require.NoError(t, err)
	assert.True(t, independent.Clean, "targets planned on their own do not drift against each other")
	require.Len(t, independent.Entries, 3)
	for i, entry := range independent.Entries {
		assert.Equal(t, DeploymentPlanned, entry.Class, "entry %d", i)
		assert.NoError(t, entry.Err, "entry %d", i)
	}

	mirrored, err := RollupDeploymentDiffs(diffs, members, PlanMirrored)
	require.NoError(t, err)
	assert.False(t, mirrored.Clean, "the same change sets are drift when members are expected to match")
	assert.Equal(t, DeploymentDiverged, mirrored.Entries[1].Class)
}

// Independent planning removes the comparison between members, not the
// requirement that each member be plannable: a member the producer could not
// diff still blocks the review closed.
func TestRollupDeploymentDiffs_IndependentMemberErrorBlocks(t *testing.T) {
	diffs := []DeploymentPlanDiff{
		rollupMember("cake", "orders-001", rollupAlterUsers("ALTER TABLE users ADD COLUMN email VARCHAR(255)")),
		rollupMember("cake", "orders-002"),
	}
	diffs[1].Err = fmt.Errorf("target unreachable")

	rollup, err := RollupDeploymentDiffs(diffs, rollupMembers(diffs), PlanIndependent)
	require.NoError(t, err)
	assert.False(t, rollup.Clean)
	assert.Equal(t, DeploymentPlanned, rollup.Entries[0].Class)
	assert.Equal(t, DeploymentErrored, rollup.Entries[1].Class)
	require.Error(t, rollup.Entries[1].Err)
	assert.Contains(t, rollup.Entries[1].Err.Error(), "target unreachable")
}

// A member whose change content will not parse under its own grammar has no
// usable plan, so it blocks even though nothing is compared against it.
func TestRollupDeploymentDiffs_IndependentUnparseableMemberBlocks(t *testing.T) {
	diffs := []DeploymentPlanDiff{
		rollupMember("cake", "orders-001", rollupAlterUsers("ALTER TABLE users ADD COLUMN email VARCHAR(255)")),
		rollupMember("cake", "orders-002", rollupAlterUsers("this is not valid DDL at all")),
	}

	rollup, err := RollupDeploymentDiffs(diffs, rollupMembers(diffs), PlanIndependent)
	require.NoError(t, err)
	assert.False(t, rollup.Clean)
	assert.Equal(t, DeploymentPlanned, rollup.Entries[0].Class)
	assert.Equal(t, DeploymentErrored, rollup.Entries[1].Class)
	require.Error(t, rollup.Entries[1].Err)
	assert.Contains(t, rollup.Entries[1].Err.Error(), "not usable")
}

// The member contract is enforced whatever the planning: independent planning
// stops members being compared to each other, it does not stop a missing or
// misidentified member from failing the rollup closed.
func TestRollupDeploymentDiffs_IndependentEnforcesMemberContract(t *testing.T) {
	diffs := []DeploymentPlanDiff{
		rollupMember("cake", "orders-001"),
		rollupMember("cake", "orders-002"),
	}

	_, err := RollupDeploymentDiffs(diffs, rollupMemberList([2]string{"cake", "orders-002"}, [2]string{"cake", "orders-001"}), PlanIndependent)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "cake/orders-002")
}
