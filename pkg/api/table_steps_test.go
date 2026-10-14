package api

import (
	"bytes"
	"log/slog"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/block/schemabot/pkg/routing"
	"github.com/block/schemabot/pkg/state"
	"github.com/block/schemabot/pkg/storage"
)

// stepPlan is a plan for target in deployment eu changing the given tables of
// namespace bikeshare, each with its own ALTER.
func stepPlan(id int64, target string, tables ...string) *storage.Plan {
	plan := primaryPlanRow(target)
	plan.ID = id
	var changes []storage.TableChange
	for _, table := range tables {
		changes = append(changes, storage.TableChange{Namespace: "bikeshare", Table: table, DDL: "ALTER TABLE `" + table + "` ADD COLUMN `note` varchar(64)", Operation: "alter"})
	}
	plan.Namespaces = map[string]*storage.NamespacePlanData{"bikeshare": {Tables: changes}}
	return plan
}

func stepMember(target string, plan *storage.Plan) applyMember {
	return applyMember{Target: routing.ExecutionTarget{Deployment: "eu", Target: target}, Plan: plan}
}

// stepRow is one operation as a layout test reads it: its key, its table step
// and the tables its tasks change.
type stepRow struct {
	key    string
	step   int
	tables []string
}

// stepLayout reads groups as stepRows, in creation order.
func stepLayout(groups []*storage.ApplyOperationWithTasks) []stepRow {
	rows := make([]stepRow, len(groups))
	for i, group := range groups {
		rows[i] = stepRow{key: group.Operation.OperationKey, step: group.Operation.RolloutStep}
		for _, task := range group.Tasks {
			rows[i].tables = append(rows[i].tables, task.TableName)
		}
	}
	return rows
}

func buildSteps(t *testing.T, applyPlan *storage.Plan, members ...applyMember) []*storage.ApplyOperationWithTasks {
	t.Helper()
	groups, sharded, err := buildApplyOperationGroups(applyPlan, applyTaskChanges(applyPlan), members, "production", storage.ApplyOptions{}, "", "", pershardTestTime())
	require.NoError(t, err)
	assert.False(t, sharded)
	return groups
}

// Three targets of one deployment alter `stations` and then `docks`. The
// rollout runs `stations` on every target before any target starts `docks`,
// so the operations are created step by step, and within a step in target
// order, which is the order the claim reads them in.
func TestBuildApplyOperationGroups_MultiTargetRunsTableByTable(t *testing.T) {
	applyPlan := stepPlan(10, "bikeshare-001", "stations", "docks")
	groups := buildSteps(t, applyPlan,
		stepMember("bikeshare-001", applyPlan),
		stepMember("bikeshare-002", stepPlan(11, "bikeshare-002", "stations", "docks")),
		stepMember("bikeshare-003", stepPlan(12, "bikeshare-003", "stations", "docks")),
	)

	assert.Equal(t, []stepRow{
		{"bikeshare-001/step-1", 1, []string{"stations"}},
		{"bikeshare-002/step-1", 1, []string{"stations"}},
		{"bikeshare-003/step-1", 1, []string{"stations"}},
		{"bikeshare-001/step-2", 2, []string{"docks"}},
		{"bikeshare-002/step-2", 2, []string{"docks"}},
		{"bikeshare-003/step-2", 2, []string{"docks"}},
	}, stepLayout(groups))
	for _, group := range groups {
		assert.Equal(t, state.ApplyOperation.Pending, group.Operation.State)
	}
	assert.Zero(t, groups[0].Operation.PlanID, "the reviewed target runs the apply's plan")
	assert.Equal(t, int64(11), groups[1].Operation.PlanID, "another target runs its own plan")
}

// Steps follow the reviewed plan's table order. A table only another target's
// plan changes comes after the reviewed ones, sorted by name, and a target
// whose plan does not change a table has no operation in that step. Two
// statements on one table are one step, run as two tasks of the target's one
// operation for it.
func TestBuildApplyOperationGroups_TableStepsFollowTheReviewedPlan(t *testing.T) {
	applyPlan := stepPlan(10, "bikeshare-001", "stations", "docks")
	applyPlan.Namespaces["bikeshare"].Tables = append(applyPlan.Namespaces["bikeshare"].Tables,
		storage.TableChange{Namespace: "bikeshare", Table: "stations", DDL: "ALTER TABLE `stations` ADD INDEX `idx_note` (`note`)", Operation: "alter"})
	groups := buildSteps(t, applyPlan,
		stepMember("bikeshare-001", applyPlan),
		stepMember("bikeshare-002", stepPlan(11, "bikeshare-002", "riders", "docks", "bikes")),
	)

	assert.Equal(t, []stepRow{
		{"bikeshare-001/step-1", 1, []string{"stations", "stations"}},
		{"bikeshare-001/step-2", 2, []string{"docks"}},
		{"bikeshare-002/step-2", 2, []string{"docks"}},
		{"bikeshare-002/step-3", 3, []string{"bikes"}},
		{"bikeshare-002/step-4", 4, []string{"riders"}},
	}, stepLayout(groups))
}

// A target whose own plan found nothing to change still belongs to the
// rollout: it gets one operation in the first step, keyed like its siblings,
// already settled, so nothing waits on it.
func TestBuildApplyOperationGroups_ConvergedTargetSettlesInTheFirstStep(t *testing.T) {
	applyPlan := stepPlan(10, "bikeshare-001", "stations", "docks")
	groups := buildSteps(t, applyPlan,
		stepMember("bikeshare-001", applyPlan),
		stepMember("bikeshare-002", &storage.Plan{ID: 11, Deployment: "eu", Target: "bikeshare-002"}),
	)

	assert.Equal(t, []stepRow{
		{"bikeshare-001/step-1", 1, []string{"stations"}},
		{"bikeshare-002/step-1", 1, nil},
		{"bikeshare-001/step-2", 2, []string{"docks"}},
	}, stepLayout(groups))
	converged := groups[1].Operation
	assert.Equal(t, state.ApplyOperation.Completed, converged.State)
	assert.True(t, converged.AlreadyConverged)
	assert.Nil(t, converged.StartedAt)
}

// A rollout where a target's plan changes a VSchema keeps each target's
// change in one operation, since the VSchema applies with that target's
// tables, so it runs member by member.
func TestBuildApplyOperationGroups_VSchemaChangeKeepsMemberOperationsWhole(t *testing.T) {
	applyPlan := stepPlan(10, "bikeshare-001", "stations", "docks")
	other := stepPlan(11, "bikeshare-002", "stations", "docks")
	other.Namespaces["bikeshare"].Artifacts = map[string]string{storage.VSchemaArtifactName: `{"sharded":false}`}
	groups := buildSteps(t, applyPlan, stepMember("bikeshare-001", applyPlan), stepMember("bikeshare-002", other))

	assert.Equal(t, []stepRow{
		{"bikeshare-001", 0, []string{"stations", "docks"}},
		{"bikeshare-002", 0, []string{"stations", "docks"}},
	}, stepLayout(groups))
}

// A rollout whose deployments each address one target has no target to run a
// table across, so each member's change stays one operation keyed as before.
func TestBuildApplyOperationGroups_SingleTargetDeploymentsKeepMemberOperations(t *testing.T) {
	applyPlan := stepPlan(10, "bikeshare", "stations", "docks")
	groups := buildSteps(t, applyPlan, applyMember{Target: routing.ExecutionTarget{Deployment: "eu"}, Plan: applyPlan})

	assert.Equal(t, []stepRow{{"", 0, []string{"stations", "docks"}}}, stepLayout(groups))
}

// A rollout that pairs a multi-target deployment with a deployment of one
// target has a member with no target to run a step beside, so the rollout
// keeps every member's change in one operation rather than step some members
// and not others. Apply creation refuses this shape first; the builder holds
// to it on its own.
func TestBuildApplyOperationGroups_MixedDeploymentShapesKeepMemberOperations(t *testing.T) {
	applyPlan := stepPlan(10, "bikeshare-001", "stations", "docks")
	other := stepPlan(11, "bikeshare-002", "stations", "docks")
	single := stepPlan(12, "", "stations", "docks")
	groups := buildSteps(t, applyPlan,
		stepMember("bikeshare-001", applyPlan),
		stepMember("bikeshare-002", other),
		applyMember{Target: routing.ExecutionTarget{Deployment: "us"}, Plan: single},
	)

	assert.Equal(t, []stepRow{
		{"bikeshare-001", 0, []string{"stations", "docks"}},
		{"bikeshare-002", 0, []string{"stations", "docks"}},
		{"", 0, []string{"stations", "docks"}},
	}, stepLayout(groups))
}

// The log of a rollout's layout says which case it was. A rollout of two
// tables logs its two steps. A rollout whose every target already holds the
// change is laid out as one settled step and runs no table, so it logs that its
// plans change no table rather than claim a table-by-table rollout.
func TestLogRolloutShape_NamesTheLayoutTheRolloutRuns(t *testing.T) {
	for name, tc := range map[string]struct {
		applyPlan *storage.Plan
		members   func(*storage.Plan) []applyMember
		want      string
	}{
		"table by table": {
			applyPlan: stepPlan(10, "bikeshare-001", "stations", "docks"),
			members: func(applyPlan *storage.Plan) []applyMember {
				return []applyMember{stepMember("bikeshare-001", applyPlan), stepMember("bikeshare-002", stepPlan(11, "bikeshare-002", "stations", "docks"))}
			},
			want: `msg="createStoredApply: queueing a multi-target rollout table by table"`,
		},
		"every target converged": {
			applyPlan: &storage.Plan{ID: 10, Deployment: "eu", Target: "bikeshare-001"},
			members: func(applyPlan *storage.Plan) []applyMember {
				return []applyMember{stepMember("bikeshare-001", applyPlan), stepMember("bikeshare-002", &storage.Plan{ID: 11, Deployment: "eu", Target: "bikeshare-002"})}
			},
			want: `msg="createStoredApply: queueing a multi-target rollout member by member: its plans change no table to step through"`,
		},
	} {
		t.Run(name, func(t *testing.T) {
			members := tc.members(tc.applyPlan)
			groups := buildSteps(t, tc.applyPlan, members...)
			var out bytes.Buffer
			logRolloutShape(slog.New(slog.NewTextHandler(&out, nil)), tc.applyPlan, "production", members, groups, false)
			assert.Contains(t, out.String(), tc.want)
		})
	}
}
