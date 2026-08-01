package api

import (
	"errors"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	ternv1 "github.com/block/schemabot/pkg/proto/ternv1"

	"github.com/block/schemabot/pkg/tern"
)

func reviewedUsersPlan(ddl string) *ternv1.PlanResponse {
	return &ternv1.PlanResponse{
		PlanId: "plan_eu",
		Engine: ternv1.Engine_ENGINE_SPIRIT,
		Changes: []*ternv1.SchemaChange{{
			Namespace: "testapp",
			TableChanges: []*ternv1.TableChange{{
				TableName:  "users",
				ChangeType: ternv1.ChangeType_CHANGE_TYPE_ALTER,
				Ddl:        ddl,
				Namespace:  "testapp",
			}},
		}},
	}
}

// When every deployment would plan the reviewed change, the rollup is clean and
// classifies the primary as its own baseline plus each deployment as a match.
func TestRollupReviewTimeDrift_CleanWhenAllMatch(t *testing.T) {
	ddl := "ALTER TABLE `users` ADD COLUMN `email` varchar(255)"
	eu := &mockTernClient{}
	us := &mockTernClient{planDiffResp: alterUsersDiff(ddl)}
	svc := twoDeploymentService(t, eu, us)

	rollup, err := svc.RollupReviewTimeDrift(t.Context(), planDiffReq(t), reviewedUsersPlan(ddl), "eu")
	require.NoError(t, err)
	assert.True(t, rollup.Clean, "matching deployments must roll up clean")
	require.Len(t, rollup.Entries, 2)
	assert.Equal(t, "eu", rollup.Entries[0].Deployment)
	assert.Equal(t, DeploymentMatch, rollup.Entries[0].Class)
	assert.Equal(t, "us", rollup.Entries[1].Deployment)
	assert.Equal(t, DeploymentMatch, rollup.Entries[1].Class)
}

// A non-primary deployment that would plan different DDL than was reviewed
// diverges, so the rollup is not clean and the diverging deployment is named.
func TestRollupReviewTimeDrift_DivergingDeploymentBlocks(t *testing.T) {
	eu := &mockTernClient{}
	us := &mockTernClient{planDiffResp: alterUsersDiff("ALTER TABLE `users` ADD COLUMN `phone` varchar(32)")}
	svc := twoDeploymentService(t, eu, us)

	reviewed := reviewedUsersPlan("ALTER TABLE `users` ADD COLUMN `email` varchar(255)")
	rollup, err := svc.RollupReviewTimeDrift(t.Context(), planDiffReq(t), reviewed, "eu")
	require.NoError(t, err)
	assert.False(t, rollup.Clean, "a diverging deployment must block the rollup")
	require.Len(t, rollup.Entries, 2)
	assert.Equal(t, DeploymentMatch, rollup.Entries[0].Class)
	assert.Equal(t, "us", rollup.Entries[1].Deployment)
	assert.Equal(t, DeploymentDiverged, rollup.Entries[1].Class)
}

// A deployment that fails to diff cannot be confirmed to match, so it is
// classified as errored and blocks the rollup rather than being treated as
// agreement.
func TestRollupReviewTimeDrift_UnreachableDeploymentBlocks(t *testing.T) {
	ddl := "ALTER TABLE `users` ADD COLUMN `email` varchar(255)"
	eu := &mockTernClient{}
	us := &mockTernClient{planDiffErr: errors.New("us unreachable")}
	svc := twoDeploymentService(t, eu, us)

	rollup, err := svc.RollupReviewTimeDrift(t.Context(), planDiffReq(t), reviewedUsersPlan(ddl), "eu")
	require.NoError(t, err)
	assert.False(t, rollup.Clean, "an unreachable deployment must block the rollup")
	require.Len(t, rollup.Entries, 2)
	assert.Equal(t, "us", rollup.Entries[1].Deployment)
	assert.Equal(t, DeploymentErrored, rollup.Entries[1].Class)
	require.Error(t, rollup.Entries[1].Err)
}

// A request for an unconfigured database cannot resolve deployment targets, so
// the rollup fails rather than silently reporting clean.
func TestRollupReviewTimeDrift_UnresolvedTargetsError(t *testing.T) {
	eu := &mockTernClient{}
	us := &mockTernClient{}
	svc := twoDeploymentService(t, eu, us)

	req := planDiffReq(t)
	req.Database = "unknown"
	_, err := svc.RollupReviewTimeDrift(t.Context(), req, reviewedUsersPlan("ALTER TABLE `users` ADD COLUMN `email` varchar(255)"), "eu")
	require.Error(t, err)
}

// A diverged deployment's warn log carries the comparator's diff so an operator
// can tell what the deployment would run that the reviewed plan does not say —
// table and operation per item, never the DDL body, capped with a "+N more"
// overflow so a badly drifted deployment cannot flood the log line.
func TestDriftDiffLogAttrs(t *testing.T) {
	diff := tern.ChangeSetDiff{
		MissingFromCandidate: []tern.ChangeSetDiffItem{
			{Namespace: "testapp", Table: "users", Operation: "alter_table", DDL: "ALTER TABLE `users` ADD COLUMN `email` varchar(255)"},
		},
		UnexpectedInCandidate: []tern.ChangeSetDiffItem{
			{Namespace: "testapp", Shard: "-80", Table: "orders", Operation: "create_table", DDL: "CREATE TABLE `orders` (`id` bigint)"},
		},
		MissingVSchema:    []string{"testapp"},
		UnexpectedVSchema: []string{"other"},
	}

	attrs := driftDiffLogAttrs(diff)
	require.Equal(t, []any{
		"diff_missing", []string{"testapp.users alter_table"},
		"diff_unexpected", []string{"testapp/-80.orders create_table"},
		"diff_missing_vschema", []string{"testapp"},
		"diff_unexpected_vschema", []string{"other"},
	}, attrs)
	for _, attr := range attrs {
		if items, ok := attr.([]string); ok {
			for _, item := range items {
				assert.NotContains(t, item, "ADD COLUMN", "diff log items must never carry DDL bodies")
			}
		}
	}
}

// Empty diff lists are omitted from the log attributes so a warn line only
// names the directions that actually differ.
func TestDriftDiffLogAttrs_OmitsEmptyLists(t *testing.T) {
	diff := tern.ChangeSetDiff{
		UnexpectedInCandidate: []tern.ChangeSetDiffItem{
			{Namespace: "testapp", Table: "orders", Operation: "drop_table"},
		},
	}

	assert.Equal(t, []any{
		"diff_unexpected", []string{"testapp.orders drop_table"},
	}, driftDiffLogAttrs(diff))
}

// A diff with more items than the log cap renders the first items and
// summarizes the remainder as "+N more" instead of flooding the log line.
func TestDriftDiffLogAttrs_CapsWithOverflow(t *testing.T) {
	var items []tern.ChangeSetDiffItem
	for i := range maxDriftDiffLogItems + 3 {
		items = append(items, tern.ChangeSetDiffItem{
			Namespace: "testapp",
			Table:     fmt.Sprintf("t%02d", i),
			Operation: "create_table",
		})
	}

	attrs := driftDiffLogAttrs(tern.ChangeSetDiff{MissingFromCandidate: items})
	require.Len(t, attrs, 2)
	require.Equal(t, "diff_missing", attrs[0])
	rendered, ok := attrs[1].([]string)
	require.True(t, ok)
	require.Len(t, rendered, maxDriftDiffLogItems+1)
	assert.Equal(t, "testapp.t00 create_table", rendered[0])
	assert.Equal(t, fmt.Sprintf("testapp.t%02d create_table", maxDriftDiffLogItems-1), rendered[maxDriftDiffLogItems-1])
	assert.Equal(t, "+3 more", rendered[maxDriftDiffLogItems])
}
