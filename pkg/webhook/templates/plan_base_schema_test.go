package templates

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func baseSchemaPlan(env string, base *BaseSchemaData) PlanCommentData {
	return PlanCommentData{
		Database: "testapp", Environment: env, IsMySQL: true,
		Changes: []KeyspaceChangeData{{
			Keyspace:   "testapp",
			Statements: []string{"CREATE TABLE `orders` (`id` bigint NOT NULL, PRIMARY KEY (`id`))"},
		}},
		BaseSchema: base,
	}
}

// A PR branched off its base before the base retargeted the environment's
// schema link. Its plan diffs the old schema, so the comment says the branch
// is missing the newer change, names the path, and says apply is blocked, all
// before the DDL the reviewer would otherwise take at face value.
func TestRenderPlanCommentWarnsBranchMissingBaseSchemaChanges(t *testing.T) {
	out := RenderPlanComment(baseSchemaPlan("staging", &BaseSchemaData{
		BaseBranch:   "main",
		ChangedPaths: []string{"schema/staging"},
	}))

	notice := "⚠️ **This branch is missing newer changes from `main` to `schema/staging`.** This plan could undo them, and apply is blocked until the branch has them. Merge `main` into this branch to plan again."
	require.Contains(t, out, notice)
	assert.Less(t, strings.Index(out, notice), strings.Index(out, "CREATE TABLE"), "the notice leads the DDL it qualifies")
}

// Every changed path is named, so a reviewer sees both a retargeted link and
// an edited schema directory.
func TestRenderPlanCommentNamesEveryChangedBaseSchemaPath(t *testing.T) {
	out := RenderPlanComment(baseSchemaPlan("staging", &BaseSchemaData{
		BaseBranch:   "main",
		ChangedPaths: []string{"schema/base", "schema/staging"},
	}))

	assert.Contains(t, out, "from `main` to `schema/base`, `schema/staging`.**")
}

// When GitHub cannot answer, the comment says the check did not run rather
// than staying silent, and that apply checks again before it runs.
func TestRenderPlanCommentSaysBaseSchemaCouldNotBeChecked(t *testing.T) {
	out := RenderPlanComment(baseSchemaPlan("staging", &BaseSchemaData{BaseBranch: "main", Unverified: true}))

	assert.Contains(t, out, "⚠️ **Could not check whether `main` has newer changes to this schema.** Apply checks again before it runs.")
	assert.NotContains(t, out, "is missing newer changes")
}

// A check that could not even read the PR has no base branch to name, so the
// notice names it generically.
func TestRenderPlanCommentBaseSchemaWithoutBranchName(t *testing.T) {
	out := RenderPlanComment(baseSchemaPlan("staging", &BaseSchemaData{Unverified: true}))

	assert.Contains(t, out, "⚠️ **Could not check whether the base branch has newer changes to this schema.**")
}

// A branch that has every base-branch change, and a plan that never checked,
// render no notice.
func TestRenderPlanCommentNoBaseSchemaNoticeWhenCurrent(t *testing.T) {
	for name, base := range map[string]*BaseSchemaData{
		"current":     {BaseBranch: "main"},
		"not checked": nil,
	} {
		t.Run(name, func(t *testing.T) {
			out := RenderPlanComment(baseSchemaPlan("staging", base))
			assert.NotContains(t, out, "is missing newer changes")
			assert.NotContains(t, out, "Could not check whether")
		})
	}
}

// In a multi-environment comment the notice belongs to the environment whose
// schema the base branch changed, so two environments with the same DDL but
// different freshness are not merged under one header.
func TestRenderMultiEnvPlanCommentScopesBaseSchemaNoticeToItsEnvironment(t *testing.T) {
	staging := baseSchemaPlan("staging", &BaseSchemaData{BaseBranch: "main", ChangedPaths: []string{"schema/staging"}})
	production := baseSchemaPlan("production", &BaseSchemaData{BaseBranch: "main"})
	require.False(t, plansIdentical(&staging, &production), "a stale environment must not dedupe with a current one")

	out := RenderMultiEnvPlanComment(MultiEnvPlanCommentData{
		Database:     "testapp",
		Environments: []string{"staging", "production"},
		Plans:        map[string]*PlanCommentData{"staging": &staging, "production": &production},
	})

	assert.Equal(t, 1, strings.Count(out, "is missing newer changes from `main` to `schema/staging`"))
	stagingAt, productionAt := strings.Index(out, "Staging"), strings.Index(out, "Production")
	require.NotEqual(t, -1, stagingAt)
	require.NotEqual(t, -1, productionAt)
	noticeAt := strings.Index(out, "is missing newer changes")
	assert.True(t, stagingAt < noticeAt && noticeAt < productionAt, "the notice renders in the staging section")
}
