package templates

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const exemptLine = "ℹ️ Tables in namespace `app` exempt from the undeclared-table verdict (archive naming): `events_archive_2025_01`, `orders_archive_2024`"

func exemptPlanData() PlanCommentData {
	return PlanCommentData{
		Database: "testapp", Environment: "staging", IsMySQL: false,
		ExemptTables: []ExemptTablesData{{Namespace: "app", Tables: []string{"events_archive_2025_01", "orders_archive_2024"}, Reason: "archive naming"}},
	}
}

func TestRenderPlanComment_ExemptTablesDisclosed(t *testing.T) {
	data := exemptPlanData()
	data.Changes = []KeyspaceChangeData{{Keyspace: "app", Statements: []string{"CREATE TABLE app.users (id bigint)"}}}
	assert.Contains(t, RenderPlanComment(data), exemptLine)
}

func TestRenderPlanComment_ExemptTablesDisclosedPerNamespace(t *testing.T) {
	data := exemptPlanData()
	data.ExemptTables = append(data.ExemptTables, ExemptTablesData{Namespace: "audit", Tables: []string{"logs_archive_2023"}, Reason: "archive naming"})
	out := RenderPlanComment(data)
	assert.Contains(t, out, exemptLine)
	assert.Contains(t, out, "ℹ️ Tables in namespace `audit` exempt from the undeclared-table verdict (archive naming): `logs_archive_2023`")
}

func TestRenderPlanComment_ExemptTablesDisclosedOnNoChanges(t *testing.T) {
	out := RenderPlanComment(exemptPlanData())
	assert.Contains(t, out, "✅ **No schema changes detected**")
	assert.Contains(t, out, exemptLine)
}

// The exemption reason is engine prose rather than a catalog identifier, so it
// is kept to one line and its markdown delimiters are escaped instead of being
// wrapped in a code span.
func TestRenderPlanComment_ExemptTablesReasonStaysInline(t *testing.T) {
	data := exemptPlanData()
	data.ExemptTables[0].Reason = "archive\nnaming `*_archive_*`"
	out := RenderPlanComment(data)
	assert.Contains(t, out, "exempt from the undeclared-table verdict (archive naming \\`\\*\\_archive\\_\\*\\`): `events_archive_2025_01`")
}

func TestRenderPlanComment_NoExemptTablesNoDisclosure(t *testing.T) {
	out := RenderPlanComment(PlanCommentData{Database: "testapp", Environment: "staging"})
	assert.NotContains(t, out, "exempt from the undeclared-table verdict")
}

// A namespace entry that carries no tables is not a disclosure: the no-changes
// comment renders byte-for-byte as if the entry were absent, with no dangling
// blank line where the disclosure would have gone.
func TestRenderPlanComment_EmptyExemptGroupRendersNothing(t *testing.T) {
	without := PlanCommentData{Database: "testapp", Environment: "staging"}
	with := without
	with.ExemptTables = []ExemptTablesData{{Namespace: "app", Reason: "archive naming"}}
	assert.Equal(t, RenderPlanComment(without), RenderPlanComment(with))
}

// The apply-rejected comments share the plan summary, so they carry the same
// disclosure a plan comment does for the same result.
func TestRenderApplyRejected_ExemptTablesDisclosed(t *testing.T) {
	data := exemptPlanData()
	data.Changes = []KeyspaceChangeData{{Keyspace: "app", Statements: []string{"ALTER TABLE app.users DROP COLUMN nickname"}}}
	data.HasUnsafeChanges = true
	data.UnsafeChanges = []UnsafeChangeData{{Table: "users", Reason: "drop column", ChangeType: "drop"}}
	data.BlockedChanges = []BlockedChangeData{{Table: "users", Reason: "engine refuses"}}

	assert.Contains(t, RenderUnsafeChangesBlocked(data), exemptLine)
	assert.Contains(t, RenderBlockedChangesApplyRejected(data), exemptLine)
}

// The plan summary discloses exempt tables on both of its branches: alongside
// the change counts, and under the no-changes message when there is nothing
// to count.
func TestWritePlanSummary_ExemptTablesDisclosedOnBothBranches(t *testing.T) {
	data := exemptPlanData()

	var noChanges strings.Builder
	writePlanSummary(&noChanges, data, 0, 0)
	assert.Contains(t, noChanges.String(), "No schema changes detected")
	assert.Contains(t, noChanges.String(), exemptLine)

	var withChanges strings.Builder
	writePlanSummary(&withChanges, data, 1, 0)
	assert.Contains(t, withChanges.String(), "1 DDL statement")
	assert.Contains(t, withChanges.String(), exemptLine)
}

// When every environment exempted the same tables, the all-clean multi-env
// comment states the shared disclosure once rather than once per environment.
func TestRenderMultiEnvPlanComment_ExemptTablesIdenticalRenderedOnce(t *testing.T) {
	staging := exemptPlanData()
	production := exemptPlanData()
	production.Environment = "production"
	out := RenderMultiEnvPlanComment(MultiEnvPlanCommentData{
		Database: "testapp", Environments: []string{"staging", "production"},
		Plans: map[string]*PlanCommentData{"staging": &staging, "production": &production},
	})
	assert.Equal(t, 1, strings.Count(out, exemptLine))
	assert.NotContains(t, out, "**Staging**: tables in namespace")
}

// When environments exempted different live tables, the all-clean multi-env
// comment attributes each disclosure to its environment so a reviewer can tell
// which target holds which archive.
func TestRenderMultiEnvPlanComment_ExemptTablesDivergentPerEnv(t *testing.T) {
	staging := exemptPlanData()
	production := exemptPlanData()
	production.Environment = "production"
	production.ExemptTables = []ExemptTablesData{{Namespace: "app", Tables: []string{"orders_archive_2024"}, Reason: "archive naming"}}
	out := RenderMultiEnvPlanComment(MultiEnvPlanCommentData{
		Database: "testapp", Environments: []string{"staging", "production"},
		Plans: map[string]*PlanCommentData{"staging": &staging, "production": &production},
	})
	assert.Contains(t, out, "ℹ️ **Staging**: tables in namespace `app` exempt from the undeclared-table verdict (archive naming): `events_archive_2025_01`, `orders_archive_2024`")
	assert.Contains(t, out, "ℹ️ **Production**: tables in namespace `app` exempt from the undeclared-table verdict (archive naming): `orders_archive_2024`")
	assert.NotContains(t, out, exemptLine)
}

// Environments that exempted the same number of tables in the same namespace
// for the same reason still diverge when the table names differ, so the
// disclosure is attributed per environment rather than stated once.
func TestRenderMultiEnvPlanComment_ExemptTablesSameShapeDifferentNames(t *testing.T) {
	staging := exemptPlanData()
	production := exemptPlanData()
	production.Environment = "production"
	production.ExemptTables = []ExemptTablesData{{Namespace: "app", Tables: []string{"events_archive_2025_02", "orders_archive_2023"}, Reason: "archive naming"}}
	out := RenderMultiEnvPlanComment(MultiEnvPlanCommentData{
		Database: "testapp", Environments: []string{"staging", "production"},
		Plans: map[string]*PlanCommentData{"staging": &staging, "production": &production},
	})
	assert.Contains(t, out, "ℹ️ **Staging**: tables in namespace `app` exempt from the undeclared-table verdict (archive naming): `events_archive_2025_01`, `orders_archive_2024`")
	assert.Contains(t, out, "ℹ️ **Production**: tables in namespace `app` exempt from the undeclared-table verdict (archive naming): `events_archive_2025_02`, `orders_archive_2023`")
	assert.NotContains(t, out, exemptLine)
}

// Namespace entries without tables do not make the all-clean multi-env comment
// render a disclosure or the blank line that introduces one.
func TestRenderMultiEnvPlanComment_EmptyExemptGroupsRenderNothing(t *testing.T) {
	render := func(groups []ExemptTablesData) string {
		staging := PlanCommentData{Database: "testapp", Environment: "staging", ExemptTables: groups}
		production := PlanCommentData{Database: "testapp", Environment: "production", ExemptTables: groups}
		return RenderMultiEnvPlanComment(MultiEnvPlanCommentData{
			Database: "testapp", Environments: []string{"staging", "production"},
			Plans: map[string]*PlanCommentData{"staging": &staging, "production": &production},
		})
	}
	assert.Equal(t, render(nil), render([]ExemptTablesData{{Namespace: "app", Reason: "archive naming"}}))
}

// When at least one environment has changes, each environment renders its own
// section and the disclosure lands under that section, unattributed, next to
// the summary or no-changes message it qualifies.
func TestRenderMultiEnvPlanComment_ExemptTablesUnderEnvironmentSections(t *testing.T) {
	staging := exemptPlanData()
	staging.Changes = []KeyspaceChangeData{{Keyspace: "app", Statements: []string{"CREATE TABLE app.users (id bigint)"}}}
	production := exemptPlanData()
	production.Environment = "production"
	production.ExemptTables = []ExemptTablesData{{Namespace: "app", Tables: []string{"orders_archive_2024"}, Reason: "archive naming"}}
	out := RenderMultiEnvPlanComment(MultiEnvPlanCommentData{
		Database: "testapp", Environments: []string{"staging", "production"},
		Plans: map[string]*PlanCommentData{"staging": &staging, "production": &production},
	})

	stagingAt := strings.Index(out, "### Staging")
	productionAt := strings.Index(out, "### Production")
	require.Greater(t, productionAt, stagingAt)
	stagingSection := out[stagingAt:productionAt]
	productionSection := out[productionAt:]

	assert.Contains(t, stagingSection, "1 DDL statement")
	assert.Contains(t, stagingSection, exemptLine)
	assert.Contains(t, productionSection, "✅ **No schema changes detected**")
	assert.Contains(t, productionSection, "ℹ️ Tables in namespace `app` exempt from the undeclared-table verdict (archive naming): `orders_archive_2024`")
	assert.NotContains(t, out, "**Staging**: tables in namespace")
}

// An environment with nothing exempted alongside one that exempted tables is a
// divergence, so the disclosure is attributed rather than stated as shared.
func TestRenderMultiEnvPlanComment_ExemptTablesOnlyInOneEnv(t *testing.T) {
	staging := exemptPlanData()
	production := PlanCommentData{Database: "testapp", Environment: "production"}
	out := RenderMultiEnvPlanComment(MultiEnvPlanCommentData{
		Database: "testapp", Environments: []string{"staging", "production"},
		Plans: map[string]*PlanCommentData{"staging": &staging, "production": &production},
	})
	assert.Equal(t, 1, strings.Count(out, "exempt from the undeclared-table verdict"))
	assert.Contains(t, out, "ℹ️ **Staging**: tables in namespace `app`")
}
