package templates

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
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
