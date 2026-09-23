package templates

import (
	"fmt"
	"strings"
	"testing"

	"github.com/block/schemabot/pkg/presentation"
	"github.com/block/schemabot/pkg/state"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// greenfieldCreate returns a CREATE TABLE of the size a first plan for a
// service typically carries per table: a handful of columns and two indexes.
func greenfieldCreate(prefix string, i int) string {
	return fmt.Sprintf("CREATE TABLE `%s_%03d` (\n"+
		"  `id` bigint unsigned NOT NULL AUTO_INCREMENT,\n"+
		"  `tenant` varchar(64) NOT NULL,\n"+
		"  `external_ref` varchar(255) NOT NULL,\n"+
		"  `state` varchar(32) NOT NULL DEFAULT 'pending',\n"+
		"  `payload` json DEFAULT NULL,\n"+
		"  `attempts` int unsigned NOT NULL DEFAULT '0',\n"+
		"  `amount_cents` bigint NOT NULL DEFAULT '0',\n"+
		"  `currency` char(3) NOT NULL DEFAULT 'USD',\n"+
		"  `metadata` json DEFAULT NULL,\n"+
		"  `deleted_at` datetime(6) DEFAULT NULL,\n"+
		"  `lease_expires_at` datetime(6) DEFAULT NULL,\n"+
		"  `created_at` datetime(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),\n"+
		"  `updated_at` datetime(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6) ON UPDATE CURRENT_TIMESTAMP(6),\n"+
		"  PRIMARY KEY (`id`),\n"+
		"  UNIQUE KEY `uq_%s_%03d_ref` (`tenant`,`external_ref`),\n"+
		"  KEY `idx_%s_%03d_work` (`state`,`lease_expires_at`,`created_at`)\n"+
		") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci", prefix, i, prefix, i, prefix, i)
}

func greenfieldStatements(prefix string, n int) []string {
	statements := make([]string, 0, n)
	for i := range n {
		statements = append(statements, greenfieldCreate(prefix, i))
	}
	return statements
}

func greenfieldPlan(env, prefix string, tables int) PlanCommentData {
	return PlanCommentData{
		Database:     "ledger",
		Environment:  env,
		DatabaseType: "mysql",
		IsMySQL:      true,
		Changes:      []KeyspaceChangeData{{Keyspace: "ledger_" + env, Statements: greenfieldStatements(prefix, tables)}},
	}
}

func greenfieldTables(prefix string, n int, status string) []TableProgressData {
	tables := make([]TableProgressData, 0, n)
	for i := range n {
		tables = append(tables, TableProgressData{
			Namespace: "ledger_production",
			TableName: fmt.Sprintf("%s_%03d", prefix, i),
			DDL:       greenfieldCreate(prefix, i),
			Status:    status,
		})
	}
	return tables
}

// A first plan for a service creates every table at once. Forty-one tables of
// ordinary width render well over 32 KiB of DDL, and a reviewer needs every one
// of them: the comment gives the DDL the room the rest of the comment leaves,
// so nothing is cut while the body stays under GitHub's limit.
func TestPlanCommentRendersAGreenfieldPlanWithoutTruncation(t *testing.T) {
	const tables = 41
	body := RenderPlanComment(greenfieldPlan("production", "events", tables))

	assert.NotContains(t, body, ddlTruncatedMarker)
	assert.LessOrEqual(t, len(body), commentBodyLimit)
	assert.Greater(t, len(body), 36000, "the plan is larger than a half-limit DDL share would carry")
	for i := range tables {
		assert.Contains(t, body, fmt.Sprintf("CREATE TABLE `events_%03d`", i))
	}
}

// When the DDL alone exceeds what a comment can hold, it is cut to exactly the
// room the rest of the comment leaves: the body lands within a few bytes of the
// limit rather than at a fixed share of it, and carries the truncation marker so
// the reader knows where the rest lives.
func TestPlanCommentDDLFillsTheCommentBeforeItIsCut(t *testing.T) {
	body := RenderPlanComment(greenfieldPlan("production", "events", 200))

	assert.Equal(t, 1, strings.Count(body, ddlTruncatedMarker))
	assert.LessOrEqual(t, len(body), commentBodyLimit)
	assert.Greater(t, len(body), commentBodyLimit-256, "the DDL takes the room the comment leaves, not a fixed share")
	assert.Contains(t, body, "schemabot apply -e production", "the sections after the DDL still render")
}

// A multi-environment plan shares one DDL budget across its environments, so
// two large, differing plans render under the limit together instead of each
// taking a full comment's worth of DDL.
func TestMultiEnvPlanCommentSharesOneDDLBudgetAcrossEnvironments(t *testing.T) {
	staging := greenfieldPlan("staging", "staging_events", 100)
	production := greenfieldPlan("production", "events", 100)
	body := RenderMultiEnvPlanComment(MultiEnvPlanCommentData{
		Database:     "ledger",
		DatabaseType: "mysql",
		IsMySQL:      true,
		Environments: []string{"staging", "production"},
		Plans:        map[string]*PlanCommentData{"staging": &staging, "production": &production},
	})

	assert.LessOrEqual(t, len(body), commentBodyLimit)
	assert.Equal(t, 2, strings.Count(body, ddlTruncatedMarker), "each environment's block is cut to its share")
	assert.Contains(t, body, "### Staging")
	assert.Contains(t, body, "### Production")
	assert.Contains(t, body, "CREATE TABLE `staging_events_000`")
	assert.Contains(t, body, "CREATE TABLE `events_000`")
}

// Identical plans render once under a combined header, and the single section
// they share is budgeted as one block, so a greenfield plan that fits in one
// environment's comment also fits when two environments agree on it.
func TestMultiEnvPlanCommentBudgetsIdenticalPlansAsOneSection(t *testing.T) {
	staging := greenfieldPlan("staging", "events", 41)
	production := greenfieldPlan("production", "events", 41)
	staging.Changes[0].Keyspace, production.Changes[0].Keyspace = "ledger", "ledger"
	body := RenderMultiEnvPlanComment(MultiEnvPlanCommentData{
		Database:     "ledger",
		DatabaseType: "mysql",
		IsMySQL:      true,
		Environments: []string{"staging", "production"},
		Plans:        map[string]*PlanCommentData{"staging": &staging, "production": &production},
	})

	assert.Contains(t, body, "### Staging & Production")
	assert.NotContains(t, body, ddlTruncatedMarker)
	assert.LessOrEqual(t, len(body), commentBodyLimit)
}

// Whether two environments' plans are the same is decided on the whole plans,
// not on what a comment has room to show of them: two greenfield plans that
// agree for far more DDL than a comment holds and differ in their last table
// render as two sections, each cut to fit, rather than as one combined section
// that silently drops the environment that differs.
func TestMultiEnvPlanCommentKeepsPlansApartThatDifferPastTheCut(t *testing.T) {
	staging := greenfieldPlan("staging", "events", 200)
	production := greenfieldPlan("production", "events", 200)
	staging.Changes[0].Keyspace, production.Changes[0].Keyspace = "ledger", "ledger"
	statements := production.Changes[0].Statements
	last := len(statements) - 1
	statements[last] = strings.Replace(statements[last], "`currency` char(3)", "`currency` char(4)", 1)
	require.NotEqual(t, staging.Changes[0].Statements[last], statements[last])
	require.Greater(t, len(RenderPlanComment(staging)), commentBodyLimit-256, "each plan alone overfills a comment")

	body := RenderMultiEnvPlanComment(MultiEnvPlanCommentData{
		Database:     "ledger",
		DatabaseType: "mysql",
		IsMySQL:      true,
		Environments: []string{"staging", "production"},
		Plans:        map[string]*PlanCommentData{"staging": &staging, "production": &production},
	})

	assert.NotContains(t, body, "### Staging & Production")
	assert.Contains(t, body, "### Staging")
	assert.Contains(t, body, "### Production")
	assert.LessOrEqual(t, len(body), commentBodyLimit)
}

// An apply comment leaves a fixed reserve under the limit for the sections the
// poster appends after rendering — the rejected-command notice and, on a
// failure, the recent-logs fold — so a large apply cannot crowd them out.
func TestApplyCommentsLeaveRoomForAppendedSections(t *testing.T) {
	limit := commentBodyLimit - applyCommentAppendReserve

	t.Run("progress", func(t *testing.T) {
		body := RenderApplyStatusComment(ApplyStatusCommentData{
			Database:    "ledger",
			Environment: "production",
			Engine:      "Spirit",
			State:       state.Apply.Running,
			Tables:      greenfieldTables("events", 200, state.Task.Running),
		})
		assert.LessOrEqual(t, len(body), limit)
		assert.Greater(t, len(body), limit-256, "the DDL takes the room the reserve leaves")
		assert.Contains(t, body, ddlTruncatedMarker)
	})

	t.Run("failed summary", func(t *testing.T) {
		body := RenderApplySummaryComment(ApplyStatusCommentData{
			Database:     "ledger",
			Environment:  "production",
			Engine:       "Spirit",
			State:        state.Apply.Failed,
			ErrorMessage: "Error 1205: Lock wait timeout exceeded",
			Tables:       greenfieldTables("events", 200, state.Task.Failed),
		})
		assert.LessOrEqual(t, len(body), limit)
		assert.Greater(t, len(body), limit-256, "the DDL takes the room the reserve leaves")
		assert.Contains(t, body, "To retry:", "the footer after the table list still renders")
	})

	t.Run("a greenfield apply renders every table's DDL", func(t *testing.T) {
		body := RenderApplySummaryComment(ApplyStatusCommentData{
			Database:    "ledger",
			Environment: "production",
			Engine:      "Spirit",
			State:       state.Apply.Completed,
			Tables:      greenfieldTables("events", 41, state.Task.Completed),
		})
		assert.NotContains(t, body, ddlTruncatedMarker)
		assert.LessOrEqual(t, len(body), limit)
	})
}

// A rollout across several deployments renders one <details> body per
// deployment inside a single comment, so the deployments share one DDL budget:
// the whole comment stays under the limit however many deployments carry DDL.
func TestMultiDeploymentApplyCommentsShareOneDDLBudget(t *testing.T) {
	withTemplateTimestamp(t, "2026-06-16 19:43:00 UTC")
	limit := commentBodyLimit - applyCommentAppendReserve
	details := map[string]ApplyStatusCommentData{}
	ops := make([]presentation.Operation, 0, 3)
	for _, dep := range []string{"us", "eu", "ap"} {
		ops = append(ops, continuingOp(dep, so.Completed))
		details[dep] = ApplyStatusCommentData{
			Database:    "payments_" + dep,
			Environment: "production",
			Engine:      "Spirit",
			State:       state.Apply.Completed,
			Tables:      greenfieldTables(dep+"_events", 60, state.Task.Completed),
		}
	}
	data := MultiDeploymentApplyData{
		Model:       presentation.Derive(ops),
		ApplyID:     "apply-123",
		Environment: "production",
		Details:     details,
	}

	summary := RenderMultiDeploymentApplySummaryComment(data)
	assert.LessOrEqual(t, len(summary), limit)
	assert.Greater(t, len(summary), limit-512, "the deployments together take the room the reserve leaves")
	for _, dep := range []string{"us", "eu", "ap"} {
		assert.Contains(t, summary, fmt.Sprintf("CREATE TABLE `%s_events_000`", dep), "every deployment renders its share")
	}

	progress := RenderMultiDeploymentApplyComment(data)
	assert.LessOrEqual(t, len(progress), limit)
}

// The fit loop offers the DDL the whole limit first, then cuts it by exactly
// what the rest of the comment turned out to need, so a comment fits in two
// passes and the DDL keeps every byte the other sections leave.
func TestRenderWithinCommentLimitCutsDDLByTheOvershoot(t *testing.T) {
	const chrome = 1000
	ddl := strings.Repeat("CREATE TABLE t (id int);\n", commentBodyLimit/len("CREATE TABLE t (id int);\n")+1)
	passes := 0
	body := renderWithinCommentLimit(1, 0, func(budget *ddlBlockBudget) string {
		passes++
		var sb strings.Builder
		sb.WriteString(strings.Repeat("h", chrome))
		writeSQLFencedBlock(&sb, ddl, budget)
		return sb.String()
	})

	require.Equal(t, 2, passes)
	assert.LessOrEqual(t, len(body), commentBodyLimit)
	assert.Greater(t, len(body), commentBodyLimit-64, "only the overshoot was cut")
	assert.True(t, strings.HasSuffix(body, ddlTruncatedMarker))

	t.Run("a comment that fits renders once", func(t *testing.T) {
		passes := 0
		body := renderWithinCommentLimit(1, 0, func(budget *ddlBlockBudget) string {
			passes++
			var sb strings.Builder
			writeSQLFencedBlock(&sb, "CREATE TABLE t (id int);", budget)
			return sb.String()
		})
		assert.Equal(t, 1, passes)
		assert.NotContains(t, body, ddlTruncatedMarker)
	})

	t.Run("the reserve is kept free", func(t *testing.T) {
		body := renderWithinCommentLimit(1, 4096, func(budget *ddlBlockBudget) string {
			var sb strings.Builder
			writeSQLFencedBlock(&sb, ddl, budget)
			return sb.String()
		})
		assert.LessOrEqual(t, len(body), commentBodyLimit-4096)
		assert.Greater(t, len(body), commentBodyLimit-4096-64)
	})
}
