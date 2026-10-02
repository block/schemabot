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

// A sharded apply's DDL draws on the same budget as a single-deployment apply's,
// so a wide keyspace changing many tables still leaves the reserve for the
// sections the poster appends, and the footer still renders.
func TestShardedApplyCommentsLeaveRoomForAppendedSections(t *testing.T) {
	limit := commentBodyLimit - applyCommentAppendReserve
	ks := ShardedKeyspace{Keyspace: "ledger_sharded"}
	for _, shard := range []string{"-80", "80-"} {
		ks.Shards = append(ks.Shards, ShardStatus{Shard: shard, Emoji: "❌", Label: "failed", State: state.ApplyOperation.Failed})
	}
	for _, table := range greenfieldTables("events", 200, state.Task.Failed) {
		ks.Tables = append(ks.Tables, ShardedTableStatus{Table: table.TableName, Status: state.Task.Failed})
		for _, shard := range ks.Shards {
			ks.Cells = append(ks.Cells, ShardCell{Shard: shard.Shard, Table: table.TableName, Statements: []string{table.DDL}})
		}
	}
	data := ShardedApplyData{
		State: state.Apply.Failed, Environment: "production", Database: "ledger", ApplyID: "apply-x",
		ErrorMessage: "Error 1205: Lock wait timeout exceeded",
		Keyspaces:    []ShardedKeyspace{ks},
	}

	for name, body := range map[string]string{
		"progress": RenderShardedApplyComment(data),
		"summary":  RenderShardedApplySummaryComment(data),
	} {
		t.Run(name, func(t *testing.T) {
			assert.LessOrEqual(t, len(body), limit)
			assert.Greater(t, len(body), limit-512, "the DDL takes the room the reserve leaves")
			assert.Contains(t, body, ddlTruncatedMarker)
			assert.Contains(t, body, "CREATE TABLE `events_000`", "the first table's DDL renders")
			assert.Contains(t, body, "To retry:", "the footer after the keyspace sections still renders")
		})
	}
}

// A rollout across several deployments renders one <details> body per
// deployment inside a single comment, so the deployments share one DDL budget:
// the whole comment stays under the limit however many deployments carry DDL.
func TestMultiDeploymentApplyCommentsShareOneDDLBudget(t *testing.T) {
	withTemplateTimestamp(t, "2026-06-16 19:43:00 UTC")
	limit := commentBodyLimit - applyCommentAppendReserve
	ops := make([]presentation.Operation, 0, 3)
	for _, dep := range []string{"us", "eu", "ap"} {
		ops = append(ops, continuingOp(dep, so.Completed))
	}
	model := presentation.Derive(ops)
	details := make([]*ApplyStatusCommentData, 0, len(model.Deployments))
	for _, d := range model.Deployments {
		details = append(details, &ApplyStatusCommentData{
			Database:    "payments_" + d.Deployment,
			Environment: "production",
			Engine:      "Spirit",
			State:       state.Apply.Completed,
			Tables:      greenfieldTables(d.Deployment+"_events", 60, state.Task.Completed),
		})
	}
	data := MultiDeploymentApplyData{
		Model:       model,
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

// A rollout whose deployments run different plans names each deployment's
// plan, so DDL an apply comment cuts in a deployment's section points at the
// command that prints that deployment's stored plan, starting with the cli
// name and scoped to the environment. A deployment whose plan the comment
// does not name keeps the schema-files marker.
func TestMultiDeploymentApplyCommentCutDDLNamesEachDeploymentsStoredPlan(t *testing.T) {
	withTemplateTimestamp(t, "2026-06-16 19:43:00 UTC")
	limit := commentBodyLimit - applyCommentAppendReserve
	ops := make([]presentation.Operation, 0, 3)
	for _, dep := range []string{"us", "eu", "ap"} {
		ops = append(ops, continuingOp(dep, so.Completed))
	}
	model := presentation.Derive(ops)
	planIDs := map[string]string{"us": "plan_us", "eu": "plan_eu"}
	details := make([]*ApplyStatusCommentData, 0, len(model.Deployments))
	for _, d := range model.Deployments {
		details = append(details, &ApplyStatusCommentData{
			Database:    "payments_" + d.Deployment,
			Environment: "production",
			Engine:      "Spirit",
			State:       state.Apply.Completed,
			Tables:      greenfieldTables(d.Deployment+"_events", 60, state.Task.Completed),
			PlanID:      planIDs[d.Deployment],
			CLIName:     "acme schemabot",
		})
	}
	data := MultiDeploymentApplyData{Model: model, ApplyID: "apply-123", Environment: "production", Details: details}

	for name, body := range map[string]string{
		"summary":  RenderMultiDeploymentApplySummaryComment(data),
		"progress": RenderMultiDeploymentApplyComment(data),
	} {
		t.Run(name, func(t *testing.T) {
			assert.LessOrEqual(t, len(body), limit)
			assert.Contains(t, body, "the full plan is available from the CLI with `acme schemabot list-plans -e production plan_us`.")
			assert.Contains(t, body, "the full plan is available from the CLI with `acme schemabot list-plans -e production plan_eu`.")
			assert.Contains(t, body, ddlTruncatedMarker, "the deployment with no named plan points at the schema files")
		})
	}
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

// renderPointedBlocks renders a comment of chrome bytes of non-DDL text and
// the given DDL blocks, each drawn from the stored plan so a block cut to fit
// carries the pointer marker, counting the passes the fit loop takes.
func renderPointedBlocks(chrome int, blocks []string, plan storedPlanRef) (body string, passes int) {
	body = renderWithinCommentLimit(len(blocks), 0, func(budget *ddlBlockBudget) string {
		passes++
		defer budget.pointAt(plan)()
		var sb strings.Builder
		sb.WriteString(strings.Repeat("h", chrome))
		for _, block := range blocks {
			writeSQLFencedBlock(&sb, block, budget)
		}
		return sb.String()
	})
	return body, passes
}

// repeatedDDL returns at least size bytes of DDL with no backtick run, so a
// block holding any prefix of it keeps the shortest fence.
func repeatedDDL(size int) string {
	const statement = "CREATE TABLE t (id int);\n"
	return strings.Repeat(statement, size/len(statement)+1)
}

// The second pass of the fit loop charges each block the pass cuts for the
// marker it will carry, however long its section's marker is, and charges no
// block twice for a marker it already carries. So a comment whose DDL is cut
// into several blocks fits on the second pass, and the DDL keeps the room the
// markers leave rather than a marker's worth per block besides.
func TestRenderWithinCommentLimitChargesEachCutBlockOneMarker(t *testing.T) {
	planID := storedPlanRef{environment: "production", id: "plan_" + strings.Repeat("7c41f9", 8)}
	pointer := planPointer("plan", planID, nil)
	require.Greater(t, len(pointer), len(ddlTruncatedMarker)+32, "the pointer marker outgrows the plain marker, so charging the plain one would undercharge")

	t.Run("blocks first cut on the second pass are charged the pointer marker", func(t *testing.T) {
		// Pass 1 offers every block the whole limit, so none is cut and none
		// carries a marker; the chrome then pushes the body over, and pass 2
		// cuts all of them.
		blocks := []string{
			repeatedDDL(commentBodyLimit / 8), repeatedDDL(commentBodyLimit / 8),
			repeatedDDL(commentBodyLimit / 8), repeatedDDL(commentBodyLimit / 8),
		}
		body, passes := renderPointedBlocks(commentBodyLimit/2+2048, blocks, planID)

		assert.Equal(t, 2, passes, "the second pass reserved room for every marker it wrote")
		assert.Equal(t, len(blocks), strings.Count(body, pointer))
		assert.LessOrEqual(t, len(body), commentBodyLimit)
	})

	t.Run("blocks already cut on the first pass are not charged again", func(t *testing.T) {
		// Every block is cut on pass 1 and carries its pointer marker there,
		// so pass 2 cuts the DDL by the overshoot alone.
		blocks := []string{
			repeatedDDL(commentBodyLimit), repeatedDDL(commentBodyLimit),
			repeatedDDL(commentBodyLimit), repeatedDDL(commentBodyLimit),
		}
		body, passes := renderPointedBlocks(1000, blocks, planID)

		assert.Equal(t, 2, passes)
		assert.Equal(t, len(blocks), strings.Count(body, pointer))
		assert.LessOrEqual(t, len(body), commentBodyLimit)
		assert.Greater(t, len(body), commentBodyLimit-len(pointer), "the DDL gave up only the overshoot, not a second marker's worth per block")
	})
}

// A comment whose sections carry markers of different lengths reserves, for
// each block the second pass cuts, the marker that block's own section writes.
// Here four blocks with no stored plan carry the schema-files marker and one
// block from a stored plan carries a pointer made long by the cli name. The
// cut DDL keeps the room only the long pointer needs, rather than giving up
// the long pointer's length for every block, and the comment still fits on
// the second pass.
func TestRenderWithinCommentLimitReservesEachBlocksOwnMarker(t *testing.T) {
	plan := storedPlanRef{cliName: strings.Repeat("w", 100), environment: "production", id: "plan_" + strings.Repeat("7c41f9", 8)}
	pointer := planPointer("plan", plan, nil)
	overReservation := 4 * (len(pointer) - len(ddlTruncatedMarker))
	require.Greater(t, overReservation, 256, "charging every block the long pointer would give up this much DDL")

	passes := 0
	block := repeatedDDL(commentBodyLimit / 10)
	body := renderWithinCommentLimit(5, 0, func(budget *ddlBlockBudget) string {
		passes++
		var sb strings.Builder
		sb.WriteString(strings.Repeat("h", commentBodyLimit/2+2048))
		for range 4 {
			writeSQLFencedBlock(&sb, block, budget)
		}
		restore := budget.pointAt(plan)
		writeSQLFencedBlock(&sb, block, budget)
		restore()
		return sb.String()
	})

	assert.Equal(t, 2, passes, "the second pass reserved room for every marker it wrote")
	assert.Equal(t, 4, strings.Count(body, ddlTruncatedMarker))
	assert.Equal(t, 1, strings.Count(body, pointer))
	assert.LessOrEqual(t, len(body), commentBodyLimit)
	assert.Greater(t, len(body), commentBodyLimit-overReservation/4, "the DDL kept the room the short markers leave")
}

// A comment whose second pass leaves a block a share too small to hold even an
// empty code fence writes that block's marker alone, so the second pass still
// fits: no block renders more DDL than its share.
func TestRenderWithinCommentLimitFitsWhenSharesCannotHoldAFence(t *testing.T) {
	const blocks, secondPassDDL = 10, 50
	require.Less(t, secondPassDDL/blocks, len("```sql\n```\n"), "the first second-pass share is too small for an empty fence")
	// Every block is cut on pass 1 and carries its marker there, so pass 2's
	// DDL budget is the limit less the chrome and the markers.
	chrome := commentBodyLimit - blocks*len(ddlTruncatedMarker) - secondPassDDL
	passes := 0
	body := renderWithinCommentLimit(blocks, 0, func(budget *ddlBlockBudget) string {
		passes++
		var sb strings.Builder
		sb.WriteString(strings.Repeat("h", chrome))
		for range blocks {
			writeSQLFencedBlock(&sb, repeatedDDL(commentBodyLimit), budget)
		}
		return sb.String()
	})

	assert.Equal(t, 2, passes, "the second pass fits")
	assert.LessOrEqual(t, len(body), commentBodyLimit)
	assert.Equal(t, blocks, strings.Count(body, ddlTruncatedMarker), "every block is still marked as cut")
	assert.True(t, strings.HasPrefix(body[chrome:], ddlTruncatedMarker), "the first block, whose share cannot hold a fence, is its marker alone")
}

// Environments whose plans are the same render as one section drawn from the
// first environment's plan, while each environment stored a plan of its own.
// A block the section cuts names the first environment's stored plan and says
// so, so another environment's operator knows the plan it names is not theirs
// and that theirs runs the same DDL.
func TestMultiEnvPlanCommentSharedSectionSaysWhosePlanItNames(t *testing.T) {
	staging := greenfieldPlan("staging", "events", 200)
	production := greenfieldPlan("production", "events", 200)
	staging.Changes[0].Keyspace, production.Changes[0].Keyspace = "ledger", "ledger"
	staging.PlanID, production.PlanID = "plan_staging1", "plan_production1"
	body := RenderMultiEnvPlanComment(MultiEnvPlanCommentData{
		Database:     "ledger",
		DatabaseType: "mysql",
		IsMySQL:      true,
		Environments: []string{"staging", "production"},
		Plans:        map[string]*PlanCommentData{"staging": &staging, "production": &production},
	})

	require.Contains(t, body, "### Staging & Production")
	assert.LessOrEqual(t, len(body), commentBodyLimit)
	assert.Contains(t, body, "the full staging plan is available from the CLI with `schemabot list-plans -e staging plan_staging1` (production runs the same DDL).")
	assert.Equal(t, 1, strings.Count(body, "_DDL truncated to fit GitHub's comment size limit; the full staging plan is available from the CLI with `schemabot list-plans -e staging plan_staging1` (production runs the same DDL)._\n"))
	assert.NotContains(t, body, "the full plan is available")
	assert.NotContains(t, body, ddlTruncatedMarker)

	// The environments the section also stands for read as prose however
	// many there are.
	for _, tc := range []struct {
		environments []string
		note         string
	}{
		{[]string{"staging", "production", "sandbox"}, "production and sandbox run the same DDL"},
		{[]string{"staging", "production", "sandbox", "demo"}, "production, sandbox and demo run the same DDL"},
	} {
		t.Run(strings.Join(tc.environments, "+"), func(t *testing.T) {
			budget := newDDLBlockBudget(1, commentBodyLimit)
			defer budget.shareAcross(tc.environments)()
			assert.Equal(t,
				"_DDL truncated to fit GitHub's comment size limit; the full staging plan is available from the CLI with `schemabot list-plans -e staging plan_staging1` ("+tc.note+")._\n",
				budget.pointerMarker(storedPlanRef{environment: "staging", id: "plan_staging1"}))
		})
	}
}

// A shared section that renders target groups cuts each group's DDL under a
// marker naming that group's stored plan, which holds only the group's
// targets, so the marker scopes the plan to the group rather than calling it
// the environment's full plan. A group of several stored its first member's
// plan, so the marker names that member and says the rest of the group runs
// the same DDL.
func TestMultiEnvPlanCommentSharedTargetGroupNamesItsTargetsPlan(t *testing.T) {
	withTargets := func(env, planID, memberPlanID string) PlanCommentData {
		plan := greenfieldPlan(env, "orders", 150)
		plan.PlanID = planID
		second := greenfieldPlan(env, "orders_eu", 150).Changes
		plan.Changes[0].Keyspace, second[0].Keyspace = "orders", "orders"
		plan.DeploymentDrift = &DeploymentDriftData{
			Computed: true, Clean: true, Independent: true,
			Deployments: []DeploymentDriftEntry{
				{Deployment: "primary", Target: "orders_1", Primary: true, Class: "planned"},
				{Deployment: "primary", Target: "orders_2", Class: "planned"},
				{Deployment: "primary", Target: "orders_3", Class: "planned"},
			},
			Plans: []DeploymentPlanGroup{
				{Members: []string{"primary/orders_1"}, Primary: true, Changes: plan.Changes},
				{Members: []string{"primary/orders_2", "primary/orders_3"}, Changes: second, PlanID: memberPlanID},
			},
		}
		return plan
	}
	staging := withTargets("staging", "plan_staging1", "plan_staging_member_2")
	production := withTargets("production", "plan_production1", "plan_production_member_2")
	body := RenderMultiEnvPlanComment(MultiEnvPlanCommentData{
		Database:     "orders",
		DatabaseType: "mysql",
		IsMySQL:      true,
		Environments: []string{"staging", "production"},
		Plans:        map[string]*PlanCommentData{"staging": &staging, "production": &production},
	})

	require.Contains(t, body, "### Staging & Production")
	assert.LessOrEqual(t, len(body), commentBodyLimit)
	group, primary, found := strings.Cut(body, "#### Target `primary/orders_1`")
	require.True(t, found, body)
	assert.Contains(t, group, "#### 2 of 3 targets\n\n`primary/orders_2`, `primary/orders_3`", "the larger group leads")
	assert.Contains(t, primary, "the full staging plan for this target is available from the CLI with `schemabot list-plans -e staging plan_staging1` (production runs the same DDL).")
	assert.Contains(t, group, "the full staging plan for `primary/orders_2` is available from the CLI with `schemabot list-plans -e staging plan_staging_member_2` (every target in this group runs the same DDL; production runs the same DDL).")
	assert.NotContains(t, body, "the full staging plan is available")
}

// DDL cut to fit the comment comes from a stored plan, so the marker under it
// names the command that prints that plan in full rather than sending the
// reader to the schema files, which hold the desired schema and not the
// statements the plan would run.
func TestPlanCommentCutDDLNamesTheStoredPlan(t *testing.T) {
	data := greenfieldPlan("production", "events", 200)
	data.PlanID = "plan_7c41f9"
	body := RenderPlanComment(data)

	assert.LessOrEqual(t, len(body), commentBodyLimit)
	assert.Equal(t, 1, strings.Count(body, "the full plan is available from the CLI with"))
	assert.Contains(t, body, "the full plan is available from the CLI with `schemabot list-plans -e production plan_7c41f9`.")
	assert.NotContains(t, body, ddlTruncatedMarker)
}

// A deployment whose operators run the CLI through a wrapper configures the
// wrapper's name, and the pointer under cut DDL starts with it, scoped to the
// stored plan's environment so the wrapper routes the lookup to the server
// that stored it. The PR-comment commands in the same comment keep
// "schemabot", the word the bot answers to. The marker's length depends on the
// name, and the comment still fits the limit with the longest name the config
// accepts.
func TestPlanCommentCutDDLStartsTheStoredPlanCommandWithTheCLIName(t *testing.T) {
	data := greenfieldPlan("staging", "events", 200)
	data.PlanID = "plan_7c41f9"
	data.CLIName = "acme schemabot"
	body := RenderPlanComment(data)

	assert.LessOrEqual(t, len(body), commentBodyLimit)
	assert.Contains(t, body, "the full plan is available from the CLI with `acme schemabot list-plans -e staging plan_7c41f9`.")
	assert.Contains(t, body, "schemabot apply -e staging", "the PR-comment command keeps the bot's trigger word")
	assert.NotContains(t, body, "acme schemabot apply", "a PR-comment command never takes the cli name")

	t.Run("a long cli name still fits", func(t *testing.T) {
		long := greenfieldPlan("production-us-east-and-west", "events", 200)
		long.PlanID = "plan_" + strings.Repeat("7c41f9", 8)
		long.CLIName = strings.Repeat("w", 100)
		body := RenderPlanComment(long)

		assert.LessOrEqual(t, len(body), commentBodyLimit)
		assert.Equal(t, 1, strings.Count(body, "the full plan is available from the CLI with `"+long.CLIName+" list-plans -e "+long.Environment+" "+long.PlanID+"`._\n"))
	})
}

// A plan whose DDL fits in full is shown in full, so the comment points at no
// stored plan: there is nothing it could not show.
func TestPlanCommentUncutDDLNamesNoStoredPlan(t *testing.T) {
	data := greenfieldPlan("production", "events", 41)
	data.PlanID = "plan_7c41f9"
	body := RenderPlanComment(data)

	assert.NotContains(t, body, "list-plans")
	assert.NotContains(t, body, ddlTruncatedMarker)
}

// A rollout whose targets run different plans renders each group's plan, and
// every member's plan is stored as its own row. When the comment cuts the DDL,
// each group's marker names the plan that group runs: the primary's group the
// reviewed plan, another group its first member's plan, and a group whose plan
// was not stored falls back to the schema files.
func TestPlanCommentCutTargetPlansNameEachGroupsStoredPlan(t *testing.T) {
	primary := greenfieldPlan("production", "orders", 150)
	primary.PlanID = "plan_reviewed"
	second := greenfieldPlan("production", "orders_eu", 150).Changes
	third := greenfieldPlan("production", "orders_ap", 150).Changes
	primary.DeploymentDrift = &DeploymentDriftData{
		Computed: true, Clean: true, Independent: true,
		Deployments: []DeploymentDriftEntry{
			{Deployment: "primary", Target: "orders_1", Primary: true, Class: "planned"},
			{Deployment: "primary", Target: "orders_2", Class: "planned"},
			{Deployment: "primary", Target: "orders_3", Class: "planned"},
		},
		Plans: []DeploymentPlanGroup{
			{Members: []string{"primary/orders_1"}, Primary: true, Changes: primary.Changes},
			{Members: []string{"primary/orders_2"}, Changes: second, PlanID: "plan_member_2"},
			{Members: []string{"primary/orders_3"}, Changes: third},
		},
	}
	body := RenderPlanComment(primary)
	assert.LessOrEqual(t, len(body), commentBodyLimit)

	first, rest, found := strings.Cut(body, "### Target `primary/orders_2`")
	require.True(t, found, body)
	middle, last, found := strings.Cut(rest, "### Target `primary/orders_3`")
	require.True(t, found, body)

	assert.Contains(t, first, "the full plan for this target is available from the CLI with `schemabot list-plans -e production plan_reviewed`.")
	assert.NotContains(t, first, "plan_member_2")
	assert.Contains(t, middle, "the full plan for this target is available from the CLI with `schemabot list-plans -e production plan_member_2`.")
	assert.NotContains(t, middle, "plan_reviewed")
	assert.Contains(t, last, ddlTruncatedMarker)
	assert.NotContains(t, last, "list-plans")
}

// A group of several targets stored one plan per member and renders its first
// member's, so a cut block's marker names that member as the owner of the plan
// it points at and says the rest of the group runs the same DDL. The primary's
// group names the reviewed plan, which is the primary member's own.
func TestPlanCommentCutTargetGroupNamesWhosePlanItPointsAt(t *testing.T) {
	primary := greenfieldPlan("production", "orders", 150)
	primary.PlanID = "plan_reviewed"
	second := greenfieldPlan("production", "orders_eu", 150).Changes
	primary.DeploymentDrift = &DeploymentDriftData{
		Computed: true, Clean: true, Independent: true,
		Deployments: []DeploymentDriftEntry{
			{Deployment: "primary", Target: "orders_1", Primary: true, Class: "planned"},
			{Deployment: "primary", Target: "orders_2", Class: "planned"},
			{Deployment: "primary", Target: "orders_3", Class: "planned"},
			{Deployment: "primary", Target: "orders_4", Class: "planned"},
		},
		Plans: []DeploymentPlanGroup{
			{Members: []string{"primary/orders_1", "primary/orders_2"}, Primary: true, Changes: primary.Changes},
			{Members: []string{"primary/orders_3", "primary/orders_4"}, Changes: second, PlanID: "plan_member_3"},
		},
	}
	body := RenderPlanComment(primary)
	assert.LessOrEqual(t, len(body), commentBodyLimit)

	first, rest, found := strings.Cut(body, "`primary/orders_3`, `primary/orders_4`")
	require.True(t, found, body)
	assert.Contains(t, first, "_DDL truncated to fit GitHub's comment size limit; the full plan for `primary/orders_1` is available from the CLI with `schemabot list-plans -e production plan_reviewed` (every target in this group runs the same DDL)._\n")
	assert.Contains(t, rest, "_DDL truncated to fit GitHub's comment size limit; the full plan for `primary/orders_3` is available from the CLI with `schemabot list-plans -e production plan_member_3` (every target in this group runs the same DDL)._\n")
	assert.NotContains(t, body, "for these targets")
	assert.NotContains(t, body, ddlTruncatedMarker)
}
