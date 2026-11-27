package templates

import (
	"fmt"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/block/schemabot/pkg/engine"
)

func refusedStatementsComment(problems map[string]RefusedStatements, errs map[string]string) string {
	return RenderMultiEnvPlanComment(MultiEnvPlanCommentData{
		Database:     "testapp",
		DatabaseType: "postgres",
		Environments: []string{"staging", "production"},
		Plans: map[string]*PlanCommentData{
			"production": {Database: "testapp", Environment: "production", DatabaseType: "postgres"},
		},
		Errors:       errs,
		PlanProblems: problems,
	})
}

// A plan refused over statements in the schema files lists every one of them,
// each with where it is, what to change and the statement's opening line in a
// SQL block, in place of the refusal's text; and its footer asks for a push,
// since re-running the plan on the same files is refused the same way.
func TestRenderMultiEnvPlanListsRefusedStatements(t *testing.T) {
	comment := refusedStatementsComment(map[string]RefusedStatements{
		"staging": {Problems: []engine.PlanProblem{
			{File: "testapp/accounts.sql", Line: 7, Message: "Comments are not managed. Set them on the database directly.", Statement: "COMMENT ON TABLE accounts IS 'People'"},
			{File: "testapp/orders.sql", Line: 12, Message: "Functions are not managed.", Statement: "CREATE FUNCTION set_updated_at() RETURNS trigger AS $$\nBEGIN\n  NEW.updated_at := now();\nEND;\n$$ LANGUAGE plpgsql"},
		}},
	}, map[string]string{"staging": "plan PostgreSQL schema: statement 3: statement kind not allowed"})

	assert.Contains(t, comment, "❌ **SchemaBot can't plan 2 statements in the schema files:**")
	assert.Contains(t, comment, "**`testapp/accounts.sql`, line 7:** Comments are not managed. Set them on the database directly.\n```sql\nCOMMENT ON TABLE accounts IS 'People'\n```\n",
		"the statement renders literally inside its SQL block")
	assert.Contains(t, comment, "**`testapp/orders.sql`, line 12:** Functions are not managed.\n```sql\nCREATE FUNCTION set_updated_at() RETURNS trigger AS $$ …\n```\n",
		"a multi-line statement shows its first line and marks the rest")
	assert.NotContains(t, comment, "NEW.updated_at", "the statement's body is not shown")
	assert.NotContains(t, comment, "statement kind not allowed", "the refusal's text gives way to the list")

	assert.Contains(t, comment, "⚠️ **Staging** plans once these statements change. Fix them in the schema files and push a commit.")
	assert.NotContains(t, comment, "schemabot plan -e staging", "a re-run is refused the same way")
	assert.NotContains(t, comment, "**Staging** failed to plan")
}

// An environment that failed for any other reason keeps its error text and its
// re-run, beside one whose statements were refused.
func TestRenderMultiEnvPlanKeepsReRunForOtherFailures(t *testing.T) {
	comment := RenderMultiEnvPlanComment(MultiEnvPlanCommentData{
		Database:     "testapp",
		DatabaseType: "postgres",
		Environments: []string{"staging", "production"},
		Plans:        map[string]*PlanCommentData{},
		Errors: map[string]string{
			"staging":    "refused",
			"production": "SchemaBot could not reach the production data plane.",
		},
		PlanProblems: map[string]RefusedStatements{
			"staging": {Problems: []engine.PlanProblem{{File: "testapp/orders.sql", Line: 3, Message: "Triggers are not managed.", Statement: "CREATE TRIGGER t BEFORE UPDATE ON orders"}}},
		},
	})
	assert.Contains(t, comment, "**Error:** SchemaBot could not reach the production data plane.")
	assert.Contains(t, comment, "⚠️ **Production** failed to plan. Resolve the error above and re-run:\n```\nschemabot plan -e production\n```")
	assert.Contains(t, comment, "⚠️ **Staging** plans once these statements change.")
}

// A file set with more refused statements than one section lists shows the
// first ones and counts the rest.
func TestRenderPlanProblemsCountsTheOnesItDoesNotList(t *testing.T) {
	var problems []engine.PlanProblem
	for i := range maxRenderedPlanProblems + 3 {
		problems = append(problems, engine.PlanProblem{File: "testapp/big.sql", Line: i + 1, Message: "Comments are not managed.", Statement: fmt.Sprintf("COMMENT ON COLUMN t.c%d IS 'x'", i)})
	}
	var sb strings.Builder
	assert.True(t, writePlanProblems(&sb, RefusedStatements{Problems: problems}, newPlanProblemBudget()))
	out := sb.String()

	assert.Contains(t, out, fmt.Sprintf("SchemaBot can't plan %d statements", maxRenderedPlanProblems+3))
	assert.Contains(t, out, fmt.Sprintf("line %d:", maxRenderedPlanProblems))
	assert.NotContains(t, out, fmt.Sprintf("line %d:", maxRenderedPlanProblems+1))
	assert.Contains(t, out, "…and 3 statements more.")
}

// Statements a data plane refused without listing them, to keep its answer
// bounded, count toward the total and the rest.
func TestRenderPlanProblemsCountsTheOnesTheDataPlaneLeftOut(t *testing.T) {
	var sb strings.Builder
	assert.True(t, writePlanProblems(&sb, RefusedStatements{
		Problems: []engine.PlanProblem{{File: "testapp/orders.sql", Line: 3, Message: "Triggers are not managed.", Statement: "CREATE TRIGGER t BEFORE UPDATE ON orders"}},
		Omitted:  5,
	}, newPlanProblemBudget()))
	out := sb.String()

	assert.Contains(t, out, "SchemaBot can't plan 6 statements in the schema files:")
	assert.Contains(t, out, "**`testapp/orders.sql`, line 3:** Triggers are not managed.")
	assert.Contains(t, out, "…and 5 statements more.")
}

// A statement too large for what is left of the budget is passed over, not a
// stopping point: the smaller ones after it are still listed.
func TestRenderPlanProblemsListsSmallerStatementsAfterOneThatDoesNotFit(t *testing.T) {
	budget := &planProblemBudget{remaining: 400}
	var sb strings.Builder
	assert.True(t, writePlanProblems(&sb, RefusedStatements{Problems: []engine.PlanProblem{
		{File: "testapp/big.sql", Line: 1, Message: strings.Repeat("m", 500), Statement: "CREATE TRIGGER big BEFORE UPDATE ON orders"},
		{File: "testapp/small.sql", Line: 2, Message: "Triggers are not managed.", Statement: "CREATE TRIGGER small BEFORE UPDATE ON orders"},
	}}, budget))
	out := sb.String()

	assert.Contains(t, out, "SchemaBot can't plan 2 statements in the schema files:")
	assert.NotContains(t, out, "testapp/big.sql")
	assert.Contains(t, out, "**`testapp/small.sql`, line 2:** Triggers are not managed.")
	assert.Contains(t, out, "…and 1 statement more.")
}

// Three environments share files that refuse 40 long statements. The first
// two list what fits the comment's budget, and the third, with no room left,
// shows its refusal text in place of a list that would name nothing.
func TestRenderPlanProblemsFallBackToTheRefusalTextWhenNoneFit(t *testing.T) {
	envs := []string{"staging", "sandbox", "production"}
	var problems []engine.PlanProblem
	for i := range 40 {
		problems = append(problems, engine.PlanProblem{
			File:      "testapp/columns.sql",
			Line:      i + 1,
			Message:   strings.Repeat("Comments are not managed. ", 20),
			Statement: fmt.Sprintf("COMMENT ON COLUMN orders.c%d IS '%s'", i, strings.Repeat("x", 100)),
		})
	}
	data := MultiEnvPlanCommentData{
		Database:     "testapp",
		DatabaseType: "postgres",
		Environments: envs,
		Plans:        map[string]*PlanCommentData{},
		Errors:       map[string]string{},
		PlanProblems: map[string]RefusedStatements{},
	}
	for _, env := range envs {
		data.Errors[env] = "40 statements in the schema files cannot be planned: COMMENT ON COLUMN is not managed"
		data.PlanProblems[env] = RefusedStatements{Problems: problems}
	}

	comment := RenderMultiEnvPlanComment(data)

	staging, rest, _ := strings.Cut(comment, "### Sandbox")
	sandbox, production, _ := strings.Cut(rest, "### Production")
	assert.Contains(t, staging, "SchemaBot can't plan 40 statements")
	assert.Contains(t, sandbox, "SchemaBot can't plan 40 statements")
	assert.NotContains(t, production, "SchemaBot can't plan", "a list with room for no statement is not shown")
	assert.Contains(t, production, "**Error:** 40 statements in the schema files cannot be planned: COMMENT ON COLUMN is not managed")
	assert.Contains(t, production, "**Production** plans once these statements change.")
}

func TestPlanProblemLocation(t *testing.T) {
	assert.Equal(t, "`app/orders.sql`, line 4", planProblemLocation(engine.PlanProblem{File: "app/orders.sql", Line: 4}))
	assert.Equal(t, "`app/orders.sql`", planProblemLocation(engine.PlanProblem{File: "app/orders.sql"}), "an unknown line is left out")
	assert.Equal(t, "Line 4", planProblemLocation(engine.PlanProblem{Line: 4}))
	assert.Empty(t, planProblemLocation(engine.PlanProblem{}))
	assert.Equal(t, "`` app/`odd`.sql ``", planProblemLocation(engine.PlanProblem{File: "app/`odd`.sql"}), "a backtick cannot close the path's code span")
}

func TestStatementExcerpt(t *testing.T) {
	assert.Equal(t, "CREATE TRIGGER t BEFORE UPDATE ON orders …", statementExcerpt("\n  CREATE TRIGGER t BEFORE UPDATE ON orders\n  FOR EACH ROW EXECUTE FUNCTION f()"))
	assert.Equal(t, "COMMENT ON TABLE t IS 'x'", statementExcerpt("COMMENT ON TABLE t IS 'x'"))
	assert.Empty(t, statementExcerpt("  \n "))

	long := "CREATE VIEW v AS SELECT " + strings.Repeat("column_name, ", 20) + "id FROM t"
	excerpt := statementExcerpt(long)
	assert.True(t, strings.HasSuffix(excerpt, " …"), excerpt)
	assert.LessOrEqual(t, len([]rune(excerpt)), maxPlanProblemExcerptRunes+2)
}

// SQL that quotes identifiers in backticks is fenced with a longer fence, so it
// cannot close its own block.
func TestRenderPlanProblemFencesBackticks(t *testing.T) {
	var sb strings.Builder
	writePlanProblem(&sb, engine.PlanProblem{File: "app/t.sql", Line: 1, Message: "Not managed.", Statement: "CREATE TRIGGER ```t``` ON `orders`"})
	assert.Contains(t, sb.String(), "````sql\nCREATE TRIGGER ```t``` ON `orders`\n````\n")
}

// Four environments that each refuse a full list of long statements share one
// budget: the comment stays inside GitHub's limit with every environment's
// heading and push guidance, and each list counts the statements it had no
// room to show.
func TestRenderPlanProblemsShareOneBudgetAcrossEnvironments(t *testing.T) {
	envs := []string{"dev", "staging", "sandbox", "production"}
	data := MultiEnvPlanCommentData{
		Database:     "testapp",
		DatabaseType: "postgres",
		Environments: envs,
		Plans:        map[string]*PlanCommentData{},
		Errors:       map[string]string{},
		PlanProblems: map[string]RefusedStatements{},
	}
	for _, env := range envs {
		var problems []engine.PlanProblem
		for i := range maxRenderedPlanProblems {
			problems = append(problems, engine.PlanProblem{
				File:      "schema/" + strings.Repeat("p", 43) + ".sql",
				Line:      i + 1,
				Message:   strings.Repeat("m", 500),
				Statement: "CREATE TRIGGER " + strings.Repeat("s", 200),
			})
		}
		data.Errors[env] = "25 statements in the schema files cannot be planned"
		data.PlanProblems[env] = RefusedStatements{Problems: problems}
	}

	comment := RenderMultiEnvPlanComment(data)

	fitted, replaced := FitGitHubComment(comment, "schemabot", "")
	assert.False(t, replaced, "the plan renders instead of the oversized-comment notice")
	assert.Equal(t, comment, fitted)
	for _, env := range envs {
		assert.Contains(t, comment, "### "+capitalizeFirst(env))
		assert.Contains(t, comment, "**"+capitalizeFirst(env)+"** plans once these statements change.")
	}
	listed := strings.Count(comment, "SchemaBot can't plan 25 statements")
	fellBack := strings.Count(comment, "**Error:** 25 statements in the schema files cannot be planned")
	assert.Positive(t, listed)
	assert.Equal(t, len(envs), listed+fellBack, "every environment lists its statements or, with no room left, shows its refusal text")
	assert.Contains(t, comment, "more.", "the statements left out for space are counted")
	assert.LessOrEqual(t, strings.Count(comment, "CREATE TRIGGER"), maxPlanProblemsBytes/500)
}
