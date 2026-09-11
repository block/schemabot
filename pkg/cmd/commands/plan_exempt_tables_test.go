package commands

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/block/schemabot/pkg/apitypes"
)

const cliExemptLine = "ℹ️  Tables in namespace app exempt from the undeclared-table verdict (archive naming): orders_archive_2024, events_archive_2025_01"

func exemptGroup(tables ...string) *apitypes.ExemptTablesResponse {
	return &apitypes.ExemptTablesResponse{Namespace: "app", Tables: tables, Reason: "archive naming"}
}

func createUsers() *apitypes.TableChangeResponse {
	return &apitypes.TableChangeResponse{DDL: "CREATE TABLE users (id BIGINT PRIMARY KEY)", ChangeType: "CREATE", TableName: "users"}
}

// The plan body discloses exempt tables under the summary when there are
// changes and under the no-changes message when there are none, in both the
// plan and apply layouts.
func TestWritePlanBody_ExemptTablesDisclosed(t *testing.T) {
	withChanges := planWithTablesAndEngine("postgres", createUsers())
	withChanges.ExemptTables = []*apitypes.ExemptTablesResponse{exemptGroup("orders_archive_2024", "events_archive_2025_01")}
	noChanges := &apitypes.PlanResponse{Engine: "postgres", ExemptTables: withChanges.ExemptTables}

	for name, isApply := range map[string]bool{"plan": false, "apply": true} {
		t.Run(name, func(t *testing.T) {
			out := stripAnsi(captureStdout(func() { writePlanBody(withChanges, isApply) }))
			summaryAt := strings.Index(out, "📋 Plan: 1 table to create")
			exemptAt := strings.Index(out, cliExemptLine)
			assert.Greater(t, exemptAt, summaryAt, "disclosure follows the summary:\n%s", out)

			out = stripAnsi(captureStdout(func() { writePlanBody(noChanges, isApply) }))
			noChangesAt := strings.Index(out, "No schema changes detected")
			exemptAt = strings.Index(out, cliExemptLine)
			assert.Greater(t, exemptAt, noChangesAt, "disclosure follows the no-changes message:\n%s", out)
		})
	}
}

// Groups without tables and nil entries are not disclosures: the body renders
// exactly as it does with no exempt tables at all.
func TestWritePlanBody_EmptyExemptGroupsRenderNothing(t *testing.T) {
	plain := planWithTablesAndEngine("postgres", createUsers())
	withEmpty := planWithTablesAndEngine("postgres", createUsers())
	withEmpty.ExemptTables = []*apitypes.ExemptTablesResponse{nil, {Namespace: "app", Reason: "archive naming"}}

	want := captureStdout(func() { writePlanBody(plain, false) })
	got := captureStdout(func() { writePlanBody(withEmpty, false) })
	assert.Equal(t, want, got)
	assert.NotContains(t, want, "exempt from the undeclared-table verdict")
}

// Two environments with the same DDL but different exempt tables are not the
// same plan to a reader, so they render as separate sections rather than one
// combined "Staging & Production" section.
func TestOutputMultiEnvPlanResult_ExemptTablesDivergeSections(t *testing.T) {
	staging := planWithTablesAndEngine("postgres", createUsers())
	staging.ExemptTables = []*apitypes.ExemptTablesResponse{exemptGroup("orders_archive_2024", "events_archive_2025_01")}
	production := planWithTablesAndEngine("postgres", createUsers())
	production.ExemptTables = []*apitypes.ExemptTablesResponse{exemptGroup("orders_archive_2024")}

	out := stripAnsi(captureStdout(func() {
		outputMultiEnvPlanResult(map[string]*apitypes.PlanResponse{"staging": staging, "production": production}, "testapp", "testapp")
	}))

	assert.NotContains(t, out, "Staging & Production")
	assert.Contains(t, out, cliExemptLine)
	assert.Contains(t, out, "ℹ️  Tables in namespace app exempt from the undeclared-table verdict (archive naming): orders_archive_2024\n")
}

// Identical DDL with identical exempt tables still collapses into one section,
// carrying the disclosure once.
func TestOutputMultiEnvPlanResult_ExemptTablesIdenticalCollapse(t *testing.T) {
	staging := planWithTablesAndEngine("postgres", createUsers())
	staging.ExemptTables = []*apitypes.ExemptTablesResponse{exemptGroup("orders_archive_2024")}
	production := planWithTablesAndEngine("postgres", createUsers())
	production.ExemptTables = []*apitypes.ExemptTablesResponse{exemptGroup("orders_archive_2024")}

	out := stripAnsi(captureStdout(func() {
		outputMultiEnvPlanResult(map[string]*apitypes.PlanResponse{"staging": staging, "production": production}, "testapp", "testapp")
	}))

	assert.Contains(t, out, "Staging & Production")
	assert.Equal(t, 1, strings.Count(out, "exempt from the undeclared-table verdict"))
}
