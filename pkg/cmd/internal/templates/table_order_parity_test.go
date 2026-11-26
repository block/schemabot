package templates

import (
	"slices"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/block/schemabot/pkg/apitypes"
	"github.com/block/schemabot/pkg/presentation"
	"github.com/block/schemabot/pkg/state"
	"github.com/block/schemabot/pkg/storage"
	webhooktemplates "github.com/block/schemabot/pkg/webhook/templates"
)

// listedOrder is the tables, by name, in the order out first mentions them.
func listedOrder(out string, tables []string) []string {
	order := slices.Clone(tables)
	slices.SortStableFunc(order, func(a, b string) int {
		return strings.Index(out, a) - strings.Index(out, b)
	})
	return order
}

// An apply where orders failed and payments and refunds have not started
// lists orders first on every surface, ahead of the stopped, running, queued,
// and finished tables, and the two queued tables keep their order. The
// terminal and the PR comment list the same tables in the same order (UX-5).
func TestFailedTableLeadsTheListOnEverySurface(t *testing.T) {
	table := func(name, status string) *apitypes.TableProgressResponse {
		return &apitypes.TableProgressResponse{
			TableName: name, DDL: "ALTER TABLE " + name + " ADD COLUMN note text", ChangeType: "alter", Status: status,
		}
	}
	// The tables arrive in neither the order they should be listed in nor the
	// order of their states.
	resp := &apitypes.ProgressResponse{
		ApplyID: "apply-5e6f7a8b", Database: "shop", Environment: "staging",
		State: state.Apply.Running, Engine: storage.EngineSpirit,
		Tables: []*apitypes.TableProgressResponse{
			table("payments", state.Task.Pending),
			table("refunds", state.Task.Pending),
			table("users", state.Task.Completed),
			table("sessions", state.Task.Running),
			table("invoices", state.Task.Stopped),
			table("orders", state.Task.Failed),
		},
	}
	names := []string{"payments", "refunds", "users", "sessions", "invoices", "orders"}
	want := []string{"orders", "invoices", "sessions", "payments", "refunds", "users"}

	cli := ansiEscape.ReplaceAllString(captureStdout(t, func() { WriteProgress(ParseProgressResponse(resp)) }), "")
	pr := webhooktemplates.RenderApplyStatusComment(webhooktemplates.ApplyStatusFromProgress(resp, "octocat"))

	require.Equal(t, want, listedOrder(cli, names), "CLI")
	require.Equal(t, want, listedOrder(pr, names), "PR comment")
	for surface, out := range map[string]string{"CLI": cli, "PR comment": pr} {
		for _, name := range names {
			assert.Contains(t, out, name, "%s lists %s", surface, name)
		}
	}
}

// A rollout across two targets where orders failed on the first target and is
// queued on the second, while sessions is copying on both, lists orders first
// in both the terminal's and the PR comment's rollup, ahead of the copying,
// queued, and finished tables (UX-5).
func TestFailedTableLeadsARolloutOnEverySurface(t *testing.T) {
	// statuses is each table's status on payments-001 and payments-002, in the
	// order the tables arrive.
	statuses := []struct {
		table    string
		onTarget [2]string
	}{
		{"invoices", [2]string{state.Task.Pending, state.Task.Pending}},
		{"users", [2]string{state.Task.Completed, state.Task.Completed}},
		{"sessions", [2]string{state.Task.Running, state.Task.Running}},
		{"orders", [2]string{state.Task.Failed, state.Task.Pending}},
	}
	names := []string{"invoices", "users", "sessions", "orders"}
	want := []string{"orders", "sessions", "invoices", "users"}

	cliData := ProgressData{ApplyID: "apply-7f3c", Database: "shop", Environment: "production", Engine: "Spirit", State: state.Apply.Running}
	var ops []presentation.Operation
	var details []*webhooktemplates.ApplyStatusCommentData
	for i, target := range []string{"payments-001", "payments-002"} {
		cliData.Operations = append(cliData.Operations, ProgressOperation{
			Deployment: "prod", Target: target, State: state.ApplyOperation.Running,
			CutoverPolicy: storage.CutoverPolicyParallel, OnFailure: storage.OnFailureContinue,
		})
		ops = append(ops, presentation.Operation{Deployment: "prod", Target: target, State: state.ApplyOperation.Running, Parallel: true, ContinueOnFailure: true})
		detail := &webhooktemplates.ApplyStatusCommentData{
			Database: strings.ReplaceAll(target, "-", "_"), State: state.Apply.Running, ApplyID: "apply-7f3c",
			Environment: "production", Engine: storage.EngineSpirit,
		}
		for _, s := range statuses {
			ddl := "ALTER TABLE " + s.table + " ADD COLUMN note text"
			cliData.Tables = append(cliData.Tables, TableProgress{
				Deployment: "prod", Target: target, Namespace: "shop", TableName: s.table,
				ChangeType: "alter", DDL: ddl, Status: s.onTarget[i],
			})
			detail.Tables = append(detail.Tables, webhooktemplates.TableProgressData{TableName: s.table, DDL: ddl, Status: s.onTarget[i]})
		}
		details = append(details, detail)
	}

	cli := renderRollout(t, cliData)
	pr := webhooktemplates.RenderMultiDeploymentApplyComment(webhooktemplates.MultiDeploymentApplyData{
		Model: presentation.Derive(ops), ApplyID: "apply-7f3c", Environment: "production", Details: details,
	})

	require.Equal(t, want, listedOrder(cli, names), "CLI")
	require.Equal(t, want, listedOrder(pr, names), "PR comment")
}
