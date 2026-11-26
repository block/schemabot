package templates

import (
	"slices"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/block/schemabot/pkg/apitypes"
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
