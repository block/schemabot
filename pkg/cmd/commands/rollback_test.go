package commands

import (
	"bytes"
	"encoding/json"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"

	"github.com/block/schemabot/pkg/apitypes"
	"github.com/block/schemabot/pkg/cmd/internal/templates"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// A declined rollback shows boxed target/source context and readable SQL under
// the target dialect, preserving quoted identifiers and values without applying.
func TestRollbackPreviewUsesTargetDialect(t *testing.T) {
	tests := []struct{ name, databaseType, sql, expected string }{
		{"mysql multiline", "mysql", "alter table orders add column note varchar(32), add column item_count int", "ALTER TABLE `orders`\n    ADD COLUMN `note` varchar(32),\n    ADD COLUMN `item_count` int;"},
		{"vitess", "vitess", "alter table orders add index idx_status(status)", "ALTER TABLE `orders` ADD INDEX `idx_status`(`status`);"},
		{"strata", "strata", "alter table orders add column note text", "ALTER TABLE `orders` ADD COLUMN `note` text;"},
		{"postgres", "postgres", `alter table "OrderHistory" add column "Label" text, add column payload jsonb`, "ALTER TABLE \"OrderHistory\"\n    ADD COLUMN \"Label\" text,\n    ADD COLUMN payload jsonb;"},
		{"mysql quoted values", "mysql", "\n  ALTER TABLE `INT`\n    ADD COLUMN `TEXT` varchar(64) DEFAULT 'KEEP INT, DEFAULT NULL';  \n", "ALTER TABLE `INT` ADD COLUMN `TEXT` varchar(64) DEFAULT 'KEEP INT, DEFAULT NULL';"},
		{"postgres literal comma", "postgres", "ALTER TABLE \"OrderHistory\"\n    ADD COLUMN \"Label\" text DEFAULT 'Keep INT, Case',\n    ADD COLUMN payload jsonb;", "ALTER TABLE \"OrderHistory\"\n    ADD COLUMN \"Label\" text DEFAULT 'Keep INT, Case',\n    ADD COLUMN payload jsonb;"},
		{"unknown dialect", "custom", "ALTER TABLE \"OrderHistory\"\n    ADD COLUMN \"Label\" text DEFAULT 'Keep Case';", "ALTER TABLE \"OrderHistory\"\n    ADD COLUMN \"Label\" text DEFAULT 'Keep Case';"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Setenv("SCHEMABOT_TOKEN", "fictional-preview-test")
			plan := apitypes.PlanResponse{Database: "shop", DatabaseType: tt.databaseType, Environment: "staging", Changes: []*apitypes.SchemaChangeResponse{{Namespace: "shop", TableChanges: []*apitypes.TableChangeResponse{{TableName: "orders", ChangeType: "alter", DDL: tt.sql}}}}}
			requests := make(chan string, 4)
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				select {
				case requests <- r.Method + " " + r.URL.Path:
				default:
					assert.Fail(t, "too many rollback preview requests", r.Method+" "+r.URL.Path)
					http.Error(w, "too many requests", http.StatusBadRequest)
					return
				}
				w.Header().Set("Content-Type", "application/json")
				switch r.Method + " " + r.URL.Path {
				case "POST /api/rollback/plan":
					assert.NoError(t, json.NewEncoder(w).Encode(plan))
				case "GET /api/status":
					assert.NoError(t, json.NewEncoder(w).Encode(apitypes.StatusResponse{}))
				default:
					http.Error(w, "unexpected request", http.StatusBadRequest)
				}
			}))
			t.Cleanup(server.Close)
			inputPath := filepath.Join(t.TempDir(), "confirmation")
			require.NoError(t, os.WriteFile(inputPath, []byte("no\n"), 0o600))
			input, err := os.Open(inputPath)
			require.NoError(t, err)
			original := os.Stdin
			os.Stdin = input
			t.Cleanup(func() {
				os.Stdin = original
				require.NoError(t, input.Close())
			})
			var runErr error
			output := captureStdout(func() {
				cmd := RollbackCmd{ApplyID: "apply-example-85", Environment: "staging", Watch: true}
				runErr = cmd.Run(t.Context(), &Globals{Endpoint: server.URL})
			})
			require.NoError(t, runErr)
			plain := stripAnsi(output)
			assert.Contains(t, plain, "Rollback Plan\n┌")
			assert.Contains(t, plain, "│  Database:      shop")
			assert.Contains(t, plain, "│  Environment:   staging")
			assert.Contains(t, plain, "│  Source apply:  apply-example-85")
			assert.Contains(t, plain, "    "+strings.ReplaceAll(tt.expected, "\n", "\n    "))
			assert.Contains(t, output, templates.ANSIBlue+"ALTER"+templates.ANSIReset)
			assert.Contains(t, plain, "Rollback cancelled.")
			require.Len(t, requests, 2)
			assert.Equal(t, "POST /api/rollback/plan", <-requests)
			assert.Equal(t, "GET /api/status", <-requests)
		})
	}
}

// newRollbackPreflightServer serves a rollback plan for shop in production and
// answers the active schema change preflight with the given status handler. It
// records each request's method and path so a test can show what a refused
// rollback never reached.
func newRollbackPreflightServer(t *testing.T, status http.HandlerFunc) (*[]string, string) {
	t.Helper()
	t.Setenv("SCHEMABOT_TOKEN", "fictional-rollback-preflight-test")
	plan := apitypes.PlanResponse{PlanID: "plan-rollback-1", Database: "shop", DatabaseType: "mysql", Environment: "production", Changes: []*apitypes.SchemaChangeResponse{{Namespace: "shop", TableChanges: []*apitypes.TableChangeResponse{{TableName: "orders", ChangeType: "alter", DDL: "ALTER TABLE `orders` ADD COLUMN `note` text"}}}}}
	var mu sync.Mutex
	var requests []string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		requests = append(requests, r.Method+" "+r.URL.Path)
		mu.Unlock()
		w.Header().Set("Content-Type", "application/json")
		switch r.Method + " " + r.URL.Path {
		case "POST /api/rollback/plan":
			assert.NoError(t, json.NewEncoder(w).Encode(plan))
		case "GET /api/status":
			status(w, r)
		default:
			http.Error(w, "unexpected request", http.StatusBadRequest)
		}
	}))
	t.Cleanup(server.Close)
	return &requests, server.URL
}

// A rollback is refused up front, with the same guidance apply gives, while a
// schema change on the target is queued, waiting, running, or cutting over. The
// operator is told what holds the target and how to watch it, before any plan,
// prompt, or lock.
func TestRollbackCmd_RefusedWhileASchemaChangeIsActive(t *testing.T) {
	cases := []struct {
		state   string
		wantMsg string
		cutover bool
	}{
		{state: "pending", wantMsg: "A schema change is queued and has not started yet."},
		{state: "waiting_for_deploy", wantMsg: "A schema change is waiting for deploy.", cutover: true},
		{state: "waiting_for_cutover", wantMsg: "A schema change is waiting for cutover.", cutover: true},
		{state: "running", wantMsg: "A schema change is already running."},
		{state: "cutting_over", wantMsg: "A schema change is currently cutting over."},
	}
	for _, tc := range cases {
		t.Run(tc.state, func(t *testing.T) {
			requests, endpoint := newRollbackPreflightServer(t, func(w http.ResponseWriter, _ *http.Request) {
				assert.NoError(t, json.NewEncoder(w).Encode(apitypes.StatusResponse{
					Applies: []*apitypes.ActiveApplyResponse{{ApplyID: "apply-busy", Database: "shop", Environment: "production", State: tc.state}},
				}))
			})

			var runErr error
			out := stripAnsi(captureStdout(func() {
				cmd := RollbackCmd{ApplyID: "apply-example-85", Environment: "production", AutoApprove: true}
				runErr = cmd.Run(t.Context(), &Globals{Endpoint: endpoint})
			}))

			require.Error(t, runErr)
			assert.Equal(t, "schema change already in progress", runErr.Error())
			assert.Contains(t, out, "⏳ Schema Change In Progress\n\nDatabase: shop\nEnvironment: production\n\n"+tc.wantMsg+"\n\n")
			assert.Contains(t, out, "To watch and manage: ")
			assert.Contains(t, out, "status apply-busy")
			if tc.cutover {
				assert.Contains(t, out, "cutover apply-busy -e production")
			} else {
				assert.NotContains(t, out, "To trigger cutover:")
			}
			assert.NotContains(t, out, "Rollback Plan", "the refusal comes before the plan is shown")
			assert.Equal(t, []string{"POST /api/rollback/plan", "GET /api/status"}, *requests, "nothing is locked or applied while the target is busy")
		})
	}
}

// A preflight that cannot run is not a refusal, for rollback as for apply: the
// command goes on to the plan and the prompt, since the server guards the same
// conflict when the apply is created, and the failed check is logged so the
// operator can see it did not run.
func TestRollbackCmd_ProceedsAndLogsWhenThePreflightFails(t *testing.T) {
	var logs bytes.Buffer
	previous := slog.Default()
	slog.SetDefault(slog.New(slog.NewTextHandler(&logs, &slog.HandlerOptions{Level: slog.LevelWarn})))
	t.Cleanup(func() { slog.SetDefault(previous) })

	requests, endpoint := newRollbackPreflightServer(t, func(w http.ResponseWriter, _ *http.Request) {
		http.Error(w, `{"error":"status unavailable"}`, http.StatusInternalServerError)
	})
	inputPath := filepath.Join(t.TempDir(), "confirmation")
	require.NoError(t, os.WriteFile(inputPath, []byte("no\n"), 0o600))
	input, err := os.Open(inputPath)
	require.NoError(t, err)
	original := os.Stdin
	os.Stdin = input
	t.Cleanup(func() {
		os.Stdin = original
		require.NoError(t, input.Close())
	})

	var runErr error
	out := stripAnsi(captureStdout(func() {
		cmd := RollbackCmd{ApplyID: "apply-example-85", Environment: "production"}
		runErr = cmd.Run(t.Context(), &Globals{Endpoint: endpoint})
	}))

	require.NoError(t, runErr)
	assert.Contains(t, out, "Rollback Plan")
	assert.Contains(t, out, "Rollback cancelled.")
	assert.Equal(t, []string{"POST /api/rollback/plan", "GET /api/status"}, *requests)
	assert.Contains(t, logs.String(), "could not check for an active schema change; proceeding, and the server will refuse the apply if a schema change holds the database")
	assert.Contains(t, logs.String(), "database=shop")
	assert.Contains(t, logs.String(), "environment=production")
}

// A rollback shows its one rollback plan, which is what every member it runs
// on runs: the server pairs each member of a mirrored environment with it, and
// refuses a rollout-wide rollback where members are planned on their own. So
// the CLI states that it showed the plan every member runs, and the server's
// rollout rendering gate does not turn away a rollback of a multi-member
// environment from a current CLI.
func TestRollbackCmd_ApplySaysItShowedThePlanEveryMemberRuns(t *testing.T) {
	t.Setenv("SCHEMABOT_TOKEN", "fictional-rollback-test")
	plan := apitypes.PlanResponse{PlanID: "plan-rollback-1", Database: "shop", DatabaseType: "mysql", Environment: "production", Changes: []*apitypes.SchemaChangeResponse{{Namespace: "shop", TableChanges: []*apitypes.TableChangeResponse{{TableName: "orders", ChangeType: "alter", DDL: "ALTER TABLE `orders` DROP COLUMN `note`"}}}}}
	applied := make(chan apitypes.ApplyRequest, 1)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch r.Method + " " + r.URL.Path {
		case "POST /api/rollback/plan":
			assert.NoError(t, json.NewEncoder(w).Encode(plan))
		case "GET /api/status":
			assert.NoError(t, json.NewEncoder(w).Encode(apitypes.StatusResponse{}))
		case "GET /api/locks/shop/mysql":
			http.Error(w, `{"error":"not found"}`, http.StatusNotFound)
		case "POST /api/locks/acquire":
			_, err := w.Write([]byte(`{"lock":{}}`))
			assert.NoError(t, err)
		case "POST /api/apply":
			var req apitypes.ApplyRequest
			assert.NoError(t, json.NewDecoder(r.Body).Decode(&req))
			applied <- req
			http.Error(w, `{"error":"recorded"}`, http.StatusInternalServerError)
		default:
			http.Error(w, "unexpected request", http.StatusBadRequest)
		}
	}))
	t.Cleanup(server.Close)

	var runErr error
	captureStdout(func() {
		cmd := RollbackCmd{ApplyID: "apply-example-85", Environment: "production", AutoApprove: true}
		runErr = cmd.Run(t.Context(), &Globals{Endpoint: server.URL})
	})
	require.Error(t, runErr, "the recording server fails the apply once it is requested")
	require.Len(t, applied, 1, "the rollback is applied: %v", runErr)
	req := <-applied
	assert.Equal(t, "plan-rollback-1", req.PlanID)
	assert.Empty(t, req.Target, "a rollback runs on every member the apply ran on")
	assert.True(t, req.RendersRollout, "the rollback says it showed the plan every member runs")
}
