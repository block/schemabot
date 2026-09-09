package commands

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
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
		{"mysql quoted values", "mysql", "ALTER TABLE `INT`\n    ADD COLUMN `TEXT` varchar(64) DEFAULT 'KEEP INT, DEFAULT NULL';", "ALTER TABLE `INT`\n    ADD COLUMN `TEXT` varchar(64) DEFAULT 'KEEP INT, DEFAULT NULL';"},
		{"postgres literal comma", "postgres", "ALTER TABLE \"OrderHistory\"\n    ADD COLUMN \"Label\" text DEFAULT 'Keep INT, Case',\n    ADD COLUMN payload jsonb;", "ALTER TABLE \"OrderHistory\"\n    ADD COLUMN \"Label\" text DEFAULT 'Keep INT, Case',\n    ADD COLUMN payload jsonb;"},
		{"unknown dialect", "custom", "ALTER TABLE \"OrderHistory\"\n    ADD COLUMN \"Label\" text DEFAULT 'Keep Case';", "ALTER TABLE \"OrderHistory\"\n    ADD COLUMN \"Label\" text DEFAULT 'Keep Case';"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Setenv("SCHEMABOT_TOKEN", "fictional-preview-test")
			plan := apitypes.PlanResponse{Database: "shop", DatabaseType: tt.databaseType, Environment: "staging", Changes: []*apitypes.SchemaChangeResponse{{Namespace: "shop", TableChanges: []*apitypes.TableChangeResponse{{TableName: "orders", ChangeType: "alter", DDL: tt.sql}}}}}
			requests := make(chan string, 4)
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				requests <- r.Method + " " + r.URL.Path
				w.Header().Set("Content-Type", "application/json")
				switch r.Method + " " + r.URL.Path {
				case "POST /api/rollback/plan":
					require.NoError(t, json.NewEncoder(w).Encode(plan))
				case "GET /api/status":
					require.NoError(t, json.NewEncoder(w).Encode(apitypes.StatusResponse{}))
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
			output := captureStdout(func() {
				cmd := RollbackCmd{ApplyID: "apply-example-85", Environment: "staging", Watch: true}
				require.NoError(t, cmd.Run(&Globals{Endpoint: server.URL}))
			})
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
			assert.Equal(t, tt.sql, plan.Changes[0].TableChanges[0].DDL)
		})
	}
}
