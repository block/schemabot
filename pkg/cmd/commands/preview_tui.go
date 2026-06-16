package commands

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sort"
	"strings"
	"sync/atomic"
	"time"

	"github.com/block/schemabot/pkg/apitypes"
	"github.com/block/schemabot/pkg/state"
	"github.com/block/schemabot/pkg/storage"
)

type tuiPreviewScenario struct {
	Name        string
	Description string
	ApplyID     string
	Progress    func(poll int64) apitypes.ProgressResponse
}

var tuiPreviewScenarios = map[string]tuiPreviewScenario{
	"tui_estimate_exceeded": {
		Name:        "tui_estimate_exceeded",
		Description: "Estimate-exceeded Spirit row copy",
		ApplyID:     "preview-estimate-exceeded",
		Progress: func(poll int64) apitypes.ProgressResponse {
			return apitypes.ProgressResponse{
				State:       state.Apply.Running,
				Engine:      "Spirit",
				ApplyID:     "preview-estimate-exceeded",
				Database:    "testapp",
				Environment: "staging",
				StartedAt:   time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC).Format(time.RFC3339),
				Tables: []*apitypes.TableProgressResponse{
					{
						TableName:       "users",
						DDL:             "ALTER TABLE `users` ADD INDEX `idx_email`(`email`);",
						ChangeType:      "alter",
						Status:          state.Task.Running,
						RowsCopied:      145000000 + poll*250000,
						RowsTotal:       100000000,
						PercentComplete: 145,
					},
				},
			}
		},
	},
	"tui_multi_deploy": {
		Name:        "tui_multi_deploy",
		Description: "Multi-deployment apply rollout",
		ApplyID:     "apply-multi-a1b2c3d4",
		Progress: func(poll int64) apitypes.ProgressResponse {
			return apitypes.ProgressResponse{
				State:       state.Apply.Running,
				Engine:      "Spirit",
				ApplyID:     "apply-multi-a1b2c3d4",
				Database:    "orders",
				Environment: "production",
				Caller:      "octocat",
				StartedAt:   time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC).Format(time.RFC3339),
				Operations: []*apitypes.ProgressOperationResponse{
					{Deployment: "us-east", Target: "orders-us-east", State: state.ApplyOperation.WaitingForCutover, CutoverPolicy: storage.CutoverPolicyBarrier, OnFailure: storage.OnFailureHalt},
					{Deployment: "eu-west", Target: "orders-eu-west", State: state.ApplyOperation.Running, CutoverPolicy: storage.CutoverPolicyBarrier, OnFailure: storage.OnFailureHalt},
					{Deployment: "ap-south", Target: "orders-ap-south", State: state.ApplyOperation.Pending, CutoverPolicy: storage.CutoverPolicyBarrier, OnFailure: storage.OnFailureHalt},
				},
				Tables: []*apitypes.TableProgressResponse{
					{Deployment: "us-east", TableName: "orders", ChangeType: "alter", DDL: "ALTER TABLE `orders` ADD COLUMN `source` varchar(32) DEFAULT NULL", Status: state.Task.WaitingForCutover, RowsCopied: 80000, RowsTotal: 80000, PercentComplete: 100},
					{Deployment: "eu-west", TableName: "orders", ChangeType: "alter", DDL: "ALTER TABLE `orders` ADD COLUMN `source` varchar(32) DEFAULT NULL", Status: state.Task.Running, RowsCopied: 42000 + poll*500, RowsTotal: 120000, PercentComplete: 35, ETASeconds: 240},
					{Deployment: "ap-south", TableName: "orders", ChangeType: "alter", DDL: "ALTER TABLE `orders` ADD COLUMN `source` varchar(32) DEFAULT NULL", Status: state.Task.Pending},
				},
			}
		},
	},
}

func previewTUI(name string, live bool) error {
	scenario, ok := tuiPreviewScenarios[name]
	if !ok {
		return fmt.Errorf("unknown TUI preview type: %s (available: %s)", name, strings.Join(tuiPreviewNames(), ", "))
	}
	if !live {
		return previewTUIStatic(scenario)
	}

	var poll atomic.Int64
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.HasPrefix(r.URL.Path, "/api/progress/apply/") {
			writePreviewJSON(w, scenario.Progress(poll.Add(1)))
			return
		}

		// Keep interactive controls from failing noisily if someone presses a key
		// while previewing. The fake scenario remains unchanged on the next poll.
		if r.Method == http.MethodPost {
			writePreviewJSON(w, apitypes.ControlResponse{
				Accepted:     false,
				ErrorMessage: "preview scenario does not mutate state",
			})
			return
		}

		http.NotFound(w, r)
	}))
	defer server.Close()

	fmt.Printf("Starting TUI preview: %s\n", scenario.Description)
	fmt.Println("Press Esc to exit.")
	fmt.Println()
	return watchApplyProgressByApplyIDWithEnvironment(server.URL, scenario.ApplyID, "staging", true)
}

func previewTUIStatic(scenario tuiPreviewScenario) error {
	progress := scenario.Progress(1)
	msg := parseProgressResult(&progress)

	model := NewWatchModel("", msg.database, msg.environment, true)
	model.applyID = msg.applyID
	model.database = msg.database
	model.environment = msg.environment
	model.engine = msg.engine
	model.metadata = msg.metadata
	model.state = msg.state
	model.tables = msg.tables
	model.operations = msg.operations
	model.errorMsg = msg.errorMsg
	model.currentVolume = msg.volume
	model.initialized = true

	fmt.Print(model.View())
	return nil
}

func tuiPreviewNames() []string {
	names := make([]string, 0, len(tuiPreviewScenarios))
	for name := range tuiPreviewScenarios {
		names = append(names, name)
	}
	sort.Strings(names)
	return names
}

func writePreviewJSON(w http.ResponseWriter, v any) {
	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(v); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
	}
}
