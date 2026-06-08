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
	model.errorMsg = msg.errorMsg
	model.currentVolume = msg.volume
	model.initialized = true

	fmt.Print(model.progressView())
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
