package templates

import (
	"fmt"
	"time"

	"github.com/block/schemabot/pkg/state"
)

func previewStatesOutput() {
	// Show all state formatting
	states := []string{
		"STATE_PENDING",
		state.Apply.Running,
		state.Apply.WaitingForCutover,
		"STATE_CUTTING_OVER",
		state.Apply.Completed,
		state.Apply.Failed,
		"STATE_IDLE",
		"STATE_NO_ACTIVE_CHANGE",
	}

	fmt.Println("State Display Formatting:")
	fmt.Println()
	for _, s := range states {
		fmt.Printf("  %-30s → %s\n", s, FormatProgressState(s))
	}
}

func previewStatusListOutput() {
	WriteStatusList(StatusListData{
		ActiveCount: 3,
		Applies: []ActiveApplyData{
			{
				ApplyID:     "apply_abc123",
				Database:    "orders-db",
				Environment: "staging",
				State:       state.Apply.Running,
				Engine:      "Spirit",
				StartedAt:   previewTime.Add(-15 * time.Minute).Format(time.RFC3339),
				UpdatedAt:   previewTime.Add(-30 * time.Second).Format(time.RFC3339),
				Volume:      4,
			},
			{
				ApplyID:     "apply_def456",
				Database:    "users-db",
				Environment: "production",
				State:       state.Apply.WaitingForCutover,
				Engine:      "Spirit",
				StartedAt:   previewTime.Add(-45 * time.Minute).Format(time.RFC3339),
				UpdatedAt:   previewTime.Add(-1 * time.Minute).Format(time.RFC3339),
				Volume:      6,
			},
			{
				ApplyID:     "apply_ghi789",
				Database:    "analytics",
				Environment: "staging",
				State:       state.Apply.Stopped,
				Engine:      "Spirit",
				StartedAt:   previewTime.Add(-2 * time.Hour).Format(time.RFC3339),
				UpdatedAt:   previewTime.Add(-30 * time.Minute).Format(time.RFC3339),
			},
		},
	})

	fmt.Println()
	fmt.Println("No active applies:")
	fmt.Println()
	WriteStatusList(StatusListData{
		ActiveCount: 0,
		Applies:     nil,
	})
}

func previewStatusDeploymentOutput() {
	WriteStatusList(StatusListData{
		ActiveCount:    1,
		ShowExternalID: true,
		Deployment:     "us-east",
		Applies: []ActiveApplyData{
			{
				ApplyID:             "apply-multi-a1b2c3d4",
				ExternalOperationID: "remote-op-us-east-001",
				Database:            "orders-db",
				Environment:         "production",
				Deployment:          "us-east",
				State:               state.Apply.WaitingForCutover,
				Engine:              "Spirit",
				Caller:              "octocat",
				StartedAt:           previewTime.Add(-8 * time.Minute).Format(time.RFC3339),
				UpdatedAt:           previewTime.Add(-30 * time.Second).Format(time.RFC3339),
			},
		},
	})

	fmt.Println()
	fmt.Println("Multiple matching operations:")
	fmt.Println()
	WriteStatusList(StatusListData{
		ActiveCount:    1,
		ShowExternalID: true,
		Deployment:     "us-east",
		Applies: []ActiveApplyData{
			{
				ApplyID:     "apply-sharded-d5e6f7g8",
				Database:    "inventory-db",
				Environment: "production",
				Deployment:  "us-east",
				State:       state.Apply.Running,
				Engine:      "Spirit",
				Caller:      "octocat",
				StartedAt:   previewTime.Add(-4 * time.Minute).Format(time.RFC3339),
				UpdatedAt:   previewTime.Add(-20 * time.Second).Format(time.RFC3339),
			},
		},
	})
}

func previewStatusHistoryOutput() {
	WriteDatabaseHistory(DatabaseHistoryData{
		Database: "orders-db",
		Applies: []ApplyHistoryData{
			{
				ApplyID:     "apply_abc123",
				Environment: "staging",
				State:       state.Apply.Completed,
				Engine:      "Spirit",
				Caller:      "cli",
				StartedAt:   previewTime.Add(-1 * time.Hour).Format(time.RFC3339),
				CompletedAt: previewTime.Add(-45 * time.Minute).Format(time.RFC3339),
			},
			{
				ApplyID:     "apply_def456",
				Environment: "staging",
				State:       state.Apply.Running,
				Engine:      "Spirit",
				Caller:      "PR 42",
				StartedAt:   previewTime.Add(-15 * time.Minute).Format(time.RFC3339),
			},
			{
				ApplyID:     "apply_ghi789",
				Environment: "production",
				State:       state.Apply.Failed,
				Engine:      "Spirit",
				Caller:      "PR 42",
				StartedAt:   previewTime.Add(-3 * time.Hour).Format(time.RFC3339),
				CompletedAt: previewTime.Add(-2*time.Hour - 30*time.Minute).Format(time.RFC3339),
				Error:       "lock timeout exceeded",
			},
			{
				ApplyID:     "apply_jkl012",
				Environment: "production",
				State:       state.Apply.Completed,
				Engine:      "Spirit",
				Caller:      "cli",
				StartedAt:   previewTime.Add(-24 * time.Hour).Format(time.RFC3339),
				CompletedAt: previewTime.Add(-23*time.Hour - 30*time.Minute).Format(time.RFC3339),
			},
		},
	})

	fmt.Println()
	fmt.Println("Empty database:")
	fmt.Println()
	WriteDatabaseHistory(DatabaseHistoryData{
		Database: "new-db",
		Applies:  nil,
	})
}
