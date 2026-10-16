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
		state.Apply.Stopped,
		state.Apply.Cancelled,
		state.Apply.RevertWindow,
		state.Apply.SkippingRevert,
		state.Apply.Reverting,
		state.Apply.Reverted,
		"STATE_IDLE",
		"STATE_NO_ACTIVE_CHANGE",
	}

	fmt.Println("State Display Formatting:")
	fmt.Println()
	for _, s := range states {
		fmt.Printf("  %-30s → %s\n", s, FormatProgressState(s))
	}
}

// previewStatusApply builds a status row that started startedAgo before the
// preview time.
func previewStatusApply(id, database, environment, applyState, caller string, startedAgo time.Duration) ActiveApplyData {
	return ActiveApplyData{
		ApplyID:     id,
		Database:    database,
		Environment: environment,
		State:       applyState,
		Engine:      "Spirit",
		Caller:      caller,
		StartedAt:   previewTime.Add(-startedAgo).Format(time.RFC3339),
		UpdatedAt:   previewTime.Add(-30 * time.Second).Format(time.RFC3339),
	}
}

func previewStatusListOutput() {
	active := []ActiveApplyData{
		previewStatusApply("apply_abc123", "orders-db", "staging", state.Apply.Running, "github:octocat@acme/shop#412", 15*time.Minute),
		previewStatusApply("apply_def456", "users-db", "production", state.Apply.WaitingForCutover, "cli:jdoe@macbook.local", 45*time.Minute),
	}
	finished := []ActiveApplyData{
		previewStatusApply("apply_ghi789", "analytics", "staging", state.Apply.Stopped, "github:hubot@acme/analytics#87", 2*time.Hour),
		previewStatusApply("apply_jkl012", "orders-db", "staging", state.Apply.Completed, "github:octocat@acme/shop#415", 3*time.Hour),
		previewStatusApply("apply_mno345", "billing-db", "staging", state.Apply.Failed, "cli:jdoe@macbook.local", 5*time.Hour),
	}
	finishedCounts := map[string]int{state.Apply.Stopped: 1, state.Apply.Completed: 41, state.Apply.Failed: 2}

	WriteStatusList(StatusListData{
		Sectioned:   true,
		ActiveCount: len(active),
		Active:      active,
		Applies:     finished,
		StateCounts: finishedCounts,
		Limit:       3,
		MaxLimit:    1000,
		HasMore:     true,
	})

	fmt.Println()
	fmt.Println("Nothing active:")
	fmt.Println()
	WriteStatusList(StatusListData{Sectioned: true, Applies: finished[1:], StateCounts: map[string]int{state.Apply.Completed: 1, state.Apply.Failed: 1}})

	fmt.Println()
	fmt.Println("No applies at all:")
	fmt.Println()
	WriteStatusList(StatusListData{Sectioned: true})

	fmt.Println()
	fmt.Println("More finished applies than the server's cap:")
	fmt.Println()
	WriteStatusList(StatusListData{
		Sectioned:   true,
		ActiveCount: len(active),
		Active:      active,
		Applies:     finished[1:],
		StateCounts: finishedCounts,
		Limit:       2,
		MaxLimit:    2,
		HasMore:     true,
	})

	fmt.Println()
	fmt.Println("The next page, reached with --after apply_mno345, lists only finished applies:")
	fmt.Println()
	WriteStatusList(StatusListData{
		Sectioned: true,
		Applies: []ActiveApplyData{
			previewStatusApply("apply_pqr678", "orders-db", "staging", state.Apply.Completed, "github:octocat@acme/shop#409", 7*time.Hour),
			previewStatusApply("apply_stu901", "billing-db", "staging", state.Apply.Failed, "cli:jdoe@macbook.local", 9*time.Hour),
		},
		StateCounts: finishedCounts,
		Limit:       2,
		MaxLimit:    2,
		HasMore:     true,
		Continued:   true,
	})
}

func previewStatusDeploymentOutput() {
	WriteStatusList(StatusListData{
		Sectioned:      true,
		ActiveCount:    1,
		ShowExternalID: true,
		Deployment:     "us-east",
		Active: []ActiveApplyData{
			{
				ApplyID:             "apply-multi-a1b2c3d4",
				ExternalID:          "remote-apply-us-east-001",
				ExternalOperationID: "remote-op-us-east-001",
				Database:            "orders-db",
				Environment:         "production",
				Deployment:          "us-east",
				State:               state.Apply.WaitingForCutover,
				Engine:              "Spirit",
				Caller:              "github:octocat@acme/shop#412",
				StartedAt:           previewTime.Add(-8 * time.Minute).Format(time.RFC3339),
				UpdatedAt:           previewTime.Add(-30 * time.Second).Format(time.RFC3339),
			},
		},
	})

	fmt.Println()
	fmt.Println("Multiple matching operations:")
	fmt.Println()
	WriteStatusList(StatusListData{
		Sectioned:      true,
		ActiveCount:    1,
		ShowExternalID: true,
		Deployment:     "us-east",
		Active: []ActiveApplyData{
			{
				ApplyID:     "apply-sharded-d5e6f7g8",
				ExternalID:  "remote-apply-us-east-002",
				Database:    "inventory-db",
				Environment: "production",
				Deployment:  "us-east",
				State:       state.Apply.Running,
				Engine:      "Spirit",
				Caller:      "github:octocat@acme/shop#412",
				StartedAt:   previewTime.Add(-4 * time.Minute).Format(time.RFC3339),
				UpdatedAt:   previewTime.Add(-20 * time.Second).Format(time.RFC3339),
			},
		},
	})

	fmt.Println()
	fmt.Println("No data-plane apply id recorded:")
	fmt.Println()
	WriteStatusList(StatusListData{
		Sectioned:      true,
		ActiveCount:    1,
		ShowExternalID: true,
		Deployment:     "us-east",
		Active: []ActiveApplyData{
			{
				ApplyID:     "apply-pending-l3m4n5o6",
				Database:    "payments-db",
				Environment: "production",
				Deployment:  "us-east",
				State:       state.Apply.Pending,
				Engine:      "Spirit",
				Caller:      "github:octocat@acme/shop#412",
				StartedAt:   previewTime.Add(-1 * time.Minute).Format(time.RFC3339),
				UpdatedAt:   previewTime.Add(-10 * time.Second).Format(time.RFC3339),
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
				Caller:      "cli:jdoe@macbook.local",
				StartedAt:   previewTime.Add(-1 * time.Hour).Format(time.RFC3339),
				CompletedAt: previewTime.Add(-45 * time.Minute).Format(time.RFC3339),
			},
			{
				ApplyID:     "apply_def456",
				Environment: "staging",
				State:       state.Apply.Running,
				Engine:      "Spirit",
				Caller:      "github:octocat@acme/shop#42",
				StartedAt:   previewTime.Add(-15 * time.Minute).Format(time.RFC3339),
			},
			{
				ApplyID:     "apply_ghi789",
				Environment: "production",
				State:       state.Apply.Failed,
				Engine:      "Spirit",
				Caller:      "github:octocat@acme/shop#42",
				StartedAt:   previewTime.Add(-3 * time.Hour).Format(time.RFC3339),
				CompletedAt: previewTime.Add(-2*time.Hour - 30*time.Minute).Format(time.RFC3339),
				Error:       "lock timeout exceeded",
			},
			{
				ApplyID:     "apply_jkl012",
				Environment: "production",
				State:       state.Apply.Completed,
				Engine:      "Spirit",
				Caller:      "cli:jdoe@macbook.local",
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
