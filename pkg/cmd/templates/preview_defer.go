package templates

import (
	"fmt"
	"strings"
	"time"

	"github.com/block/schemabot/pkg/state"
)

func previewDeferRunningOutput() {
	fmt.Println("Atomic mode (--defer-cutover): All tables copy rows, then cutover together")
	fmt.Println()

	data := ProgressData{
		State:     state.Apply.Running,
		Engine:    "Spirit",
		ApplyID:   "apply-a1b2c3d4e5f6",
		StartedAt: previewTime.Add(-8 * time.Minute).Format(time.RFC3339),
		Tables: []TableProgress{
			{
				TableName:       "orders",
				DDL:             "ALTER TABLE `orders` ADD INDEX `idx_user_status` (`user_id`, `status`)",
				RowsCopied:      1800000,
				RowsTotal:       2500000,
				PercentComplete: 72,
				ETASeconds:      195, // 3m 15s
			},
			{
				TableName:       "products",
				DDL:             "ALTER TABLE `products` ADD INDEX `idx_category` (`category`)",
				RowsCopied:      450000,
				RowsTotal:       1000000,
				PercentComplete: 45,
				ETASeconds:      380, // 6m 20s
			},
			{
				TableName:       "users",
				DDL:             "ALTER TABLE `users` ADD INDEX `idx_email_created` (`email`, `created_at`)",
				RowsCopied:      6400000,
				RowsTotal:       7200000,
				PercentComplete: 89,
				ETASeconds:      105, // 1m 45s
			},
		},
	}
	WriteProgress(data)

	fmt.Println("All tables copy rows simultaneously. Cutover happens atomically")
	fmt.Println("after all tables complete row copy.")
}

func previewDeferSingleOutput() {
	fmt.Println("Defer cutover: Single table waiting")
	fmt.Println("(--defer-cutover flag with single table)")
	fmt.Println()

	data := ProgressData{
		State:     state.Apply.WaitingForCutover,
		Engine:    "Spirit",
		ApplyID:   "apply-a1b2c3d4e5f6",
		StartedAt: previewTime.Add(-10 * time.Minute).Format(time.RFC3339),
		Tables: []TableProgress{
			{
				TableName: "users",
				DDL:       "ALTER TABLE `users` ADD INDEX `idx_email_created` (`email`, `created_at`)",
				Status:    state.Apply.WaitingForCutover,
			},
		},
	}
	WriteProgress(data)

	fmt.Println("Row copy complete. All data has been copied and new writes")
	fmt.Println("continue to be replicated to keep the shadow table in sync.")
	fmt.Println()
	fmt.Println("Press Enter to proceed with cutover (or Ctrl+C to detach): _")
}

func previewDeferWaitingOutput() {
	fmt.Println("Atomic mode (--defer-cutover): All tables waiting for cutover")
	fmt.Println()

	data := ProgressData{
		State:     state.Apply.WaitingForCutover,
		Engine:    "Spirit",
		ApplyID:   "apply-a1b2c3d4e5f6",
		StartedAt: previewTime.Add(-15 * time.Minute).Format(time.RFC3339),
		Tables: []TableProgress{
			{
				TableName: "orders",
				DDL:       "ALTER TABLE `orders` ADD INDEX `idx_user_status` (`user_id`, `status`)",
				Status:    state.Apply.WaitingForCutover,
			},
			{
				TableName: "products",
				DDL:       "ALTER TABLE `products` ADD INDEX `idx_category` (`category`)",
				Status:    state.Apply.WaitingForCutover,
			},
			{
				TableName: "users",
				DDL:       "ALTER TABLE `users` ADD INDEX `idx_email_created` (`email`, `created_at`)",
				Status:    state.Apply.WaitingForCutover,
			},
		},
	}
	WriteProgress(data)

	fmt.Println("Row copy complete for all tables. New writes continue to be")
	fmt.Println("replicated to keep shadow tables in sync.")
	fmt.Println()
	fmt.Println("Press Enter to proceed with cutover (or Ctrl+C to detach): _")
}

func previewDeferSeqWaitOutput() {
	fmt.Println("Defer cutover: Sequential mode, first complete, second waiting")
	fmt.Println("(--defer-cutover flag with sequential execution)")
	fmt.Println()

	data := ProgressData{
		State:     state.Apply.WaitingForCutover,
		Engine:    "Spirit",
		ApplyID:   "apply-a1b2c3d4e5f6",
		StartedAt: previewTime.Add(-12 * time.Minute).Format(time.RFC3339),
		Tables: []TableProgress{
			{
				TableName: "users",
				DDL:       seqDDLs[0].ddl,
				Status:    state.Apply.Completed,
			},
			{
				TableName: "orders",
				DDL:       seqDDLs[1].ddl,
				Status:    state.Apply.WaitingForCutover,
			},
			{
				TableName: "products",
				DDL:       seqDDLs[2].ddl,
				Status:    state.Apply.Pending,
			},
		},
	}
	WriteProgress(data)

	fmt.Println("Row copy complete for current table. Press Enter to cutover")
	fmt.Println("and start the next table (or Ctrl+C to detach): _")
}

func previewDeferStoppedOutput() {
	fmt.Println("Defer cutover: Stopped by user (s)")
	fmt.Println("(all tables show stopped state in atomic mode)")
	fmt.Println()

	// In defer-cutover (atomic) mode, when stopped ALL tables show as stopped
	// because they all share the same atomic state - no individual completion
	data := ProgressData{
		State:     state.Apply.Stopped,
		Engine:    "Spirit",
		ApplyID:   "apply-a1b2c3d4e5f6",
		StartedAt: previewTime.Add(-8 * time.Minute).Format(time.RFC3339),
		Tables: []TableProgress{
			{
				TableName:       "orders",
				DDL:             "ALTER TABLE `orders` ADD INDEX `idx_user_status` (`user_id`, `status`)",
				Status:          state.Apply.Stopped,
				RowsCopied:      1800000,
				RowsTotal:       2500000,
				PercentComplete: 72,
			},
			{
				TableName:       "products",
				DDL:             "ALTER TABLE `products` ADD INDEX `idx_category` (`category`)",
				Status:          state.Apply.Stopped,
				RowsCopied:      450000,
				RowsTotal:       1000000,
				PercentComplete: 45,
			},
			{
				TableName:       "users",
				DDL:             "ALTER TABLE `users` ADD INDEX `idx_email_created` (`email`, `created_at`)",
				Status:          state.Apply.Stopped,
				RowsCopied:      6400000,
				RowsTotal:       7200000,
				PercentComplete: 89,
			},
		},
	}
	WriteProgress(data)

	fmt.Println("Use 'schemabot start --apply-id <apply_id>' to resume.")
}

func previewDeferDetachedOutput() {
	fmt.Println("Defer cutover: Detached state (user pressed Ctrl+C)")
	fmt.Println()
	fmt.Println("You have detached from the progress view.")
	fmt.Println()
	fmt.Println("The schema change is still running in the background.")
	fmt.Println("Row copy is complete and the shadow table is kept in sync.")
	fmt.Println()
	fmt.Println("To check status:")
	fmt.Println("  schemabot status <apply_id>")
	fmt.Println()
	fmt.Println("To proceed with cutover:")
	fmt.Println("  schemabot cutover --apply-id <apply_id>")
	fmt.Println()
	fmt.Println("To abort the schema change:")
	fmt.Println("  schemabot stop --apply-id <apply_id>")
}

func previewDeferCuttingOutput() {
	fmt.Println("Defer cutover: Cutting over in progress")
	fmt.Println("(After user pressed Enter or ran `schemabot cutover`)")
	fmt.Println()

	data := ProgressData{
		State:     state.Apply.CuttingOver,
		Engine:    "Spirit",
		ApplyID:   "apply-a1b2c3d4e5f6",
		StartedAt: previewTime.Add(-15 * time.Minute).Format(time.RFC3339),
		Tables: []TableProgress{
			{TableName: "orders", DDL: "ALTER TABLE `orders` ADD INDEX `idx_user_status` (`user_id`, `status`)", Status: state.Apply.CuttingOver},
			{TableName: "products", DDL: "ALTER TABLE `products` ADD INDEX `idx_category` (`category`)", Status: state.Apply.CuttingOver},
			{TableName: "users", DDL: "ALTER TABLE `users` ADD INDEX `idx_email_created` (`email`, `created_at`)", Status: state.Apply.CuttingOver},
		},
	}
	WriteProgress(data)

	fmt.Println("Cutover in progress. This typically completes within seconds.")
	fmt.Println("Tables are being renamed atomically...")
}

func previewDeferAllOutput() {
	sections := []struct {
		name string
		fn   func()
	}{
		{"ATOMIC MODE RUNNING", previewDeferRunningOutput},
		{"SINGLE TABLE WAITING", previewDeferSingleOutput},
		{"MULTIPLE TABLES WAITING (ATOMIC)", previewDeferWaitingOutput},
		{"SEQUENTIAL MODE WAITING", previewDeferSeqWaitOutput},
		{"STOPPED BY USER", previewDeferStoppedOutput},
		{"DETACHED STATE", previewDeferDetachedOutput},
		{"CUTTING OVER", previewDeferCuttingOutput},
	}

	for i, s := range sections {
		if i > 0 {
			fmt.Println()
		}
		fmt.Println("---", s.name, strings.Repeat("-", 50-len(s.name)))
		fmt.Println()
		s.fn()
	}
}

// =============================================================================
// Apply Watch Mode Previews
// =============================================================================
