package templates

import (
	"fmt"
	"strings"
	"time"

	"github.com/block/schemabot/pkg/state"
)

func previewProgressOutput() {
	// Sample progress with running state (single table - most common case)
	fmt.Println("Single table progress (default):")
	fmt.Println()

	data := ProgressData{
		State:     state.Apply.Running,
		Engine:    "Spirit",
		ApplyID:   "apply-a1b2c3d4e5f6",
		StartedAt: previewTime.Add(-5 * time.Minute).Format(time.RFC3339),
		Tables: []TableProgress{
			{
				TableName:       "users",
				DDL:             "ALTER TABLE `users` ADD INDEX `idx_email_created` (`email`, `created_at`)",
				RowsCopied:      3500000,
				RowsTotal:       7200000,
				PercentComplete: 48,
				ETASeconds:      330, // 5m 30s
			},
		},
	}
	WriteProgress(data)
}

func previewWaitingForCutoverOutput() {
	data := ProgressData{
		State:     state.Apply.WaitingForCutover,
		Engine:    "Spirit",
		ApplyID:   "apply-a1b2c3d4e5f6",
		StartedAt: previewTime.Add(-10 * time.Minute).Format(time.RFC3339),
		Tables: []TableProgress{
			{
				TableName: "order_items",
				DDL:       "ALTER TABLE `order_items` ADD INDEX `idx_product_id` (`product_id`)",
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

	fmt.Println("Row copy complete. All data has been copied and new writes")
	fmt.Println("continue to be replicated to keep the shadow table in sync.")
	fmt.Println()
	fmt.Println("Press Enter to proceed with cutover (or Ctrl+C to detach): _")
	fmt.Println()
	fmt.Println("--- If detached, user sees: ---")
	fmt.Println()
	fmt.Println("Row copy complete. All data has been copied and new writes")
	fmt.Println("continue to be replicated to keep the shadow table in sync.")
	fmt.Println()
	fmt.Println("To proceed: schemabot cutover --apply-id <apply_id>")
	fmt.Println("Watching for cutover... (Ctrl+C to detach)")
}

func previewCuttingOverOutput() {
	data := ProgressData{
		State:     state.Apply.CuttingOver,
		Engine:    "Spirit",
		ApplyID:   "apply-a1b2c3d4e5f6",
		StartedAt: previewTime.Add(-12 * time.Minute).Format(time.RFC3339),
		Tables: []TableProgress{
			{
				TableName: "order_items",
				DDL:       "ALTER TABLE `order_items` ADD INDEX `idx_product_id` (`product_id`)",
				Status:    state.Apply.CuttingOver,
			},
			{
				TableName: "users",
				DDL:       "ALTER TABLE `users` ADD INDEX `idx_email_created` (`email`, `created_at`)",
				Status:    state.Apply.CuttingOver,
			},
		},
	}
	WriteProgress(data)
}

func previewCompletedOutput() {
	data := ProgressData{
		State:       state.Apply.Completed,
		Engine:      "Spirit",
		ApplyID:     "apply-a1b2c3d4e5f6",
		StartedAt:   previewTime.Add(-12 * time.Minute).Format(time.RFC3339),
		CompletedAt: previewTime.Add(-30 * time.Second).Format(time.RFC3339),
		Tables: []TableProgress{
			{
				TableName: "order_items",
				DDL:       "ALTER TABLE `order_items` ADD INDEX `idx_product_id` (`product_id`)",
				Status:    state.Apply.Completed,
			},
			{
				TableName: "users",
				DDL:       "ALTER TABLE `users` ADD INDEX `idx_email_created` (`email`, `created_at`)",
				Status:    state.Apply.Completed,
			},
		},
	}
	WriteProgress(data)
	fmt.Println("✓ Apply complete!")
}

func previewFailedOutput() {
	// Sample progress with failed state
	startedAt := previewTime.Add(-8 * time.Minute).Format(time.RFC3339)
	completedAt := previewTime.Add(-10 * time.Second).Format(time.RFC3339)
	data := ProgressData{
		State:        state.Apply.Failed,
		Engine:       "Spirit",
		ApplyID:      "apply-a1b2c3d4e5f6",
		StartedAt:    startedAt,
		CompletedAt:  completedAt,
		ErrorMessage: "lock wait timeout exceeded; try restarting transaction",
		Tables: []TableProgress{
			{
				TableName: "users",
				DDL:       "ALTER TABLE `users` ADD INDEX `idx_email_created` (`email`, `created_at`)",
				Status:    state.Apply.Failed,
			},
		},
	}
	WriteProgress(data)
}

func previewStoppedOutput() {
	// Sample progress with stopped state (mid-apply stop)
	startedAt := previewTime.Add(-3 * time.Minute).Format(time.RFC3339)
	data := ProgressData{
		State:     state.Apply.Stopped,
		Engine:    "Spirit",
		ApplyID:   "apply-a1b2c3d4e5f6",
		StartedAt: startedAt,
		Tables: []TableProgress{
			{
				TableName:       "users",
				DDL:             "ALTER TABLE `users` ADD INDEX `idx_email_created` (`email`, `created_at`)",
				Status:          state.Apply.Stopped,
				RowsCopied:      156342,
				RowsTotal:       397453,
				PercentComplete: 39,
			},
			{
				TableName:       "orders",
				DDL:             "ALTER TABLE `orders` ADD INDEX `idx_total_cents` (`total_cents`)",
				Status:          state.Apply.Stopped,
				PercentComplete: 0, // Never started
			},
		},
	}
	WriteProgress(data)
	fmt.Println("\nStopped. Use 'schemabot start --apply-id <apply_id>' to resume from checkpoint.")
}

func previewApplyWatchOutput() {
	fmt.Println("Apply watch mode: Running with footer controls")
	fmt.Println("(schemabot apply -s ./schema -e staging)")
	fmt.Println()

	// In-progress state
	data := ProgressData{
		State:     state.Apply.Running,
		Engine:    "Spirit",
		ApplyID:   "apply-a1b2c3d4e5f6",
		StartedAt: previewTime.Add(-8 * time.Minute).Format(time.RFC3339),
		Tables: []TableProgress{
			{TableName: "orders", DDL: "ALTER TABLE `orders` ADD INDEX `idx_user_id` (`user_id`)", Status: state.Apply.Completed},
			{
				TableName:       "users",
				DDL:             "ALTER TABLE `users` ADD INDEX `idx_email_created` (`email`, `created_at`)",
				Status:          state.Apply.Running,
				RowsCopied:      914707,
				RowsTotal:       1466232,
				PercentComplete: 62,
				ETASeconds:      195, // 3m 15s
			},
			{TableName: "products", DDL: "ALTER TABLE `products` ADD INDEX `idx_price` (`price_cents`)", Status: state.Apply.Pending},
		},
	}
	WriteProgress(data)

	fmt.Println(FormatWatchFooter())
	fmt.Println()
	fmt.Println("--- After completion: ---")
	fmt.Println()

	// Completed state
	dataComplete := ProgressData{
		State:       state.Apply.Completed,
		Engine:      "Spirit",
		ApplyID:     "apply-a1b2c3d4e5f6",
		StartedAt:   previewTime.Add(-12 * time.Minute).Format(time.RFC3339),
		CompletedAt: previewTime.Add(-30 * time.Second).Format(time.RFC3339),
		Tables: []TableProgress{
			{TableName: "orders", DDL: "ALTER TABLE `orders` ADD INDEX `idx_user_id` (`user_id`)", Status: state.Apply.Completed},
			{TableName: "products", DDL: "ALTER TABLE `products` ADD INDEX `idx_price` (`price_cents`)", Status: state.Apply.Completed},
			{TableName: "users", DDL: "ALTER TABLE `users` ADD INDEX `idx_email_created` (`email`, `created_at`)", Status: state.Apply.Completed},
		},
	}
	WriteProgress(dataComplete)
	fmt.Println(FormatApplyComplete())
}

func previewApplyStoppedOutput() {
	fmt.Println("Apply watch mode: Stopped by user")
	fmt.Println("(user ran schemabot stop)")
	fmt.Println()

	data := ProgressData{
		State:     state.Apply.Stopped,
		Engine:    "Spirit",
		ApplyID:   "apply-a1b2c3d4e5f6",
		StartedAt: previewTime.Add(-8 * time.Minute).Format(time.RFC3339),
		Tables: []TableProgress{
			{TableName: "orders", DDL: "ALTER TABLE `orders` ADD INDEX `idx_user_id` (`user_id`)", Status: state.Apply.Completed},
			{
				TableName:       "users",
				DDL:             "ALTER TABLE `users` ADD INDEX `idx_email_created` (`email`, `created_at`)",
				Status:          state.Apply.Stopped,
				RowsCopied:      45000,
				RowsTotal:       100000,
				PercentComplete: 45,
			},
			{TableName: "products", DDL: "ALTER TABLE `products` ADD INDEX `idx_price` (`price_cents`)", Status: state.Apply.Stopped, PercentComplete: 0},
		},
	}
	WriteProgress(data)

	fmt.Printf("%s\n", FormatApplyStopped())
	fmt.Println("Use 'schemabot start --apply-id <apply_id>' to resume.")
}

// =============================================================================
// Stop/Start Command Output Previews
// =============================================================================

func previewStopCommandOutput() {
	fmt.Println("Stop command: User runs 'schemabot stop --apply-id <apply_id>'")
	fmt.Println()

	WriteStopSuccess(StopData{
		Database:     "myapp",
		Environment:  "staging",
		ApplyID:      "apply-a1b2c3d4e5f67890",
		StoppedCount: 2,
		SkippedCount: 1,
	})

	fmt.Println()
	fmt.Println("--- After stop, progress shows: ---")
	fmt.Println()

	data := ProgressData{
		State:     state.Apply.Stopped,
		Engine:    "Spirit",
		ApplyID:   "apply-a1b2c3d4e5f6",
		StartedAt: previewTime.Add(-8 * time.Minute).Format(time.RFC3339),
		Tables: []TableProgress{
			{TableName: "orders", DDL: "ALTER TABLE `orders` ADD INDEX `idx_user_id` (`user_id`)", Status: state.Apply.Completed},
			{
				TableName:       "users",
				DDL:             "ALTER TABLE `users` ADD INDEX `idx_email_created` (`email`, `created_at`)",
				Status:          state.Apply.Stopped,
				RowsCopied:      156342,
				RowsTotal:       397453,
				PercentComplete: 39,
			},
			{TableName: "products", DDL: "ALTER TABLE `products` ADD INDEX `idx_price` (`price_cents`)", Status: state.Apply.Stopped, PercentComplete: 0},
		},
	}
	WriteProgress(data)
}

func previewStartCommandOutput() {
	fmt.Println("Start command: User runs 'schemabot start --apply-id <apply_id>'")
	fmt.Println()

	WriteStartSuccess(StartData{
		Database:     "myapp",
		Environment:  "staging",
		ApplyID:      "apply-a1b2c3d4e5f67890",
		StartedCount: 2,
		SkippedCount: 1,
	})

	fmt.Println()
	fmt.Println("--- Then progress resumes: ---")
	fmt.Println()

	data := ProgressData{
		State:     state.Apply.Running,
		Engine:    "Spirit",
		ApplyID:   "apply-a1b2c3d4e5f6",
		StartedAt: previewTime.Add(-8 * time.Minute).Format(time.RFC3339),
		Tables: []TableProgress{
			{TableName: "orders", DDL: "ALTER TABLE `orders` ADD INDEX `idx_user_id` (`user_id`)", Status: state.Apply.Completed},
			{
				TableName:       "users",
				DDL:             "ALTER TABLE `users` ADD INDEX `idx_email_created` (`email`, `created_at`)",
				Status:          state.Apply.Running,
				RowsCopied:      158000, // Resumed from checkpoint, slightly more progress
				RowsTotal:       397453,
				PercentComplete: 40,
				ETASeconds:      480,
			},
			{TableName: "products", DDL: "ALTER TABLE `products` ADD INDEX `idx_price` (`price_cents`)", Status: state.Apply.Pending},
		},
	}
	WriteProgress(data)

	fmt.Println(FormatWatchFooter())
}

func previewVolumeBarOutput() {
	fmt.Println("Volume bar: Visual representation at different levels")
	fmt.Println()

	fmt.Println("Volume levels 1-11:")
	fmt.Println()

	for _, vol := range []int{1, 4, 7, 11} {
		filled := strings.Repeat("█", vol)
		empty := strings.Repeat("░", 11-vol)
		fmt.Printf("  Volume: %s%s %d/11\n", filled, empty, vol)
	}
	fmt.Println()

	fmt.Println("--- Standard footer (volume hidden by default): ---")
	fmt.Println()
	fmt.Println(FormatWatchFooter())
}

func previewVolumeModeOutput() {
	fmt.Println("Volume mode: Interactive volume adjustment")
	fmt.Println("(Press 'v' during apply to enter volume mode)")
	fmt.Println()

	// Helper to render simple volume mode
	renderVolumeMode := func(vol int) {
		filled := strings.Repeat("█", vol)
		empty := strings.Repeat("░", 11-vol)
		fmt.Printf("Volume: %s%s %d/11\n", filled, empty, vol)
		fmt.Printf("%s↑↓ adjust • 1-9 direct • ESC done%s\n", ANSIDim, ANSIReset)
	}

	fmt.Println("--- In volume mode (default 4): ---")
	fmt.Println()
	renderVolumeMode(4)

	fmt.Println()
	fmt.Println("--- After adjusting to 8: ---")
	fmt.Println()
	renderVolumeMode(8)

	fmt.Println()
	fmt.Println("--- After adjusting to 2: ---")
	fmt.Println()
	renderVolumeMode(2)
}
