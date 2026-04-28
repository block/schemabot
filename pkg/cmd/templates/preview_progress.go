package templates

import (
	"fmt"
	"strings"
	"time"

	"github.com/block/schemabot/pkg/state"
	"vitess.io/vitess/go/vt/key"
)

func previewPreparingBranchOutput() {
	data := ProgressData{
		State:       state.Apply.PreparingBranch,
		Engine:      "PlanetScale",
		ApplyID:     "apply-a1b2c3d4e5f6",
		Database:    "myapp",
		Environment: "staging",
		Tables: []TableProgress{
			{
				TableName: "users", Namespace: "myapp_sharded",
				DDL:    "ALTER TABLE `users` ADD COLUMN `phone` varchar(20) DEFAULT NULL",
				Status: state.Apply.Pending,
			},
		},
	}
	WriteProgress(data)
}

func previewRefreshingBranchOutput() {
	data := ProgressData{
		State:       state.Apply.PreparingBranch,
		Engine:      "PlanetScale",
		ApplyID:     "apply-b7c8d9e0f1a2",
		Database:    "myapp",
		Environment: "staging",
		Metadata: map[string]string{
			"existing_branch": "my-reusable-branch",
			"branch_name":     "my-reusable-branch",
		},
		Tables: []TableProgress{
			{
				TableName: "users", Namespace: "myapp_sharded",
				DDL:    "ALTER TABLE `users` ADD COLUMN `region` varchar(50) DEFAULT NULL",
				Status: state.Apply.Pending,
			},
		},
	}
	WriteProgress(data)
}

func previewApplyingBranchChangesOutput() {
	data := ProgressData{
		State:       state.Apply.ApplyingBranchChanges,
		Engine:      "PlanetScale",
		ApplyID:     "apply-a1b2c3d4e5f6",
		Database:    "myapp",
		Environment: "staging",
		Metadata: map[string]string{
			"branch_name":   "schemabot-myapp-28471035",
			"status_detail": "Applied keyspace myapp_sharded_003 (8/12)",
		},
	}
	WriteProgress(data)
}

func previewCreatingDeployRequestOutput() {
	data := ProgressData{
		State:       state.Apply.CreatingDeployRequest,
		Engine:      "PlanetScale",
		ApplyID:     "apply-a1b2c3d4e5f6",
		Database:    "myapp",
		Environment: "staging",
		Metadata: map[string]string{
			"branch_name": "schemabot-myapp-28471035",
		},
		Tables: []TableProgress{
			{
				TableName: "users", Namespace: "myapp_sharded",
				DDL:    "ALTER TABLE `users` ADD COLUMN `phone` varchar(20) DEFAULT NULL",
				Status: state.Apply.Pending,
			},
		},
	}
	WriteProgress(data)
}

func previewVitessVSchemaOnlyOutput() {
	data := ProgressData{
		State:       state.Apply.Running,
		Engine:      "PlanetScale",
		ApplyID:     "apply-a1b2c3d4e5f6",
		Database:    "myapp",
		Environment: "staging",
		StartedAt:   previewTime.Add(-10 * time.Second).Format(time.RFC3339),
		Metadata: map[string]string{
			"branch_name":        "schemabot-myapp-28471035",
			"deploy_request_url": "https://app.planetscale.com/my-org/myapp/deploy-requests/43",
		},
		Tables: []TableProgress{
			{
				TableName: "VSchema: myapp_sharded", Namespace: "myapp_sharded",
				DDL:    `+ "xxhash": {"type": "xxhash"}`,
				Status: state.Apply.Running,
			},
		},
	}
	WriteProgress(data)
}

func previewVitessMultiKeyspaceOutput() {
	data := ProgressData{
		State:       state.Apply.Running,
		Engine:      "PlanetScale",
		ApplyID:     "apply-a1b2c3d4e5f6",
		Database:    "myapp",
		Environment: "staging",
		StartedAt:   previewTime.Add(-20 * time.Second).Format(time.RFC3339),
		Metadata: map[string]string{
			"branch_name":        "schemabot-myapp-28471035",
			"deploy_request_url": "https://app.planetscale.com/my-org/myapp/deploy-requests/44",
		},
		Tables: []TableProgress{
			{
				TableName: "users", Namespace: "myapp_sharded",
				DDL:    "ALTER TABLE `users` ADD COLUMN `phone` varchar(20) DEFAULT NULL",
				Status: state.Apply.Completed,
			},
			{
				TableName: "orders_seq",
				Namespace: "myapp_unsharded",
				DDL:       "CREATE TABLE `orders_seq` (`id` int unsigned NOT NULL DEFAULT '0', `next_id` bigint unsigned, `cache` bigint unsigned, PRIMARY KEY (`id`)) ENGINE InnoDB",
				Status:    state.Apply.Running,
			},
		},
	}
	WriteProgress(data)
}

func previewVitessDDLWithVSchemaOutput() {
	data := ProgressData{
		State:       state.Apply.Running,
		Engine:      "PlanetScale",
		ApplyID:     "apply-a1b2c3d4e5f6",
		Database:    "myapp",
		Environment: "staging",
		StartedAt:   previewTime.Add(-15 * time.Second).Format(time.RFC3339),
		Metadata: map[string]string{
			"branch_name":        "schemabot-myapp-28471035",
			"deploy_request_url": "https://app.planetscale.com/my-org/myapp/deploy-requests/45",
		},
		Tables: []TableProgress{
			{
				TableName: "users", Namespace: "myapp_sharded",
				DDL:    "ALTER TABLE `users` ADD COLUMN `phone` varchar(20) DEFAULT NULL",
				Status: state.Apply.Running,
			},
			{
				TableName: "VSchema: myapp_sharded", Namespace: "myapp_sharded",
				DDL:    `+ "users": {"column_vindexes": [{"column": "id", "name": "hash"}]}`,
				Status: state.Apply.Running,
			},
		},
	}
	WriteProgress(data)
}

func previewVitessShardProgressOutput() {
	data := ProgressData{
		State:       state.Apply.Running,
		ApplyID:     "apply-a1b2c3d4e5f6",
		Database:    "myapp",
		Environment: "staging",
		StartedAt:   previewTime.Add(-45 * time.Second).Format(time.RFC3339),
		Metadata: map[string]string{
			"branch_name":        "schemabot-myapp-28471035",
			"deploy_request_url": "https://app.planetscale.com/my-org/myapp/deploy-requests/46",
		},
		Tables: []TableProgress{
			{
				TableName: "orders", Namespace: "myapp_sharded",
				DDL:             "ALTER TABLE `orders` ADD INDEX `idx_total` (`total_cents`)",
				Status:          state.Apply.Running,
				RowsCopied:      2800000,
				RowsTotal:       4000000,
				PercentComplete: 70,
				ETASeconds:      120,
				Shards: []ShardProgress{
					{Shard: "-80", Status: state.Task.Running, RowsCopied: 2000000, RowsTotal: 2100000, PercentComplete: 95, ETASeconds: 10},
					{Shard: "80-", Status: state.Task.Running, RowsCopied: 800000, RowsTotal: 1900000, PercentComplete: 42, ETASeconds: 120},
				},
			},
		},
	}
	WriteProgress(data)
}

func previewVitessCutoverRetryOutput() {
	data := ProgressData{
		State:       state.Apply.Running,
		ApplyID:     "apply-a1b2c3d4e5f6",
		Database:    "myapp",
		Environment: "production",
		StartedAt:   previewTime.Add(-5 * time.Minute).Format(time.RFC3339),
		Metadata: map[string]string{
			"branch_name":        "schemabot-myapp-28471035",
			"deploy_request_url": "https://app.planetscale.com/my-org/myapp/deploy-requests/51",
		},
		Tables: []TableProgress{
			{
				TableName: "orders", Namespace: "myapp_sharded",
				DDL:             "ALTER TABLE `orders` ADD INDEX `idx_total` (`total_cents`)",
				Status:          state.Task.WaitingForCutover,
				RowsCopied:      4000000,
				RowsTotal:       4000000,
				PercentComplete: 100,
				Shards: []ShardProgress{
					{Shard: "-80", Status: state.Task.WaitingForCutover, RowsCopied: 2100000, RowsTotal: 2100000, PercentComplete: 100, CutoverAttempts: 3},
					{Shard: "80-", Status: state.Task.WaitingForCutover, RowsCopied: 1900000, RowsTotal: 1900000, PercentComplete: 100, CutoverAttempts: 3},
				},
			},
		},
	}
	WriteProgress(data)
}

func previewVitessInstantDDLOutput() {
	data := ProgressData{
		State:       state.Apply.Completed,
		Engine:      "PlanetScale",
		ApplyID:     "apply-a1b2c3d4e5f6",
		Database:    "myapp",
		Environment: "staging",
		StartedAt:   previewTime.Add(-3 * time.Second).Format(time.RFC3339),
		CompletedAt: previewTime.Format(time.RFC3339),
		Metadata: map[string]string{
			"branch_name":        "schemabot-myapp-28471035",
			"deploy_request_url": "https://app.planetscale.com/my-org/myapp/deploy-requests/47",
		},
		Tables: []TableProgress{
			{
				TableName: "users", Namespace: "myapp_sharded",
				DDL:       "ALTER TABLE `users` ADD COLUMN `phone` varchar(20) DEFAULT NULL",
				Status:    state.Apply.Completed,
				IsInstant: true,
			},
		},
	}
	WriteProgress(data)
}

func previewVitessRevertWindowOutput() {
	data := ProgressData{
		State:       state.Apply.RevertWindow,
		Engine:      "PlanetScale",
		ApplyID:     "apply-a1b2c3d4e5f6",
		Database:    "myapp",
		Environment: "production",
		StartedAt:   previewTime.Add(-2 * time.Minute).Format(time.RFC3339),
		CompletedAt: previewTime.Add(-90 * time.Second).Format(time.RFC3339),
		Options:     map[string]string{},
		Metadata: map[string]string{
			"branch_name":        "schemabot-myapp-28471035",
			"deploy_request_url": "https://app.planetscale.com/my-org/myapp/deploy-requests/48",
			"revert_expires_at":  time.Now().Add(28*time.Minute + 30*time.Second).Format(time.RFC3339),
		},
		Tables: []TableProgress{
			{
				TableName: "orders", Namespace: "myapp_sharded",
				DDL:    "ALTER TABLE `orders` ADD INDEX `idx_total` (`total_cents`)",
				Status: state.Apply.RevertWindow,
			},
		},
	}
	WriteProgress(data)
}

func previewVitessStagingOutput() {
	data := ProgressData{
		State:       state.Apply.Running,
		Engine:      "PlanetScale",
		ApplyID:     "apply-a1b2c3d4e5f6",
		Database:    "myapp",
		Environment: "staging",
		StartedAt:   previewTime.Add(-45 * time.Second).Format(time.RFC3339),
		Tables: []TableProgress{
			{
				TableName: "customers", Namespace: "myapp_sharded",
				DDL:    "ALTER TABLE `customers` ADD INDEX `idx_created_at`(`created_at`)",
				Status: state.Apply.Running,
				// RowsTotal > 0 but RowsCopied == 0 triggers "Staging schema changes..."
				RowsCopied: 0,
				RowsTotal:  124760460,
				Shards: []ShardProgress{
					{Shard: "-80", Status: state.Task.Running, RowsCopied: 0, RowsTotal: 60483380},
					{Shard: "80-", Status: state.Task.Running, RowsCopied: 0, RowsTotal: 64277080},
				},
			},
		},
	}
	WriteProgress(data)
}

func previewVitessRunningOutput() {
	data := ProgressData{
		State:       state.Apply.Running,
		Engine:      "PlanetScale",
		ApplyID:     "apply-a1b2c3d4e5f6",
		Database:    "myapp",
		Environment: "staging",
		StartedAt:   previewTime.Add(-30 * time.Second).Format(time.RFC3339),
		Metadata: map[string]string{
			"branch_name":        "schemabot-myapp-28471035",
			"deploy_request_url": "https://app.planetscale.com/my-org/myapp/deploy-requests/42",
		},
		Tables: []TableProgress{
			{
				TableName: "users", Namespace: "myapp_sharded",
				DDL:    "ALTER TABLE `users` ADD COLUMN `phone` varchar(20) DEFAULT NULL",
				Status: state.Apply.Running,
			},
		},
	}
	WriteProgress(data)
}

func previewVitessCompletedOutput() {
	data := ProgressData{
		State:       state.Apply.Completed,
		Engine:      "PlanetScale",
		ApplyID:     "apply-a1b2c3d4e5f6",
		Database:    "myapp",
		Environment: "staging",
		StartedAt:   previewTime.Add(-90 * time.Second).Format(time.RFC3339),
		CompletedAt: previewTime.Format(time.RFC3339),
		Metadata: map[string]string{
			"branch_name":        "schemabot-myapp-28471035",
			"deploy_request_url": "https://app.planetscale.com/my-org/myapp/deploy-requests/42",
		},
		Tables: []TableProgress{
			{
				TableName: "users", Namespace: "myapp_sharded",
				DDL:    "ALTER TABLE `users` ADD COLUMN `phone` varchar(20) DEFAULT NULL",
				Status: state.Apply.Completed,
			},
		},
	}
	WriteProgress(data)
}

func previewVitessMultiKeyspaceCompletedWatchOutput() {
	ddl := "ALTER TABLE `events` ADD COLUMN `source_region_id` int(11) NULL AFTER `account_id`"
	data := ProgressData{
		State:       state.Apply.Completed,
		Engine:      "PlanetScale",
		ApplyID:     "apply-19b23a035ad54ffb",
		Database:    "commerce",
		Environment: "production",
		StartedAt:   previewTime.Add(-2 * time.Minute).Format(time.RFC3339),
		CompletedAt: previewTime.Format(time.RFC3339),
		Metadata: map[string]string{
			"branch_name":        "schemabot-commerce-72511904",
			"deploy_request_url": "https://app.planetscale.com/my-org/commerce/deploy-requests/86",
		},
		Tables: []TableProgress{
			{
				TableName: "events",
				Namespace: "commerce_sharded",
				DDL:       ddl,
				Status:    state.Apply.Completed,
			},
			{
				TableName: "events",
				Namespace: "commerce_sharded_006",
				DDL:       ddl,
				Status:    state.Apply.Completed,
			},
		},
	}
	WriteProgress(data)
	fmt.Println(FormatApplyComplete())
}

func previewVitessFailedOutput() {
	data := ProgressData{
		State:        state.Apply.Failed,
		Engine:       "PlanetScale",
		ApplyID:      "apply-a1b2c3d4e5f6",
		Database:     "myapp",
		Environment:  "staging",
		ErrorMessage: "deploy request #42 failed during preparation (state: error)",
		StartedAt:    previewTime.Add(-60 * time.Second).Format(time.RFC3339),
		Metadata: map[string]string{
			"branch_name":        "schemabot-myapp-28471035",
			"deploy_request_url": "https://app.planetscale.com/my-org/myapp/deploy-requests/42",
		},
		Tables: []TableProgress{
			{
				TableName: "users", Namespace: "myapp_sharded",
				DDL:    "ALTER TABLE `users` ADD COLUMN `phone` varchar(20) DEFAULT NULL",
				Status: state.Apply.Failed,
			},
		},
	}
	WriteProgress(data)
}

func previewVitessWaitingForCutoverOutput() {
	data := ProgressData{
		State:       state.Apply.WaitingForCutover,
		Engine:      "PlanetScale",
		ApplyID:     "apply-a1b2c3d4e5f6",
		Database:    "myapp",
		Environment: "production",
		StartedAt:   previewTime.Add(-3 * time.Minute).Format(time.RFC3339),
		Options: map[string]string{
			"defer_cutover": "true",
		},
		Metadata: map[string]string{
			"branch_name":        "schemabot-myapp-28471035",
			"deploy_request_url": "https://app.planetscale.com/my-org/myapp/deploy-requests/49",
		},
		Tables: []TableProgress{
			{
				TableName: "orders", Namespace: "myapp_sharded",
				DDL:             "ALTER TABLE `orders` ADD INDEX `idx_total` (`total_cents`)",
				Status:          state.Apply.WaitingForCutover,
				RowsCopied:      4000000,
				RowsTotal:       4000000,
				PercentComplete: 100,
				Shards: []ShardProgress{
					{Shard: "-80", Status: state.Task.WaitingForCutover, RowsCopied: 2100000, RowsTotal: 2100000, PercentComplete: 100},
					{Shard: "80-", Status: state.Task.WaitingForCutover, RowsCopied: 1900000, RowsTotal: 1900000, PercentComplete: 100},
				},
			},
		},
	}
	WriteProgress(data)
}

func previewVitessCuttingOverOutput() {
	data := ProgressData{
		State:       state.Apply.CuttingOver,
		Engine:      "PlanetScale",
		ApplyID:     "apply-a1b2c3d4e5f6",
		Database:    "myapp",
		Environment: "production",
		StartedAt:   previewTime.Add(-4 * time.Minute).Format(time.RFC3339),
		Options: map[string]string{
			"defer_cutover": "true",
		},
		Metadata: map[string]string{
			"branch_name":        "schemabot-myapp-28471035",
			"deploy_request_url": "https://app.planetscale.com/my-org/myapp/deploy-requests/49",
		},
		Tables: []TableProgress{
			{
				TableName: "orders", Namespace: "myapp_sharded",
				DDL:             "ALTER TABLE `orders` ADD INDEX `idx_total` (`total_cents`)",
				Status:          state.Apply.CuttingOver,
				RowsCopied:      4000000,
				RowsTotal:       4000000,
				PercentComplete: 100,
				Shards: []ShardProgress{
					{Shard: "-80", Status: state.Task.CuttingOver, RowsCopied: 2100000, RowsTotal: 2100000, PercentComplete: 100},
					{Shard: "80-", Status: state.Task.CuttingOver, RowsCopied: 1900000, RowsTotal: 1900000, PercentComplete: 100},
				},
			},
		},
	}
	WriteProgress(data)
}

func previewVitessCancelledOutput() {
	data := ProgressData{
		State:       state.Apply.Cancelled,
		Engine:      "PlanetScale",
		ApplyID:     "apply-a1b2c3d4e5f6",
		Database:    "myapp",
		Environment: "staging",
		StartedAt:   previewTime.Add(-2 * time.Minute).Format(time.RFC3339),
		Metadata: map[string]string{
			"branch_name":        "schemabot-myapp-28471035",
			"deploy_request_url": "https://app.planetscale.com/my-org/myapp/deploy-requests/50",
		},
		Tables: []TableProgress{
			{
				TableName: "orders", Namespace: "myapp_sharded",
				DDL:             "ALTER TABLE `orders` ADD INDEX `idx_total` (`total_cents`)",
				Status:          state.Apply.Cancelled,
				RowsCopied:      1200000,
				RowsTotal:       4000000,
				PercentComplete: 30,
				Shards: []ShardProgress{
					{Shard: "-80", Status: state.Task.Cancelled, RowsCopied: 800000, RowsTotal: 2100000, PercentComplete: 38},
					{Shard: "80-", Status: state.Task.Cancelled, RowsCopied: 400000, RowsTotal: 1900000, PercentComplete: 21},
				},
			},
		},
	}
	WriteProgress(data)
}

func previewVitessLargeShardCountOutput() {
	// Simulates a 256-shard table mid-copy.
	// Most shards are copying at different rates, some already ready.
	const shardCount = 256
	shardNames, _ := key.GenerateShardRanges(shardCount, 0)
	shards := make([]ShardProgress, shardCount)
	var totalCopied, totalRows int64
	for i := range shards {
		shardName := shardNames[i]
		rowsTotal := int64(500000 + i*2000) // each shard ~500k-1M rows
		var rowsCopied int64
		var pct int
		shardStatus := state.Task.Running
		switch {
		case i < 30: // first 30 shards done copying
			rowsCopied = rowsTotal
			pct = 100
			shardStatus = state.Task.WaitingForCutover
		case i < 240: // middle shards at various progress
			pct = 95 - (i-30)/3
			rowsCopied = rowsTotal * int64(pct) / 100
		default: // last 16 shards still early
			pct = 10 + (255-i)/2
			rowsCopied = rowsTotal * int64(pct) / 100
		}
		totalCopied += rowsCopied
		totalRows += rowsTotal
		shards[i] = ShardProgress{
			Shard:           shardName,
			Status:          shardStatus,
			RowsCopied:      rowsCopied,
			RowsTotal:       rowsTotal,
			PercentComplete: pct,
			ETASeconds:      int64(300 - pct*2),
		}
	}
	overallPct := int(totalCopied * 100 / totalRows)
	// Table ETA = slowest shard's ETA (the lagging shard determines completion)
	var maxETA int64
	for _, s := range shards {
		if s.ETASeconds > maxETA {
			maxETA = s.ETASeconds
		}
	}

	data := ProgressData{
		State:       state.Apply.Running,
		Engine:      "PlanetScale",
		ApplyID:     "apply-a1b2c3d4e5f6",
		Database:    "commerce",
		Environment: "production",
		StartedAt:   previewTime.Add(-12 * time.Minute).Format(time.RFC3339),
		Metadata: map[string]string{
			"branch_name":        "schemabot-commerce-99182746",
			"deploy_request_url": "https://app.planetscale.com/my-org/commerce/deploy-requests/28",
		},
		Tables: []TableProgress{
			{
				TableName: "transactions", Namespace: "commerce_sharded",
				DDL:             "ALTER TABLE `transactions` ADD COLUMN `region_id` int DEFAULT NULL",
				Status:          state.Apply.Running,
				RowsCopied:      totalCopied,
				RowsTotal:       totalRows,
				PercentComplete: overallPct,
				ETASeconds:      maxETA,
				Shards:          shards,
			},
		},
	}
	WriteProgress(data)
}

func previewVitessManyKeyspacesOutput() {
	// Simulates a 33-keyspace deploy: 32 unsharded + 1 sharded keyspace.
	// Real-world pattern: numbered unsharded keyspaces (each single-shard)
	// plus one sharded keyspace with 32 Vitess shards.
	var tables []TableProgress

	// 32 unsharded keyspaces — instant DDL, all completed
	for i := 1; i <= 32; i++ {
		ks := fmt.Sprintf("commerce_%03d", i)
		tables = append(tables, TableProgress{
			TableName: "transactions",
			Namespace: ks,
			DDL:       "ALTER TABLE `transactions` ADD COLUMN `region_id` int DEFAULT NULL",
			Status:    state.Apply.Completed,
			IsInstant: true,
		})
	}

	// Sharded keyspace — online DDL, mid-copy with 32 shards
	shardNames, _ := key.GenerateShardRanges(32, 0)
	shards := make([]ShardProgress, 32)
	var totalCopied, totalRows int64
	for i := range shards {
		rowsTotal := int64(1200000 + i*10000)
		var rowsCopied int64
		var pct int
		shardStatus := state.Task.Running
		if i < 12 {
			rowsCopied = rowsTotal
			pct = 100
			shardStatus = state.Task.WaitingForCutover
		} else {
			pct = 80 - (i-12)*3
			rowsCopied = rowsTotal * int64(pct) / 100
		}
		totalCopied += rowsCopied
		totalRows += rowsTotal
		shards[i] = ShardProgress{
			Shard:           shardNames[i],
			Status:          shardStatus,
			RowsCopied:      rowsCopied,
			RowsTotal:       rowsTotal,
			PercentComplete: pct,
			ETASeconds:      int64(180 - pct),
		}
	}
	var maxETA int64
	for _, s := range shards {
		if s.ETASeconds > maxETA {
			maxETA = s.ETASeconds
		}
	}
	tables = append(tables, TableProgress{
		TableName:       "transactions",
		Namespace:       "commerce_sharded",
		DDL:             "ALTER TABLE `transactions` ADD COLUMN `region_id` int DEFAULT NULL",
		Status:          state.Apply.Running,
		RowsCopied:      totalCopied,
		RowsTotal:       totalRows,
		PercentComplete: int(totalCopied * 100 / totalRows),
		ETASeconds:      maxETA,
		Shards:          shards,
	})

	data := ProgressData{
		State:       state.Apply.Running,
		Engine:      "PlanetScale",
		ApplyID:     "apply-a1b2c3d4e5f6",
		Database:    "commerce",
		Environment: "production",
		StartedAt:   previewTime.Add(-8 * time.Minute).Format(time.RFC3339),
		Metadata: map[string]string{
			"branch_name":        "schemabot-commerce-99182746",
			"deploy_request_url": "https://app.planetscale.com/my-org/commerce/deploy-requests/29",
		},
		Tables: tables,
	}
	WriteProgress(data)
}

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
				Namespace:       "testapp",
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
				Namespace: "testapp",
				DDL:       "ALTER TABLE `order_items` ADD INDEX `idx_product_id` (`product_id`)",
				Status:    state.Apply.WaitingForCutover,
			},
			{
				TableName: "users",
				Namespace: "testapp",
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
				TableName: "order_items", Namespace: "testapp",
				DDL:    "ALTER TABLE `order_items` ADD INDEX `idx_product_id` (`product_id`)",
				Status: state.Apply.CuttingOver,
			},
			{
				TableName: "users", Namespace: "testapp",
				DDL:    "ALTER TABLE `users` ADD INDEX `idx_email_created` (`email`, `created_at`)",
				Status: state.Apply.CuttingOver,
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
				TableName: "order_items", Namespace: "testapp",
				DDL:    "ALTER TABLE `order_items` ADD INDEX `idx_product_id` (`product_id`)",
				Status: state.Apply.Completed,
			},
			{
				TableName: "users", Namespace: "testapp",
				DDL:    "ALTER TABLE `users` ADD INDEX `idx_email_created` (`email`, `created_at`)",
				Status: state.Apply.Completed,
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
				TableName: "users", Namespace: "testapp",
				DDL:    "ALTER TABLE `users` ADD INDEX `idx_email_created` (`email`, `created_at`)",
				Status: state.Apply.Failed,
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
				TableName: "users", Namespace: "testapp",
				DDL:             "ALTER TABLE `users` ADD INDEX `idx_email_created` (`email`, `created_at`)",
				Status:          state.Apply.Stopped,
				RowsCopied:      156342,
				RowsTotal:       397453,
				PercentComplete: 39,
			},
			{
				TableName: "orders", Namespace: "testapp",
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
			{TableName: "orders", Namespace: "testapp", DDL: "ALTER TABLE `orders` ADD INDEX `idx_user_id` (`user_id`)", Status: state.Apply.Completed},
			{
				TableName: "users", Namespace: "testapp",
				DDL:             "ALTER TABLE `users` ADD INDEX `idx_email_created` (`email`, `created_at`)",
				Status:          state.Apply.Running,
				RowsCopied:      914707,
				RowsTotal:       1466232,
				PercentComplete: 62,
				ETASeconds:      195, // 3m 15s
			},
			{TableName: "products", Namespace: "testapp", DDL: "ALTER TABLE `products` ADD INDEX `idx_price` (`price_cents`)", Status: state.Apply.Pending},
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
			{TableName: "orders", Namespace: "testapp", DDL: "ALTER TABLE `orders` ADD INDEX `idx_user_id` (`user_id`)", Status: state.Apply.Completed},
			{TableName: "products", Namespace: "testapp", DDL: "ALTER TABLE `products` ADD INDEX `idx_price` (`price_cents`)", Status: state.Apply.Completed},
			{TableName: "users", Namespace: "testapp", DDL: "ALTER TABLE `users` ADD INDEX `idx_email_created` (`email`, `created_at`)", Status: state.Apply.Completed},
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
			{TableName: "orders", Namespace: "testapp", DDL: "ALTER TABLE `orders` ADD INDEX `idx_user_id` (`user_id`)", Status: state.Apply.Completed},
			{
				TableName: "users", Namespace: "testapp",
				DDL:             "ALTER TABLE `users` ADD INDEX `idx_email_created` (`email`, `created_at`)",
				Status:          state.Apply.Stopped,
				RowsCopied:      45000,
				RowsTotal:       100000,
				PercentComplete: 45,
			},
			{TableName: "products", Namespace: "testapp", DDL: "ALTER TABLE `products` ADD INDEX `idx_price` (`price_cents`)", Status: state.Apply.Stopped, PercentComplete: 0},
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
			{TableName: "orders", Namespace: "testapp", DDL: "ALTER TABLE `orders` ADD INDEX `idx_user_id` (`user_id`)", Status: state.Apply.Completed},
			{
				TableName: "users", Namespace: "testapp",
				DDL:             "ALTER TABLE `users` ADD INDEX `idx_email_created` (`email`, `created_at`)",
				Status:          state.Apply.Stopped,
				RowsCopied:      156342,
				RowsTotal:       397453,
				PercentComplete: 39,
			},
			{TableName: "products", Namespace: "testapp", DDL: "ALTER TABLE `products` ADD INDEX `idx_price` (`price_cents`)", Status: state.Apply.Stopped, PercentComplete: 0},
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
			{TableName: "orders", Namespace: "testapp", DDL: "ALTER TABLE `orders` ADD INDEX `idx_user_id` (`user_id`)", Status: state.Apply.Completed},
			{
				TableName: "users", Namespace: "testapp",
				DDL:             "ALTER TABLE `users` ADD INDEX `idx_email_created` (`email`, `created_at`)",
				Status:          state.Apply.Running,
				RowsCopied:      158000, // Resumed from checkpoint, slightly more progress
				RowsTotal:       397453,
				PercentComplete: 40,
				ETASeconds:      480,
			},
			{TableName: "products", Namespace: "testapp", DDL: "ALTER TABLE `products` ADD INDEX `idx_price` (`price_cents`)", Status: state.Apply.Pending},
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
