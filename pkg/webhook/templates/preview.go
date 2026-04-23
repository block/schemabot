package templates

import (
	"time"

	"github.com/block/schemabot/pkg/state"
)

// Shared preview error messages — used by both PR comment and CLI preview functions
// to keep failure scenarios consistent across output formats.
const (
	PreviewErrorFirstFailed  = "Error 1061: Duplicate key name 'idx_user_id'"
	PreviewErrorMiddleFailed = "lock wait timeout exceeded; try restarting transaction"
)

// PreviewCommentPlan renders a sample plan comment with DDL changes and lint violations.
func PreviewCommentPlan() string {
	return RenderPlanComment(PlanCommentData{
		Database:    "testapp",
		SchemaName:  "testapp",
		Environment: "staging",
		RequestedBy: "aparajon",
		IsMySQL:     true,
		Changes: []KeyspaceChangeData{
			{
				Keyspace: "testapp",
				Statements: []string{
					"CREATE TABLE `users` (\n  `id` bigint unsigned NOT NULL AUTO_INCREMENT,\n  `email` varchar(255) NOT NULL,\n  `created_at` timestamp DEFAULT CURRENT_TIMESTAMP,\n  PRIMARY KEY (`id`),\n  INDEX `idx_email` (`email`)\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;",
					"CREATE TABLE `orders` (\n  `id` bigint unsigned NOT NULL AUTO_INCREMENT,\n  `user_id` bigint NOT NULL,\n  `total_cents` bigint NOT NULL,\n  `status` varchar(50) NOT NULL DEFAULT 'pending',\n  PRIMARY KEY (`id`),\n  INDEX `idx_user_id` (`user_id`)\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;",
					"ALTER TABLE `products` ADD INDEX `idx_category_price` (`category`, `price`);",
				},
			},
		},
		LintViolations: []LintViolationData{
			{Message: "New column uses floating-point data type", Table: "orders", LinterName: "has_float"},
			{Message: "Column added without DEFAULT value", Table: "users", LinterName: "no_default"},
		},
	})
}

// PreviewCommentPlanNoChanges renders a sample plan comment with no changes detected.
func PreviewCommentPlanNoChanges() string {
	return RenderPlanComment(PlanCommentData{
		Database:    "testapp",
		SchemaName:  "testapp",
		Environment: "staging",
		RequestedBy: "aparajon",
		IsMySQL:     true,
		Changes:     nil,
	})
}

// PreviewCommentHelp renders the help command reference comment.
func PreviewCommentHelp() string {
	return RenderHelpComment()
}

// PreviewCommentErrorNoConfig renders the "no config found" error comment.
func PreviewCommentErrorNoConfig() string {
	return RenderNoConfig(SchemaErrorData{
		RequestedBy: "aparajon",
		Timestamp:   "2026-01-15 14:30:00",
		Environment: "staging",
		CommandName: "plan",
	})
}

// PreviewCommentErrorMultiple renders the "multiple databases" error comment.
func PreviewCommentErrorMultiple() string {
	return RenderMultipleConfigs(SchemaErrorData{
		RequestedBy:        "aparajon",
		Timestamp:          "2026-01-15 14:30:00",
		Environment:        "staging",
		CommandName:        "plan",
		AvailableDatabases: "- `testapp` (schema/testapp/schemabot.yaml)\n- `payments` (schema/payments/schemabot.yaml)",
	})
}

// PreviewCommentErrorNotFound renders the "database not found" error comment.
func PreviewCommentErrorNotFound() string {
	return RenderDatabaseNotFound(SchemaErrorData{
		RequestedBy:  "aparajon",
		Timestamp:    "2026-01-15 14:30:00",
		Environment:  "staging",
		DatabaseName: "nonexistent-db",
		CommandName:  "plan",
	})
}

// PreviewCommentErrorInvalid renders the "invalid config" error comment.
func PreviewCommentErrorInvalid() string {
	return RenderInvalidConfig(SchemaErrorData{
		RequestedBy: "aparajon",
		Timestamp:   "2026-01-15 14:30:00",
		Environment: "staging",
		CommandName: "plan",
	})
}

// PreviewCommentErrorGeneric renders a generic plan failure error comment.
func PreviewCommentErrorGeneric() string {
	return RenderGenericError(SchemaErrorData{
		RequestedBy: "aparajon",
		Timestamp:   "2026-01-15 14:30:00",
		Environment: "staging",
		CommandName: "plan",
		ErrorDetail: "failed to fetch repository contents: API rate limit exceeded",
	})
}

// PreviewCommentMissingEnv renders the "missing -e flag" error comment.
func PreviewCommentMissingEnv() string {
	return RenderMissingEnv("plan")
}

// PreviewCommentInvalidCmd renders the "invalid command" error comment.
func PreviewCommentInvalidCmd() string {
	return RenderInvalidCommand()
}

// =============================================================================
// Apply Command Previews
// =============================================================================

// PreviewCommentApplyStarted renders a sample "apply started" notification.
func PreviewCommentApplyStarted() string {
	return RenderApplyStarted(ApplyStatusCommentData{
		Database:    "testapp",
		Environment: "staging",
		RequestedBy: "aparajon",
		ApplyID:     "apply-a1b2c3d4e5f6",
	})
}

// PreviewCommentUnlockSuccess renders a sample "lock released" confirmation.
func PreviewCommentUnlockSuccess() string {
	return RenderUnlockSuccess("testapp", "staging", "aparajon")
}

// PreviewCommentApplyBlockedByOtherPR renders a sample "blocked by other PR" comment.
func PreviewCommentApplyBlockedByOtherPR() string {
	return RenderApplyBlockedByOtherPR(ApplyLockConflictData{
		Database:     "testapp",
		DatabaseType: "mysql",
		Environment:  "staging",
		RequestedBy:  "aparajon",
		LockOwner:    "block/myapp#42",
		LockRepo:     "block/myapp",
		LockPR:       42,
		LockCreated:  sampleTime().Add(-2 * time.Hour),
	})
}

// PreviewCommentApplyBlockedByCLI renders a sample "blocked by CLI session" comment.
func PreviewCommentApplyBlockedByCLI() string {
	return RenderApplyBlockedByOtherPR(ApplyLockConflictData{
		Database:     "testapp",
		DatabaseType: "mysql",
		Environment:  "staging",
		RequestedBy:  "aparajon",
		LockOwner:    "cli:aparajon@macbook.local",
		LockPR:       0,
		LockCreated:  sampleTime().Add(-30 * time.Minute),
	})
}

// PreviewCommentApplyInProgress renders a sample "apply already in progress" comment.
func PreviewCommentApplyInProgress() string {
	return RenderApplyInProgress(ApplyLockConflictData{
		Database:     "testapp",
		DatabaseType: "mysql",
		Environment:  "staging",
		RequestedBy:  "aparajon",
		ApplyID:      "apply-a1b2c3d4e5f6",
		ApplyState:   "running",
	})
}

// PreviewCommentApplyConfirmNoLock renders a sample "no lock found" comment.
func PreviewCommentApplyConfirmNoLock() string {
	return RenderApplyConfirmNoLock("testapp", "staging")
}

// PreviewCommentReviewRequired renders a sample "review required" comment (CODEOWNERS gate).
func PreviewCommentReviewRequired() string {
	return RenderReviewRequired(ReviewGateData{
		Database:    "testapp",
		Environment: "staging",
		RequestedBy: "aparajon",
		Owners:      []string{"acme/schema-reviewers", "jdoe"},
		PRAuthor:    "aparajon",
	})
}

// PreviewCommentReviewGateError renders a sample review gate error comment (fail-closed).
func PreviewCommentReviewGateError() string {
	return RenderGenericError(SchemaErrorData{
		RequestedBy: "aparajon",
		Environment: "staging",
		CommandName: "apply",
		ErrorDetail: "Review gate check failed: expand team @acme/schema-reviewers: API rate limit exceeded. An admin can run schema changes directly via the CLI to bypass the review gate.",
	})
}

// PreviewCommentApplyBlockedByPriorEnv renders a sample "production blocked by staging" comment.
func PreviewCommentApplyBlockedByPriorEnv() string {
	return RenderApplyBlockedByPriorEnv("testapp", "production", "staging", "has pending changes", "Apply staging first")
}

// PreviewCommentApplyBlockedByPriorEnvFailed renders a sample "production blocked by failed staging" comment.
func PreviewCommentApplyBlockedByPriorEnvFailed() string {
	return RenderApplyBlockedByPriorEnv("testapp", "production", "staging", "failed", "Fix the issue and re-apply staging")
}

// PreviewCommentApplyBlockedByPriorEnvInProgress renders a sample "production blocked by in-progress staging" comment.
func PreviewCommentApplyBlockedByPriorEnvInProgress() string {
	return RenderApplyBlockedByPriorEnvInProgress("testapp", "production", "staging")
}

// sampleTime returns a fixed time for preview rendering consistency.
func sampleTime() time.Time {
	return time.Date(2026, 3, 15, 14, 30, 0, 0, time.UTC)
}

// samplePlanChanges returns reusable sample plan changes for preview functions.
func samplePlanChanges() []KeyspaceChangeData {
	return []KeyspaceChangeData{
		{
			Keyspace: "testapp",
			Statements: []string{
				"CREATE TABLE `users` (\n  `id` bigint unsigned NOT NULL AUTO_INCREMENT,\n  `email` varchar(255) NOT NULL,\n  `created_at` timestamp DEFAULT CURRENT_TIMESTAMP,\n  PRIMARY KEY (`id`),\n  INDEX `idx_email` (`email`)\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;",
				"CREATE TABLE `orders` (\n  `id` bigint unsigned NOT NULL AUTO_INCREMENT,\n  `user_id` bigint NOT NULL,\n  `total_cents` bigint NOT NULL,\n  `status` varchar(50) NOT NULL DEFAULT 'pending',\n  PRIMARY KEY (`id`),\n  INDEX `idx_user_id` (`user_id`)\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;",
				"ALTER TABLE `products` ADD INDEX `idx_category_price` (`category`, `price`);",
			},
		},
	}
}

// PreviewCommentUnsafeBlocked renders a sample "unsafe changes blocked" comment.
func PreviewCommentUnsafeBlocked() string {
	return RenderUnsafeChangesBlocked(PlanCommentData{
		Database:    "testapp",
		SchemaName:  "testapp",
		Environment: "staging",
		RequestedBy: "aparajon",
		IsMySQL:     true,
		Changes: []KeyspaceChangeData{
			{
				Keyspace: "testapp",
				Statements: []string{
					"ALTER TABLE `users` DROP INDEX `idx_email`",
					"ALTER TABLE `orders` DROP COLUMN `notes`",
				},
			},
		},
		HasUnsafeChanges: true,
		UnsafeChanges: []UnsafeChangeData{
			{Table: "users", Reason: "DROP INDEX idx_email"},
			{Table: "orders", Reason: "DROP COLUMN notes"},
		},
	})
}

// PreviewCommentApplyPlan renders a sample locked apply-plan comment.
func PreviewCommentApplyPlan() string {
	return RenderPlanComment(PlanCommentData{
		Database:     "testapp",
		SchemaName:   "testapp",
		Environment:  "staging",
		RequestedBy:  "aparajon",
		IsMySQL:      true,
		Changes:      samplePlanChanges(),
		IsLocked:     true,
		LockOwner:    "acme/myapp#42",
		LockAcquired: "2026-03-14 10:30:00 UTC",
	})
}

// PreviewCommentApplyPlanOptions renders a locked apply-plan with options (defer cutover, skip revert).
func PreviewCommentApplyPlanOptions() string {
	return RenderPlanComment(PlanCommentData{
		Database:     "testapp",
		SchemaName:   "testapp",
		Environment:  "staging",
		RequestedBy:  "aparajon",
		IsMySQL:      true,
		Changes:      samplePlanChanges(),
		DeferCutover: true,
		SkipRevert:   true,
		IsLocked:     true,
		LockOwner:    "acme/myapp#42",
		LockAcquired: "2026-03-14 10:30:00 UTC",
	})
}

// PreviewCommentApplyPlanUnsafe renders a sample locked apply-plan with unsafe warning.
func PreviewCommentApplyPlanUnsafe() string {
	return RenderPlanComment(PlanCommentData{
		Database:    "testapp",
		SchemaName:  "testapp",
		Environment: "staging",
		RequestedBy: "aparajon",
		IsMySQL:     true,
		Changes: []KeyspaceChangeData{
			{
				Keyspace: "testapp",
				Statements: []string{
					"ALTER TABLE `users` DROP INDEX `idx_email`",
					"ALTER TABLE `orders` DROP COLUMN `notes`",
				},
			},
		},
		HasUnsafeChanges: true,
		AllowUnsafe:      true,
		UnsafeChanges: []UnsafeChangeData{
			{Table: "users", Reason: "DROP INDEX idx_email"},
			{Table: "orders", Reason: "DROP COLUMN notes"},
		},
		IsLocked:     true,
		LockOwner:    "acme/myapp#42",
		LockAcquired: "2026-03-14 10:30:00 UTC",
	})
}

// =============================================================================
// Vitess Plan Comment Previews
// =============================================================================

// sampleVitessPlanChanges returns sample Vitess plan changes with multiple keyspaces and VSchema.
func sampleVitessPlanChanges() []KeyspaceChangeData {
	return []KeyspaceChangeData{
		{
			Keyspace: "commerce",
			Statements: []string{
				"CREATE TABLE `address_seq` (\n  `id` tinyint unsigned NOT NULL DEFAULT '0',\n  `next_id` bigint unsigned DEFAULT NULL,\n  `cache` bigint unsigned DEFAULT NULL,\n  PRIMARY KEY (`id`)\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci COMMENT='vitess_sequence';",
			},
			VSchemaChanged: true,
			VSchemaDiff: `--- a/commerce.json
+++ b/commerce.json
@@ -4,5 +4,8 @@
     "orders_seq": {
       "type": "sequence"
+    },
+    "address_seq": {
+      "type": "sequence"
     }
   }
 }`,
		},
		{
			Keyspace: "commerce_sharded",
			Statements: []string{
				"CREATE TABLE `addresses` (\n  `id` bigint unsigned NOT NULL,\n  `customer_id` bigint unsigned NOT NULL,\n  `street` varchar(255) NOT NULL,\n  `city` varchar(100) NOT NULL,\n  PRIMARY KEY (`id`),\n  INDEX `idx_customer_id` (`customer_id`)\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;",
			},
			VSchemaChanged: true,
			VSchemaDiff: `--- a/commerce_sharded.json
+++ b/commerce_sharded.json
@@ -15,5 +15,16 @@
         }
       ]
     }
+    "addresses": {
+      "column_vindexes": [
+        {
+          "column": "customer_id",
+          "name": "hash"
+        }
+      ],
+      "auto_increment": {
+        "column": "id",
+        "sequence": "commerce.address_seq"
+      }
+    }
   }
 }`,
		},
	}
}

// PreviewCommentVitessPlan renders a sample Vitess plan comment with keyspaces and VSchema diff.
func PreviewCommentVitessPlan() string {
	return RenderPlanComment(PlanCommentData{
		Database:    "commerce",
		SchemaName:  "commerce",
		Environment: "staging",
		RequestedBy: "aparajon",
		IsMySQL:     false,
		Changes:     sampleVitessPlanChanges(),
	})
}

// PreviewCommentVitessApplyPlan renders a sample locked Vitess apply-plan with options.
func PreviewCommentVitessApplyPlan() string {
	return RenderPlanComment(PlanCommentData{
		Database:     "commerce",
		SchemaName:   "commerce",
		Environment:  "staging",
		RequestedBy:  "aparajon",
		IsMySQL:      false,
		Changes:      sampleVitessPlanChanges(),
		DeferCutover: true,
		SkipRevert:   true,
		IsLocked:     true,
		LockOwner:    "acme/myapp#42",
		LockAcquired: "2026-03-14 10:30:00 UTC",
	})
}

// PreviewCommentMySQLMultiSchema renders a MySQL plan with multiple schema names.
func PreviewCommentMySQLMultiSchema() string {
	return RenderPlanComment(PlanCommentData{
		Database:    "myapp",
		Environment: "staging",
		RequestedBy: "aparajon",
		IsMySQL:     true,
		Changes: []KeyspaceChangeData{
			{
				Keyspace: "app_primary",
				Statements: []string{
					"ALTER TABLE `users` ADD INDEX `idx_email` (`email`)",
					"ALTER TABLE `sessions` ADD COLUMN `device` varchar(100) DEFAULT ''",
				},
			},
			{
				Keyspace: "app_analytics",
				Statements: []string{
					"CREATE TABLE `metrics` (\n  `id` bigint unsigned NOT NULL AUTO_INCREMENT,\n  `name` varchar(255) NOT NULL,\n  `value` double NOT NULL,\n  `recorded_at` timestamp DEFAULT CURRENT_TIMESTAMP,\n  PRIMARY KEY (`id`),\n  INDEX `idx_name_recorded` (`name`, `recorded_at`)\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci",
				},
			},
		},
	})
}

// =============================================================================
// Multi-Environment Plan Comment Previews
// =============================================================================

// PreviewCommentMultiEnvPlan renders a sample multi-environment plan comment
// where staging and production have identical changes (deduplicated).
func PreviewCommentMultiEnvPlan() string {
	changes := samplePlanChanges()
	return RenderMultiEnvPlanComment(MultiEnvPlanCommentData{
		Database:     "testapp",
		SchemaName:   "testapp",
		IsMySQL:      true,
		RequestedBy:  "aparajon",
		Environments: []string{"staging", "production"},
		Plans: map[string]*PlanCommentData{
			"staging": {
				Database:    "testapp",
				Environment: "staging",
				RequestedBy: "aparajon",
				IsMySQL:     true,
				Changes:     changes,
			},
			"production": {
				Database:    "testapp",
				Environment: "production",
				RequestedBy: "aparajon",
				IsMySQL:     true,
				Changes:     changes,
			},
		},
		Errors: map[string]string{},
	})
}

// PreviewCommentMultiEnvPlanLint renders a sample multi-environment plan comment
// with lint violations included in each environment's plan.
func PreviewCommentMultiEnvPlanLint() string {
	changes := samplePlanChanges()
	lintViolations := []LintViolationData{
		{Message: "Primary key uses signed integer type (should be UNSIGNED)", Table: "orders", LinterName: "primary_key"},
		{Message: "Column uses utf8 charset (should be utf8mb4)", Table: "users", LinterName: "allow_charset"},
	}
	return RenderMultiEnvPlanComment(MultiEnvPlanCommentData{
		Database:     "testapp",
		SchemaName:   "testapp",
		IsMySQL:      true,
		RequestedBy:  "",
		Environments: []string{"staging", "production"},
		Plans: map[string]*PlanCommentData{
			"staging":    {Database: "testapp", Environment: "staging", IsMySQL: true, Changes: changes, LintViolations: lintViolations},
			"production": {Database: "testapp", Environment: "production", IsMySQL: true, Changes: changes, LintViolations: lintViolations},
		},
		Errors: map[string]string{},
	})
}

// PreviewCommentMultiEnvPlanError renders a sample multi-environment plan comment
// where one environment has an error.
func PreviewCommentMultiEnvPlanError() string {
	changes := samplePlanChanges()
	return RenderMultiEnvPlanComment(MultiEnvPlanCommentData{
		Database:     "testapp",
		SchemaName:   "testapp",
		IsMySQL:      true,
		RequestedBy:  "aparajon",
		Environments: []string{"staging", "production"},
		Plans: map[string]*PlanCommentData{
			"staging": {
				Database:    "testapp",
				Environment: "staging",
				RequestedBy: "aparajon",
				IsMySQL:     true,
				Changes:     changes,
			},
		},
		Errors: map[string]string{
			"production": "tern client: resolve DSN for testapp/production: connection refused",
		},
	})
}

// PreviewCommentMultiEnvPlanDiff renders a sample multi-environment plan comment
// where staging and production have different changes (separate sections).
func PreviewCommentMultiEnvPlanDiff() string {
	return RenderMultiEnvPlanComment(MultiEnvPlanCommentData{
		Database:     "testapp",
		SchemaName:   "testapp",
		IsMySQL:      true,
		RequestedBy:  "aparajon",
		Environments: []string{"staging", "production"},
		Plans: map[string]*PlanCommentData{
			"staging": {
				Database:    "testapp",
				Environment: "staging",
				RequestedBy: "aparajon",
				IsMySQL:     true,
				Changes:     nil,
			},
			"production": {
				Database:    "testapp",
				Environment: "production",
				RequestedBy: "aparajon",
				IsMySQL:     true,
				Changes:     samplePlanChanges(),
			},
		},
		Errors: map[string]string{},
	})
}

// =============================================================================
// Apply Status Comment Previews (sequential progression)
// =============================================================================

// sampleApplyTables returns reusable sample tables for apply preview functions.
func sampleApplyTables() []TableProgressData {
	return []TableProgressData{
		{
			Namespace: "testapp",
			TableName: "orders",
			DDL:       "ALTER TABLE `orders` ADD INDEX `idx_user_id` (`user_id`)",
		},
		{
			Namespace: "testapp",
			TableName: "users",
			DDL:       "ALTER TABLE `users` ADD INDEX `idx_email` (`email`)",
		},
		{
			Namespace: "testapp",
			TableName: "products",
			DDL:       "ALTER TABLE `products` ADD INDEX `idx_price` (`price_cents`)",
		},
	}
}

func sampleApplyData(s string, tables []TableProgressData) ApplyStatusCommentData {
	return ApplyStatusCommentData{
		Database:    "testapp",
		Environment: "staging",
		RequestedBy: "aparajon",
		State:       s,
		Engine:      "Spirit",
		ApplyID:     "apply-a1b2c3d4e5f6",
		StartedAt:   sampleTime().Add(-8 * time.Minute).UTC().Format(time.RFC3339),
		Tables:      tables,
	}
}

// PreviewCommentApplyAllPending renders an apply comment where all tables are queued.
func PreviewCommentApplyAllPending() string {
	tables := sampleApplyTables()
	for i := range tables {
		tables[i].Status = state.Task.Pending
	}
	return RenderApplyStatusComment(sampleApplyData(state.Apply.Running, tables))
}

// PreviewCommentApplyFirstRunning renders an apply comment where the first table is running.
func PreviewCommentApplyFirstRunning() string {
	tables := sampleApplyTables()
	tables[0].Status = state.Task.Running
	tables[0].RowsCopied = 321450
	tables[0].RowsTotal = 1466232
	tables[0].PercentComplete = 22
	tables[0].ETASeconds = 340
	tables[1].Status = state.Task.Pending
	tables[2].Status = state.Task.Pending
	return RenderApplyStatusComment(sampleApplyData(state.Apply.Running, tables))
}

// PreviewCommentApplyProgress renders an apply comment where the second table is running.
func PreviewCommentApplyProgress() string {
	tables := sampleApplyTables()
	tables[0].Status = state.Task.Completed
	tables[1].Status = state.Task.Running
	tables[1].RowsCopied = 914707
	tables[1].RowsTotal = 1466232
	tables[1].PercentComplete = 62
	tables[1].ETASeconds = 195
	tables[2].Status = state.Task.Pending
	return RenderApplyStatusComment(sampleApplyData(state.Apply.Running, tables))
}

// PreviewCommentApplyThirdRunning renders an apply comment where the third table is running.
func PreviewCommentApplyThirdRunning() string {
	tables := sampleApplyTables()
	tables[0].Status = state.Task.Completed
	tables[1].Status = state.Task.Completed
	tables[2].Status = state.Task.Running
	tables[2].RowsCopied = 87231
	tables[2].RowsTotal = 523140
	tables[2].PercentComplete = 17
	tables[2].ETASeconds = 420
	return RenderApplyStatusComment(sampleApplyData(state.Apply.Running, tables))
}

// PreviewCommentApplyCompleted renders a sample apply-completed comment.
func PreviewCommentApplyCompleted() string {
	tables := sampleApplyTables()
	for i := range tables {
		tables[i].Status = state.Task.Completed
	}
	return RenderApplyStatusComment(sampleApplyData(state.Apply.Completed, tables))
}

// PreviewCommentApplyFirstFailed renders an apply comment where the first table failed.
func PreviewCommentApplyFirstFailed() string {
	tables := sampleApplyTables()
	tables[0].Status = state.Task.Failed
	tables[0].RowsCopied = 12045
	tables[0].RowsTotal = 1466232
	tables[0].PercentComplete = 1
	tables[1].Status = state.Task.Cancelled
	tables[2].Status = state.Task.Cancelled
	data := sampleApplyData(state.Apply.Failed, tables)
	data.ErrorMessage = PreviewErrorFirstFailed
	return RenderApplyStatusComment(data)
}

// PreviewCommentApplyFailed renders an apply comment where the middle table failed.
func PreviewCommentApplyFailed() string {
	tables := sampleApplyTables()
	tables[0].Status = state.Task.Completed
	tables[1].Status = state.Task.Failed
	tables[1].RowsCopied = 439870
	tables[1].RowsTotal = 1466232
	tables[1].PercentComplete = 30
	tables[2].Status = state.Task.Cancelled
	data := sampleApplyData(state.Apply.Failed, tables)
	data.ErrorMessage = PreviewErrorMiddleFailed
	return RenderApplyStatusComment(data)
}

// PreviewCommentApplyStopped renders a sample apply-stopped comment.
func PreviewCommentApplyStopped() string {
	tables := sampleApplyTables()[:2]
	tables[0].Status = state.Task.Completed
	tables[1].Status = state.Task.Stopped
	tables[1].RowsCopied = 1055687
	tables[1].RowsTotal = 1466232
	tables[1].PercentComplete = 72
	return RenderApplyStatusComment(sampleApplyData(state.Apply.Stopped, tables))
}

// PreviewCommentApplyWaitingForCutover renders a sample waiting-for-cutover comment.
func PreviewCommentApplyWaitingForCutover() string {
	tables := sampleApplyTables()
	for i := range tables {
		tables[i].Status = state.Task.WaitingForCutover
	}
	return RenderApplyStatusComment(sampleApplyData(state.Apply.WaitingForCutover, tables))
}

// PreviewCommentApplyCuttingOver renders a sample cutting-over comment.
func PreviewCommentApplyCuttingOver() string {
	tables := sampleApplyTables()
	for i := range tables {
		tables[i].Status = state.Task.CuttingOver
	}
	return RenderApplyStatusComment(sampleApplyData(state.Apply.CuttingOver, tables))
}

// =============================================================================
// Single-Table Apply Previews (most common case)
// =============================================================================

// sampleSingleTable returns a single table for single-table preview functions.
func sampleSingleTable() TableProgressData {
	return TableProgressData{
		TableName: "users",
		DDL:       "ALTER TABLE `users` ADD INDEX `idx_email_created` (`email`, `created_at`)",
	}
}

func sampleSingleApplyData(s string, table TableProgressData) ApplyStatusCommentData {
	return ApplyStatusCommentData{
		Database:    "testapp",
		Environment: "staging",
		RequestedBy: "aparajon",
		State:       s,
		Engine:      "Spirit",
		ApplyID:     "apply-a1b2c3d4e5f6",
		StartedAt:   sampleTime().Add(-8 * time.Minute).UTC().Format(time.RFC3339),
		Tables:      []TableProgressData{table},
	}
}

// PreviewCommentApplySingleProgress renders a single-table apply in progress.
func PreviewCommentApplySingleProgress() string {
	table := sampleSingleTable()
	table.Status = state.Task.Running
	table.RowsCopied = 3500000
	table.RowsTotal = 7200000
	table.PercentComplete = 48
	table.ETASeconds = 330
	return RenderApplyStatusComment(sampleSingleApplyData(state.Apply.Running, table))
}

// PreviewCommentApplySingleCompleted renders a single-table apply completed.
func PreviewCommentApplySingleCompleted() string {
	table := sampleSingleTable()
	table.Status = state.Task.Completed
	return RenderApplyStatusComment(sampleSingleApplyData(state.Apply.Completed, table))
}

// PreviewCommentApplySingleFailed renders a single-table apply failed.
func PreviewCommentApplySingleFailed() string {
	table := sampleSingleTable()
	table.Status = state.Task.Failed
	table.PercentComplete = 1
	data := sampleSingleApplyData(state.Apply.Failed, table)
	data.ErrorMessage = PreviewErrorMiddleFailed
	return RenderApplyStatusComment(data)
}

// PreviewCommentApplySingleStopped renders a single-table apply stopped.
func PreviewCommentApplySingleStopped() string {
	table := sampleSingleTable()
	table.Status = state.Task.Stopped
	table.RowsCopied = 156342
	table.RowsTotal = 397453
	table.PercentComplete = 39
	return RenderApplyStatusComment(sampleSingleApplyData(state.Apply.Stopped, table))
}

// =============================================================================
// Apply Summary Comment Previews
// =============================================================================

// sampleSummaryData builds an ApplyStatusCommentData with both StartedAt and CompletedAt
// set so the summary shows a Duration. Default duration is 8 minutes.
func sampleSummaryData(s string, tables []TableProgressData) ApplyStatusCommentData {
	return sampleSummaryDataWithDuration(s, tables, 8*time.Minute)
}

func sampleSummaryDataWithDuration(s string, tables []TableProgressData, duration time.Duration) ApplyStatusCommentData {
	data := sampleApplyData(s, tables)
	data.StartedAt = sampleTime().Add(-duration).UTC().Format(time.RFC3339)
	data.CompletedAt = sampleTime().UTC().Format(time.RFC3339)
	return data
}

// PreviewCommentSummaryCompleted renders a sample completed summary comment.
func PreviewCommentSummaryCompleted() string {
	tables := sampleApplyTables()
	for i := range tables {
		tables[i].Status = state.Task.Completed
	}
	return RenderApplySummaryComment(sampleSummaryData(state.Apply.Completed, tables))
}

// PreviewCommentSummaryFailed renders a sample failed summary comment.
func PreviewCommentSummaryFailed() string {
	tables := sampleApplyTables()
	tables[0].Status = state.Task.Completed
	tables[1].Status = state.Task.Failed
	tables[1].DDL = "ALTER TABLE `users` DROP COLUMN `full_name`, MODIFY COLUMN `id` bigint(20) unsigned NOT NULL AUTO_INCREMENT, ADD COLUMN `name` varchar(255) NOT NULL AFTER `id`, MODIFY COLUMN `created_at` datetime NOT NULL DEFAULT current_timestamp(), DROP INDEX `idx_created_at`, DROP INDEX `idx_email`, DROP INDEX `idx_full_name`, ADD UNIQUE `idx_email`(`email`)"
	tables[1].RowsCopied = 439870
	tables[1].RowsTotal = 1466232
	tables[1].PercentComplete = 30
	tables[2].Status = state.Task.Cancelled
	data := sampleSummaryData(state.Apply.Failed, tables)
	data.ErrorMessage = "table users failed: schema change failed: unsafe warning: Field 'name' doesn't have a default value"
	return RenderApplySummaryComment(data)
}

// PreviewCommentSummaryStopped renders a sample stopped summary comment.
func PreviewCommentSummaryStopped() string {
	tables := sampleApplyTables()[:2]
	tables[0].Status = state.Task.Completed
	tables[1].Status = state.Task.Stopped
	tables[1].RowsCopied = 1055687
	tables[1].RowsTotal = 1466232
	tables[1].PercentComplete = 72
	return RenderApplySummaryComment(sampleSummaryDataWithDuration(state.Apply.Stopped, tables, 45*time.Minute))
}

// PreviewCommentSummaryCompletedLarge renders a completed summary with 8 tables (rollup format).
func PreviewCommentSummaryCompletedLarge() string {
	tables := []TableProgressData{
		{Namespace: "testapp", TableName: "orders", DDL: "ALTER TABLE `orders` ADD INDEX `idx_user_id` (`user_id`)", Status: state.Task.Completed},
		{Namespace: "testapp", TableName: "users", DDL: "ALTER TABLE `users` ADD INDEX `idx_email` (`email`)", Status: state.Task.Completed},
		{Namespace: "testapp", TableName: "products", DDL: "ALTER TABLE `products` ADD INDEX `idx_price` (`price_cents`)", Status: state.Task.Completed},
		{Namespace: "testapp", TableName: "payments", DDL: "ALTER TABLE `payments` ADD INDEX `idx_order_id` (`order_id`)", Status: state.Task.Completed},
		{Namespace: "testapp", TableName: "addresses", DDL: "ALTER TABLE `addresses` ADD INDEX `idx_user_id` (`user_id`)", Status: state.Task.Completed},
		{Namespace: "testapp", TableName: "sessions", DDL: "ALTER TABLE `sessions` ADD INDEX `idx_expires_at` (`expires_at`)", Status: state.Task.Completed},
		{Namespace: "testapp", TableName: "audit_logs", DDL: "ALTER TABLE `audit_logs` ADD INDEX `idx_created_at` (`created_at`)", Status: state.Task.Completed},
		{Namespace: "testapp", TableName: "notifications", DDL: "ALTER TABLE `notifications` ADD INDEX `idx_user_status` (`user_id`, `status`)", Status: state.Task.Completed},
	}
	return RenderApplySummaryComment(sampleSummaryDataWithDuration(state.Apply.Completed, tables, 17*24*time.Hour+5*time.Hour))
}

// PreviewCommentSummaryFailedLarge renders a failed summary with 8 tables (rollup format).
func PreviewCommentSummaryFailedLarge() string {
	tables := []TableProgressData{
		{Namespace: "testapp", TableName: "orders", DDL: "ALTER TABLE `orders` ADD INDEX `idx_user_id` (`user_id`)", Status: state.Task.Completed},
		{Namespace: "testapp", TableName: "users", DDL: "ALTER TABLE `users` ADD INDEX `idx_email` (`email`)", Status: state.Task.Completed},
		{Namespace: "testapp", TableName: "products", DDL: "ALTER TABLE `products` ADD INDEX `idx_price` (`price_cents`)", Status: state.Task.Completed},
		{Namespace: "testapp", TableName: "payments", DDL: "ALTER TABLE `payments` ADD INDEX `idx_order_id` (`order_id`)", Status: state.Task.Completed},
		{Namespace: "testapp", TableName: "addresses", DDL: "ALTER TABLE `addresses` ADD INDEX `idx_user_id` (`user_id`)", Status: state.Task.Failed, PercentComplete: 45},
		{Namespace: "testapp", TableName: "sessions", DDL: "ALTER TABLE `sessions` ADD INDEX `idx_expires_at` (`expires_at`)", Status: state.Task.Cancelled},
		{Namespace: "testapp", TableName: "audit_logs", DDL: "ALTER TABLE `audit_logs` ADD INDEX `idx_created_at` (`created_at`)", Status: state.Task.Cancelled},
		{Namespace: "testapp", TableName: "notifications", DDL: "ALTER TABLE `notifications` ADD INDEX `idx_user_status` (`user_id`, `status`)", Status: state.Task.Cancelled},
	}
	data := sampleSummaryDataWithDuration(state.Apply.Failed, tables, 3*time.Hour+30*time.Minute)
	data.ErrorMessage = "Error 1062: Duplicate entry '12345' for key 'addresses.idx_user_id'"
	return RenderApplySummaryComment(data)
}

// PreviewCommentSummaryMultiNamespaceFailed renders a failed summary with tables from multiple namespaces.
func PreviewCommentSummaryMultiNamespaceFailed() string {
	tables := []TableProgressData{
		{Namespace: "commerce", TableName: "orders", DDL: "ALTER TABLE `orders` ADD INDEX `idx_user_id` (`user_id`)", Status: state.Task.Completed},
		{Namespace: "commerce", TableName: "payments", DDL: "ALTER TABLE `payments` ADD INDEX `idx_order_id` (`order_id`)", Status: state.Task.Completed},
		{Namespace: "customers", TableName: "users", DDL: "ALTER TABLE `users` ADD COLUMN `phone` varchar(20) DEFAULT NULL", Status: state.Task.Completed},
		{Namespace: "customers", TableName: "addresses", DDL: "ALTER TABLE `addresses` ADD INDEX `idx_zip` (`zip_code`)", Status: state.Task.Failed, PercentComplete: 60},
		{Namespace: "analytics", TableName: "events", DDL: "ALTER TABLE `events` ADD INDEX `idx_created_at` (`created_at`)", Status: state.Task.Cancelled},
	}
	data := sampleSummaryData(state.Apply.Failed, tables)
	data.ErrorMessage = "table customers.addresses failed: Error 1205: Lock wait timeout exceeded"
	return RenderApplySummaryComment(data)
}

// PreviewCommentSummaryMultiNamespaceCompleted renders a completed summary with tables from multiple namespaces.
func PreviewCommentSummaryMultiNamespaceCompleted() string {
	tables := []TableProgressData{
		{Namespace: "commerce", TableName: "orders", DDL: "ALTER TABLE `orders` ADD INDEX `idx_user_id` (`user_id`)", Status: state.Task.Completed},
		{Namespace: "commerce", TableName: "payments", DDL: "ALTER TABLE `payments` ADD INDEX `idx_order_id` (`order_id`)", Status: state.Task.Completed},
		{Namespace: "customers", TableName: "users", DDL: "ALTER TABLE `users` ADD COLUMN `phone` varchar(20) DEFAULT NULL", Status: state.Task.Completed},
		{Namespace: "customers", TableName: "addresses", DDL: "ALTER TABLE `addresses` ADD INDEX `idx_zip` (`zip_code`)", Status: state.Task.Completed},
		{Namespace: "analytics", TableName: "events", DDL: "ALTER TABLE `events` ADD INDEX `idx_created_at` (`created_at`)", Status: state.Task.Completed},
	}
	return RenderApplySummaryComment(sampleSummaryDataWithDuration(state.Apply.Completed, tables, 3*24*time.Hour+4*time.Hour))
}
