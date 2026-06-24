package templates

import (
	"fmt"
	"time"

	"github.com/block/schemabot/pkg/apitypes"
	"github.com/block/schemabot/pkg/presentation"
	"github.com/block/schemabot/pkg/state"
	"github.com/block/schemabot/pkg/webhook/action"
)

// Shared preview error messages — used by both PR comment and CLI preview functions
// to keep failure scenarios consistent across output formats.
const (
	PreviewErrorFirstFailed  = "Error 1061: Duplicate key name 'idx_user_id'"
	PreviewErrorMiddleFailed = "lock wait timeout exceeded; try restarting transaction"
	previewRepository        = "block/schemabot"
	previewHeadSHA           = "abcdef1234567890abcdef1234567890abcdef12"
	previewRequestedBy       = "jackjackbits"
)

func previewSupportChannel() SupportChannelData {
	return SupportChannelData{
		Name: "#schema-help",
		URL:  "https://chat.example.com/schema-help",
	}
}

// PreviewCommentPlan renders a sample plan comment with DDL changes and lint violations.
func PreviewCommentPlan() string {
	return RenderPlanComment(PlanCommentData{
		Database:    "testapp",
		SchemaName:  "testapp",
		Environment: "staging",
		HeadSHA:     previewHeadSHA,
		Repository:  previewRepository,
		RequestedBy: previewRequestedBy,
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
		HeadSHA:     previewHeadSHA,
		Repository:  previewRepository,
		RequestedBy: previewRequestedBy,
		IsMySQL:     true,
		Changes:     nil,
	})
}

// PreviewCommentNoManagedSchemaChanges renders the safe empty-diff comment when
// SchemaBot has no apply-owned state for the PR.
func PreviewCommentNoManagedSchemaChanges() string {
	return RenderNoManagedSchemaChanges(SchemaErrorData{
		RequestedBy: previewRequestedBy,
		Timestamp:   "2026-03-15 14:30:00",
		Environment: "staging",
		CommandName: action.Plan,
	})
}

// PreviewCommentSchemaReconciliationInProgress renders the reconciliation
// comment for a PR whose current diff no longer contains managed schema files
// while a stored apply from that PR is still running.
func PreviewCommentSchemaReconciliationInProgress() string {
	return RenderSupportChannelFooter(RenderSchemaChangeReconciliationRequired(SchemaChangeReconciliationData{
		RequestedBy: previewRequestedBy,
		Timestamp:   "2026-03-15 14:30:00",
		Items: []SchemaChangeReconciliationItem{
			{
				Database:    "testapp",
				Environment: "staging",
				ApplyID:     "apply-09f8ba28fb67492e",
				State:       state.Apply.Running,
				InProgress:  true,
			},
		},
	}), previewSupportChannel())
}

// PreviewCommentSchemaReconciliationCompleted renders the reconciliation
// comment for a PR whose current diff no longer contains managed schema files
// after a stored apply from that PR has completed.
func PreviewCommentSchemaReconciliationCompleted() string {
	return RenderSupportChannelFooter(RenderSchemaChangeReconciliationRequired(SchemaChangeReconciliationData{
		RequestedBy: previewRequestedBy,
		Timestamp:   "2026-03-15 14:30:00",
		Items: []SchemaChangeReconciliationItem{
			{
				Database:    "testapp",
				Environment: "staging",
				ApplyID:     "apply-09f8ba28fb67492e",
				State:       state.Apply.Completed,
			},
		},
	}), previewSupportChannel())
}

// PreviewCommentHelp renders the help command reference comment.
func PreviewCommentHelp() string {
	return RenderHelpComment()
}

// PreviewCommentSupportChannel renders a sample error comment with a support-channel footer.
func PreviewCommentSupportChannel() string {
	return "### Invalid command\n\n" +
		RenderSupportChannelFooter(RenderInvalidCommand(), previewSupportChannel()) +
		"\n\n### Apply failure\n\n" +
		RenderSupportChannelFooter(PreviewCommentApplyFailed(), previewSupportChannel())
}

// PreviewCommentErrorNoConfig renders the "no config found" error comment.
func PreviewCommentErrorNoConfig() string {
	return RenderNoConfig(SchemaErrorData{
		RequestedBy: previewRequestedBy,
		Timestamp:   "2026-01-15 14:30:00",
		Environment: "staging",
		CommandName: action.Plan,
	})
}

// PreviewCommentErrorMultiple renders the "multiple databases" error comment.
func PreviewCommentErrorMultiple() string {
	return RenderMultipleConfigs(SchemaErrorData{
		RequestedBy:        previewRequestedBy,
		Timestamp:          "2026-01-15 14:30:00",
		Environment:        "staging",
		CommandName:        action.Plan,
		AvailableDatabases: "- `testapp` (schema/testapp/schemabot.yaml)\n- `payments` (schema/payments/schemabot.yaml)",
	})
}

// PreviewCommentErrorNotFound renders the "database not found" error comment.
func PreviewCommentErrorNotFound() string {
	return RenderDatabaseNotFound(SchemaErrorData{
		RequestedBy:  previewRequestedBy,
		Timestamp:    "2026-01-15 14:30:00",
		Environment:  "staging",
		DatabaseName: "nonexistent-db",
		CommandName:  action.Plan,
	})
}

// PreviewCommentErrorInvalid renders the "invalid config" error comment.
func PreviewCommentErrorInvalid() string {
	return RenderInvalidConfig(SchemaErrorData{
		RequestedBy: previewRequestedBy,
		Timestamp:   "2026-01-15 14:30:00",
		Environment: "staging",
		CommandName: action.Plan,
	})
}

// PreviewCommentErrorGeneric renders a generic plan failure error comment.
func PreviewCommentErrorGeneric() string {
	return RenderGenericError(SchemaErrorData{
		RequestedBy: previewRequestedBy,
		Timestamp:   "2026-01-15 14:30:00",
		Environment: "staging",
		CommandName: action.Plan,
		ErrorDetail: "failed to fetch repository contents: API rate limit exceeded",
	})
}

// PreviewCommentMissingEnv renders the "missing -e flag" error comment.
func PreviewCommentMissingEnv() string {
	return RenderMissingEnv(action.Plan)
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
		RequestedBy: previewRequestedBy,
		ApplyID:     "apply-a1b2c3d4e5f6",
	})
}

// PreviewCommentUnlockSuccess renders a sample "lock released" confirmation.
func PreviewCommentUnlockSuccess() string {
	return RenderUnlockSuccess("testapp", "staging", previewRequestedBy)
}

// PreviewCommentApplyBlockedByOtherPR renders a sample "blocked by other PR" comment.
func PreviewCommentApplyBlockedByOtherPR() string {
	return RenderApplyBlockedByOtherPR(ApplyLockConflictData{
		Database:     "testapp",
		DatabaseType: "mysql",
		Environment:  "staging",
		RequestedBy:  previewRequestedBy,
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
		RequestedBy:  previewRequestedBy,
		LockOwner:    "cli:jackjackbits@macbook.local",
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
		RequestedBy:  previewRequestedBy,
		ApplyID:      "apply-a1b2c3d4e5f6",
		ApplyState:   "running",
	})
}

// PreviewCommentApplyConfirmNoLock renders a sample "no lock found" comment.
func PreviewCommentApplyConfirmNoLock() string {
	return RenderApplyConfirmNoLock("testapp", "staging")
}

// PreviewCommentReviewRequired renders a sample "review required" comment.
func PreviewCommentReviewRequired() string {
	return RenderReviewRequired(ReviewGateData{
		Database:    "testapp",
		Environment: "staging",
		RequestedBy: previewRequestedBy,
		Reviewers:   []string{"acme/schema-reviewers", "jdoe"},
		PRAuthor:    previewRequestedBy,
	})
}

// PreviewCommentReviewGateError renders a sample review gate error comment (fail-closed).
func PreviewCommentReviewGateError() string {
	return RenderGenericError(SchemaErrorData{
		RequestedBy: previewRequestedBy,
		Environment: "staging",
		CommandName: action.Apply,
		ErrorDetail: "Review gate check failed: expand team @acme/schema-reviewers: team membership cannot be read. If approval is granted through a GitHub team, verify the GitHub App can read organization members and team membership.",
	})
}

// PreviewCommentPRCommandNotAuthorized renders a sample actor authorization
// denial for apply/apply-confirm PR comments.
func PreviewCommentPRCommandNotAuthorized() string {
	return RenderPRCommandNotAuthorized(ActorAuthorizationCommentData{
		RequestedBy: "mona",
		CommandName: action.Apply,
		Database:    "orders",
		Environment: "staging",
	})
}

// PreviewCommentPRCommandAuthorizationUnavailable renders a sample fail-closed
// actor authorization error for apply/apply-confirm PR comments.
func PreviewCommentPRCommandAuthorizationUnavailable() string {
	return RenderPRCommandAuthorizationUnavailable(ActorAuthorizationCommentData{
		RequestedBy: "mona",
		CommandName: action.ApplyConfirm,
		Database:    "orders",
		Environment: "production",
	})
}

// PreviewCommentPRCommandDatabaseNotConfigured renders a sample comment for a
// mutating PR command that targets a database not configured on this instance.
func PreviewCommentPRCommandDatabaseNotConfigured() string {
	return RenderPRCommandDatabaseNotConfigured(ActorAuthorizationCommentData{
		RequestedBy: "mona",
		CommandName: action.Unlock,
		Database:    "payments",
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
		HeadSHA:     previewHeadSHA,
		Repository:  previewRepository,
		RequestedBy: previewRequestedBy,
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

// PreviewCommentDropColumnBlocked renders a sample plan where a destructive
// column drop is blocked until the application rollout is safe.
func PreviewCommentDropColumnBlocked() string {
	return RenderUnsafeChangesBlocked(PlanCommentData{
		Database:    "testapp",
		SchemaName:  "testapp",
		Environment: "staging",
		HeadSHA:     previewHeadSHA,
		Repository:  previewRepository,
		RequestedBy: previewRequestedBy,
		IsMySQL:     true,
		Changes: []KeyspaceChangeData{
			{
				Keyspace: "testapp",
				Statements: []string{
					"ALTER TABLE `customers` DROP COLUMN `nickname`;",
				},
			},
		},
		HasUnsafeChanges: true,
		UnsafeChanges: []UnsafeChangeData{
			{
				Table:  "customers",
				Reason: "Unsafe operation detected: DROP COLUMN `nickname`",
			},
		},
	})
}

// PreviewCommentApplyPlan renders a sample locked apply-plan comment.
func PreviewCommentApplyPlan() string {
	return RenderPlanComment(PlanCommentData{
		Database:     "testapp",
		SchemaName:   "testapp",
		Environment:  "staging",
		HeadSHA:      previewHeadSHA,
		Repository:   previewRepository,
		RequestedBy:  previewRequestedBy,
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
		HeadSHA:      previewHeadSHA,
		Repository:   previewRepository,
		RequestedBy:  previewRequestedBy,
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
		HeadSHA:     previewHeadSHA,
		Repository:  previewRepository,
		RequestedBy: previewRequestedBy,
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
		HeadSHA:     previewHeadSHA,
		Repository:  previewRepository,
		RequestedBy: previewRequestedBy,
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
		HeadSHA:      previewHeadSHA,
		Repository:   previewRepository,
		RequestedBy:  previewRequestedBy,
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
		HeadSHA:     previewHeadSHA,
		Repository:  previewRepository,
		RequestedBy: previewRequestedBy,
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
		HeadSHA:      previewHeadSHA,
		Repository:   previewRepository,
		IsMySQL:      true,
		RequestedBy:  previewRequestedBy,
		Environments: []string{"staging", "production"},
		Plans: map[string]*PlanCommentData{
			"staging": {
				Database:    "testapp",
				Environment: "staging",
				RequestedBy: previewRequestedBy,
				IsMySQL:     true,
				Changes:     changes,
			},
			"production": {
				Database:    "testapp",
				Environment: "production",
				RequestedBy: previewRequestedBy,
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
		HeadSHA:      previewHeadSHA,
		Repository:   previewRepository,
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
		HeadSHA:      previewHeadSHA,
		Repository:   previewRepository,
		IsMySQL:      true,
		RequestedBy:  previewRequestedBy,
		Environments: []string{"staging", "production"},
		Plans: map[string]*PlanCommentData{
			"staging": {
				Database:    "testapp",
				Environment: "staging",
				RequestedBy: previewRequestedBy,
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
		HeadSHA:      previewHeadSHA,
		Repository:   previewRepository,
		IsMySQL:      true,
		RequestedBy:  previewRequestedBy,
		Environments: []string{"staging", "production"},
		Plans: map[string]*PlanCommentData{
			"staging": {
				Database:    "testapp",
				Environment: "staging",
				RequestedBy: previewRequestedBy,
				IsMySQL:     true,
				Changes:     nil,
			},
			"production": {
				Database:    "testapp",
				Environment: "production",
				RequestedBy: previewRequestedBy,
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
		RequestedBy: previewRequestedBy,
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

// PreviewCommentApplyEstimateExceeded renders an apply comment where the active row copy has exceeded MySQL's initial estimate.
func PreviewCommentApplyEstimateExceeded() string {
	tables := sampleApplyTables()
	tables[0].Status = state.Task.Completed
	tables[1].Status = state.Task.Running
	tables[1].RowsCopied = 145000000
	tables[1].RowsTotal = 100000000
	tables[1].PercentComplete = 100
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

// PreviewCommentApplyVitessVSchemaOnly renders a VSchema-only Vitess apply,
// which has no per-table tasks — only the VSchema status section.
func PreviewCommentApplyVitessVSchemaOnly() string {
	data := sampleApplyData(state.Apply.Running, nil)
	data.Engine = "PlanetScale"
	data.VSchemaChanges = []apitypes.VSchemaChange{
		{Namespace: "myapp_sharded", Status: "applying", Diff: `+ "xxhash": {"type": "xxhash"}`},
	}
	return RenderApplyStatusComment(data)
}

// PreviewCommentApplyVitessDDLWithVSchema renders a Vitess apply where a DDL
// table has completed while the VSchema change is still applying.
func PreviewCommentApplyVitessDDLWithVSchema() string {
	tables := []TableProgressData{
		{
			TableName:       "users",
			DDL:             "ALTER TABLE `users` ADD COLUMN `phone` varchar(20) DEFAULT NULL",
			Status:          state.Task.Completed,
			RowsCopied:      50000,
			RowsTotal:       50000,
			PercentComplete: 100,
		},
	}
	data := sampleApplyData(state.Apply.Running, tables)
	data.Engine = "PlanetScale"
	data.VSchemaChanges = []apitypes.VSchemaChange{
		{Namespace: "myapp_sharded", Status: "applying", Diff: `+ "xxhash": {"type": "xxhash"}`},
	}
	return RenderApplyStatusComment(data)
}

// PreviewCommentApplyVitessMultiKeyspaceVSchema renders a Vitess apply that
// changes VSchema in multiple keyspaces, each tracked independently — one
// already applied, one still applying.
func PreviewCommentApplyVitessMultiKeyspaceVSchema() string {
	data := sampleApplyData(state.Apply.Running, nil)
	data.Engine = "PlanetScale"
	data.VSchemaChanges = []apitypes.VSchemaChange{
		{Namespace: "commerce", Status: "applied", Diff: `+ "lookup_orders": {"type": "lookup_hash"}`},
		{Namespace: "commerce_sharded", Status: "applying", Diff: `+ "xxhash": {"type": "xxhash"}`},
	}
	return RenderApplyStatusComment(data)
}

// PreviewCommentApplyShardProgress renders a sharded apply where one table is
// copying across a handful of shards, exercising the inline per-shard summary
// (each shard listed, a percent only on the actively-copying shards).
func PreviewCommentApplyShardProgress() string {
	shards := []ShardProgressData{
		{Shard: "-40", Status: state.Task.Completed, PercentComplete: 100},
		{Shard: "40-80", Status: state.Task.Running, PercentComplete: 62},
		{Shard: "80-c0", Status: state.Task.Running, PercentComplete: 31},
		{Shard: "c0-", Status: state.Task.Pending},
	}
	return RenderApplyStatusComment(ApplyStatusCommentData{
		Database:    "commerce",
		Environment: "staging",
		RequestedBy: "jackjackbits",
		ApplyID:     "apply-7aa13cf03496454b",
		State:       state.Apply.Running,
		Engine:      "Vitess",
		Tables: []TableProgressData{{
			Namespace:       "commerce",
			TableName:       "users",
			DDL:             "ALTER TABLE `users` ADD INDEX `idx_email` (`email`)",
			Status:          state.Task.Running,
			RowsCopied:      914707,
			RowsTotal:       1466232,
			PercentComplete: 62,
			ETASeconds:      195,
			Shards:          shards,
		}},
	})
}

// PreviewCommentApplyManyShardProgress renders a sharded apply where a table is
// copying across 256 shards, exercising the collapsed per-shard summary
// (per-state counts plus the slowest copier, so the line stays compact).
func PreviewCommentApplyManyShardProgress() string {
	const total = 256
	shards := make([]ShardProgressData, 0, total)
	for i := range total {
		sh := ShardProgressData{Shard: fmt.Sprintf("%02x-", i)}
		switch {
		case i < 200:
			sh.Status = state.Task.Completed
			sh.PercentComplete = 100
		case i == 247:
			sh.Status = state.Task.Running
			sh.PercentComplete = 12 // the laggard the collapsed line names
		case i < 252:
			sh.Status = state.Task.Running
			sh.PercentComplete = 55 + i%20
		default:
			sh.Status = state.Task.Pending
		}
		shards = append(shards, sh)
	}
	return RenderApplyStatusComment(ApplyStatusCommentData{
		Database:    "commerce",
		Environment: "staging",
		RequestedBy: "jackjackbits",
		ApplyID:     "apply-7aa13cf03496454b",
		State:       state.Apply.Running,
		Engine:      "Vitess",
		Tables: []TableProgressData{{
			Namespace:       "commerce",
			TableName:       "orders",
			DDL:             "ALTER TABLE `orders` ADD COLUMN `region` varchar(32)",
			Status:          state.Task.Running,
			RowsCopied:      4200000000,
			RowsTotal:       6000000000,
			PercentComplete: 70,
			ETASeconds:      5400,
			Shards:          shards,
		}},
	})
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

// PreviewCommentApplyResuming renders a sample comment for an apply that has just
// been started again after a stop. During the resuming window the data plane has
// not yet reported whether the change continues from its checkpoint or restarts
// from scratch, so the row-copy percent is indeterminate: non-terminal tables
// render state-only ("Resuming…") despite carrying their pre-stop counters, while
// already-completed tables keep their final state.
func PreviewCommentApplyResuming() string {
	tables := sampleApplyTables()[:2]
	tables[0].Status = state.Task.Completed
	tables[1].Status = state.Task.Running
	tables[1].RowsCopied = 1055687
	tables[1].RowsTotal = 1466232
	tables[1].PercentComplete = 72
	return RenderApplyStatusComment(sampleApplyData(state.Apply.Resuming, tables))
}

// PreviewCommentApplyCancelled renders a sample apply-cancelled comment. A
// cancelled change is permanent (e.g. a PlanetScale deploy request cancellation)
// and cannot be resumed — the operator must open a new schema change.
func PreviewCommentApplyCancelled() string {
	tables := sampleApplyTables()[:2]
	tables[0].Status = state.Task.Completed
	tables[1].Status = state.Task.Cancelled
	tables[1].RowsCopied = 1055687
	tables[1].RowsTotal = 1466232
	tables[1].PercentComplete = 72
	return RenderApplyStatusComment(sampleApplyData(state.Apply.Cancelled, tables))
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
// Multi-Deployment Apply Previews (MD-10)
// =============================================================================
//
// These render the aggregate PR comment for an apply that fans out across more
// than one deployment of a single environment. The per-deployment <details>
// bodies reuse the single-deployment renderer, so each scenario seeds both the
// derived rollup (presentation.Derive) and the per-deployment detail data.

// sampleDeploymentDetail builds one deployment's single-deployment comment data
// for the per-deployment <details> body, with its own database name.
func sampleDeploymentDetail(database, applyState string, tables []TableProgressData) ApplyStatusCommentData {
	return ApplyStatusCommentData{
		Database:    database,
		Environment: "production",
		RequestedBy: "aparajon",
		State:       applyState,
		Engine:      "Spirit",
		ApplyID:     "apply-a1b2c3d4e5f6",
		StartedAt:   sampleTime().Add(-8 * time.Minute).UTC().Format(time.RFC3339),
		Tables:      tables,
	}
}

// PreviewCommentMultiDeploymentApplyInProgress renders a barrier rollout
// mid-flight: one deployment parked ready for cutover, one copying, two queued.
func PreviewCommentMultiDeploymentApplyInProgress() string {
	model := presentation.Derive([]presentation.Operation{
		{Deployment: "eu", State: state.ApplyOperation.WaitingForCutover, Barrier: true, HaltOnFailure: true},
		{Deployment: "us", State: state.ApplyOperation.Running, Barrier: true, HaltOnFailure: true},
		{Deployment: "au", State: state.ApplyOperation.Pending, Barrier: true, HaltOnFailure: true},
		{Deployment: "ca", State: state.ApplyOperation.Pending, Barrier: true, HaltOnFailure: true},
	})

	euTables := sampleApplyTables()
	for i := range euTables {
		euTables[i].Status = state.Task.WaitingForCutover
	}

	usTables := sampleApplyTables()
	usTables[0].Status = state.Task.Completed
	usTables[1].Status = state.Task.Running
	usTables[1].RowsCopied = 914707
	usTables[1].RowsTotal = 1466232
	usTables[1].PercentComplete = 62
	usTables[1].ETASeconds = 195
	usTables[2].Status = state.Task.Pending

	return RenderMultiDeploymentApplyComment(MultiDeploymentApplyData{
		Model:       model,
		ApplyID:     "apply-a1b2c3d4e5f6",
		Environment: "production",
		RequestedBy: "aparajon",
		StartedAt:   sampleTime().Add(-12 * time.Minute).UTC().Format(time.RFC3339),
		Details: map[string]ApplyStatusCommentData{
			"eu": sampleDeploymentDetail("payments_eu", state.Apply.WaitingForCutover, euTables),
			"us": sampleDeploymentDetail("payments_us", state.Apply.Running, usTables),
		},
	})
}

// PreviewCommentMultiDeploymentApplyFailed renders a halt-on-failure rollout
// where one deployment failed: completed deployments stay completed, later
// deployments are halted, and the aggregate is failed with retry as next action.
func PreviewCommentMultiDeploymentApplyFailed() string {
	model := presentation.Derive([]presentation.Operation{
		{Deployment: "eu", State: state.ApplyOperation.Completed, HaltOnFailure: true},
		{Deployment: "us", State: state.ApplyOperation.Failed, HaltOnFailure: true, Error: PreviewErrorMiddleFailed},
		{Deployment: "au", State: state.ApplyOperation.Pending, HaltOnFailure: true},
		{Deployment: "ca", State: state.ApplyOperation.Pending, HaltOnFailure: true},
	})

	euTables := sampleApplyTables()
	for i := range euTables {
		euTables[i].Status = state.Task.Completed
	}

	usTables := sampleApplyTables()
	usTables[0].Status = state.Task.Completed
	usTables[1].Status = state.Task.Failed
	usTables[1].RowsCopied = 439870
	usTables[1].RowsTotal = 1466232
	usTables[1].PercentComplete = 30
	usTables[2].Status = state.Task.Cancelled
	usDetail := sampleDeploymentDetail("payments_us", state.Apply.Failed, usTables)
	usDetail.ErrorMessage = PreviewErrorMiddleFailed

	return RenderMultiDeploymentApplyComment(MultiDeploymentApplyData{
		Model:       model,
		ApplyID:     "apply-a1b2c3d4e5f6",
		Environment: "production",
		RequestedBy: "aparajon",
		StartedAt:   sampleTime().Add(-20 * time.Minute).UTC().Format(time.RFC3339),
		Details: map[string]ApplyStatusCommentData{
			"eu": sampleDeploymentDetail("payments_eu", state.Apply.Completed, euTables),
			"us": usDetail,
		},
	})
}

// PreviewCommentMultiDeploymentApplyCompleted renders a fully completed rollout
// across all deployments — aggregate applied, no pending operator action.
func PreviewCommentMultiDeploymentApplyCompleted() string {
	model := presentation.Derive([]presentation.Operation{
		{Deployment: "eu", State: state.ApplyOperation.Completed, HaltOnFailure: true},
		{Deployment: "us", State: state.ApplyOperation.Completed, HaltOnFailure: true},
		{Deployment: "au", State: state.ApplyOperation.Completed, HaltOnFailure: true},
	})

	completedTables := func() []TableProgressData {
		tables := sampleApplyTables()
		for i := range tables {
			tables[i].Status = state.Task.Completed
		}
		return tables
	}

	return RenderMultiDeploymentApplyComment(MultiDeploymentApplyData{
		Model:       model,
		ApplyID:     "apply-a1b2c3d4e5f6",
		Environment: "production",
		RequestedBy: "aparajon",
		StartedAt:   sampleTime().Add(-30 * time.Minute).UTC().Format(time.RFC3339),
		CompletedAt: sampleTime().Add(-2 * time.Minute).UTC().Format(time.RFC3339),
		Details: map[string]ApplyStatusCommentData{
			"eu": sampleDeploymentDetail("payments_eu", state.Apply.Completed, completedTables()),
			"us": sampleDeploymentDetail("payments_us", state.Apply.Completed, completedTables()),
			"au": sampleDeploymentDetail("payments_au", state.Apply.Completed, completedTables()),
		},
	})
}

// PreviewCommentMultiDeploymentApplySummaryCompleted renders the final summary
// comment for a fully completed rollout: the aggregate applied header with each
// deployment's terminal summary in its section.
func PreviewCommentMultiDeploymentApplySummaryCompleted() string {
	model := presentation.Derive([]presentation.Operation{
		{Deployment: "eu", State: state.ApplyOperation.Completed, HaltOnFailure: true},
		{Deployment: "us", State: state.ApplyOperation.Completed, HaltOnFailure: true},
		{Deployment: "au", State: state.ApplyOperation.Completed, HaltOnFailure: true},
	})

	completedTables := func() []TableProgressData {
		tables := sampleApplyTables()
		for i := range tables {
			tables[i].Status = state.Task.Completed
		}
		return tables
	}

	return RenderMultiDeploymentApplySummaryComment(MultiDeploymentApplyData{
		Model:       model,
		ApplyID:     "apply-a1b2c3d4e5f6",
		Environment: "production",
		RequestedBy: "aparajon",
		StartedAt:   sampleTime().Add(-30 * time.Minute).UTC().Format(time.RFC3339),
		CompletedAt: sampleTime().Add(-2 * time.Minute).UTC().Format(time.RFC3339),
		Details: map[string]ApplyStatusCommentData{
			"eu": sampleDeploymentDetail("payments_eu", state.Apply.Completed, completedTables()),
			"us": sampleDeploymentDetail("payments_us", state.Apply.Completed, completedTables()),
			"au": sampleDeploymentDetail("payments_au", state.Apply.Completed, completedTables()),
		},
	})
}

// PreviewCommentMultiDeploymentApplySummaryFailed renders the final summary
// comment for a halt-on-failure rollout where one deployment failed: completed
// deployments carry their success summary, the failed deployment carries its
// error and retry guidance, and later deployments are halted.
func PreviewCommentMultiDeploymentApplySummaryFailed() string {
	model := presentation.Derive([]presentation.Operation{
		{Deployment: "eu", State: state.ApplyOperation.Completed, HaltOnFailure: true},
		{Deployment: "us", State: state.ApplyOperation.Failed, HaltOnFailure: true, Error: PreviewErrorMiddleFailed},
		{Deployment: "au", State: state.ApplyOperation.Pending, HaltOnFailure: true},
		{Deployment: "ca", State: state.ApplyOperation.Pending, HaltOnFailure: true},
	})

	euTables := sampleApplyTables()
	for i := range euTables {
		euTables[i].Status = state.Task.Completed
	}

	usTables := sampleApplyTables()
	usTables[0].Status = state.Task.Completed
	usTables[1].Status = state.Task.Failed
	usTables[2].Status = state.Task.Cancelled
	usDetail := sampleDeploymentDetail("payments_us", state.Apply.Failed, usTables)
	usDetail.ErrorMessage = PreviewErrorMiddleFailed

	return RenderMultiDeploymentApplySummaryComment(MultiDeploymentApplyData{
		Model:       model,
		ApplyID:     "apply-a1b2c3d4e5f6",
		Environment: "production",
		RequestedBy: "aparajon",
		StartedAt:   sampleTime().Add(-20 * time.Minute).UTC().Format(time.RFC3339),
		CompletedAt: sampleTime().Add(-1 * time.Minute).UTC().Format(time.RFC3339),
		Details: map[string]ApplyStatusCommentData{
			"eu": sampleDeploymentDetail("payments_eu", state.Apply.Completed, euTables),
			"us": usDetail,
		},
	})
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
		RequestedBy: previewRequestedBy,
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

// PreviewCommentSummaryCancelled renders a sample cancelled summary comment. A
// cancelled change is permanent and cannot be resumed.
func PreviewCommentSummaryCancelled() string {
	tables := sampleApplyTables()[:2]
	tables[0].Status = state.Task.Completed
	tables[1].Status = state.Task.Cancelled
	tables[1].RowsCopied = 1055687
	tables[1].RowsTotal = 1466232
	tables[1].PercentComplete = 72
	return RenderApplySummaryComment(sampleSummaryDataWithDuration(state.Apply.Cancelled, tables, 45*time.Minute))
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
