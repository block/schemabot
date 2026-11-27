package templates

import (
	"github.com/block/schemabot/pkg/apitypes"
	"github.com/block/schemabot/pkg/engine"
	"github.com/block/schemabot/pkg/presentation"
	"github.com/block/schemabot/pkg/schema"
)

func previewPlanOutput() {
	WritePlanHeader(PlanHeaderData{
		Database:    "testapp",
		SchemaName:  "testapp",
		Environment: "staging",
		IsMySQL:     true,
	})

	changes := samplePlanChanges()
	WriteSQLChanges(changes, schema.DialectMySQL)
	WriteLintViolations(samplePlanLintViolations())
	WritePlanSummary(changes)
	WriteOptions(true, false) // Show defer cutover option
}

// previewPartitionedPlanOutput shows a plan for partitioned tables, each
// partition definition on its own line.
func previewPartitionedPlanOutput() {
	WritePlanHeader(PlanHeaderData{
		Database:    "testapp",
		SchemaName:  "testapp",
		Environment: "staging",
		IsMySQL:     true,
	})

	changes := samplePartitionedPlanChanges()
	WriteSQLChanges(changes, schema.DialectMySQL)
	WritePlanSummary(changes)
}

func previewVitessPlanOutput() {
	WritePlanHeader(PlanHeaderData{
		Database:    "commerce",
		SchemaName:  "commerce",
		Environment: "staging",
		IsMySQL:     false,
	})

	namespaces := []NamespaceChange{
		{
			Namespace: "commerce",
			Changes: []DDLChange{
				{TableName: "orders", DDL: "ALTER TABLE `orders` ADD COLUMN `region` varchar(50) NOT NULL DEFAULT '', ADD INDEX `idx_region` (`region`)", ChangeType: "alter"},
			},
			VSchemaChanged: true,
			VSchemaDiff: `--- a/commerce.json
+++ b/commerce.json
@@ -12,6 +12,10 @@
       "auto_increment": {
         "column": "id",
         "sequence": "orders_seq"
+      },
+      "column_vindexes": [
+        { "column": "region", "name": "region_map" }
+      ]
       }
     }
   }`,
		},
		{
			Namespace: "customer",
			Changes: []DDLChange{
				{TableName: "addresses", DDL: "CREATE TABLE `addresses` (\n  `id` bigint unsigned NOT NULL AUTO_INCREMENT,\n  `customer_id` bigint NOT NULL,\n  `street` varchar(255) NOT NULL,\n  `city` varchar(100) NOT NULL,\n  PRIMARY KEY (`id`),\n  INDEX `idx_customer_id` (`customer_id`)\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci", ChangeType: "create"},
			},
		},
	}
	WriteNamespaceChanges(namespaces, false, "commerce", schema.DialectMySQL)

	// Flat summary across all namespaces
	var allChanges []DDLChange
	for _, ns := range namespaces {
		allChanges = append(allChanges, ns.Changes...)
	}
	WritePlanSummary(allChanges)
}

// previewPostgresPlanOutput renders a PostgreSQL plan whose standalone index
// build on an existing table is named as an index to create in the summary.
func previewPostgresPlanOutput() {
	WritePlanHeader(PlanHeaderData{
		Engine:      "postgres",
		Database:    "testapp",
		SchemaName:  "testapp",
		Environment: "staging",
		IsMySQL:     true,
	})

	changes := []DDLChange{
		{ChangeType: "CREATE", TableName: "sessions", DDL: "CREATE TABLE sessions (id uuid PRIMARY KEY, user_id bigint NOT NULL, payload jsonb, created_at timestamptz NOT NULL DEFAULT now())"},
		{ChangeType: "ALTER", TableName: "users", DDL: "ALTER TABLE users ADD COLUMN last_seen_at timestamptz, ADD COLUMN preferences jsonb"},
		{ChangeType: "CREATE_INDEX", TableName: "orders", DDL: "CREATE INDEX CONCURRENTLY idx_orders_placed_at ON orders USING btree (placed_at)"},
	}
	WriteSQLChanges(changes, schema.DialectPostgres)
	WritePlanSummary(changes)
}

// previewPostgresBlockedPlanOutput is a PostgreSQL plan the engine refuses:
// the table is over the native-safe size limit, so every step of the NOT NULL
// change is blocked. The four steps are refused for one reason, so they are
// named once, and the plan closes on that verdict after its summary.
func previewPostgresBlockedPlanOutput() {
	WritePlanHeader(PlanHeaderData{
		Engine:      "postgres",
		Database:    "testapp",
		SchemaName:  "testapp",
		Environment: "staging",
	})

	changes := []DDLChange{
		{ChangeType: "ALTER", TableName: "events", DDL: `ALTER TABLE "public"."events" ADD CONSTRAINT "events_kind_not_null_check" CHECK ("kind" IS NOT NULL) NOT VALID`},
		{ChangeType: "ALTER", TableName: "events", DDL: `ALTER TABLE "public"."events" VALIDATE CONSTRAINT "events_kind_not_null_check"`},
		{ChangeType: "ALTER", TableName: "events", DDL: `ALTER TABLE "public"."events" ALTER COLUMN "kind" SET NOT NULL`},
		{ChangeType: "ALTER", TableName: "events", DDL: `ALTER TABLE "public"."events" DROP CONSTRAINT "events_kind_not_null_check"`},
	}
	WriteSQLChanges(changes, schema.DialectPostgres)
	reason := `statement for table "events": the table is 1.4 GiB with its indexes, over the 1.0 GiB limit SchemaBot sets for a native-safe apply, not a PostgreSQL limit; raise postgres.native_safe_table_size_limit_bytes to change a table this size, then plan again`
	WritePlanSummary(changes)
	plan := &apitypes.PlanResponse{Database: "testapp", Changes: []*apitypes.SchemaChangeResponse{{Namespace: "public"}}}
	for range changes {
		plan.Changes[0].TableChanges = append(plan.Changes[0].TableChanges, &apitypes.TableChangeResponse{
			TableName: "events", ExecutionMode: engine.ExecutionModeBlocked, ModeReason: reason,
		})
	}
	WriteBlockedChanges(BlockedChangeNotices(plan), presentation.BlockedChangesHeading, presentation.BlockedChangesRemedy)
}

func previewPlanNoChangesOutput() {
	WritePlanHeader(PlanHeaderData{
		Database:    "testapp",
		SchemaName:  "testapp",
		Environment: "staging",
		IsMySQL:     true,
	})
	WriteNoChanges()
}

func previewMultiEnvPlanOutput() {
	// Multi-env identical: no environment in header, plans deduplicated
	WritePlanHeader(PlanHeaderData{
		Database:   "testapp",
		SchemaName: "testapp",
		IsMySQL:    true,
	})
	changes := samplePlanChanges()
	WriteSQLChanges(changes, schema.DialectMySQL)
	WritePlanSummary(changes)
}

func previewMultiEnvPlanDiffOutput() {
	// Multi-env different: separate per-environment sections
	WritePlanHeader(PlanHeaderData{
		Database:   "testapp",
		SchemaName: "testapp",
		IsMySQL:    true,
	})
	WriteEnvironmentHeader("staging")
	WriteNoChanges()
	WriteEnvironmentHeader("production")
	changes := samplePlanChanges()
	WriteSQLChanges(changes, schema.DialectMySQL)
	WritePlanSummary(changes)
}

func previewMultiEnvPlanLintOutput() {
	// Multi-env identical with lint violations
	WritePlanHeader(PlanHeaderData{
		Database:   "testapp",
		SchemaName: "testapp",
		IsMySQL:    true,
	})
	changes := samplePlanChanges()
	WriteSQLChanges(changes, schema.DialectMySQL)
	WriteLintViolations(samplePlanLintViolations())
	WritePlanSummary(changes)
}
