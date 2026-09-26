package templates

import "github.com/block/schemabot/pkg/schema"

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

// previewStrataPlanVSchemaRefreshOutput renders a Strata plan that adds a
// table without touching vschema.json: the keyspace prints its DDL and a note
// that its VSchema entries are refreshed from it, and the summary counts only
// the DDL.
func previewStrataPlanVSchemaRefreshOutput() {
	WritePlanHeader(PlanHeaderData{
		EngineLabel: "Strata",
		Database:    "reviews",
		SchemaName:  "reviews",
		Environment: "staging",
		IsMySQL:     false,
	})

	changes := []DDLChange{
		{TableName: "review_assignments", ChangeType: "create", DDL: "CREATE TABLE `review_assignments` (\n  `id` bigint unsigned NOT NULL AUTO_INCREMENT,\n  `review_id` bigint unsigned NOT NULL,\n  `assignee` varchar(255) NOT NULL,\n  PRIMARY KEY (`id`),\n  KEY `idx_review_id` (`review_id`)\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci"},
		{TableName: "review_versions", ChangeType: "alter", DDL: "ALTER TABLE `review_versions` ADD COLUMN `notified_at` datetime NULL"},
	}
	WriteNamespaceChanges([]NamespaceChange{{
		Namespace:          "reviews_001",
		Changes:            changes,
		VSchemaChanged:     true,
		VSchemaDerivedOnly: true,
	}}, false, "reviews", schema.DialectMySQL)
	WritePlanSummaryWithVSchema(changes, []VSchemaChange{{Keyspace: "reviews_001", DerivedOnly: true}})
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
