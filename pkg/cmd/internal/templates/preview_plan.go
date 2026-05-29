package templates

func previewPlanOutput() {
	WritePlanHeader(PlanHeaderData{
		Database:    "testapp",
		SchemaName:  "testapp",
		Environment: "staging",
		IsMySQL:     true,
	})

	changes := samplePlanChanges()
	WriteSQLChanges(changes)
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
	WriteNamespaceChanges(namespaces, false, "commerce")

	// Flat summary across all namespaces
	var allChanges []DDLChange
	for _, ns := range namespaces {
		allChanges = append(allChanges, ns.Changes...)
	}
	WritePlanSummary(allChanges)
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
	WriteSQLChanges(changes)
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
	WriteSQLChanges(changes)
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
	WriteSQLChanges(changes)
	WriteLintViolations(samplePlanLintViolations())
	WritePlanSummary(changes)
}
