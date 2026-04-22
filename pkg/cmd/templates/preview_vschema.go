package templates

import "fmt"

// previewVSchemaPlanOutput shows a plan with both DDL and VSchema changes in one keyspace.
func previewVSchemaPlanOutput() {
	fmt.Println("Vitess plan: DDL + VSchema changes in a sharded keyspace")
	fmt.Println("(schemabot plan -s examples/vitess/schema -e staging)")
	fmt.Println()

	WritePlanHeader(PlanHeaderData{
		Database:    "testapp-vitess",
		SchemaName:  "testapp_sharded",
		Environment: "staging",
		IsMySQL:     false,
		IsApply:     false,
	})

	changes := []DDLChange{
		{
			ChangeType: "ALTER",
			TableName:  "users",
			DDL:        "ALTER TABLE `users` ADD COLUMN `email_verified` BOOLEAN DEFAULT FALSE",
		},
		{
			ChangeType: "CREATE",
			TableName:  "user_preferences",
			DDL:        "CREATE TABLE `user_preferences` (`id` bigint NOT NULL AUTO_INCREMENT, `user_id` bigint NOT NULL, `key` varchar(255) NOT NULL, `value` text, PRIMARY KEY (`id`), INDEX `idx_user_id` (`user_id`))",
		},
	}

	vschemaChanges := []VSchemaChange{
		{
			Keyspace: "testapp_sharded",
			Diff: `--- current
+++ new
@@ -5,6 +5,15 @@
   },
   "tables": {
     "users": {
+      "column_vindexes": [
+        {"column": "id", "name": "hash"}
+      ]
+    },
+    "user_preferences": {
+      "column_vindexes": [
+        {"column": "user_id", "name": "hash"}
+      ]
     }
   }
 }
`,
		},
	}

	namespaces := []NamespaceChange{
		{
			Namespace:      "testapp_sharded",
			Changes:        changes,
			VSchemaChanged: true,
			VSchemaDiff:    vschemaChanges[0].Diff,
		},
	}
	WriteNamespaceChanges(namespaces, false, "testapp-vitess")
	WritePlanSummaryWithVSchema(changes, vschemaChanges)
}

// previewVSchemaOnlyOutput shows a plan with only VSchema changes (no DDL).
func previewVSchemaOnlyOutput() {
	fmt.Println("Vitess plan: VSchema-only update (no table DDL changes)")
	fmt.Println("(schemabot plan -s examples/vitess/schema -e staging)")
	fmt.Println()

	WritePlanHeader(PlanHeaderData{
		Database:    "testapp-vitess",
		SchemaName:  "testapp_sharded",
		Environment: "staging",
		IsMySQL:     false,
		IsApply:     false,
	})

	vschemaChanges := []VSchemaChange{
		{
			Keyspace: "testapp_sharded",
			Diff: `--- current
+++ new
@@ -1,6 +1,10 @@
 {
   "sharded": true,
   "vindexes": {
-    "hash": {"type": "hash"}
+    "hash": {"type": "hash"},
+    "lookup_email": {
+      "type": "consistent_lookup",
+      "params": {"table": "users_email_idx", "from": "email", "to": "user_id"}
+    }
   },
   "tables": {
`,
		},
	}

	namespaces := []NamespaceChange{
		{
			Namespace:      "testapp_sharded",
			VSchemaChanged: true,
			VSchemaDiff:    vschemaChanges[0].Diff,
		},
	}
	WriteNamespaceChanges(namespaces, false, "testapp-vitess")
	WritePlanSummaryWithVSchema(nil, vschemaChanges)
}

// previewMultiKeyspacePlanOutput shows a plan spanning multiple keyspaces with VSchema.
func previewMultiKeyspacePlanOutput() {
	fmt.Println("Vitess plan: Multi-keyspace with DDL + VSchema across keyspaces")
	fmt.Println("(schemabot plan -s examples/vitess/schema -e staging)")
	fmt.Println()

	WritePlanHeader(PlanHeaderData{
		Database:    "testapp-vitess",
		SchemaName:  "testapp",
		Environment: "staging",
		IsMySQL:     false,
		IsApply:     false,
	})

	changes := []DDLChange{
		{ChangeType: "ALTER", TableName: "users", DDL: "ALTER TABLE `users` ADD COLUMN `email_verified` BOOLEAN DEFAULT FALSE"},
		{ChangeType: "ALTER", TableName: "orders", DDL: "ALTER TABLE `orders` ADD INDEX `idx_status_created` (`status`, `created_at`)"},
		{ChangeType: "CREATE", TableName: "users_email_idx", DDL: "CREATE TABLE `users_email_idx` (`email` varchar(255) NOT NULL, `user_id` bigint NOT NULL, PRIMARY KEY (`email`, `user_id`))"},
	}

	vschemaChanges := []VSchemaChange{
		{
			Keyspace: "testapp_sharded",
			Diff: `--- current
+++ new
@@ -3,6 +3,11 @@
   "vindexes": {
-    "hash": {"type": "hash"}
+    "hash": {"type": "hash"},
+    "lookup_email": {
+      "type": "consistent_lookup",
+      "params": {"table": "users_email_idx", "from": "email", "to": "user_id"}
+    }
   },
`,
		},
		{
			Keyspace: "testapp",
			Diff: `--- current
+++ new
@@ -1,4 +1,7 @@
 {
-  "tables": {}
+  "tables": {
+    "users_email_idx": {},
+    "users_seq": {"type": "sequence"},
+    "orders_seq": {"type": "sequence"}
+  }
 }
`,
		},
	}

	// Build VSchema diff map by keyspace
	vsDiffByKS := make(map[string]string)
	for _, vc := range vschemaChanges {
		vsDiffByKS[vc.Keyspace] = vc.Diff
	}

	namespaces := []NamespaceChange{
		{
			Namespace:      "testapp_sharded",
			Changes:        changes[:2],
			VSchemaChanged: true,
			VSchemaDiff:    vsDiffByKS["testapp_sharded"],
		},
		{
			Namespace:      "testapp",
			Changes:        changes[2:],
			VSchemaChanged: true,
			VSchemaDiff:    vsDiffByKS["testapp"],
		},
	}
	WriteNamespaceChanges(namespaces, false, "testapp-vitess")
	WritePlanSummaryWithVSchema(changes, vschemaChanges)
}
