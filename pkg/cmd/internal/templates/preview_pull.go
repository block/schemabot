package templates

import "github.com/block/schemabot/pkg/apitypes"

// previewPullVitessSchemaOutput shows a multi-keyspace Vitess pull: each
// keyspace renders as its own namespace section with its VSchema artifact,
// and the summary box carries the namespace count.
func previewPullVitessSchemaOutput() {
	WritePullSchema(&apitypes.PullSchemaResponse{
		Database:    "orders-db",
		Type:        "vitess",
		Environment: "production",
		TableCount:  2,
		Namespaces: map[string]*apitypes.PulledNamespace{
			"commerce": {
				Tables: map[string]string{
					"settings": "CREATE TABLE `settings` (\n" +
						"  `id` bigint NOT NULL,\n" +
						"  `name` varchar(255) DEFAULT NULL,\n" +
						"  PRIMARY KEY (`id`)\n" +
						") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci\n",
				},
				Artifacts: map[string]string{
					"vschema.json": "{\"sharded\":false}\n",
				},
			},
			"commerce_sharded": {
				Tables: map[string]string{
					"users": "CREATE TABLE `users` (\n" +
						"  `id` bigint NOT NULL,\n" +
						"  `email` varchar(255) DEFAULT NULL,\n" +
						"  PRIMARY KEY (`id`)\n" +
						") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci\n",
				},
				Artifacts: map[string]string{
					"vschema.json": "{\"sharded\":true,\"vindexes\":{\"hash\":{\"type\":\"hash\"}},\"tables\":{\"users\":{\"column_vindexes\":[{\"column\":\"id\",\"name\":\"hash\"}]}}}\n",
				},
			},
		},
	})
}

// previewPullSchemaOutput shows a pulled live schema with a lint audit: the
// summary box, per-namespace DDL as executable SQL, and lint findings as
// comments.
func previewPullSchemaOutput() {
	WritePullSchema(&apitypes.PullSchemaResponse{
		Database:    "orders-db",
		Type:        "mysql",
		Environment: "staging",
		TableCount:  2,
		Namespaces: map[string]*apitypes.PulledNamespace{
			"orders": {
				Tables: map[string]string{
					"orders": "CREATE TABLE `orders` (\n" +
						"  `order_id` bigint unsigned NOT NULL AUTO_INCREMENT,\n" +
						"  `customer_ref` varchar(64) NOT NULL,\n" +
						"  `status` enum('pending','paid','shipped') NOT NULL DEFAULT 'pending',\n" +
						"  `created_at` datetime(3) DEFAULT CURRENT_TIMESTAMP(3),\n" +
						"  PRIMARY KEY (`order_id`),\n" +
						"  KEY `idx_customer` (`customer_ref`)\n" +
						") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci\n",
					"order_lines": "CREATE TABLE `order_lines` (\n" +
						"  `order_id` bigint unsigned NOT NULL,\n" +
						"  `line_no` int NOT NULL,\n" +
						"  `sku` varchar(64) NOT NULL,\n" +
						"  `quantity` int NOT NULL DEFAULT '1',\n" +
						"  PRIMARY KEY (`order_id`,`line_no`)\n" +
						") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci\n",
				},
				Lint: []*apitypes.LintViolationResponse{
					{
						Table:    "orders",
						Severity: "warning",
						Message:  `Column "created_at" uses TIMESTAMP which overflows on 2038-01-19. Consider using DATETIME instead.`,
					},
				},
			},
		},
	})
}
