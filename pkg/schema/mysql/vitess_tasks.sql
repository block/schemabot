CREATE TABLE `vitess_tasks` (
  `id` bigint unsigned NOT NULL AUTO_INCREMENT,
  `apply_id` bigint unsigned NOT NULL,
  `keyspace` varchar(255) NOT NULL,
  `task_type` varchar(50) NOT NULL COMMENT 'vschema, routing_rules',
  `state` varchar(100) NOT NULL DEFAULT 'pending',
  `payload` json DEFAULT NULL,
  `created_at` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP,
  `updated_at` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (`id`),
  UNIQUE KEY `uk_apply_keyspace_type` (`apply_id`,`keyspace`,`task_type`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci
