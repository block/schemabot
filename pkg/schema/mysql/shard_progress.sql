CREATE TABLE `shard_progress` (
  `id` bigint unsigned NOT NULL AUTO_INCREMENT,
  `apply_operation_id` bigint unsigned NOT NULL,
  `namespace` varchar(255) NOT NULL,
  `table_name` varchar(255) NOT NULL,
  `shard` varchar(255) NOT NULL,
  `state` varchar(100) NOT NULL DEFAULT 'pending',
  `progress_percent` int NOT NULL DEFAULT '0',
  `rows_copied` bigint NOT NULL DEFAULT '0',
  `rows_total` bigint NOT NULL DEFAULT '0',
  `eta_seconds` bigint NOT NULL DEFAULT '0',
  `cutover_attempts` int NOT NULL DEFAULT '0',
  `ready_to_complete` tinyint(1) NOT NULL DEFAULT '0',
  `created_at` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP,
  `updated_at` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (`id`),
  UNIQUE KEY `uk_operation_namespace_table_shard` (`apply_operation_id`,`namespace`,`table_name`,`shard`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci
