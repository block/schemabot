CREATE TABLE `vitess_apply_data` (
  `id` bigint unsigned NOT NULL AUTO_INCREMENT,
  `apply_id` bigint unsigned NOT NULL,
  `branch_name` varchar(255) NOT NULL,
  `deploy_request_id` bigint unsigned DEFAULT NULL,
  `migration_context` varchar(255) DEFAULT NULL,
  `deploy_request_url` varchar(512) DEFAULT NULL,
  `is_instant` tinyint(1) NOT NULL DEFAULT '0',
  `deferred_deploy` tinyint(1) NOT NULL DEFAULT '0',
  `revert_skipped_at` datetime DEFAULT NULL,
  `created_at` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP,
  `updated_at` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (`id`),
  UNIQUE KEY `uk_apply_id` (`apply_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci
