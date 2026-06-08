CREATE TABLE `engine_resume_states` (
  `id` bigint unsigned NOT NULL AUTO_INCREMENT,
  `apply_id` bigint unsigned NOT NULL,
  `engine` varchar(100) NOT NULL,
  `migration_context` varchar(255) DEFAULT NULL,
  `metadata` json NOT NULL,
  `created_at` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP,
  `updated_at` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (`id`),
  UNIQUE KEY `uk_apply_id` (`apply_id`),
  KEY `idx_engine` (`engine`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci
