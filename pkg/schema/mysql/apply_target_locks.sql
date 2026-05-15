CREATE TABLE `apply_target_locks` (
  `id` bigint unsigned NOT NULL AUTO_INCREMENT,
  `database_name` varchar(255) NOT NULL,
  `database_type` varchar(50) NOT NULL,
  `environment` varchar(50) NOT NULL,
  `created_at` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP,
  `updated_at` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (`id`),
  UNIQUE KEY `idx_apply_target` (`database_name`,`database_type`,`environment`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci
