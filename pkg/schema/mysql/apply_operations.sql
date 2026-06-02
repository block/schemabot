CREATE TABLE `apply_operations` (
  `id` bigint unsigned NOT NULL AUTO_INCREMENT,
  `apply_id` bigint unsigned NOT NULL,
  `deployment` varchar(255) NOT NULL,
  `target` varchar(255) NOT NULL DEFAULT '',
  `state` varchar(100) NOT NULL DEFAULT 'pending',
  `error_message` text,
  `started_at` datetime DEFAULT NULL,
  `completed_at` datetime DEFAULT NULL,
  `created_at` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP,
  `updated_at` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (`id`),
  UNIQUE KEY `idx_apply_operation` (`apply_id`,`deployment`),
  KEY `idx_deployment_state` (`deployment`,`state`),
  KEY `idx_state_created_id` (`state`,`created_at`,`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci
