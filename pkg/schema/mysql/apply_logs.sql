CREATE TABLE `apply_logs` (
  `id` bigint unsigned NOT NULL AUTO_INCREMENT,
  `apply_id` bigint unsigned NOT NULL,
  `task_id` bigint unsigned DEFAULT NULL,
  `level` varchar(20) NOT NULL DEFAULT 'info',
  `event_type` varchar(100) NOT NULL,
  `source` varchar(100) NOT NULL DEFAULT 'schemabot',
  `message` text NOT NULL,
  `old_state` varchar(100) DEFAULT NULL,
  `new_state` varchar(100) DEFAULT NULL,
  `metadata` json DEFAULT NULL,
  `created_at` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY (`id`),
  KEY `idx_apply_created` (`apply_id`,`created_at`),
  KEY `idx_task_id` (`task_id`),
  KEY `idx_level` (`level`),
  KEY `idx_event_type` (`event_type`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci
