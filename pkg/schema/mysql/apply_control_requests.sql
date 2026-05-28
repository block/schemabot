CREATE TABLE `apply_control_requests` (
  `id` bigint unsigned NOT NULL AUTO_INCREMENT,
  `apply_id` bigint unsigned NOT NULL,
  `operation` varchar(50) NOT NULL,
  `status` varchar(50) NOT NULL,
  `requested_by` varchar(255) NOT NULL DEFAULT '',
  `error_message` text,
  `metadata` json NOT NULL,
  `completed_at` datetime DEFAULT NULL,
  `created_at` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP,
  `updated_at` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (`id`),
  UNIQUE KEY `idx_apply_control_request_apply_operation` (`apply_id`,`operation`),
  KEY `idx_apply_control_request_status` (`status`,`created_at`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci
