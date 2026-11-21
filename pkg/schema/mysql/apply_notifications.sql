CREATE TABLE `apply_notifications` (
  `id` bigint unsigned NOT NULL AUTO_INCREMENT,
  `apply_id` bigint unsigned NOT NULL,
  `claim_token` varchar(64) DEFAULT NULL,
  `claimed_until` datetime(6) DEFAULT NULL,
  `published_at` datetime(6) DEFAULT NULL,
  `last_error` text,
  PRIMARY KEY (`id`),
  UNIQUE KEY `idx_apply_id` (`apply_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci
