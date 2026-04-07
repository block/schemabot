CREATE TABLE `audit_log` (
  `id` bigint unsigned NOT NULL AUTO_INCREMENT,
  `transaction_id` bigint unsigned NOT NULL,
  `action` varchar(50) NOT NULL,
  `details` text,
  `created_at` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY (`id`),
  KEY `idx_transaction_id` (`transaction_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;
