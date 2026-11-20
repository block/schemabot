CREATE TABLE `freeze_epochs` (
  `id` bigint unsigned NOT NULL AUTO_INCREMENT,
  `environment` varchar(255) NOT NULL,
  `epoch` bigint unsigned NOT NULL DEFAULT '0',
  `updated_at` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (`id`),
  UNIQUE KEY `idx_environment` (`environment`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci
