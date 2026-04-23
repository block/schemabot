CREATE TABLE `products` (
  `id` bigint unsigned NOT NULL,
  `name` varchar(255) NOT NULL,
  `description` text,
  `price_cents` bigint NOT NULL,
  `sku` varchar(100) NOT NULL,
  `created_at` timestamp NULL DEFAULT CURRENT_TIMESTAMP,
  `updated_at` timestamp NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (`id`),
  UNIQUE KEY `idx_sku` (`sku`),
  KEY `idx_name` (`name`),
  KEY `idx_price` (`price_cents`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci
