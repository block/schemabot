CREATE TABLE `orders_seq` (
  `id` int unsigned NOT NULL DEFAULT '0',
  `next_id` bigint unsigned DEFAULT NULL,
  `cache` bigint unsigned DEFAULT NULL,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci COMMENT='vitess_sequence'
