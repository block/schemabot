CREATE TABLE `apply_comments` (
  `id` bigint unsigned NOT NULL AUTO_INCREMENT,
  `apply_id` bigint unsigned NOT NULL,
  `comment_state` varchar(50) NOT NULL,
  `github_comment_id` bigint NOT NULL,
  `created_at` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP,
  `updated_at` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (`id`),
  UNIQUE KEY `idx_apply_comment_state` (`apply_id`,`comment_state`),
  KEY `idx_github_comment` (`github_comment_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci
