CREATE TABLE `plan_comments` (
  `id` bigint unsigned NOT NULL AUTO_INCREMENT,
  `repository` varchar(255) NOT NULL,
  `pull_request` int unsigned NOT NULL,
  `database_name` varchar(255) NOT NULL,
  `database_type` varchar(50) NOT NULL,
  `environment_scope` varchar(255) NOT NULL DEFAULT '',
  `head_sha` varchar(64) NOT NULL,
  `github_comment_id` bigint NOT NULL,
  `github_node_id` varchar(255) NOT NULL,
  `minimized_at` datetime DEFAULT NULL,
  `created_at` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP,
  `updated_at` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (`id`),
  KEY `idx_slot` (`repository`,`pull_request`,`database_name`,`database_type`,`minimized_at`),
  KEY `idx_github_comment` (`github_comment_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci
