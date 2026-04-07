CREATE TABLE orders (
  id bigint unsigned NOT NULL,
  user_id bigint unsigned NOT NULL,
  total_cents bigint NOT NULL,
  status varchar(50) NOT NULL DEFAULT 'pending',
  created_at timestamp DEFAULT CURRENT_TIMESTAMP,
  updated_at timestamp DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (id),
  KEY idx_user_id (user_id),
  KEY idx_status (status),
  KEY idx_created_at (created_at)
);
