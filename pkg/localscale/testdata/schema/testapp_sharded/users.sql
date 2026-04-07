CREATE TABLE users (
  id bigint unsigned NOT NULL,
  email varchar(255) NOT NULL,
  full_name varchar(255) NULL,
  created_at timestamp DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY (id),
  UNIQUE KEY idx_email (email),
  KEY idx_created_at (created_at)
);
