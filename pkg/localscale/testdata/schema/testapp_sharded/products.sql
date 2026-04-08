CREATE TABLE products (
  id bigint unsigned NOT NULL,
  name varchar(255) NOT NULL,
  description text NULL,
  price_cents bigint NOT NULL,
  sku varchar(100) NOT NULL,
  created_at timestamp DEFAULT CURRENT_TIMESTAMP,
  updated_at timestamp DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (id),
  KEY idx_name (name),
  UNIQUE KEY idx_sku (sku),
  KEY idx_price (price_cents)
);
