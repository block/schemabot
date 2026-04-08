CREATE TABLE products_seq (
  id int unsigned NOT NULL DEFAULT '0',
  next_id bigint unsigned,
  cache bigint unsigned,
  PRIMARY KEY (id)
) COMMENT 'vitess_sequence';
