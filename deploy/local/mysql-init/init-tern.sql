-- Create testapp database for schema changes (Spirit target)
-- This is separate from the tern database (used for SchemaBot storage)

CREATE DATABASE IF NOT EXISTS testapp;

GRANT ALL PRIVILEGES ON testapp.* TO 'root'@'%';
FLUSH PRIVILEGES;
