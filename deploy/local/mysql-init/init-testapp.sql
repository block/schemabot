-- Create testapp database for schema change testing
-- This runs on first MySQL startup only

CREATE DATABASE IF NOT EXISTS testapp;

-- Grant permissions (root already has them, but explicit is good)
GRANT ALL PRIVILEGES ON testapp.* TO 'root'@'%';
FLUSH PRIVILEGES;
