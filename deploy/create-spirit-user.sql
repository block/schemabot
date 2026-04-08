-- Manual reference for the MySQL grants that SchemaBot needs.
-- Prefer using 'schemabot init' which does this automatically:
--
--   schemabot init --dsn "root:password@tcp(localhost:3306)/" -d myapp
--
-- If you prefer manual setup, run this as the admin/master user:
--   mysql -h <host> -u admin -p < deploy/create-spirit-user.sql

-- Replace <password> with a strong password.
CREATE USER IF NOT EXISTS 'schemabot'@'%' IDENTIFIED BY '<password>';

-- Global privileges required by Spirit:
--   REPLICATION CLIENT + REPLICATION SLAVE: stream binlog events for change capture
--   RELOAD: FLUSH TABLES during cutover
--   PROCESS: monitor running queries
GRANT REPLICATION CLIENT, REPLICATION SLAVE, RELOAD, PROCESS ON *.* TO 'schemabot'@'%';

-- Force-kill privileges:
--   CONNECTION_ADMIN: kill blocking transactions during cutover
--   SELECT on performance_schema: identify locking transactions
GRANT CONNECTION_ADMIN ON *.* TO 'schemabot'@'%';
GRANT SELECT ON `performance_schema`.* TO 'schemabot'@'%';

-- SchemaBot storage database (internal state: locks, plans, applies).
CREATE DATABASE IF NOT EXISTS `schemabot`;
GRANT ALL PRIVILEGES ON `schemabot`.* TO 'schemabot'@'%';

-- Per-database privileges. Repeat this block for each target database.
-- Replace <database> with your application database name.
GRANT ALTER, CREATE, DELETE, DROP, INDEX, INSERT, LOCK TABLES, SELECT, TRIGGER, UPDATE ON `<database>`.* TO 'schemabot'@'%';
