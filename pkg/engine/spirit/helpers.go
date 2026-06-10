package spirit

import (
	"database/sql"
	"fmt"
	"strings"

	"github.com/block/spirit/pkg/dbconn"
	"github.com/block/spirit/pkg/statement"
	"github.com/go-sql-driver/mysql"

	"github.com/block/schemabot/pkg/ddl"
	"github.com/block/schemabot/pkg/schema"
)

// parseDSN extracts connection info from a MySQL DSN using the mysql driver's parser.
func parseDSN(dsn string) (host, username, password, database string, err error) {
	cfg, err := mysql.ParseDSN(dsn)
	if err != nil {
		return "", "", "", "", fmt.Errorf("parse DSN: %w", err)
	}
	return cfg.Addr, cfg.User, cfg.Passwd, cfg.DBName, nil
}

func openMySQL(dsn string) (*sql.DB, error) {
	connectionDSN, err := mysqlConnectionDSN(dsn)
	if err != nil {
		return nil, err
	}
	db, err := sql.Open("mysql", connectionDSN)
	if err != nil {
		return nil, fmt.Errorf("open MySQL connection: %w", err)
	}
	return db, nil
}

func mysqlConnectionDSN(dsn string) (string, error) {
	cfg, err := mysql.ParseDSN(dsn)
	if err != nil {
		return "", fmt.Errorf("parse DSN: %w", err)
	}
	if cfg.TLSConfig != "" {
		return dsn, nil
	}
	tlsMode, ok := mysqlTLSModeForHost(cfg.Addr)
	if !ok {
		return dsn, nil
	}
	dbConfig := dbconn.NewDBConfig()
	dbConfig.TLSMode = tlsMode
	connectionDSN, err := dbconn.EnhanceDSNWithTLS(dsn, dbConfig)
	if err != nil {
		return "", fmt.Errorf("enhance RDS DSN with TLS: %w", err)
	}
	return connectionDSN, nil
}

func mysqlTLSModeForHost(addr string) (string, bool) {
	if dbconn.IsRDSHost(addr) {
		return "REQUIRED", true
	}
	return "", false
}

// namespaceForTable finds which namespace a table belongs to by checking
// which namespace's schema files define it. Uses Spirit's parser for accurate
// table name extraction. Returns an error if no namespace can be matched.
func namespaceForTable(table string, sf schema.SchemaFiles) (string, error) {
	for nsName, ns := range sf {
		for filename, content := range ns.Files {
			// Check filename match first (fast path)
			baseName := strings.TrimSuffix(filename, ".sql")
			if baseName == table {
				return nsName, nil
			}
			// Parse the file content — only match CREATE TABLE (the defining statement)
			stmtType, tbl, err := ddl.ClassifyStatement(content)
			if err == nil && stmtType == statement.StatementCreateTable && tbl == table {
				return nsName, nil
			}
		}
	}
	// Single namespace: return the only one
	for nsName := range sf {
		return nsName, nil
	}
	return "", fmt.Errorf("no namespace found for table %q in schema files", table)
}
