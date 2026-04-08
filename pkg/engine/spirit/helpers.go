package spirit

import (
	"fmt"
	"strings"

	"github.com/go-sql-driver/mysql"

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

// namespaceForTable finds which namespace a table belongs to by checking
// which namespace's schema files define it. Returns an error if no namespace
// can be matched.
func namespaceForTable(table string, sf schema.SchemaFiles) (string, error) {
	for nsName, ns := range sf {
		for filename, content := range ns.Files {
			// Check if this file defines the table (filename match or content match)
			baseName := strings.TrimSuffix(filename, ".sql")
			if baseName == table {
				return nsName, nil
			}
			if strings.Contains(content, "CREATE TABLE "+table) ||
				strings.Contains(content, "CREATE TABLE `"+table+"`") {
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
