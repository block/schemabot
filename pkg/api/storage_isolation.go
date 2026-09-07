package api

import (
	"fmt"
	"log/slog"
	"strings"

	"github.com/block/mysql"
	"github.com/jackc/pgx/v5"

	"github.com/block/schemabot/pkg/schema"
)

// ValidateStorageIsolation requires a distinct database name for state and each
// directly connected target in the same database family. Hostnames cannot prove
// separation because aliases and tunnels can reach the same server. Targets without a
// locally configured DSN need isolation checks where their concrete connection
// and namespace are resolved; this name comparison cannot verify them.
func (c *ServerConfig) ValidateStorageIsolation() error {
	dialect, err := c.Storage.ResolveDialect()
	if err != nil {
		return fmt.Errorf("resolve storage dialect: %w", err)
	}
	dsn, err := c.StorageDSN()
	if err != nil {
		return fmt.Errorf("resolve storage: %w", err)
	}
	storageName, err := connectionDatabaseName(dialect, dsn)
	if err != nil {
		return fmt.Errorf("storage: %w", err)
	}
	for name, db := range c.Databases {
		targetDialect := schema.DialectForDatabaseType(db.Type)
		for environment, target := range db.Environments {
			if !target.HasLocalDSN() {
				slog.Debug("storage isolation requires a resolved target connection and namespace", "database", name, "environment", environment)
				continue
			}
			targetDSN, err := target.ResolveDSN()
			if err != nil {
				return fmt.Errorf("resolve database %q environment %q: %w", name, environment, err)
			}
			targetName, err := connectionDatabaseName(targetDialect, targetDSN)
			if err != nil {
				return fmt.Errorf("database %q environment %q: %w", name, environment, err)
			}
			if dialect == targetDialect && strings.EqualFold(storageName, targetName) {
				return fmt.Errorf("storage database must have a different name from target %q", name)
			}
		}
	}
	return nil
}

// Parse failures deliberately omit connection material from operator errors.
func connectionDatabaseName(dialect schema.Dialect, dsn string) (string, error) {
	var name string
	switch dialect {
	case schema.DialectMySQL:
		cfg, err := mysql.ParseDSN(dsn)
		if err != nil {
			return "", fmt.Errorf("invalid MySQL DSN")
		}
		name = cfg.DBName
	case schema.DialectPostgres:
		cfg, err := pgx.ParseConfig(dsn)
		if err != nil {
			return "", fmt.Errorf("invalid PostgreSQL DSN")
		}
		name = cfg.Database
	default:
		return "", fmt.Errorf("unsupported connection dialect %q", dialect)
	}
	if name == "" {
		return "", fmt.Errorf("DSN must name a database")
	}
	return name, nil
}
