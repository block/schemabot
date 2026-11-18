package api

import (
	"fmt"
	"net/url"
	"strings"

	"github.com/block/mysql"
	"github.com/jackc/pgx/v5"
)

// Resolve the reference first and change only its database, preserving connection
// options and credential rotation. Resolved credentials never enter config files.
func storageDatabaseDSN(dialect, dsn, name string) (string, error) {
	switch dialect {
	case "", "mysql":
		cfg, err := mysql.ParseDSN(dsn)
		if err != nil {
			return "", fmt.Errorf("invalid MySQL storage connection")
		}
		cfg.DBName = name
		return cfg.FormatDSN(), nil
	case "postgres":
		if _, err := pgx.ParseConfig(dsn); err != nil {
			return "", fmt.Errorf("invalid PostgreSQL storage connection")
		}
		if strings.HasPrefix(dsn, "postgres://") || strings.HasPrefix(dsn, "postgresql://") {
			u, err := url.Parse(dsn)
			if err != nil {
				return "", fmt.Errorf("invalid PostgreSQL storage connection")
			}
			u.Path = "/" + name
			u.RawPath = ""
			q := u.Query()
			q.Del("dbname")
			u.RawQuery = q.Encode()
			return u.String(), nil
		}
		name = strings.ReplaceAll(strings.ReplaceAll(name, `\`, `\\`), `'`, `\'`)
		return dsn + " dbname='" + name + "'", nil
	default:
		return "", fmt.Errorf("unsupported storage dialect")
	}
}
