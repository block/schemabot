package mysqlconn

import (
	"database/sql"
	"fmt"

	"github.com/block/spirit/pkg/dbconn"
	"github.com/go-sql-driver/mysql"
)

var openSQL = sql.Open

// Open returns a MySQL connection using the same target-DSN normalization as Spirit.
func Open(dsn string) (*sql.DB, error) {
	connectionDSN, err := ConnectionDSN(dsn)
	if err != nil {
		return nil, err
	}
	db, err := openSQL("mysql", connectionDSN)
	if err != nil {
		return nil, fmt.Errorf("open MySQL connection: %w", err)
	}
	return db, nil
}

// ConnectionDSN returns a MySQL DSN with required transport settings applied.
func ConnectionDSN(dsn string) (string, error) {
	cfg, err := mysql.ParseDSN(dsn)
	if err != nil {
		return "", fmt.Errorf("parse DSN: %w", err)
	}
	if cfg.TLSConfig != "" {
		return dsn, nil
	}
	tlsMode, ok := tlsModeForHost(cfg.Addr)
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

func tlsModeForHost(addr string) (string, bool) {
	if dbconn.IsRDSHost(addr) {
		return "REQUIRED", true
	}
	return "", false
}
