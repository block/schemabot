package mysqlconn

import (
	"database/sql"
	"fmt"
	"time"

	"github.com/block/spirit/pkg/dbconn"
	"github.com/go-sql-driver/mysql"
)

// Transport timeouts applied to every SchemaBot-managed MySQL connection.
// These exist so a black-holed connection (dropped packets, dead NAT entry,
// half-open TCP) cannot block a goroutine forever — the invariant is that
// every wire operation is bounded, not that it is fast. A DSN that sets its
// own value keeps it.
//
// Spirit's runner connections are unaffected: Spirit opens its own
// connections via its dbconn package, so long copy phases never traverse a
// connection opened here.
const (
	// connectTimeout bounds establishing a new connection (TCP + handshake).
	connectTimeout = 30 * time.Second

	// connReadTimeout bounds each network read. It applies per read, so a
	// streamed result set only needs the server to keep sending; only a
	// statement that is silent on the wire for the whole duration hits it.
	// It must comfortably exceed the longest legitimate silent wait on a
	// SchemaBot-managed connection — the EnsureSchema advisory-lock wait and
	// DDL statements (stale-table drops, pending-drop cleanup) — so it is
	// deliberately long.
	connReadTimeout = 30 * time.Minute

	// connWriteTimeout bounds each network write. Writes are query payloads
	// (statements, schema files), so they complete quickly on a healthy
	// connection.
	connWriteTimeout = 1 * time.Minute
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

// ConnectionDSN returns a MySQL DSN with required transport settings applied:
// connect/read/write timeouts (when the DSN does not set its own) and TLS for
// hosts that require it.
func ConnectionDSN(dsn string) (string, error) {
	cfg, err := mysql.ParseDSN(dsn)
	if err != nil {
		return "", fmt.Errorf("parse DSN: %w", err)
	}
	applyTimeouts(cfg)
	if cfg.TLSConfig != "" {
		return cfg.FormatDSN(), nil
	}
	tlsMode, ok := tlsModeForHost(cfg.Addr)
	if !ok {
		return cfg.FormatDSN(), nil
	}
	dbConfig := dbconn.NewDBConfig()
	dbConfig.TLSMode = tlsMode
	connectionDSN, err := dbconn.EnhanceDSNWithTLS(cfg.FormatDSN(), dbConfig)
	if err != nil {
		return "", fmt.Errorf("enhance RDS DSN with TLS: %w", err)
	}
	return connectionDSN, nil
}

// applyTimeouts fills in the transport timeouts, keeping any value the DSN
// already carries.
func applyTimeouts(cfg *mysql.Config) {
	if cfg.Timeout == 0 {
		cfg.Timeout = connectTimeout
	}
	if cfg.ReadTimeout == 0 {
		cfg.ReadTimeout = connReadTimeout
	}
	if cfg.WriteTimeout == 0 {
		cfg.WriteTimeout = connWriteTimeout
	}
}

func tlsModeForHost(addr string) (string, bool) {
	if dbconn.IsRDSHost(addr) {
		return "REQUIRED", true
	}
	return "", false
}
