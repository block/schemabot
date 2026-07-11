package mysqlconn

import (
	"context"
	"database/sql"
	"fmt"
	"log/slog"

	"github.com/block/spirit/pkg/dbconn"
	dsndriver "github.com/go-mysql/hotswap-dsn-driver"
	"github.com/go-sql-driver/mysql"
)

var openSQL = sql.Open

// hotswapDriverName is Daniel Nichter's (https://github.com/daniel-nichter)
// hot-swap DSN driver, a drop-in replacement for github.com/go-sql-driver/mysql
// that re-reads credentials on an access-denied error. See OpenReloadable.
const hotswapDriverName = "mysql-hotswap-dsn"

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

// OpenReloadable opens a connection whose credentials survive rotation of the
// underlying secret. When a new connection is rejected with MySQL error 1045
// (access denied) — the signature of a password that was rotated out from under
// a running pod — the driver calls reload to fetch a freshly resolved DSN
// (re-reading the mounted secret) and retries, so rotation is transparent and
// does not require a restart. reload returns the raw DSN; transport settings
// are re-applied here. A reload error keeps the current credentials so a
// transient resolve failure cannot wedge the pool with an empty DSN.
//
//	secret rotated ──► new conn ──► 1045 access denied
//	                                      │
//	                                      ▼
//	                    reload: re-resolve DSN (re-read secret)
//	                                      │ (on error: keep current DSN)
//	                                      ▼
//	                    retry with fresh credentials ──► success
//
// The reload callback is registered process-global on the hot-swap driver and
// applies to every connection opened with that driver; each OpenReloadable call
// replaces it. OpenReloadable is the only path that opens with the hot-swap
// driver, so reserve it for the single long-lived storage pool. Target-database
// connections use Open, whose credentials come from the apply request rather
// than the storage secret.
func OpenReloadable(dsn string, reload func() (string, error)) (*sql.DB, error) {
	connectionDSN, err := ConnectionDSN(dsn)
	if err != nil {
		return nil, err
	}
	dsndriver.SetHotswapFunc(func(_ context.Context, _ string) string {
		return reloadConnectionDSN(reload)
	})
	db, err := openSQL(hotswapDriverName, connectionDSN)
	if err != nil {
		return nil, fmt.Errorf("open reloadable MySQL connection: %w", err)
	}
	return db, nil
}

// reloadConnectionDSN resolves a fresh DSN and re-applies transport settings for
// the hot-swap driver. It returns "" — meaning "keep the current DSN" — when the
// reload or transport step fails, so a transient error cannot wedge the pool
// with an empty DSN.
func reloadConnectionDSN(reload func() (string, error)) string {
	rawDSN, err := reload()
	if err != nil {
		slog.Error("reload storage DSN after access-denied failed; keeping current credentials", "error", err)
		return ""
	}
	reloadedDSN, err := ConnectionDSN(rawDSN)
	if err != nil {
		slog.Error("apply transport settings to reloaded storage DSN failed; keeping current credentials", "error", err)
		return ""
	}
	slog.Info("reloaded storage credentials after access-denied error")
	return reloadedDSN
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
