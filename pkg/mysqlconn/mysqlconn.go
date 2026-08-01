package mysqlconn

import (
	"context"
	"database/sql"
	"fmt"
	"log/slog"
	"time"

	"github.com/block/spirit/pkg/dbconn"
	dsndriver "github.com/go-mysql/hotswap-dsn-driver"
	"github.com/go-sql-driver/mysql"
)

var openSQL = sql.Open

// Option customizes the parsed MySQL config before the DSN is reassembled.
// Options are applied in ConnectionDSN, so they flow through Open,
// OpenReloadable, and the credential-reload path alike.
type Option func(*mysql.Config)

// WithConnectTimeout bounds a single connection attempt (TCP dial plus
// handshake) by setting the driver's `timeout` DSN parameter. A non-positive
// duration is ignored, leaving the driver default in place. It does not bound
// query execution — use context deadlines for that.
func WithConnectTimeout(d time.Duration) Option {
	return func(cfg *mysql.Config) {
		if d > 0 {
			cfg.Timeout = d
		}
	}
}

// hotswapDriverName is Daniel Nichter's (https://github.com/daniel-nichter)
// hot-swap DSN driver, a drop-in replacement for github.com/go-sql-driver/mysql
// that re-reads credentials on an access-denied error. See OpenReloadable.
const hotswapDriverName = "mysql-hotswap-dsn"

// Open returns a MySQL connection using the same target-DSN normalization as
// Spirit. Options customize the DSN (for example WithConnectTimeout) before the
// pool is opened.
func Open(dsn string, opts ...Option) (*sql.DB, error) {
	connectionDSN, err := ConnectionDSN(dsn, opts...)
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
func OpenReloadable(dsn string, reload func() (string, error), opts ...Option) (*sql.DB, error) {
	connectionDSN, err := ConnectionDSN(dsn, opts...)
	if err != nil {
		return nil, err
	}
	dsndriver.SetHotswapFunc(func(_ context.Context, _ string) string {
		return reloadConnectionDSN(reload, opts...)
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
func reloadConnectionDSN(reload func() (string, error), opts ...Option) string {
	rawDSN, err := reload()
	if err != nil {
		slog.Error("reload storage DSN after access-denied failed; keeping current credentials", "error", err)
		return ""
	}
	reloadedDSN, err := ConnectionDSN(rawDSN, opts...)
	if err != nil {
		slog.Error("apply transport settings to reloaded storage DSN failed; keeping current credentials", "error", err)
		return ""
	}
	slog.Info("reloaded storage credentials after access-denied error")
	return reloadedDSN
}

// ConnectionDSN returns a MySQL DSN with required transport settings applied,
// plus any caller-supplied options (for example WithConnectTimeout). Options
// are applied on every return path so they take effect regardless of whether
// the DSN also needs RDS TLS enhancement.
func ConnectionDSN(dsn string, opts ...Option) (string, error) {
	cfg, err := mysql.ParseDSN(dsn)
	if err != nil {
		return "", fmt.Errorf("parse DSN: %w", err)
	}
	// An explicit TLS config or a non-RDS host needs no TLS enhancement; apply
	// options directly to the parsed config and reassemble.
	if cfg.TLSConfig != "" {
		return applyOptions(dsn, cfg, opts...), nil
	}
	tlsMode, ok := tlsModeForHost(cfg.Addr)
	if !ok {
		return applyOptions(dsn, cfg, opts...), nil
	}
	dbConfig := dbconn.NewDBConfig()
	dbConfig.TLSMode = tlsMode
	connectionDSN, err := dbconn.EnhanceDSNWithTLS(dsn, dbConfig)
	if err != nil {
		return "", fmt.Errorf("enhance RDS DSN with TLS: %w", err)
	}
	if len(opts) == 0 {
		return connectionDSN, nil
	}
	// Re-parse the enhanced DSN so options layer on top of the injected TLS
	// settings rather than discarding them.
	enhanced, err := mysql.ParseDSN(connectionDSN)
	if err != nil {
		return "", fmt.Errorf("parse enhanced DSN: %w", err)
	}
	return applyOptions(connectionDSN, enhanced, opts...), nil
}

// applyOptions runs each option against cfg and returns the reassembled DSN.
// When no options are supplied it returns raw unchanged, preserving the
// original DSN string exactly (FormatDSN may otherwise reorder parameters).
func applyOptions(raw string, cfg *mysql.Config, opts ...Option) string {
	if len(opts) == 0 {
		return raw
	}
	for _, opt := range opts {
		opt(cfg)
	}
	return cfg.FormatDSN()
}

func tlsModeForHost(addr string) (string, bool) {
	if dbconn.IsRDSHost(addr) {
		return "REQUIRED", true
	}
	return "", false
}
