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

// Default transport timeouts for SchemaBot-managed MySQL connections, applied
// only when neither the DSN nor an option sets its own value. The connect
// timeout bounds the TCP dial plus handshake, so a connection attempt against
// an unreachable or half-open endpoint fails and is retried instead of
// blocking its caller indefinitely. The write timeout bounds a single network
// write; statement text is small, so a write blocked this long means the peer
// is gone. No read-timeout default is set, deliberately: the Go MySQL driver's
// read timeout caps a single network read, and a legitimately long-running
// statement streams no bytes until it completes — for example a large DROP
// TABLE or a DDL waiting on a metadata lock — so a read timeout would kill it
// mid-flight. Reads are bounded by context deadlines at the call sites that
// need them.
const (
	defaultConnectTimeout = 30 * time.Second
	defaultWriteTimeout   = time.Minute
)

// Option customizes the parsed MySQL config before the DSN is reassembled.
// Options are applied in ConnectionDSN, so they flow through Open,
// OpenReloadable, and the credential-reload path alike.
type Option func(*mysql.Config)

// WithConnectTimeout bounds a single connection attempt (TCP dial plus
// handshake) by setting the driver's `timeout` DSN parameter. A non-positive
// duration is ignored, leaving the package's default connect timeout in
// place. It does not bound query execution — use context deadlines for that.
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

// ConnectionDSN returns a MySQL DSN with required connection settings applied
// (RDS TLS, client-side parameter interpolation, default transport timeouts),
// plus any caller-supplied options (for example WithConnectTimeout). Settings
// and options are applied on every return path so they take effect regardless
// of whether the DSN also needs RDS TLS enhancement.
func ConnectionDSN(dsn string, opts ...Option) (string, error) {
	cfg, err := mysql.ParseDSN(dsn)
	if err != nil {
		return "", fmt.Errorf("parse DSN: %w", err)
	}
	// An explicit TLS config or a non-RDS host needs no TLS enhancement; apply
	// the required settings and options directly to the parsed config and
	// reassemble.
	if cfg.TLSConfig != "" {
		return requiredSettingsDSN(cfg, opts...), nil
	}
	tlsMode, ok := tlsModeForHost(cfg.Addr)
	if !ok {
		return requiredSettingsDSN(cfg, opts...), nil
	}
	dbConfig := dbconn.NewDBConfig()
	dbConfig.TLSMode = tlsMode
	connectionDSN, err := dbconn.EnhanceDSNWithTLS(dsn, dbConfig)
	if err != nil {
		return "", fmt.Errorf("enhance RDS DSN with TLS: %w", err)
	}
	// Re-parse the enhanced DSN so the required settings and options layer on
	// top of the injected TLS settings rather than discarding them.
	enhanced, err := mysql.ParseDSN(connectionDSN)
	if err != nil {
		return "", fmt.Errorf("parse enhanced DSN: %w", err)
	}
	return requiredSettingsDSN(enhanced, opts...), nil
}

// requiredSettingsDSN applies any caller-supplied options, then default
// transport timeouts for values still unset, then the settings every
// SchemaBot-managed MySQL connection needs, and returns the reassembled DSN.
// Required settings are applied after options so no option can override them;
// the timeout defaults fill only zero values so a DSN or option that sets its
// own timeout wins.
func requiredSettingsDSN(cfg *mysql.Config, opts ...Option) string {
	for _, opt := range opts {
		opt(cfg)
	}
	if cfg.Timeout == 0 {
		cfg.Timeout = defaultConnectTimeout
	}
	if cfg.WriteTimeout == 0 {
		cfg.WriteTimeout = defaultWriteTimeout
	}
	// Interpolate query parameters client-side instead of using server-side
	// prepared statements. database/sql prepares, executes once, and closes on
	// every parameterized query, so server-side preparation buys no statement
	// reuse — it costs extra round trips and consumes the server-global
	// max_prepared_stmt_count budget, which other clients of a shared target
	// can exhaust (MySQL error 1461). The Go MySQL driver escapes interpolated
	// values and refuses to interpolate under charsets where escaping is
	// unsafe.
	cfg.InterpolateParams = true
	return cfg.FormatDSN()
}

func tlsModeForHost(addr string) (string, bool) {
	if dbconn.IsRDSHost(addr) {
		return "REQUIRED", true
	}
	return "", false
}
