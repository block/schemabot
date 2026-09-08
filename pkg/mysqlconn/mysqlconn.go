package mysqlconn

import (
	"database/sql"
	"fmt"
	"time"

	"github.com/block/mysql"
	"github.com/block/schemabot/pkg/connreload"
	"github.com/block/spirit/pkg/dbconn"
)

var openSQL = sql.Open

// Default transport timeouts for SchemaBot-managed MySQL connections, applied
// whenever the parsed value is unset — a DSN or option must set a positive
// timeout to override them, and the zero "no timeout" value is deliberately
// replaced so a managed connection can never be unbounded. The connect
// timeout bounds the TCP dial only: the Go MySQL driver's handshake and auth
// exchange run on the caller's context, so a half-open endpoint that accepts
// the dial but never sends the server greeting unwinds via context
// cancellation like any other read (the PostgreSQL side differs — pgconn's
// ConnectTimeout bounds the whole connection process including TLS and auth).
// The write timeout bounds a single network write; statement text is small,
// so a write blocked this long means the peer is gone. No read-timeout
// default is set, deliberately: the Go MySQL driver's read timeout caps a
// single network read, and a legitimately long-running statement streams no
// bytes until it completes — for example a large DROP TABLE or a DDL waiting
// on a metadata lock — so a read timeout would kill it mid-flight. Reads
// unwind when the caller's context is cancelled (the driver closes the
// connection on cancellation); a call site that must bound a read on its own
// sets an explicit context deadline.
const (
	defaultConnectTimeout = 30 * time.Second
	defaultWriteTimeout   = time.Minute
)

// Option customizes the parsed MySQL config before the DSN is reassembled.
// Options are applied in ConnectionDSN, so they flow through Open,
// OpenReloadable, and the credential-reload path alike.
type Option func(*mysql.Config)

// WithConnectTimeout bounds the TCP dial of a single connection attempt by
// setting the driver's `timeout` DSN parameter; the handshake that follows
// runs on the caller's context. A non-positive duration is ignored, leaving
// any DSN-carried value, or the package default, in place. It does not bound
// query execution — use context deadlines for that.
func WithConnectTimeout(d time.Duration) Option {
	return func(cfg *mysql.Config) {
		if d > 0 {
			cfg.Timeout = d
		}
	}
}

// driverName is block/mysql, Block's fork of go-sql-driver/mysql. It registers
// itself as "block-mysql" rather than "mysql" so that a binary whose dependency
// graph still reaches upstream can link both without two sql.Register calls
// colliding under one name. SchemaBot's own graph no longer reaches upstream at
// all, but the fork's registered name is not SchemaBot's to choose.
//
// Every pool in this package dials through this one driver, and that is
// load-bearing rather than tidy: ConnectionDSN injects tls=rds, and a tls=
// value is a *name* that only resolves inside the registry of the driver
// package that registered it. Spirit registers "rds" into block/mysql. A pool
// opened through any other MySQL driver — as the reloadable pool once was, via
// a hot-swap driver that embedded upstream — cannot resolve the name and fails
// to open against an RDS host at all.
const driverName = "block-mysql"

// Open returns a MySQL connection using the same target-DSN normalization as
// Spirit. Options customize the DSN (for example WithConnectTimeout) before the
// pool is opened.
func Open(dsn string, opts ...Option) (*sql.DB, error) {
	connectionDSN, err := ConnectionDSN(dsn, opts...)
	if err != nil {
		return nil, err
	}
	db, err := openSQL(driverName, connectionDSN)
	if err != nil {
		return nil, fmt.Errorf("open MySQL connection: %w", err)
	}
	return db, nil
}

// OpenReloadable opens a connection pool whose credentials survive rotation of
// the underlying secret. When a new connection is refused with MySQL error 1045
// (access denied) — the signature of a password that was rotated out from under
// a running pod — the pool calls reload to fetch a freshly resolved DSN
// (re-reading the mounted secret) and retries once, so rotation is transparent
// and does not require a restart. reload returns the raw DSN; transport
// settings and options are re-applied here. A reload error keeps the current
// credentials so a transient resolve failure cannot wedge the pool, and starts
// a short cooldown during which further refused dials skip the reload — the DSN
// may resolve through a remote secrets backend, and an outage there must not
// turn every refused dial into a resolve call.
//
//	secret rotated ──► new conn ──► 1045 access denied
//	                                      │
//	                                      ▼
//	                    reload: re-resolve DSN (re-read secret)
//	                                      │ (on error: keep current credentials)
//	                                      ▼
//	                    retry with fresh credentials ──► success
//
// Established connections authenticated before the rotation keep working; only
// new physical connections take the reload path. reload runs only after an
// access-denied failure — never per connection — so a DSN resolved through a
// remote secrets backend is not re-fetched on every dial. The callback belongs
// to this pool alone, so opening two reloadable pools does not have one
// silently inherit the other's credentials. Reserve OpenReloadable for the
// single long-lived storage pool; target-database connections use Open, whose
// credentials come from the apply request rather than the storage secret.
//
// The scheduling — how many reloads a burst of refused dials costs, and how a
// failing secrets backend is backed off — is pkg/connreload's and is shared
// with the PostgreSQL storage pool; see reloadConfig for the MySQL-specific
// half.
func OpenReloadable(dsn string, reload func() (string, error), opts ...Option) (*sql.DB, error) {
	connector, err := connreload.New(dsn, reloadConfig(reload, opts))
	if err != nil {
		return nil, fmt.Errorf("open reloadable MySQL connection: %w", err)
	}
	return sql.OpenDB(connector), nil
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
// the timeout defaults fill non-positive values, so a positive DSN or option
// timeout wins while zero ("no timeout") and negative values — which the
// driver would silently treat as unbounded — are replaced: a managed
// connection is never unbounded.
func requiredSettingsDSN(cfg *mysql.Config, opts ...Option) string {
	for _, opt := range opts {
		opt(cfg)
	}
	if cfg.Timeout <= 0 {
		cfg.Timeout = defaultConnectTimeout
	}
	if cfg.WriteTimeout <= 0 {
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
