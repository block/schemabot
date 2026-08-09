// Package postgresconn opens SchemaBot-managed PostgreSQL connections with
// the same guarantees mysqlconn provides for MySQL: centralized DSN
// normalization (required TLS for RDS targets) and a storage pool whose
// credentials survive secret rotation. Use Open for target-database
// connections and OpenReloadable for the single long-lived storage pool.
package postgresconn

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
	"log/slog"
	"net/url"
	"regexp"
	"strings"
	"sync"
	"time"

	"github.com/block/spirit/pkg/dbconn"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/stdlib"
)

// connectConfig dials one physical connection for the given config. It is a
// seam so tests can exercise the credential-reload path without a server.
var connectConfig = func(ctx context.Context, cfg pgx.ConnConfig) (driver.Conn, error) {
	return stdlib.GetConnector(cfg).Connect(ctx)
}

// Option customizes the parsed PostgreSQL config before the pool is opened.
// Options are applied in connectionConfig, so they flow through Open,
// OpenReloadable, and the credential-reload path alike.
type Option func(*pgx.ConnConfig)

// WithConnectTimeout bounds a single connection attempt (TCP dial plus
// handshake). A non-positive duration is ignored, leaving the driver default
// in place. It does not bound query execution — use context deadlines for
// that.
func WithConnectTimeout(d time.Duration) Option {
	return func(cfg *pgx.ConnConfig) {
		if d > 0 {
			cfg.ConnectTimeout = d
		}
	}
}

// Open returns a PostgreSQL connection pool with SchemaBot's required
// transport settings applied (see ConnectionDSN). Options customize the
// parsed config (for example WithConnectTimeout) before the pool is opened.
func Open(dsn string, opts ...Option) (*sql.DB, error) {
	cfg, err := connectionConfig(dsn, opts...)
	if err != nil {
		return nil, err
	}
	return sql.OpenDB(stdlib.GetConnector(*cfg)), nil
}

// OpenReloadable opens a connection pool whose credentials survive rotation
// of the underlying secret. When a new connection is rejected with an
// authentication error (SQLSTATE 28P01/28000) — the signature of a password
// that was rotated out from under a running pod — the pool calls reload to
// fetch a freshly resolved DSN (re-reading the mounted secret) and retries
// once, so rotation is transparent and does not require a restart. reload
// returns the raw DSN; transport settings and options are re-applied here. A
// reload error keeps the current credentials so a transient resolve failure
// cannot wedge the pool, and starts a short cooldown during which further
// failed dials skip the reload — the DSN may resolve through a remote secrets
// backend, and an outage there must not turn every rejected dial into a
// resolve call.
//
//	secret rotated ──► new conn ──► 28P01 invalid password
//	                                      │
//	                                      ▼
//	                    reload: re-resolve DSN (re-read secret)
//	                                      │ (on error: keep current config)
//	                                      ▼
//	                    retry with fresh credentials ──► success
//
// Established connections authenticated before the rotation keep working;
// only new physical connections take the reload path. reload runs only after
// an authentication failure — never per connection — so a DSN resolved
// through a remote secrets backend is not re-fetched on every dial. Reserve
// OpenReloadable for the single long-lived storage pool; target-database
// connections use Open, whose credentials come from the apply request rather
// than the storage secret.
func OpenReloadable(dsn string, reload func() (string, error), opts ...Option) (*sql.DB, error) {
	cfg, err := connectionConfig(dsn, opts...)
	if err != nil {
		return nil, err
	}
	return sql.OpenDB(&reloadableConnector{cfg: cfg, reload: reload, opts: opts}), nil
}

// reloadCooldown bounds how often a failing reload is retried. After a reload
// fails, further authentication-failed dials within this window surface their
// dial error without invoking reload again, so a secrets-backend outage costs
// at most one resolve attempt per window instead of one per rejected dial.
const reloadCooldown = 30 * time.Second

// reloadableConnector dials with the most recently resolved credentials and
// refreshes them, at most once per failed attempt, when a dial is rejected as
// unauthenticated. gen counts credential swaps so concurrent failed dials
// trigger a single reload: a dial that failed with an already-superseded
// config retries with the current one instead of reloading again.
type reloadableConnector struct {
	reload func() (string, error)
	opts   []Option
	now    func() time.Time // test seam; nil means time.Now

	mu             sync.Mutex
	cfg            *pgx.ConnConfig
	gen            uint64
	lastReloadFail time.Time
}

var _ driver.Connector = (*reloadableConnector)(nil)

func (c *reloadableConnector) Connect(ctx context.Context) (driver.Conn, error) {
	cfg, gen := c.snapshot()
	conn, err := connectConfig(ctx, *cfg)
	if err == nil || !isAuthError(err) {
		return conn, err
	}
	fresh, ok := c.refresh(gen)
	if !ok {
		// Reload failed; surface the authentication error that triggered it.
		return nil, err
	}
	return connectConfig(ctx, *fresh)
}

func (c *reloadableConnector) Driver() driver.Driver { return stdlib.GetDefaultDriver() }

func (c *reloadableConnector) snapshot() (*pgx.ConnConfig, uint64) {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.cfg, c.gen
}

// refresh resolves fresh credentials after a dial using generation failedGen
// was rejected as unauthenticated. When another dial already swapped the
// config, the current one is returned without reloading again. A reload or
// parse error keeps the current config and reports false, so a transient
// resolve failure cannot wedge the pool; it also arms reloadCooldown so a
// secrets-backend outage is retried once per window, not once per rejected
// dial.
func (c *reloadableConnector) refresh(failedGen uint64) (*pgx.ConnConfig, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.gen != failedGen {
		return c.cfg, true
	}
	now := time.Now
	if c.now != nil {
		now = c.now
	}
	if !c.lastReloadFail.IsZero() && now().Sub(c.lastReloadFail) < reloadCooldown {
		slog.Debug("skipping storage DSN reload during cooldown after a failed reload; surfacing the dial error")
		return nil, false
	}
	rawDSN, err := c.reload()
	if err != nil {
		c.lastReloadFail = now()
		slog.Error("reload storage DSN after authentication failure failed; keeping current credentials", "error", err)
		return nil, false
	}
	cfg, err := connectionConfig(rawDSN, c.opts...)
	if err != nil {
		c.lastReloadFail = now()
		slog.Error("parse reloaded storage DSN failed; keeping current credentials", "error", err)
		return nil, false
	}
	c.cfg = cfg
	c.gen++
	c.lastReloadFail = time.Time{}
	slog.Info("reloaded storage credentials after authentication failure")
	return cfg, true
}

// isAuthError reports whether err is the server rejecting the connection's
// credentials: 28P01 (invalid_password) is the rotation signature, and 28000
// (invalid_authorization_specification) covers rotation strategies that swap
// the role itself. A spurious reload on a non-rotation 28000 is harmless —
// the reload either resolves the same DSN or fails and keeps the current one.
func isAuthError(err error) bool {
	var pgErr *pgconn.PgError
	if !errors.As(err, &pgErr) {
		return false
	}
	return pgErr.Code == "28P01" || pgErr.Code == "28000"
}

// dsnParseError wraps a DSN parse failure without echoing the DSN itself.
// A *url.Error in the chain reproduces the full URL — password included — in
// its message, so only its underlying cause is kept; pgx's own
// *pgconn.ParseConfigError already redacts the password in its message but
// can carry a *url.Error inside, so the chain is checked either way.
func dsnParseError(err error) error {
	var ue *url.Error
	if errors.As(err, &ue) {
		return fmt.Errorf("parse PostgreSQL DSN: %w", ue.Err)
	}
	return fmt.Errorf("parse PostgreSQL DSN: %w", err)
}

// connectionConfig parses a normalized DSN (see ConnectionDSN) and applies
// caller-supplied options to the resulting config.
func connectionConfig(dsn string, opts ...Option) (*pgx.ConnConfig, error) {
	normalized, err := ConnectionDSN(dsn)
	if err != nil {
		return nil, err
	}
	cfg, err := pgx.ParseConfig(normalized)
	if err != nil {
		return nil, dsnParseError(err)
	}
	for _, opt := range opts {
		opt(cfg)
	}
	return cfg, nil
}

// ConnectionDSN returns a PostgreSQL DSN with required transport settings
// applied: an RDS host with no explicit sslmode gets sslmode=require, the
// counterpart of the TLS mode mysqlconn injects for RDS MySQL targets. An
// explicit sslmode — including disable — always wins, and non-RDS hosts are
// left untouched. Both DSN forms are handled: URL
// (postgres://user:pass@host/db) and keyword/value (host=... user=...).
// RDS detection considers only the DSN's first host: a multi-host DSN whose
// RDS host is a fallback gets no injection, so spell out sslmode explicitly
// in multi-host DSNs.
func ConnectionDSN(dsn string) (string, error) {
	cfg, err := pgx.ParseConfig(dsn)
	if err != nil {
		return "", dsnParseError(err)
	}
	if !dbconn.IsRDSHost(cfg.Host) {
		return dsn, nil
	}
	if isURLForm(dsn) {
		return enhanceURLDSN(dsn)
	}
	return enhanceKeywordDSN(dsn), nil
}

func isURLForm(dsn string) bool {
	return strings.HasPrefix(dsn, "postgres://") || strings.HasPrefix(dsn, "postgresql://")
}

// enhanceURLDSN sets sslmode=require in the query string of a URL-form DSN
// unless the DSN already carries an explicit sslmode.
func enhanceURLDSN(dsn string) (string, error) {
	u, err := url.Parse(dsn)
	if err != nil {
		return "", dsnParseError(err)
	}
	q := u.Query()
	if q.Has("sslmode") {
		return dsn, nil
	}
	q.Set("sslmode", "require")
	u.RawQuery = q.Encode()
	return u.String(), nil
}

// keywordSSLMode matches an sslmode keyword in a keyword/value DSN. The
// keyword parser accepts whitespace around '=', so `sslmode = disable` is a
// legal, explicit setting and must be detected as such.
var keywordSSLMode = regexp.MustCompile(`(^|\s)sslmode\s*=`)

// enhanceKeywordDSN appends sslmode=require to a keyword/value DSN unless a
// sslmode keyword is already present. In the keyword form a repeated keyword's
// last occurrence wins, so appending is only safe when absent — an explicit
// sslmode is never overridden. A quoted value that itself contains " sslmode="
// would be a false positive, which fails open to pgx's own defaults rather
// than overriding the caller.
func enhanceKeywordDSN(dsn string) string {
	if keywordSSLMode.MatchString(dsn) {
		return dsn
	}
	return dsn + " sslmode=require"
}
