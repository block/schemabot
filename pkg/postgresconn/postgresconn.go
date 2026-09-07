// Package postgresconn opens SchemaBot-managed PostgreSQL connections:
// centralized DSN normalization (required TLS for RDS targets, mirroring the
// TLS mode mysqlconn injects for RDS MySQL), a UTC session timezone unless
// the DSN sets one, and a storage pool whose credentials survive secret
// rotation. Use Open for target-database connections and OpenReloadable for
// the single long-lived storage pool.
package postgresconn

import (
	"context"
	"crypto/x509"
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
	"log/slog"
	"math"
	"net/url"
	"regexp"
	"strconv"
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

// defaultConnectTimeout bounds a single connection attempt for every
// SchemaBot-managed PostgreSQL connection whose parsed connect timeout is not
// positive — the DSN or an option must set a positive connect_timeout to
// override it, and the zero "wait indefinitely" value is deliberately
// replaced — so an attempt against an unreachable or half-open endpoint fails
// and is retried instead of blocking its caller indefinitely. pgconn applies
// it to the whole connection process: dial, TLS, startup, and auth.
const defaultConnectTimeout = 30 * time.Second

// WithConnectTimeout bounds a single connection attempt — pgconn applies it
// to the whole connection process: dial, TLS, startup, and auth. A
// non-positive duration is ignored, leaving any DSN-carried value, or the
// package default, in place. It does not bound query execution — use context
// deadlines for that.
func WithConnectTimeout(d time.Duration) Option {
	return func(cfg *pgx.ConnConfig) {
		if d > 0 {
			cfg.ConnectTimeout = d
		}
	}
}

// MaxStatementTimeout is the largest budget PostgreSQL accepts, since
// statement_timeout is a millisecond integer GUC and the server rejects
// anything above the signed 32-bit maximum. Exceeding it is not a clamp but a
// FATAL raised while the backend applies the startup packet, so every
// connection fails at dial rather than one statement failing late. Exported so
// config validation can refuse the value where an operator can still see it.
const MaxStatementTimeout = time.Duration(math.MaxInt32) * time.Millisecond

// WithStatementTimeout bounds how long the server lets a single statement run
// on every connection the pool opens, as a session statement_timeout carried
// in the startup packet. A zero duration disables the budget explicitly
// (statement_timeout=0), which is not the same as omitting the option: the
// option always writes the parameter, so the connection runs under SchemaBot's
// stated budget rather than whatever the platform set at the role or database
// level. Omitting it inherits that ambient value.
//
// A negative duration leaves the parameter untouched, so a DSN-carried
// statement_timeout keeps whatever it set. A positive duration finer than the
// millisecond statement_timeout is expressed in rounds up, never down to the
// zero that would disable it.
//
// There is deliberately no package-level default. statement_timeout bounds
// *any* statement, including one that is legitimately blocking: the
// EnsureSchema advisory lock waits inside SELECT pg_advisory_lock() for as
// long as its lock_timeout allows, and a default budget below that wait would
// cancel a trailing pod's legitimate queue for the leader's bootstrap. Callers
// opt in with a budget they can justify for the statements they run.
func WithStatementTimeout(d time.Duration) Option {
	return func(cfg *pgx.ConnConfig) {
		if d < 0 {
			return
		}
		setRuntimeParam(cfg, "statement_timeout", strconv.FormatInt(statementTimeoutMillis(d), 10))
	}
}

// statementTimeoutMillis converts d to the whole milliseconds
// statement_timeout is expressed in, rounding a positive duration up.
// Truncating instead would let a sub-millisecond budget land on 0, which the
// server reads as no budget at all — turning the shortest budget a caller can
// ask for into its absence, the one direction this option exists to rule out.
// Zero is passed through, because there it is the caller's explicit disable
// rather than a rounding artifact.
func statementTimeoutMillis(d time.Duration) int64 {
	if d == 0 {
		return 0
	}
	// (d-1)/ms + 1 rounds up without the overflow that (d+ms-1)/ms risks near
	// the maximum duration.
	return int64((d-1)/time.Millisecond) + 1
}

// setRuntimeParam sets a startup-packet parameter, first removing any
// differently-cased spelling of the same name. GUC names are case-insensitive
// on the server and pgx preserves DSN key case in RuntimeParams, so a plain
// assignment could leave two spellings of one parameter in the startup packet
// and let map iteration order decide which wins.
func setRuntimeParam(cfg *pgx.ConnConfig, key, value string) {
	for k := range cfg.RuntimeParams {
		if strings.EqualFold(k, key) {
			delete(cfg.RuntimeParams, k)
		}
	}
	cfg.RuntimeParams[key] = value
}

// WithRootCAs pins the certificate authorities the connection trusts when the
// DSN requests certificate verification, replacing the trust the DSN or the
// RDS default would otherwise install. The caller parses the bundle, so a
// missing or malformed bundle fails closed where it is resolved rather than
// inside a later dial. A DSN that does not negotiate TLS carries no trust to
// pin, so the option leaves it untouched. Pinning also removes pgx's
// parse-time fallbacks (a plaintext retry under sslmode=prefer, a weaker TLS
// retry under allow): a fallback would bypass the pinned bundle entirely, so
// the connection either verifies against it or fails. The pin only takes
// effect when the DSN verifies the server certificate (sslmode=verify-full or
// verify-ca): require and prefer negotiate TLS without verification, so roots
// pinned onto those modes are never consulted, just as a disable DSN
// negotiates no TLS at all. Callers that must enforce the pin should gate on
// VerifiesServerCertificate and refuse the other modes.
func WithRootCAs(roots *x509.CertPool) Option {
	return func(cfg *pgx.ConnConfig) {
		if cfg.TLSConfig == nil {
			return
		}
		cfg.TLSConfig.RootCAs = roots
		cfg.Fallbacks = nil
	}
}

// Open returns a PostgreSQL connection pool with SchemaBot's required
// transport settings applied (see ConnectionDSN). A DSN that requests
// certificate verification against an RDS host without naming a root bundle
// (sslmode=verify-full, no sslrootcert) verifies against the embedded RDS
// global CA bundle. Options customize the parsed config (for example
// WithConnectTimeout) before the pool is opened.
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
// The window is also armed when a reload succeeds but the dial retrying with
// the reloaded credentials is rejected too — a backend that keeps answering
// with a credential the server refuses (stale secret sync, dropped role,
// pg_hba mismatch) likewise costs one resolve per window, not one per
// connection.
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
	reloading      chan struct{} // non-nil while a reload for the current generation is in flight; closed when it finishes
}

var _ driver.Connector = (*reloadableConnector)(nil)

func (c *reloadableConnector) Connect(ctx context.Context) (driver.Conn, error) {
	cfg, gen := c.snapshot()
	conn, err := connectConfig(ctx, *cfg)
	if err == nil || !isAuthError(err) {
		return conn, err
	}
	fresh, freshGen, ok := c.refresh(ctx, gen)
	if !ok {
		// Reload failed; surface the authentication error that triggered it.
		return nil, err
	}
	conn, err = connectConfig(ctx, *fresh)
	if err != nil && isAuthError(err) {
		// The freshly resolved credentials are no better: the secret store
		// keeps answering with a credential the server refuses. Arm the
		// cooldown so subsequent rejected dials back off instead of
		// resolving once per connection.
		c.armReloadCooldown(freshGen)
		slog.Warn("dial with reloaded storage credentials was also rejected; backing off further reloads", "error", err)
	}
	return conn, err
}

// armReloadCooldown starts a reloadCooldown window as if a reload had failed,
// bounding resolve traffic when reloads succeed but the credentials they
// return keep being rejected. rejectedGen is the generation whose credentials
// were rejected: when the connector has already advanced past it, the arm is
// a stale verdict on superseded credentials and must not suppress the newer
// generation's reloads.
func (c *reloadableConnector) armReloadCooldown(rejectedGen uint64) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.gen != rejectedGen {
		slog.Debug("skipping reload cooldown arm: credentials advanced past the rejected generation",
			"rejected_gen", rejectedGen, "current_gen", c.gen)
		return
	}
	c.lastReloadFail = c.clock()
}

func (c *reloadableConnector) Driver() driver.Driver { return stdlib.GetDefaultDriver() }

func (c *reloadableConnector) snapshot() (*pgx.ConnConfig, uint64) {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.cfg, c.gen
}

// clock returns the current time, honoring the test seam.
func (c *reloadableConnector) clock() time.Time {
	if c.now != nil {
		return c.now()
	}
	return time.Now()
}

// refresh resolves fresh credentials after a dial using generation failedGen
// was rejected as unauthenticated. When another dial already swapped the
// config, the current one is returned without reloading again. A reload or
// parse error keeps the current config and reports false, so a transient
// resolve failure cannot wedge the pool; it also arms reloadCooldown so a
// secrets-backend outage is retried once per window, not once per rejected
// dial. On success it also returns the generation the returned config
// belongs to, so a later rejection of that config can be attributed to the
// right generation.
//
// The reload callback runs detached from every dial — outside the connector
// mutex and on its own goroutine — so a hung secret resolution can neither
// block healthy dials from snapshotting the current config nor pin the dial
// that elected it: database/sql counts a dial against the pool's connection
// budget before Connect runs, so a pinned dial would hold a pool slot for as
// long as the reload hangs. The reloading guard keeps it to one reload in
// flight at a time: every same-generation failure, the electing dial
// included, waits for the reload's outcome — or gives up when its own dial
// context ends, surfacing the dial error.
func (c *reloadableConnector) refresh(ctx context.Context, failedGen uint64) (*pgx.ConnConfig, uint64, bool) {
	for {
		c.mu.Lock()
		if c.gen != failedGen {
			cfg, gen := c.cfg, c.gen
			c.mu.Unlock()
			return cfg, gen, true
		}
		if !c.lastReloadFail.IsZero() && c.clock().Sub(c.lastReloadFail) < reloadCooldown {
			c.mu.Unlock()
			slog.Debug("skipping storage DSN reload during cooldown after a failed reload; surfacing the dial error")
			return nil, 0, false
		}
		done := c.reloading
		if done == nil {
			done = make(chan struct{})
			c.reloading = done
			go c.runReload(done)
		}
		c.mu.Unlock()
		select {
		case <-done:
			// The reload finished; re-check the connector state to pick up
			// the swapped config or the armed cooldown.
		case <-ctx.Done():
			slog.Debug("dial context ended while waiting for an in-flight storage DSN reload; surfacing the dial error")
			return nil, 0, false
		}
	}
}

// runReload invokes the reload callback and parses its DSN outside the
// connector mutex, then publishes the outcome under it: success swaps the
// config, advances the generation, and clears the cooldown; failure arms
// reloadCooldown. It runs on its own goroutine, detached from the dial that
// elected it, so waiters observe the outcome through the connector state
// rather than a return value. The publish runs in a defer so the reloading
// guard is released and waiters are unblocked even if the callback panics;
// the panic is recovered and treated as a failed reload — a detached
// goroutine has no caller to propagate it to, and a panicking secret
// resolver must leave the pool on its current credentials, not crash the
// process.
func (c *reloadableConnector) runReload(done chan struct{}) {
	var fresh *pgx.ConnConfig
	defer func() {
		if r := recover(); r != nil {
			slog.Error("reload storage DSN after authentication failure panicked; keeping current credentials", "panic", r)
		}
		c.mu.Lock()
		defer c.mu.Unlock()
		c.reloading = nil
		close(done)
		if fresh == nil {
			c.lastReloadFail = c.clock()
			return
		}
		c.cfg = fresh
		c.gen++
		c.lastReloadFail = time.Time{}
		slog.Info("reloaded storage credentials after authentication failure")
	}()

	rawDSN, err := c.reload()
	if err != nil {
		slog.Error("reload storage DSN after authentication failure failed; keeping current credentials", "error", err)
		return
	}
	cfg, err := connectionConfig(rawDSN, c.opts...)
	if err != nil {
		slog.Error("parse reloaded storage DSN failed; keeping current credentials", "error", err)
		return
	}
	fresh = cfg
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

// connectionConfig parses a normalized DSN (see ConnectionDSN), pins the
// session timezone to UTC, and applies caller-supplied options to the
// resulting config.
func connectionConfig(dsn string, opts ...Option) (*pgx.ConnConfig, error) {
	normalized, err := ConnectionDSN(dsn)
	if err != nil {
		return nil, err
	}
	cfg, err := pgx.ParseConfig(normalized)
	if err != nil {
		return nil, dsnParseError(err)
	}
	// Sessions default to timezone=UTC so server-side now() evaluates in UTC
	// regardless of the server's TimeZone setting. Storage compares plain
	// timestamp columns against now() in lease-expiry and staleness
	// predicates, so a non-UTC session would skew those comparisons. An
	// explicit timezone wins: GUC names are case-insensitive on the server,
	// and pgx preserves DSN key case in RuntimeParams, so the check must be
	// case-insensitive too or ?TimeZone=... would coexist with the pin in the
	// startup packet in nondeterministic map order. PGTZ also lands in
	// RuntimeParams at parse time (libpq env fallback semantics), so an
	// exported PGTZ counts as an explicit setting and skips the pin.
	if !hasRuntimeParam(cfg.RuntimeParams, "timezone") {
		cfg.RuntimeParams["timezone"] = "UTC"
	}
	// A verifying TLS config (sslmode=verify-full) against an RDS host with no
	// explicit sslrootcert would fall back to the ambient system trust store,
	// which does not carry the private Amazon RDS roots — every handshake would
	// fail with an unknown-authority error. Verify against the embedded RDS
	// global bundle instead, matching the root pool pg-sprite's data-plane
	// pools use, so both layers trust the same roots. sslmode=require is left
	// untouched: pgx marks it InsecureSkipVerify (encrypted, unauthenticated),
	// and injecting roots there would not change what it proves.
	if tc := cfg.TLSConfig; tc != nil && !tc.InsecureSkipVerify && tc.RootCAs == nil && dbconn.IsRDSHost(strings.ToLower(cfg.Host)) {
		roots, err := rdsRootPool()
		if err != nil {
			return nil, err
		}
		tc.RootCAs = roots
	}
	for _, opt := range opts {
		opt(cfg)
	}
	// The default fills non-positive values, so a positive DSN connect_timeout
	// or WithConnectTimeout option wins while zero ("wait indefinitely") and
	// negative values — which pgconn would silently treat as unbounded — are
	// replaced: a managed connection attempt is never unbounded.
	if cfg.ConnectTimeout <= 0 {
		cfg.ConnectTimeout = defaultConnectTimeout
	}
	return cfg, nil
}

// rdsRootPool returns the certificate pool holding the embedded AWS RDS global
// CA bundle. RDS server certificates chain to private Amazon roots that no
// system trust store carries, so verifying an RDS connection requires this
// pool explicitly. The pool is built once and shared: it is read-only after
// construction.
var rdsRootPool = sync.OnceValues(func() (*x509.CertPool, error) {
	pool := x509.NewCertPool()
	if !pool.AppendCertsFromPEM(dbconn.GetEmbeddedRDSBundle()) {
		return nil, fmt.Errorf("embedded RDS global CA bundle contains no usable certificates")
	}
	return pool, nil
})

// VerifiesServerCertificate reports whether the DSN's transport settings
// authenticate the server certificate: sslmode=verify-full checks the chain
// and hostname, and sslmode=verify-ca checks the chain through pgx's custom
// verification callback (which pgx also applies to sslmode=require when the
// DSN names an sslrootcert, matching libpq). require and prefer otherwise
// encrypt without authenticating, allow's first attempt and disable carry no
// TLS at all — trust pinned onto any of those modes is never consulted. The
// DSN is normalized the same way Open normalizes it, so an RDS host with no
// explicit sslmode is judged by the injected mode.
func VerifiesServerCertificate(dsn string) (bool, error) {
	cfg, err := connectionConfig(dsn)
	if err != nil {
		return false, err
	}
	tc := cfg.TLSConfig
	if tc == nil {
		return false, nil
	}
	return !tc.InsecureSkipVerify || tc.VerifyPeerCertificate != nil, nil
}

// hasRuntimeParam reports whether params carries key under PostgreSQL's
// case-insensitive GUC name matching, so TimeZone and timezone are the same
// parameter.
func hasRuntimeParam(params map[string]string, key string) bool {
	for k := range params {
		if strings.EqualFold(k, key) {
			return true
		}
	}
	return false
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
	// DNS names are case-insensitive, so an uppercase RDS endpoint must get
	// the same injection.
	if !dbconn.IsRDSHost(strings.ToLower(cfg.Host)) {
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
