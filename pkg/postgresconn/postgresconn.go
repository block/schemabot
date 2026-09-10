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
	"math"
	"net/url"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/block/mysql"
	"github.com/block/schemabot/pkg/connreload"
	"github.com/block/spirit/pkg/dbconn"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/stdlib"
)

// getConnector returns the connector that dials physical connections for the
// given config. It is a seam so tests can exercise the credential-reload path
// without a server. It is narrower than stdlib.GetConnector on purpose: the
// variadic options are not used here, and a seam whose signature is exactly
// what the package needs is one a fake cannot get subtly wrong.
var getConnector func(pgx.ConnConfig) driver.Connector = defaultConnector

func defaultConnector(cfg pgx.ConnConfig) driver.Connector { return stdlib.GetConnector(cfg) }

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
// it to dial, TLS, startup and auth; this package applies the same budget to
// the session setup it runs on the new connection afterwards, which pgconn
// leaves on the caller's context.
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
// anything above the signed 32-bit maximum. Exceeding it is not a clamp but an
// error the server raises when the budget is set, and that happens as the
// connection is established, so every connection fails at dial rather than one
// statement failing late. Exported so config validation can refuse the value
// where an operator can still see it.
const MaxStatementTimeout = time.Duration(math.MaxInt32) * time.Millisecond

// WithStatementTimeout bounds how long the server lets a single statement run
// on every connection the pool opens, as a session statement_timeout set on
// the new session. A zero duration disables the budget explicitly
// (statement_timeout=0), which is not the same as omitting the option: the
// option always sets the parameter, so the connection runs under SchemaBot's
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
		// Applied with SET on the new session rather than carried in the startup
		// packet. A connection pooler in front of storage passes through only the
		// startup parameters it knows, and statement_timeout is not one of them:
		// stock PgBouncer answers an unknown parameter with a FATAL, so every dial
		// fails rather than one statement running unbudgeted. SET reaches the same
		// session GUC over a protocol the pooler does forward. Any DSN-carried
		// statement_timeout stays in the startup packet, so it is cleared here to
		// keep this option the single source of the budget.
		//
		// The reach that makes SET work is also its cost. Against a
		// transaction-mode pooler the GUC lands on a shared server backend and
		// outlives the client connection, so a storage endpoint that is refused
		// for lacking session affinity can leave the budget behind on a backend
		// in its own (database, user) pool. That residue is the price of
		// connecting at all: the startup-packet form fails every dial through
		// such a pooler, whatever its pool mode.
		stmt := statementTimeoutSQL(d)
		clearRuntimeParam(cfg, "statement_timeout")
		cfg.AfterConnect = func(ctx context.Context, conn *pgconn.PgConn) error {
			// pgconn applies ConnectTimeout to the dial, TLS, startup and auth
			// and then runs this hook on the caller's context, which the pool
			// supplies with no deadline of its own. An endpoint that finishes
			// auth and then stops answering — a half-open path, a wedged
			// pooler backend — would leave this statement waiting forever on
			// the single goroutine connections are opened on, stalling every
			// caller queued behind it. Reading the budget here rather than at
			// option time picks up the package default, which is filled in
			// after every option has run.
			if timeout := cfg.ConnectTimeout; timeout > 0 {
				var cancel context.CancelFunc
				ctx, cancel = context.WithTimeout(ctx, timeout)
				defer cancel()
			}
			if err := execStatementTimeout(ctx, conn, stmt); err != nil {
				return fmt.Errorf("%s: %w", stmt, err)
			}
			return nil
		}
	}
}

// execStatementTimeout runs the budget statement on a newly connected session.
// It is a variable so a unit test can observe the statement the hook actually
// sends: pgconn's AfterConnect takes a concrete *pgconn.PgConn, so without a
// seam here the only assertion available off a live server is that some hook
// was installed — which stays true no matter which statement it carries.
var execStatementTimeout = func(ctx context.Context, conn *pgconn.PgConn, stmt string) error {
	_, err := conn.Exec(ctx, stmt).ReadAll()
	return err
}

// statementTimeoutSQL is the statement the option runs on each new session to
// arm the budget. The value is an integer this package derives, never caller
// text, so it is safe to interpolate — and it has to be, since SET does not
// take a bind parameter.
func statementTimeoutSQL(d time.Duration) string {
	return "SET statement_timeout = " + strconv.FormatInt(statementTimeoutMillis(d), 10)
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

// clearRuntimeParam removes a startup-packet parameter under every spelling
// of its name. GUC names are case-insensitive on the server and pgx preserves
// DSN key case in RuntimeParams, so deleting only the canonical spelling could
// leave a differently-cased one behind and still in force.
func clearRuntimeParam(cfg *pgx.ConnConfig, key string) {
	for k := range cfg.RuntimeParams {
		if strings.EqualFold(k, key) {
			delete(cfg.RuntimeParams, k)
		}
	}
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
// The scheduling — how many reloads a burst of rejected dials costs, and how a
// failing secrets backend is backed off — is pkg/connreload's and is shared
// with the MySQL storage pool; see reloadConfig for the PostgreSQL-specific
// half.
func OpenReloadable(dsn string, reload func() (string, error), opts ...Option) (*sql.DB, error) {
	connector, err := connreload.New(dsn, reloadConfig(reload, opts))
	if err != nil {
		return nil, err
	}
	return sql.OpenDB(connector), nil
}

// resolveConnector normalizes a raw DSN the way every SchemaBot-managed
// PostgreSQL connection is normalized (see connectionConfig), applies the
// caller's options, and returns the connector that dials it. It is the Resolve
// half of the reloadable pool: it runs once at open and once per reload, never
// per dial.
func resolveConnector(dsn string, opts ...Option) (driver.Connector, error) {
	cfg, err := connectionConfig(dsn, opts...)
	if err != nil {
		return nil, err
	}
	return getConnector(*cfg), nil
}

// reloadConfig describes the PostgreSQL storage pool to pkg/connreload.
// Everything about *when* to reload lives there and is shared with the MySQL
// pool. What is PostgreSQL's, and all that is PostgreSQL's, is the two
// functions below.
func reloadConfig(reload func() (string, error), opts []Option) connreload.Config {
	return connreload.Config{
		Resolve: func(dsn string) (driver.Connector, error) { return resolveConnector(dsn, opts...) },
		Refused: isAuthError,
		Reload:  reload,
		Driver:  stdlib.GetDefaultDriver(),
		Name:    "postgres-storage",
	}
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
//
// The bundle comes from block/mysql, which is now the one copy of it in the
// dependency graph — spirit used to embed its own and expose the bytes, and
// dropped both when it started delegating to the driver. The pool is Postgres's
// here, but the roots are the same: RDS issues from the same private Amazon
// CAs regardless of engine.
//
// RDSTLSConfig clones its pool per call, so taking RootCAs off it does not
// alias anything the MySQL side is using — appending here could not widen trust
// for MySQL connections even if a caller tried.
var rdsRootPool = sync.OnceValues(func() (*x509.CertPool, error) {
	pool := mysql.RDSTLSConfig().RootCAs
	if pool == nil {
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
// (postgres://user:pass@host/db) and keyword/value (host=... user=...). sadscan:disable np.postgres.1
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
