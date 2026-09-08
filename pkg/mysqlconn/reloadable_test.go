package mysqlconn

import (
	"context"
	"database/sql/driver"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/block/mysql"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// The scheduling of reloads is pkg/connreload's and is tested there. What is
// tested here is the MySQL-specific half: which error means "the server
// rejected these credentials", and that a reloaded DSN gets the same
// normalization and options as the DSN the pool was opened with.

// stubConn is the minimal driver.Conn a fake dial can hand back.
type stubConn struct{}

func (stubConn) Prepare(string) (driver.Stmt, error) { return nil, errors.New("not implemented") }
func (stubConn) Close() error                        { return nil }
func (stubConn) Begin() (driver.Tx, error)           { return nil, errors.New("not implemented") }

// fakeConnector dials for one resolved config, recording the password of every
// attempt into the shared log and taking its outcome from results, consumed in
// order across every generation.
type fakeConnector struct {
	t         *testing.T
	password  string
	passwords *[]string
	results   []error
	mu        *sync.Mutex
}

func (f *fakeConnector) Connect(context.Context) (driver.Conn, error) {
	f.mu.Lock()
	*f.passwords = append(*f.passwords, f.password)
	i := len(*f.passwords) - 1
	f.mu.Unlock()
	// assert (not require): this runs on the dialing goroutine, and testify's
	// FailNow is only valid on the test goroutine. The error return fails the
	// dial cleanly instead.
	if !assert.Less(f.t, i, len(f.results), "unexpected extra dial attempt") {
		return nil, errors.New("unexpected extra dial attempt")
	}
	if err := f.results[i]; err != nil {
		return nil, err
	}
	return stubConn{}, nil
}

func (f *fakeConnector) Driver() driver.Driver { return mysql.MySQLDriver{} }

// fakeDial replaces the newConnector seam and returns the log recording the
// password of every dial attempt, in order.
func fakeDial(t *testing.T, results []error) *[]string {
	t.Helper()
	var passwords []string
	var mu sync.Mutex
	original := newConnector
	t.Cleanup(func() { newConnector = original })
	newConnector = func(cfg *mysql.Config) (driver.Connector, error) {
		return &fakeConnector{t: t, password: cfg.Passwd, passwords: &passwords, results: results, mu: &mu}, nil
	}
	return &passwords
}

func accessDeniedError() error {
	return fmt.Errorf("connect: %w", &mysql.MySQLError{
		Number:  erAccessDenied,
		Message: "Access denied for user 'schemabot'@'10.0.0.1' (using password: YES)",
	})
}

func TestIsAccessDenied(t *testing.T) {
	assert.True(t, isAccessDenied(&mysql.MySQLError{Number: erAccessDenied}))
	// database/sql wraps, so recognizing a bare error is not enough.
	assert.True(t, isAccessDenied(accessDeniedError()))
	// 1698 is access-denied for an account that authenticates by something
	// other than the password sent, which no rotation of the secret changes.
	assert.False(t, isAccessDenied(&mysql.MySQLError{Number: 1698}))
	// Any other MySQL error is not a rotation signal: a deadlock or an unknown
	// database must not cost a secret resolution.
	assert.False(t, isAccessDenied(&mysql.MySQLError{Number: 1049}))
	assert.False(t, isAccessDenied(&mysql.MySQLError{Number: 1213}))
	assert.False(t, isAccessDenied(errors.New("connection refused")))
	assert.False(t, isAccessDenied(nil))
}

func TestResolveConnectorAppliesOptionsAndNormalization(t *testing.T) {
	// A reloaded raw DSN gets the same treatment as the boot DSN, because both
	// go through resolveConnector: caller options are re-applied, an RDS host
	// gets TLS injected, and the required settings every managed connection
	// carries are re-imposed. The assertions read the config the seam is
	// handed, which is what the pool will dial with.
	original := newConnector
	t.Cleanup(func() { newConnector = original })
	var got *mysql.Config
	newConnector = func(cfg *mysql.Config) (driver.Connector, error) {
		got = cfg
		return nil, nil
	}

	_, err := resolveConnector(
		"schemabot:rotated@tcp(database.cluster-abc123.us-west-2.rds.amazonaws.com:3306)/app",
		WithConnectTimeout(7*time.Second),
	)
	require.NoError(t, err)
	require.NotNil(t, got)
	assert.Equal(t, "rotated", got.Passwd)
	assert.Equal(t, 7*time.Second, got.Timeout, "options must flow through the resolve path")
	assert.Equal(t, "rds", got.TLSConfig, "an RDS DSN must get TLS injected")
	assert.Equal(t, defaultWriteTimeout, got.WriteTimeout, "required settings must be imposed")
	assert.True(t, got.InterpolateParams)
}

func TestResolveConnectorRejectsBadDSN(t *testing.T) {
	_, err := resolveConnector("not-a-dsn")
	require.Error(t, err)
}

// The reloadable storage pool must reach an RDS host *with TLS* — not merely
// open. This is the case that was broken and could not be seen: `tls=rds` is a
// name, and a name only resolves inside the registry of the driver package
// that registered it. Spirit registers it into block/mysql, and the reloadable
// pool used to be opened with a hot-swap driver that embedded upstream
// go-sql-driver, whose registry has no "rds" — so a MySQL storage pool on an
// RDS host failed to open at all and the server did not start. No CI job points
// storage at an *.rds.amazonaws.com address, so the break was host-shaped
// rather than code-shaped and nothing in the suite could catch it.
//
// Both pools now dial through block/mysql, so there is one registry and the
// name resolves on both paths. The assertions are on the resolved trust rather
// than on the DSN text, because the failure this replaces would have been
// equally invisible to a test that only checked the pool opened: dropping the
// tls= injection also "opens", by connecting in the clear.
func TestReloadablePoolReachesRDSHostWithVerifiedTLS(t *testing.T) {
	const host = "sb.cluster-abc123.us-west-2.rds.amazonaws.com"
	const rawDSN = "u:p@tcp(" + host + ":3306)/schemabot"

	// First half: the DSN this pool is opened with names a TLS config, and the
	// driver the pool dials through resolves that name. This is the assertion
	// the bug would have failed — the name is registered in block/mysql, and a
	// pool dialing through any other MySQL driver cannot resolve it.
	//
	// It asserts the mechanism, not just the outcome, and that is deliberate:
	// block/mysql applies RDS TLS on its own for an RDS address even with no
	// tls= at all, so the resolved-trust assertions below hold either way and
	// cannot tell the two apart. Dropping the injection and leaning on the
	// driver's own auto-TLS may well be right later; this line is here so that
	// is a decision someone makes, rather than a silent change in which layer
	// is responsible for encrypting the connection that carries every
	// credential and lease.
	connectionDSN, err := ConnectionDSN(rawDSN)
	require.NoError(t, err)
	assert.Contains(t, connectionDSN, "tls=rds", "the RDS TLS config name must be injected")
	named, err := mysql.ParseDSN(connectionDSN)
	require.NoError(t, err, "the driver this pool dials through cannot resolve the injected TLS config name")
	require.NotNil(t, named.TLS)

	original := newConnector
	t.Cleanup(func() { newConnector = original })
	var dialed *mysql.Config
	var dials atomic.Int32
	newConnector = func(cfg *mysql.Config) (driver.Connector, error) {
		dialed = cfg
		return connectFunc(func(context.Context) (driver.Conn, error) {
			dials.Add(1)
			return stubConn{}, nil
		}), nil
	}

	db, err := OpenReloadable(rawDSN, func() (string, error) {
		return "", errors.New("no rotation in this test")
	})
	require.NoError(t, err, "the storage pool must open against an RDS host")
	t.Cleanup(func() { assert.NoError(t, db.Close()) })

	require.NoError(t, db.PingContext(t.Context()))
	require.Equal(t, int32(1), dials.Load())

	// Second half: what the pool actually dials with authenticates the server —
	// real roots, the right ServerName, no skipped verification, no plaintext
	// fallback. A pool that merely opened, in the clear, fails every line here.
	require.NotNil(t, dialed)
	require.NotNil(t, dialed.TLS, "the pool would connect in the clear")
	assert.Equal(t, host, dialed.TLS.ServerName)
	assert.False(t, dialed.TLS.InsecureSkipVerify, "the RDS trust store must actually be verified against")
	assert.NotNil(t, dialed.TLS.RootCAs, "verification against a nil root pool is the ambient system store, which has no RDS roots")
	assert.False(t, dialed.AllowFallbackToPlaintext, "a plaintext fallback would make the TLS above optional")
}

// connectFunc adapts a dial function to driver.Connector.
type connectFunc func(context.Context) (driver.Conn, error)

func (f connectFunc) Connect(ctx context.Context) (driver.Conn, error) { return f(ctx) }
func (connectFunc) Driver() driver.Driver                              { return mysql.MySQLDriver{} }

// End to end through pkg/connreload: a pool whose first dial is refused
// re-resolves its DSN and retries with the rotated password, without the
// caller doing anything.
func TestOpenReloadableRotatesCredentialsOnAccessDenied(t *testing.T) {
	passwords := fakeDial(t, []error{accessDeniedError(), nil})
	var reloads atomic.Int32

	db, err := OpenReloadable("schemabot:old@tcp(localhost:3306)/app", func() (string, error) {
		reloads.Add(1)
		return "schemabot:rotated@tcp(localhost:3306)/app", nil
	})
	require.NoError(t, err)
	t.Cleanup(func() { assert.NoError(t, db.Close()) })

	require.NoError(t, db.PingContext(t.Context()))
	assert.Equal(t, []string{"old", "rotated"}, *passwords, "the retry must dial with the reloaded credentials")
	assert.Equal(t, int32(1), reloads.Load())
}

func TestOpenReloadableRejectsBadDSN(t *testing.T) {
	_, err := OpenReloadable("not-a-dsn", func() (string, error) { return "", nil })
	require.Error(t, err)
	assert.Contains(t, err.Error(), "open reloadable MySQL connection")
}
