package postgresconn

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"database/sql/driver"
	"encoding/pem"
	"errors"
	"fmt"
	"math/big"
	"net/url"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestConnectionDSN(t *testing.T) {
	tests := []struct {
		name       string
		dsn        string
		want       string
		wantErrSub string
	}{
		{
			name: "RDS URL host gets sslmode=require",
			dsn:  "postgres://schemabot:secret@database.cluster-abc123.us-west-2.rds.amazonaws.com:5432/app", // sadscan:disable np.postgres.1
			want: "postgres://schemabot:secret@database.cluster-abc123.us-west-2.rds.amazonaws.com:5432/app?sslmode=require",
		},
		{
			name: "RDS URL host with explicit sslmode is unchanged",
			dsn:  "postgres://schemabot:secret@database.cluster-abc123.us-west-2.rds.amazonaws.com:5432/app?sslmode=verify-full",
			want: "postgres://schemabot:secret@database.cluster-abc123.us-west-2.rds.amazonaws.com:5432/app?sslmode=verify-full",
		},
		{
			name: "RDS URL host with explicit sslmode=disable is unchanged",
			dsn:  "postgres://schemabot:secret@database.cluster-abc123.us-west-2.rds.amazonaws.com:5432/app?sslmode=disable",
			want: "postgres://schemabot:secret@database.cluster-abc123.us-west-2.rds.amazonaws.com:5432/app?sslmode=disable",
		},
		{
			name: "RDS keyword host gets sslmode=require",
			dsn:  "host=database.cluster-abc123.us-west-2.rds.amazonaws.com port=5432 user=schemabot password=secret dbname=app",
			want: "host=database.cluster-abc123.us-west-2.rds.amazonaws.com port=5432 user=schemabot password=secret dbname=app sslmode=require",
		},
		{
			name: "RDS keyword host with explicit sslmode is unchanged",
			dsn:  "host=database.cluster-abc123.us-west-2.rds.amazonaws.com user=schemabot password=secret dbname=app sslmode=disable",
			want: "host=database.cluster-abc123.us-west-2.rds.amazonaws.com user=schemabot password=secret dbname=app sslmode=disable",
		},
		{
			name: "RDS keyword host with sslmode = disable (spaces around =) is unchanged",
			dsn:  "host=database.cluster-abc123.us-west-2.rds.amazonaws.com user=schemabot password=secret dbname=app sslmode = disable",
			want: "host=database.cluster-abc123.us-west-2.rds.amazonaws.com user=schemabot password=secret dbname=app sslmode = disable",
		},
		{
			name: "RDS keyword host with sslmode = verify-full (spaces around =) is unchanged",
			dsn:  "host=database.cluster-abc123.us-west-2.rds.amazonaws.com user=schemabot password=secret dbname=app sslmode = verify-full",
			want: "host=database.cluster-abc123.us-west-2.rds.amazonaws.com user=schemabot password=secret dbname=app sslmode = verify-full",
		},
		{
			name: "RDS keyword host with sslmode =disable (space before =) is unchanged",
			dsn:  "host=database.cluster-abc123.us-west-2.rds.amazonaws.com user=schemabot password=secret dbname=app sslmode =disable",
			want: "host=database.cluster-abc123.us-west-2.rds.amazonaws.com user=schemabot password=secret dbname=app sslmode =disable",
		},
		{
			name: "non-RDS URL host is unchanged",
			dsn:  "postgres://schemabot:secret@localhost:5432/app", // sadscan:disable np.postgres.1
			want: "postgres://schemabot:secret@localhost:5432/app",
		},
		{
			// RDS detection considers only the first host, so a multi-host
			// DSN whose RDS host is a fallback gets no injection — spell out
			// sslmode explicitly for multi-host DSNs.
			name: "multi-host DSN with RDS fallback host gets no injection",
			dsn:  "postgres://schemabot:secret@localhost:5432,database.cluster-abc123.us-west-2.rds.amazonaws.com:5432/app",
			want: "postgres://schemabot:secret@localhost:5432,database.cluster-abc123.us-west-2.rds.amazonaws.com:5432/app",
		},
		{
			name: "non-RDS keyword host is unchanged",
			dsn:  "host=localhost user=schemabot password=secret dbname=app",
			want: "host=localhost user=schemabot password=secret dbname=app",
		},
		{
			name:       "invalid DSN returns context",
			dsn:        "postgres://schemabot:secret@localhost:not-a-port/app", // sadscan:disable np.postgres.1
			wantErrSub: "parse PostgreSQL DSN",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := ConnectionDSN(tt.dsn)
			if tt.wantErrSub != "" {
				require.Error(t, err)
				assert.Contains(t, err.Error(), tt.wantErrSub)
				return
			}
			require.NoError(t, err)
			assert.Equal(t, tt.want, got)

			// The normalized DSN must itself parse.
			_, err = pgx.ParseConfig(got)
			require.NoError(t, err)
		})
	}
}

// A verifying DSN (sslmode=verify-full) against an RDS host names no root
// bundle of its own, so the connection verifies against the embedded RDS
// global CA bundle — the ambient system trust store does not carry the
// private Amazon RDS roots and would fail every handshake.
func TestConnectionConfigVerifiesRDSHostsWithEmbeddedRoots(t *testing.T) {
	cfg, err := connectionConfig("postgres://schemabot:secret@db.example.rds.amazonaws.com:5432/app?sslmode=verify-full") // sadscan:disable np.postgres.1
	require.NoError(t, err)
	require.NotNil(t, cfg.TLSConfig)
	assert.False(t, cfg.TLSConfig.InsecureSkipVerify)
	assert.NotNil(t, cfg.TLSConfig.RootCAs)

	// DNS names are case-insensitive: an uppercase RDS endpoint gets the same
	// roots.
	cfg, err = connectionConfig("postgres://schemabot:secret@DB.EXAMPLE.RDS.AMAZONAWS.COM:5432/app?sslmode=verify-full") // sadscan:disable np.postgres.1
	require.NoError(t, err)
	require.NotNil(t, cfg.TLSConfig)
	assert.NotNil(t, cfg.TLSConfig.RootCAs)

	// sslmode=require encrypts without authenticating (pgx marks it
	// InsecureSkipVerify); the RDS roots would prove nothing, so the config is
	// left untouched.
	cfg, err = connectionConfig("postgres://schemabot:secret@db.example.rds.amazonaws.com:5432/app?sslmode=require")
	require.NoError(t, err)
	require.NotNil(t, cfg.TLSConfig)
	assert.True(t, cfg.TLSConfig.InsecureSkipVerify)
	assert.Nil(t, cfg.TLSConfig.RootCAs)

	// The embedded bundle holds RDS roots only: a non-RDS host gets no
	// implicit trust material.
	cfg, err = connectionConfig("postgres://schemabot:secret@db.internal.example:5432/app?sslmode=verify-full") // sadscan:disable np.postgres.1
	require.NoError(t, err)
	require.NotNil(t, cfg.TLSConfig)
	assert.Nil(t, cfg.TLSConfig.RootCAs)
}

// An explicit sslrootcert in the DSN is the operator's chosen trust root:
// the connection verifies against the pool pgx built from that file, and the
// embedded RDS bundle is not layered on top even for an RDS host.
func TestConnectionConfigExplicitSSLRootCertWins(t *testing.T) {
	caPath := writeSelfSignedCA(t)

	cfg, err := connectionConfig("postgres://schemabot:secret@db.example.rds.amazonaws.com:5432/app?sslmode=verify-full&sslrootcert=" + url.QueryEscape(caPath))
	require.NoError(t, err)
	require.NotNil(t, cfg.TLSConfig)
	assert.False(t, cfg.TLSConfig.InsecureSkipVerify)
	require.NotNil(t, cfg.TLSConfig.RootCAs)

	pemBytes, err := os.ReadFile(caPath)
	require.NoError(t, err)
	fromFile := x509.NewCertPool()
	require.True(t, fromFile.AppendCertsFromPEM(pemBytes))
	assert.True(t, cfg.TLSConfig.RootCAs.Equal(fromFile))

	rds, err := rdsRootPool()
	require.NoError(t, err)
	assert.False(t, cfg.TLSConfig.RootCAs.Equal(rds))
}

// writeSelfSignedCA generates a self-signed CA certificate, writes it in PEM
// form to a temp file, and returns the file path.
func writeSelfSignedCA(t *testing.T) string {
	t.Helper()
	_, key, err := ed25519.GenerateKey(rand.Reader)
	require.NoError(t, err)
	tmpl := &x509.Certificate{
		SerialNumber:          big.NewInt(1),
		Subject:               pkix.Name{CommonName: "postgresconn test CA"},
		NotBefore:             time.Now().Add(-time.Hour),
		NotAfter:              time.Now().Add(time.Hour),
		IsCA:                  true,
		KeyUsage:              x509.KeyUsageCertSign,
		BasicConstraintsValid: true,
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, key.Public(), key)
	require.NoError(t, err)
	path := filepath.Join(t.TempDir(), "ca.pem")
	require.NoError(t, os.WriteFile(path, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}), 0o600))
	return path
}

// A caller-supplied bundle always wins: WithRootCAs replaces whatever trust
// the DSN installed, and a DSN that negotiates no TLS has nothing to pin.
func TestWithRootCAsPinsVerificationTrust(t *testing.T) {
	roots := x509.NewCertPool()
	cfg, err := connectionConfig("postgres://schemabot:secret@db.cluster-abc123.eu-west-1.rds.amazonaws.com:5432/app?sslmode=verify-full", WithRootCAs(roots)) // sadscan:disable np.postgres.1
	require.NoError(t, err)
	require.NotNil(t, cfg.TLSConfig)
	assert.Same(t, roots, cfg.TLSConfig.RootCAs)
}

// Pinning trust removes pgx's parse-time fallbacks: under sslmode=prefer a
// failed TLS handshake would otherwise retry in plaintext, silently bypassing
// the bundle the caller named.
func TestWithRootCAsClearsFallbacks(t *testing.T) {
	roots := x509.NewCertPool()
	cfg, err := connectionConfig("postgres://schemabot:secret@postgres.internal.example:5432/app?sslmode=prefer", WithRootCAs(roots)) // sadscan:disable np.postgres.1
	require.NoError(t, err)
	require.NotNil(t, cfg.TLSConfig)
	assert.Same(t, roots, cfg.TLSConfig.RootCAs)
	assert.Empty(t, cfg.Fallbacks)
}

// TestVerifiesServerCertificate pins which sslmodes actually authenticate the
// server: verify-full and verify-ca consult the trust roots, require and
// prefer encrypt without verifying, and disable negotiates no TLS — except
// that require with an explicit sslrootcert is upgraded by pgx to verify-ca
// semantics, matching libpq.
func TestVerifiesServerCertificate(t *testing.T) {
	base := "postgres://schemabot:secret@postgres.internal.example:5432/app?sslmode="
	tests := []struct {
		name string
		dsn  string
		want bool
	}{
		{name: "verify-full authenticates", dsn: base + "verify-full", want: true},
		{name: "verify-ca authenticates", dsn: base + "verify-ca", want: true},
		{name: "require encrypts without verifying", dsn: base + "require", want: false},
		{name: "prefer encrypts without verifying", dsn: base + "prefer", want: false},
		{name: "allow does not verify", dsn: base + "allow", want: false},
		{name: "disable negotiates no TLS", dsn: base + "disable", want: false},
		{name: "absent sslmode defaults to non-verifying", dsn: "postgres://schemabot:secret@postgres.internal.example:5432/app", want: false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := VerifiesServerCertificate(tt.dsn)
			require.NoError(t, err)
			assert.Equal(t, tt.want, got)
		})
	}

	t.Run("require with sslrootcert verifies like verify-ca", func(t *testing.T) {
		caPath := writeSelfSignedCA(t)
		got, err := VerifiesServerCertificate(base + "require&sslrootcert=" + url.QueryEscape(caPath))
		require.NoError(t, err)
		assert.True(t, got)
	})

	t.Run("unparseable DSN is refused", func(t *testing.T) {
		_, err := VerifiesServerCertificate("postgres://schemabot@:not-a-port/app")
		require.Error(t, err)
	})
}

func TestWithRootCAsLeavesNonTLSConnectionsUntouched(t *testing.T) {
	cfg, err := connectionConfig("postgres://schemabot:secret@localhost:5432/app?sslmode=disable", WithRootCAs(x509.NewCertPool()))
	require.NoError(t, err)
	assert.Nil(t, cfg.TLSConfig)
}

// Every SchemaBot-managed connection gets a bounded connect timeout so an
// unreachable target surfaces as an error instead of a hung dial. A positive
// timeout carried in the DSN or supplied as an option wins over the package
// default, while a non-positive value — absent, zero, or negative via a raw
// option closure — is filled with the default so no managed connection
// attempt is ever unbounded.
func TestWithConnectTimeout(t *testing.T) {
	t.Run("option overrides the default", func(t *testing.T) {
		cfg, err := connectionConfig("postgres://schemabot:secret@localhost:5432/app", WithConnectTimeout(7*time.Second))
		require.NoError(t, err)
		assert.Equal(t, 7*time.Second, cfg.ConnectTimeout)
	})

	t.Run("non-positive timeout falls back to the package default", func(t *testing.T) {
		cfg, err := connectionConfig("postgres://schemabot:secret@localhost:5432/app", WithConnectTimeout(0))
		require.NoError(t, err)
		assert.Equal(t, defaultConnectTimeout, cfg.ConnectTimeout)
	})

	t.Run("negative timeout from a raw option closure is replaced by the default", func(t *testing.T) {
		cfg, err := connectionConfig(
			"postgres://schemabot:secret@localhost:5432/app",
			func(c *pgx.ConnConfig) { c.ConnectTimeout = -time.Second },
		)
		require.NoError(t, err)
		assert.Equal(t, defaultConnectTimeout, cfg.ConnectTimeout)
	})

	t.Run("plain DSN gets the default connect timeout", func(t *testing.T) {
		cfg, err := connectionConfig("postgres://schemabot:secret@localhost:5432/app")
		require.NoError(t, err)
		assert.Equal(t, defaultConnectTimeout, cfg.ConnectTimeout)
	})

	t.Run("DSN-carried connect_timeout is preserved", func(t *testing.T) {
		cfg, err := connectionConfig("postgres://schemabot:secret@localhost:5432/app?connect_timeout=3")
		require.NoError(t, err)
		assert.Equal(t, 3*time.Second, cfg.ConnectTimeout)
	})
}

// Sessions are pinned to timezone=UTC so server-side now() is UTC on any
// server default, keeping storage's timestamp comparisons consistent across
// pods. An explicit timezone in the DSN wins, in either DSN form and in any
// GUC-name case, and the pin never duplicates an existing setting under a
// different spelling.
func TestConnectionConfigPinsUTCTimezone(t *testing.T) {
	// PGTZ is a libpq env fallback that pgx maps into RuntimeParams and
	// therefore counts as an explicit setting; clear it so the assertions
	// below reflect the DSN alone.
	t.Setenv("PGTZ", "")

	cfg, err := connectionConfig("postgres://schemabot:secret@localhost:5432/app")
	require.NoError(t, err)
	assert.Equal(t, "UTC", cfg.RuntimeParams["timezone"])

	cfg, err = connectionConfig("host=localhost user=schemabot password=secret dbname=app")
	require.NoError(t, err)
	assert.Equal(t, "UTC", cfg.RuntimeParams["timezone"])

	cfg, err = connectionConfig("postgres://schemabot:secret@localhost:5432/app?timezone=America/New_York")
	require.NoError(t, err)
	assert.Equal(t, "America/New_York", cfg.RuntimeParams["timezone"])

	cfg, err = connectionConfig("host=localhost user=schemabot timezone=America/New_York")
	require.NoError(t, err)
	assert.Equal(t, "America/New_York", cfg.RuntimeParams["timezone"])

	// PostgreSQL matches GUC names case-insensitively, and pgx preserves the
	// DSN's key case: the documented TimeZone spelling must win without the
	// pin adding a second, conflicting timezone entry.
	cfg, err = connectionConfig("postgres://schemabot:secret@localhost:5432/app?TimeZone=America/New_York")
	require.NoError(t, err)
	assert.Equal(t, "America/New_York", cfg.RuntimeParams["TimeZone"])
	assert.NotContains(t, cfg.RuntimeParams, "timezone")

	cfg, err = connectionConfig("host=localhost user=schemabot TimeZone=America/New_York")
	require.NoError(t, err)
	assert.Equal(t, "America/New_York", cfg.RuntimeParams["TimeZone"])
	assert.NotContains(t, cfg.RuntimeParams, "timezone")
}

// PGTZ follows libpq fallback semantics: it reaches RuntimeParams before the
// pin runs, so it is honored as an explicit operator setting rather than
// overridden to UTC.
func TestConnectionConfigHonorsPGTZ(t *testing.T) {
	t.Setenv("PGTZ", "America/Los_Angeles")

	cfg, err := connectionConfig("postgres://schemabot:secret@localhost:5432/app")
	require.NoError(t, err)
	assert.Equal(t, "America/Los_Angeles", cfg.RuntimeParams["timezone"])
}

// The scheduling of credential reloads is pkg/connreload's and is tested
// there. What is tested here is the PostgreSQL-specific half: which error
// means "the server rejected these credentials", and that a reloaded DSN gets
// the same normalization and options as the DSN the pool was opened with.

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

func (f *fakeConnector) Driver() driver.Driver { return stubDriver{} }

type stubDriver struct{}

func (stubDriver) Open(string) (driver.Conn, error) { return nil, errors.New("not implemented") }

// fakeDial replaces the getConnector seam and returns the log recording the
// password of every dial attempt, in order.
func fakeDial(t *testing.T, results []error) *[]string {
	t.Helper()
	var passwords []string
	var mu sync.Mutex
	original := getConnector
	t.Cleanup(func() { getConnector = original })
	getConnector = func(cfg pgx.ConnConfig) driver.Connector {
		return &fakeConnector{t: t, password: cfg.Password, passwords: &passwords, results: results, mu: &mu}
	}
	return &passwords
}

func authError() error {
	return fmt.Errorf("connect: %w", &pgconn.PgError{Code: "28P01", Message: "password authentication failed"})
}

func TestIsAuthError(t *testing.T) {
	assert.True(t, isAuthError(&pgconn.PgError{Code: "28P01"}))
	assert.True(t, isAuthError(&pgconn.PgError{Code: "28000"}))
	// database/sql wraps, so recognizing a bare error is not enough.
	assert.True(t, isAuthError(authError()))
	assert.False(t, isAuthError(&pgconn.PgError{Code: "55P03"}))
	assert.False(t, isAuthError(errors.New("connection refused")))
	assert.False(t, isAuthError(nil))
}

func TestResolveConnectorAppliesOptionsAndNormalization(t *testing.T) {
	// A reloaded raw DSN gets the same treatment as the boot DSN, because both
	// go through resolveConnector: caller options are re-applied and an RDS
	// host gets sslmode=require injected. The assertions read the config the
	// seam is handed, which is what the pool will dial with.
	original := getConnector
	t.Cleanup(func() { getConnector = original })
	var got pgx.ConnConfig
	getConnector = func(cfg pgx.ConnConfig) driver.Connector {
		got = cfg
		return nil
	}

	_, err := resolveConnector(
		"postgres://schemabot:rotated@database.cluster-abc123.us-west-2.rds.amazonaws.com:5432/app", // sadscan:disable np.postgres.1
		WithConnectTimeout(7*time.Second),
	)
	require.NoError(t, err)
	assert.Equal(t, "rotated", got.Password)
	assert.Equal(t, 7*time.Second, got.ConnectTimeout, "options must flow through the resolve path")
	assert.NotNil(t, got.TLSConfig, "a reloaded RDS DSN must get sslmode=require injected")
	// sslmode=prefer would also set TLSConfig but keep a plaintext fallback;
	// require is distinguished by that fallback's absence.
	assert.Empty(t, got.Fallbacks, "sslmode=require must not leave a plaintext fallback")
}

// End to end through pkg/connreload: a pool whose first dial is rejected
// re-resolves its DSN and retries with the rotated password, without the
// caller doing anything.
func TestOpenReloadableRotatesCredentialsOnAuthFailure(t *testing.T) {
	passwords := fakeDial(t, []error{authError(), nil})
	var reloads atomic.Int32

	db, err := OpenReloadable("postgres://schemabot:old@localhost:5432/app", func() (string, error) { // sadscan:disable np.postgres.1
		reloads.Add(1)
		return "postgres://schemabot:rotated@localhost:5432/app", nil // sadscan:disable np.postgres.1
	})
	require.NoError(t, err)
	t.Cleanup(func() { assert.NoError(t, db.Close()) })

	require.NoError(t, db.PingContext(t.Context()))
	assert.Equal(t, []string{"old", "rotated"}, *passwords, "the retry must dial with the reloaded credentials")
	assert.Equal(t, int32(1), reloads.Load())
}

func TestDSNParseErrorsRedactCredentials(t *testing.T) {
	// A DSN whose URL fails to parse must not have its password echoed in the
	// error: *url.Error reproduces the full URL, password included.
	const password = "sup3r-secret"
	badDSN := "postgres://schemabot:" + password + "@localhost:not-a-port/app"

	t.Run("ConnectionDSN", func(t *testing.T) {
		_, err := ConnectionDSN(badDSN)
		require.Error(t, err)
		assert.NotContains(t, err.Error(), password)
	})

	t.Run("Open", func(t *testing.T) {
		_, err := Open(badDSN)
		require.Error(t, err)
		assert.NotContains(t, err.Error(), password)
	})

	t.Run("OpenReloadable", func(t *testing.T) {
		_, err := OpenReloadable(badDSN, func() (string, error) { return "", nil })
		require.Error(t, err)
		assert.NotContains(t, err.Error(), password)
	})

	t.Run("resolveConnector", func(t *testing.T) {
		_, err := resolveConnector(badDSN)
		require.Error(t, err)
		assert.NotContains(t, err.Error(), password)
	})
}
