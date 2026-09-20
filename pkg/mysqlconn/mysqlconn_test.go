package mysqlconn

import (
	"bytes"
	"database/sql"
	"errors"
	"log/slog"
	"testing"
	"time"

	"github.com/block/mysql"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const nonVerifyingRDSWarning = "MySQL RDS connection uses a non-verifying TLS mode; the configured mode is honored for compatibility"

// captureWarnings routes the default logger into a buffer for the test and
// clears the per-process warning set so the test observes the first warning
// for its endpoints regardless of test order or -count.
func captureWarnings(t *testing.T) *bytes.Buffer {
	t.Helper()
	var logs bytes.Buffer
	originalLogger := slog.Default()
	slog.SetDefault(slog.New(slog.NewTextHandler(&logs, nil)))
	warnedNonVerifyingRDS.Clear()
	t.Cleanup(func() {
		slog.SetDefault(originalLogger)
		warnedNonVerifyingRDS.Clear()
	})
	return &logs
}

func TestConnectionDSNWarnsForNonVerifyingRDSTLS(t *testing.T) {
	tests := []struct {
		name        string
		host        string
		tlsMode     string
		opts        []Option
		wantWarning bool
		wantMode    string
		wantTLS     string
	}{
		{name: "disabled TLS on RDS", host: "disabled.cluster-abc123.us-west-2.rds.amazonaws.com:3306", tlsMode: "false", wantWarning: true, wantTLS: "false"},
		{name: "unverified TLS on RDS", host: "unverified.cluster-abc123.us-west-2.rds.amazonaws.com:3306", tlsMode: "skip-verify", wantWarning: true, wantTLS: "skip-verify"},
		{name: "preferred TLS on RDS", host: "preferred.cluster-abc123.us-west-2.rds.amazonaws.com:3306", tlsMode: "preferred", wantWarning: true, wantTLS: "preferred"},
		{name: "disabled TLS on uppercase RDS endpoint", host: "Disabled.Cluster-ABC123.US-WEST-2.RDS.AMAZONAWS.COM:3306", tlsMode: "false", wantWarning: true, wantTLS: "false"},
		{
			name: "option weakens implicit verified TLS on RDS",
			host: "weakened.cluster-abc123.us-west-2.rds.amazonaws.com:3306",
			opts: []Option{func(cfg *mysql.Config) { cfg.TLSConfig = "skip-verify" }},
			// The option runs after the RDS enhancement, so the mode that is
			// dialed — and warned about — is the option's, not the injected one.
			wantWarning: true,
			wantMode:    "skip-verify",
			wantTLS:     "skip-verify",
		},
		{name: "implicit verified TLS on RDS", host: "verified.cluster-abc123.us-west-2.rds.amazonaws.com:3306", wantTLS: "rds"},
		{name: "disabled TLS off RDS", host: "database.example.com:3306", tlsMode: "false", wantTLS: "false"},
		{name: "disabled TLS on GovCloud RDS", host: "gov.cluster-abc123.us-gov-west-1.rds.amazonaws.com:3306", tlsMode: "false", wantTLS: "false"},
		{name: "verified TLS on RDS", host: "explicit.cluster-abc123.us-west-2.rds.amazonaws.com:3306", tlsMode: "true", wantTLS: "true"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			logs := captureWarnings(t)

			dsn := "schemabot:secret@tcp(" + tt.host + ")/app"
			if tt.tlsMode != "" {
				dsn += "?tls=" + tt.tlsMode
			}
			got, err := ConnectionDSN(dsn, tt.opts...)
			require.NoError(t, err)

			cfg, err := mysql.ParseDSN(got)
			require.NoError(t, err)
			assert.Equal(t, tt.wantTLS, cfg.TLSConfig, "the explicit TLS mode must be preserved")
			if !tt.wantWarning {
				assert.Empty(t, logs.String())
				return
			}
			wantMode := tt.wantMode
			if wantMode == "" {
				wantMode = tt.tlsMode
			}
			assert.Contains(t, logs.String(), nonVerifyingRDSWarning)
			assert.Contains(t, logs.String(), "host="+tt.host)
			assert.Contains(t, logs.String(), "tls_mode="+wantMode)
			assert.NotContains(t, logs.String(), "secret", "the warning must not carry credentials")
			_, err = ConnectionDSN(dsn, tt.opts...)
			require.NoError(t, err)
			assert.Equal(t, 1, bytes.Count(logs.Bytes(), []byte(nonVerifyingRDSWarning)), "the same endpoint and mode warns once")
		})
	}
}

// The warning is keyed on the endpoint and mode, not on the DSN: a rotated
// password or a different database on the same endpoint is the same posture
// and does not warn again, while a different mode on the same endpoint is a
// different posture and does.
func TestNonVerifyingRDSTLSWarningDedupesPerEndpointAndMode(t *testing.T) {
	logs := captureWarnings(t)
	host := "shared.cluster-abc123.us-west-2.rds.amazonaws.com:3306"

	for _, dsn := range []string{
		"schemabot:secret@tcp(" + host + ")/app?tls=false",
		"schemabot:rotated@tcp(" + host + ")/app?tls=false",
		"schemabot:secret@tcp(" + host + ")/other?tls=false",
	} {
		_, err := ConnectionDSN(dsn)
		require.NoError(t, err)
	}
	assert.Equal(t, 1, bytes.Count(logs.Bytes(), []byte(nonVerifyingRDSWarning)), "one endpoint and mode warns once across DSNs")

	_, err := ConnectionDSN("schemabot:secret@tcp(" + host + ")/app?tls=skip-verify")
	require.NoError(t, err)
	assert.Equal(t, 2, bytes.Count(logs.Bytes(), []byte(nonVerifyingRDSWarning)), "a different mode on the same endpoint warns again")
	assert.Contains(t, logs.String(), "tls_mode=skip-verify")
}

func TestConnectionDSN(t *testing.T) {
	tests := []struct {
		name          string
		dsn           string
		wantTLS       string
		wantParseTime bool
		wantErrSub    string
	}{
		{
			name:          "RDS host gets TLS",
			dsn:           "spirit:secret@tcp(database.cluster-abc123.us-west-2.rds.amazonaws.com:3306)/app?parseTime=true",
			wantTLS:       "rds",
			wantParseTime: true,
		},
		{
			name:          "non-RDS host gets no TLS",
			dsn:           "root:secret@tcp(localhost:3306)/app?parseTime=true",
			wantParseTime: true,
		},
		{
			name:          "database alias gets no TLS",
			dsn:           "spirit:secret@tcp(database.example.com:3306)/app?parseTime=true",
			wantParseTime: true,
		},
		{
			name:    "explicit TLS is preserved",
			dsn:     "spirit:secret@tcp(database.cluster-abc123.us-west-2.rds.amazonaws.com:3306)/app?tls=skip-verify",
			wantTLS: "skip-verify",
		},
		{
			name:    "explicit disabled TLS is preserved",
			dsn:     "spirit:secret@tcp(database.cluster-abc123.us-west-2.rds.amazonaws.com:3306)/app?tls=false",
			wantTLS: "false",
		},
		{
			name:       "invalid DSN returns context",
			dsn:        "not-a-dsn",
			wantErrSub: "parse DSN",
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
			cfg, err := mysql.ParseDSN(got)
			require.NoError(t, err)
			assert.Equal(t, tt.wantTLS, cfg.TLSConfig)
			// Client-side parameter interpolation is a required setting on
			// every SchemaBot-managed connection, so parameterized queries
			// never create server-side prepared statements.
			assert.True(t, cfg.InterpolateParams)
			// A signed tinyint(1) is read as the number stored in it rather
			// than as a Go bool. The driver keeps the parsed field unexported,
			// so the assertion is on the DSN the pool is opened with — which is
			// also what a driver that stopped emitting the parameter would
			// change, rather than reverting the behavior silently.
			assert.Contains(t, got, "tinyInt1IsBool=false")
			// Caller-supplied params survive the reassembly.
			assert.Equal(t, tt.wantParseTime, cfg.ParseTime)
			_, err = mysql.NewConnector(cfg)
			require.NoError(t, err)
		})
	}
}

func TestConnectionDSN_WithConnectTimeout(t *testing.T) {
	t.Run("sets timeout on a plain DSN", func(t *testing.T) {
		got, err := ConnectionDSN(
			"root:secret@tcp(localhost:3306)/app?parseTime=true",
			WithConnectTimeout(4*time.Second),
		)
		require.NoError(t, err)

		cfg, err := mysql.ParseDSN(got)
		require.NoError(t, err)
		assert.Equal(t, 4*time.Second, cfg.Timeout)
		// Existing params survive.
		assert.True(t, cfg.ParseTime)
	})

	t.Run("layers timeout on top of injected RDS TLS", func(t *testing.T) {
		got, err := ConnectionDSN(
			"spirit:secret@tcp(database.cluster-abc123.us-west-2.rds.amazonaws.com:3306)/app?parseTime=true",
			WithConnectTimeout(7*time.Second),
		)
		require.NoError(t, err)

		cfg, err := mysql.ParseDSN(got)
		require.NoError(t, err)
		assert.Equal(t, 7*time.Second, cfg.Timeout)
		// RDS TLS enhancement is preserved alongside the timeout.
		assert.Equal(t, "rds", cfg.TLSConfig)
	})

	t.Run("non-positive timeout falls back to the package default", func(t *testing.T) {
		got, err := ConnectionDSN(
			"root:secret@tcp(localhost:3306)/app?parseTime=true",
			WithConnectTimeout(0),
		)
		require.NoError(t, err)

		cfg, err := mysql.ParseDSN(got)
		require.NoError(t, err)
		assert.Equal(t, defaultConnectTimeout, cfg.Timeout)
	})
}

// Every SchemaBot-managed connection gets bounded connect and write timeouts
// so a target that stops responding surfaces as an error instead of a hung
// drive. A positive timeout carried in the DSN or supplied as an option wins
// over the package default, while a non-positive value — absent, zero, or
// negative via a raw option closure — is filled with the default so no
// managed connection is ever unbounded. Reads stay unbounded because
// long-running DDL can legitimately stream no bytes for longer than any safe
// fixed window.
func TestConnectionDSN_TimeoutDefaults(t *testing.T) {
	t.Run("plain DSN gets default connect and write timeouts", func(t *testing.T) {
		got, err := ConnectionDSN("root:secret@tcp(localhost:3306)/app?parseTime=true")
		require.NoError(t, err)

		cfg, err := mysql.ParseDSN(got)
		require.NoError(t, err)
		assert.Equal(t, defaultConnectTimeout, cfg.Timeout)
		assert.Equal(t, defaultWriteTimeout, cfg.WriteTimeout)
		assert.Equal(t, time.Duration(0), cfg.ReadTimeout)
	})

	t.Run("DSN-carried timeouts are preserved", func(t *testing.T) {
		got, err := ConnectionDSN("root:secret@tcp(localhost:3306)/app?timeout=3s&writeTimeout=9s")
		require.NoError(t, err)

		cfg, err := mysql.ParseDSN(got)
		require.NoError(t, err)
		assert.Equal(t, 3*time.Second, cfg.Timeout)
		assert.Equal(t, 9*time.Second, cfg.WriteTimeout)
	})

	t.Run("connect timeout option overrides the default, write default still applies", func(t *testing.T) {
		got, err := ConnectionDSN(
			"root:secret@tcp(localhost:3306)/app",
			WithConnectTimeout(5*time.Second),
		)
		require.NoError(t, err)

		cfg, err := mysql.ParseDSN(got)
		require.NoError(t, err)
		assert.Equal(t, 5*time.Second, cfg.Timeout)
		assert.Equal(t, defaultWriteTimeout, cfg.WriteTimeout)
	})

	t.Run("negative timeouts from a raw option closure are replaced by the defaults", func(t *testing.T) {
		got, err := ConnectionDSN(
			"root:secret@tcp(localhost:3306)/app",
			func(cfg *mysql.Config) {
				cfg.Timeout = -time.Second
				cfg.WriteTimeout = -time.Second
			},
		)
		require.NoError(t, err)

		cfg, err := mysql.ParseDSN(got)
		require.NoError(t, err)
		assert.Equal(t, defaultConnectTimeout, cfg.Timeout)
		assert.Equal(t, defaultWriteTimeout, cfg.WriteTimeout)
	})
}

func TestOpenNormalizesRDSDSNBeforeOpening(t *testing.T) {
	originalOpenSQL := openSQL
	t.Cleanup(func() { openSQL = originalOpenSQL })

	openErr := errors.New("stop before network connection")
	var gotDriver string
	var gotDSN string
	openSQL = func(driverName, dsn string) (*sql.DB, error) {
		gotDriver = driverName
		gotDSN = dsn
		return nil, openErr
	}

	_, err := Open("spirit:secret@tcp(database.cluster-abc123.us-west-2.rds.amazonaws.com:3306)/app?parseTime=true")

	require.ErrorIs(t, err, openErr)
	// Not "mysql": that name belongs to upstream go-sql-driver. Nothing in
	// SchemaBot's graph registers it any more, so opening under it would fail
	// outright — but a dependency that reaches upstream again would make it
	// resolve to the wrong driver silently, which is what this pins.
	assert.Equal(t, "block-mysql", gotDriver)
	cfg, parseErr := mysql.ParseDSN(gotDSN)
	require.NoError(t, parseErr)
	assert.Equal(t, "rds", cfg.TLSConfig)
}
