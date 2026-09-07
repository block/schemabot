package mysqlconn

import (
	"database/sql"
	"errors"
	"testing"
	"time"

	"github.com/block/mysql"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

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
	// Not "mysql": that name still belongs to upstream go-sql-driver, which
	// remains linked because the hot-swap driver embeds it. Opening under it
	// here would silently bypass the fork rather than fail.
	assert.Equal(t, "block-mysql", gotDriver)
	cfg, parseErr := mysql.ParseDSN(gotDSN)
	require.NoError(t, parseErr)
	assert.Equal(t, "rds", cfg.TLSConfig)
}

func TestOpenReloadableUsesHotswapDriver(t *testing.T) {
	originalOpenSQL := openSQL
	t.Cleanup(func() { openSQL = originalOpenSQL })

	openErr := errors.New("stop before network connection")
	var gotDriver string
	openSQL = func(driverName, _ string) (*sql.DB, error) {
		gotDriver = driverName
		return nil, openErr
	}

	_, err := OpenReloadable("spirit:secret@tcp(127.0.0.1:3306)/app", func() (string, error) {
		return "", nil
	})

	require.ErrorIs(t, err, openErr)
	assert.Equal(t, hotswapDriverName, gotDriver)
}

func TestReloadConnectionDSN(t *testing.T) {
	t.Run("re-applies RDS transport to the reloaded DSN", func(t *testing.T) {
		got := reloadConnectionDSN(func() (string, error) {
			return "spirit:rotated@tcp(database.cluster-abc123.us-west-2.rds.amazonaws.com:3306)/app", nil
		})

		cfg, err := mysql.ParseDSN(got)
		require.NoError(t, err)
		assert.Equal(t, "rotated", cfg.Passwd)
		assert.Equal(t, "rds", cfg.TLSConfig)
	})

	t.Run("keeps current DSN when reload fails", func(t *testing.T) {
		got := reloadConnectionDSN(func() (string, error) {
			return "", errors.New("secret file unreadable")
		})

		assert.Empty(t, got)
	})

	t.Run("keeps current DSN when the reloaded DSN is unparseable", func(t *testing.T) {
		got := reloadConnectionDSN(func() (string, error) {
			return "not-a-valid-dsn", nil
		})

		assert.Empty(t, got)
	})
}
