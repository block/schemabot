package mysqlconn

import (
	"database/sql"
	"errors"
	"testing"
	"time"

	"github.com/go-sql-driver/mysql"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestConnectionDSN(t *testing.T) {
	tests := []struct {
		name       string
		dsn        string
		wantTLS    string
		wantErrSub string
	}{
		{
			name:    "RDS host gets TLS",
			dsn:     "spirit:secret@tcp(database.cluster-abc123.us-west-2.rds.amazonaws.com:3306)/app?parseTime=true",
			wantTLS: "rds",
		},
		{
			name:    "non-RDS host gets no TLS",
			dsn:     "root:secret@tcp(localhost:3306)/app?parseTime=true",
			wantTLS: "",
		},
		{
			name:    "database alias gets no TLS",
			dsn:     "spirit:secret@tcp(database.example.com:3306)/app?parseTime=true",
			wantTLS: "",
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

			// Every normalized DSN carries transport timeouts so a
			// black-holed connection cannot block a goroutine forever.
			assert.Equal(t, connectTimeout, cfg.Timeout)
			assert.Equal(t, connReadTimeout, cfg.ReadTimeout)
			assert.Equal(t, connWriteTimeout, cfg.WriteTimeout)

			// Normalization must not alter the connection identity or
			// existing params.
			want, err := mysql.ParseDSN(tt.dsn)
			require.NoError(t, err)
			assert.Equal(t, want.Addr, cfg.Addr)
			assert.Equal(t, want.User, cfg.User)
			assert.Equal(t, want.Passwd, cfg.Passwd)
			assert.Equal(t, want.DBName, cfg.DBName)
			assert.Equal(t, want.ParseTime, cfg.ParseTime)

			_, err = mysql.NewConnector(cfg)
			require.NoError(t, err)
		})
	}
}

// A DSN that sets its own transport timeouts keeps them — the defaults only
// fill in missing values.
func TestConnectionDSNKeepsExplicitTimeouts(t *testing.T) {
	got, err := ConnectionDSN("root:secret@tcp(localhost:3306)/app?timeout=5s&readTimeout=10s&writeTimeout=15s")
	require.NoError(t, err)

	cfg, err := mysql.ParseDSN(got)
	require.NoError(t, err)
	assert.Equal(t, 5*time.Second, cfg.Timeout)
	assert.Equal(t, 10*time.Second, cfg.ReadTimeout)
	assert.Equal(t, 15*time.Second, cfg.WriteTimeout)
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

	t.Run("non-positive timeout leaves the default connect timeout", func(t *testing.T) {
		got, err := ConnectionDSN(
			"root:secret@tcp(localhost:3306)/app?parseTime=true",
			WithConnectTimeout(0),
		)
		require.NoError(t, err)

		cfg, err := mysql.ParseDSN(got)
		require.NoError(t, err)
		assert.Equal(t, connectTimeout, cfg.Timeout)
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
	assert.Equal(t, "mysql", gotDriver)
	cfg, parseErr := mysql.ParseDSN(gotDSN)
	require.NoError(t, parseErr)
	assert.Equal(t, "rds", cfg.TLSConfig)
	assert.Equal(t, connReadTimeout, cfg.ReadTimeout)
	assert.Equal(t, connWriteTimeout, cfg.WriteTimeout)
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
