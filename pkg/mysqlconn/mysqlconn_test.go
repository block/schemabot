package mysqlconn

import (
	"database/sql"
	"errors"
	"testing"

	"github.com/go-sql-driver/mysql"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestConnectionDSN(t *testing.T) {
	tests := []struct {
		name       string
		dsn        string
		wantTLS    string
		wantSame   bool
		wantErrSub string
	}{
		{
			name:    "RDS host gets TLS",
			dsn:     "spirit:secret@tcp(database.cluster-abc123.us-west-2.rds.amazonaws.com:3306)/app?parseTime=true",
			wantTLS: "rds",
		},
		{
			name:     "non-RDS host is unchanged",
			dsn:      "root:secret@tcp(localhost:3306)/app?parseTime=true",
			wantSame: true,
		},
		{
			name:     "database alias is unchanged",
			dsn:      "spirit:secret@tcp(database.example.com:3306)/app?parseTime=true",
			wantSame: true,
		},
		{
			name:     "explicit TLS is preserved",
			dsn:      "spirit:secret@tcp(database.cluster-abc123.us-west-2.rds.amazonaws.com:3306)/app?tls=skip-verify",
			wantTLS:  "skip-verify",
			wantSame: true,
		},
		{
			name:     "explicit disabled TLS is preserved",
			dsn:      "spirit:secret@tcp(database.cluster-abc123.us-west-2.rds.amazonaws.com:3306)/app?tls=false",
			wantTLS:  "false",
			wantSame: true,
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
			if tt.wantSame {
				assert.Equal(t, tt.dsn, got)
			}

			cfg, err := mysql.ParseDSN(got)
			require.NoError(t, err)
			assert.Equal(t, tt.wantTLS, cfg.TLSConfig)
			_, err = mysql.NewConnector(cfg)
			require.NoError(t, err)
		})
	}
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
