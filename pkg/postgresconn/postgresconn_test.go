package postgresconn

import (
	"context"
	"database/sql/driver"
	"errors"
	"fmt"
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
			dsn:  "postgres://schemabot:secret@database.cluster-abc123.us-west-2.rds.amazonaws.com:5432/app",
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
			name: "non-RDS URL host is unchanged",
			dsn:  "postgres://schemabot:secret@localhost:5432/app",
			want: "postgres://schemabot:secret@localhost:5432/app",
		},
		{
			name: "non-RDS keyword host is unchanged",
			dsn:  "host=localhost user=schemabot password=secret dbname=app",
			want: "host=localhost user=schemabot password=secret dbname=app",
		},
		{
			name:       "invalid DSN returns context",
			dsn:        "postgres://schemabot:secret@localhost:not-a-port/app",
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

func TestWithConnectTimeout(t *testing.T) {
	cfg, err := connectionConfig("postgres://schemabot:secret@localhost:5432/app", WithConnectTimeout(7*time.Second))
	require.NoError(t, err)
	assert.Equal(t, 7*time.Second, cfg.ConnectTimeout)

	cfg, err = connectionConfig("postgres://schemabot:secret@localhost:5432/app", WithConnectTimeout(0))
	require.NoError(t, err)
	assert.Equal(t, time.Duration(0), cfg.ConnectTimeout)
}

// stubConn is the minimal driver.Conn a fake dial can hand back.
type stubConn struct{}

func (stubConn) Prepare(string) (driver.Stmt, error) { return nil, errors.New("not implemented") }
func (stubConn) Close() error                        { return nil }
func (stubConn) Begin() (driver.Tx, error)           { return nil, errors.New("not implemented") }

// fakeDial replaces the connectConfig seam for the test and records the
// password of every dial attempt. Each attempt's outcome comes from results,
// consumed in order.
func fakeDial(t *testing.T, results []error) *[]string {
	t.Helper()
	var passwords []string
	original := connectConfig
	t.Cleanup(func() { connectConfig = original })
	connectConfig = func(_ context.Context, cfg pgx.ConnConfig) (driver.Conn, error) {
		passwords = append(passwords, cfg.Password)
		require.Less(t, len(passwords)-1, len(results), "unexpected extra dial attempt")
		if err := results[len(passwords)-1]; err != nil {
			return nil, err
		}
		return stubConn{}, nil
	}
	return &passwords
}

func authError() error {
	return fmt.Errorf("connect: %w", &pgconn.PgError{Code: "28P01", Message: "password authentication failed"})
}

func newReloadableConnector(t *testing.T, dsn string, reload func() (string, error)) *reloadableConnector {
	t.Helper()
	cfg, err := connectionConfig(dsn)
	require.NoError(t, err)
	return &reloadableConnector{cfg: cfg, reload: reload}
}

func TestReloadableConnectorReloadsOnAuthFailure(t *testing.T) {
	passwords := fakeDial(t, []error{authError(), nil, nil})
	var reloads atomic.Int32
	c := newReloadableConnector(t, "postgres://schemabot:old@localhost:5432/app", func() (string, error) {
		reloads.Add(1)
		return "postgres://schemabot:rotated@localhost:5432/app", nil
	})

	conn, err := c.Connect(t.Context())
	require.NoError(t, err)
	require.NotNil(t, conn)
	assert.Equal(t, []string{"old", "rotated"}, *passwords, "retry must dial with the reloaded credentials")
	assert.Equal(t, int32(1), reloads.Load())

	// The reloaded credentials stick for subsequent dials without another reload.
	conn, err = c.Connect(t.Context())
	require.NoError(t, err)
	require.NotNil(t, conn)
	assert.Equal(t, []string{"old", "rotated", "rotated"}, *passwords)
	assert.Equal(t, int32(1), reloads.Load())
}

func TestReloadableConnectorKeepsCredentialsWhenReloadFails(t *testing.T) {
	passwords := fakeDial(t, []error{authError()})
	c := newReloadableConnector(t, "postgres://schemabot:old@localhost:5432/app", func() (string, error) {
		return "", errors.New("secret backend unavailable")
	})

	conn, err := c.Connect(t.Context())
	require.Error(t, err)
	assert.Nil(t, conn)
	assert.Contains(t, err.Error(), "password authentication failed", "the original auth error surfaces, not the reload error")
	assert.Equal(t, []string{"old"}, *passwords, "no retry without fresh credentials")

	cfg, gen := c.snapshot()
	assert.Equal(t, "old", cfg.Password, "current credentials are kept")
	assert.Equal(t, uint64(0), gen)
}

func TestReloadableConnectorIgnoresNonAuthErrors(t *testing.T) {
	dialErr := errors.New("connection refused")
	passwords := fakeDial(t, []error{dialErr})
	reloadCalled := false
	c := newReloadableConnector(t, "postgres://schemabot:old@localhost:5432/app", func() (string, error) {
		reloadCalled = true
		return "postgres://schemabot:rotated@localhost:5432/app", nil
	})

	_, err := c.Connect(t.Context())
	require.ErrorIs(t, err, dialErr)
	assert.False(t, reloadCalled, "a non-authentication failure must not trigger a reload")
	assert.Equal(t, []string{"old"}, *passwords)
}

func TestReloadableConnectorRefreshDedupesConcurrentFailures(t *testing.T) {
	var reloads atomic.Int32
	c := newReloadableConnector(t, "postgres://schemabot:old@localhost:5432/app", func() (string, error) {
		reloads.Add(1)
		return "postgres://schemabot:rotated@localhost:5432/app", nil
	})

	cfg, ok := c.refresh(0)
	require.True(t, ok)
	assert.Equal(t, "rotated", cfg.Password)
	require.Equal(t, int32(1), reloads.Load())

	// A dial that failed against the already-superseded generation reuses the
	// swapped config instead of reloading again.
	cfg, ok = c.refresh(0)
	require.True(t, ok)
	assert.Equal(t, "rotated", cfg.Password)
	assert.Equal(t, int32(1), reloads.Load(), "stale-generation refresh must not reload")
}

func TestIsAuthError(t *testing.T) {
	assert.True(t, isAuthError(&pgconn.PgError{Code: "28P01"}))
	assert.True(t, isAuthError(&pgconn.PgError{Code: "28000"}))
	assert.True(t, isAuthError(fmt.Errorf("wrapped: %w", &pgconn.PgError{Code: "28P01"})))
	assert.False(t, isAuthError(&pgconn.PgError{Code: "55P03"}))
	assert.False(t, isAuthError(errors.New("connection refused")))
	assert.False(t, isAuthError(nil))
}
