//go:build integration

package postgresconn

import (
	"net/url"
	"sync/atomic"
	"testing"

	"github.com/block/spirit/pkg/utils"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/block/schemabot/pkg/testutil"
)

// Open returns a working pool against a real PostgreSQL server: the
// normalized DSN parses, the pool dials, and queries execute.
func TestOpenAgainstPostgres(t *testing.T) {
	dsn, _ := testutil.StartPostgres(t, "postgresconn_open")

	db, err := Open(dsn)
	require.NoError(t, err)
	t.Cleanup(func() { utils.CloseAndLog(db) })
	require.NoError(t, db.PingContext(t.Context()))

	var one int
	require.NoError(t, db.QueryRowContext(t.Context(), "SELECT 1").Scan(&one))
	assert.Equal(t, 1, one)
}

// Sessions run in UTC even when the database default is a different
// timezone, so server-side now() feeds storage's timestamp comparisons
// consistently across pods regardless of server configuration. Startup-packet
// parameters take precedence over ALTER DATABASE defaults, which this pins.
func TestOpenPinsSessionTimezoneToUTC(t *testing.T) {
	dsn, adminDB := testutil.StartPostgres(t, "postgresconn_tz")

	_, err := adminDB.ExecContext(t.Context(),
		`ALTER DATABASE postgresconn_tz SET timezone = 'America/New_York'`)
	require.NoError(t, err)

	db, err := Open(dsn)
	require.NoError(t, err)
	t.Cleanup(func() { utils.CloseAndLog(db) })
	require.NoError(t, db.PingContext(t.Context()))

	var tz string
	require.NoError(t, db.QueryRowContext(t.Context(), "SHOW timezone").Scan(&tz))
	assert.Equal(t, "UTC", tz)
}

// Rotating the storage password out from under a running pool must be
// transparent: the next fresh physical connection is rejected with an
// authentication error, the pool re-resolves the DSN through reload, retries,
// and the query succeeds without a restart.
func TestOpenReloadableSurvivesPasswordRotation(t *testing.T) {
	dsn, adminDB := testutil.StartPostgres(t, "postgresconn_rotate")

	// Pin one session for the rotation DDL. The adminDB pool itself holds the
	// pre-rotation password, so only this already-authenticated session is
	// safe to use after the rotation.
	adminConn, err := adminDB.Conn(t.Context())
	require.NoError(t, err)
	t.Cleanup(func() { utils.CloseAndLog(adminConn) })

	rotatedDSN := withPassword(t, dsn, "rotated")
	var reloads atomic.Int32
	db, err := OpenReloadable(dsn, func() (string, error) {
		reloads.Add(1)
		return rotatedDSN, nil
	})
	require.NoError(t, err)
	t.Cleanup(func() { utils.CloseAndLog(db) })

	// Every query dials a fresh physical connection so the rotation is
	// observed immediately instead of being masked by idle pooled sessions.
	db.SetMaxIdleConns(0)

	var one int
	require.NoError(t, db.QueryRowContext(t.Context(), "SELECT 1").Scan(&one))
	require.Equal(t, 1, one)
	require.Equal(t, int32(0), reloads.Load(), "no reload while the original credentials work")

	_, err = adminConn.ExecContext(t.Context(), `ALTER ROLE schemabot WITH PASSWORD 'rotated'`)
	require.NoError(t, err)

	require.NoError(t, db.QueryRowContext(t.Context(), "SELECT 1").Scan(&one))
	assert.Equal(t, 1, one)
	assert.Equal(t, int32(1), reloads.Load(), "the rejected dial reloads once and retries")

	require.NoError(t, db.QueryRowContext(t.Context(), "SELECT 1").Scan(&one))
	assert.Equal(t, int32(1), reloads.Load(), "reloaded credentials stick; no reload per dial")
}

// withPassword returns dsn (URL form) with its password replaced.
func withPassword(t *testing.T, dsn, password string) string {
	t.Helper()
	u, err := url.Parse(dsn)
	require.NoError(t, err)
	require.NotNil(t, u.User)
	u.User = url.UserPassword(u.User.Username(), password)
	return u.String()
}
