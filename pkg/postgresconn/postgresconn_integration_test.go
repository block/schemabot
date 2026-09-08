//go:build integration

package postgresconn

import (
	"database/sql"
	"net/url"
	"sync/atomic"
	"testing"
	"time"

	"github.com/block/spirit/pkg/utils"
	"github.com/jackc/pgx/v5/pgconn"
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
	// PGTZ is a libpq env fallback that reaches every session below as an
	// explicit timezone; clear it so both the baseline and the pin assert
	// server-versus-connection behavior alone.
	t.Setenv("PGTZ", "")

	dsn, adminDB := testutil.StartPostgres(t, "postgresconn_tz")

	_, err := adminDB.ExecContext(t.Context(),
		`ALTER DATABASE postgresconn_tz SET timezone = 'America/New_York'`)
	require.NoError(t, err)

	// Baseline: a fresh unpinned session must observe the non-UTC database
	// default. ALTER DATABASE only affects new sessions, so a raw pool opened
	// now proves the default took effect — without this, a UTC assertion on
	// the pinned pool holds on any UTC-default server even if Open never
	// pinned anything.
	baseline, err := sql.Open("pgx", dsn)
	require.NoError(t, err)
	t.Cleanup(func() { utils.CloseAndLog(baseline) })
	var tz string
	require.NoError(t, baseline.QueryRowContext(t.Context(), "SHOW timezone").Scan(&tz))
	require.Equal(t, "America/New_York", tz)

	db, err := Open(dsn)
	require.NoError(t, err)
	t.Cleanup(func() { utils.CloseAndLog(db) })
	require.NoError(t, db.PingContext(t.Context()))

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

// requireSessionStatementTimeout asserts the session-level statement_timeout a
// pool's connections run under, as the server reports it.
func requireSessionStatementTimeout(t *testing.T, db *sql.DB, want string) {
	t.Helper()
	var got string
	require.NoError(t, db.QueryRowContext(t.Context(), "SHOW statement_timeout").Scan(&got))
	assert.Equal(t, want, got)
}

// A statement budget SchemaBot states wins over one the platform imposed at
// the database level. Hosted PostgreSQL providers set statement_timeout on the
// role or database and tune it for API queries, so a SchemaBot connection that
// set nothing would silently run DDL under someone else's budget.
// Startup-packet parameters take precedence over ALTER DATABASE defaults,
// which this pins.
func TestWithStatementTimeoutOverridesDatabaseDefault(t *testing.T) {
	dsn, adminDB := testutil.StartPostgres(t, "postgresconn_stmt")

	_, err := adminDB.ExecContext(t.Context(),
		`ALTER DATABASE postgresconn_stmt SET statement_timeout = '50ms'`)
	require.NoError(t, err)

	// Baseline: a fresh session with no option must observe the hostile
	// database default, so the assertion below tests the option rather than a
	// server that never had a budget.
	baseline, err := Open(dsn)
	require.NoError(t, err)
	t.Cleanup(func() { utils.CloseAndLog(baseline) })
	requireSessionStatementTimeout(t, baseline, "50ms")

	db, err := Open(dsn, WithStatementTimeout(45*time.Second))
	require.NoError(t, err)
	t.Cleanup(func() { utils.CloseAndLog(db) })
	require.NoError(t, db.PingContext(t.Context()))

	requireSessionStatementTimeout(t, db, "45s")

	// The budget is real, not just reported: a statement that outlives it is
	// cancelled with 57014 rather than running on.
	shortDB, err := Open(dsn, WithStatementTimeout(100*time.Millisecond))
	require.NoError(t, err)
	t.Cleanup(func() { utils.CloseAndLog(shortDB) })
	_, err = shortDB.ExecContext(t.Context(), "SELECT pg_sleep(5)")
	require.Error(t, err)
	var pgErr *pgconn.PgError
	require.ErrorAs(t, err, &pgErr)
	assert.Equal(t, "57014", pgErr.Code)
}

// A zero budget disables statement_timeout explicitly rather than falling back
// to the platform's value. The bootstrap advisory-lock connection depends on
// this: it must be free to block inside pg_advisory_lock for the leader's
// whole bootstrap, and a database-level budget shorter than that wait would
// otherwise cancel a trailing pod's legitimate queue.
func TestWithStatementTimeoutZeroDisablesInheritedBudget(t *testing.T) {
	dsn, adminDB := testutil.StartPostgres(t, "postgresconn_stmt_zero")

	_, err := adminDB.ExecContext(t.Context(),
		`ALTER DATABASE postgresconn_stmt_zero SET statement_timeout = '50ms'`)
	require.NoError(t, err)

	db, err := Open(dsn, WithStatementTimeout(0))
	require.NoError(t, err)
	t.Cleanup(func() { utils.CloseAndLog(db) })
	require.NoError(t, db.PingContext(t.Context()))

	requireSessionStatementTimeout(t, db, "0")

	// A statement that would have died under the inherited 50ms budget runs to
	// completion.
	_, err = db.ExecContext(t.Context(), "SELECT pg_sleep(0.5)")
	assert.NoError(t, err)
}

// Omitting the option leaves whatever the platform set in place, so callers
// that have not chosen a budget are not silently given one.
func TestWithStatementTimeoutNegativeLeavesInheritedBudget(t *testing.T) {
	dsn, adminDB := testutil.StartPostgres(t, "postgresconn_stmt_unset")

	_, err := adminDB.ExecContext(t.Context(),
		`ALTER DATABASE postgresconn_stmt_unset SET statement_timeout = '50ms'`)
	require.NoError(t, err)

	db, err := Open(dsn, WithStatementTimeout(-1))
	require.NoError(t, err)
	t.Cleanup(func() { utils.CloseAndLog(db) })
	require.NoError(t, db.PingContext(t.Context()))

	requireSessionStatementTimeout(t, db, "50ms")
}
