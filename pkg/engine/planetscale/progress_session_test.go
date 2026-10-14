package planetscale

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"fmt"
	"io"
	"log/slog"
	"os"
	"regexp"
	"strings"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/block/mysql"
	"github.com/block/spirit/pkg/utils"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// sessionVtgate stands in for a vtgate that serves several keyspaces over one
// pool. Like vtgate, it routes a keyspace-qualified statement on the keyspace
// the statement names, routes an unqualified one on the connection's session
// keyspace, and lets USE switch that session keyspace for the rest of the
// connection's life.
type sessionVtgate struct {
	defaultKeyspace string
	// tables lists the table with an in-flight schema change per keyspace.
	tables map[string]string
}

var sessionVtgateSeq atomic.Int64

// openSessionVtgate registers a driver of its own, since driver names are
// process-wide, and returns a single-connection pool so every statement in the
// test reuses the one connection, the way a quiet shared pool does.
func openSessionVtgate(t *testing.T, v *sessionVtgate) *sql.DB {
	t.Helper()
	name := fmt.Sprintf("planetscale-session-vtgate-%d", sessionVtgateSeq.Add(1))
	sql.Register(name, sessionVtgateDriver{v})
	db, err := sql.Open(name, "")
	require.NoError(t, err)
	db.SetMaxOpenConns(1)
	t.Cleanup(func() { utils.CloseAndLog(db) })
	return db
}

type sessionVtgateDriver struct{ vtgate *sessionVtgate }

func (d sessionVtgateDriver) Open(string) (driver.Conn, error) {
	return &sessionVtgateConn{vtgate: d.vtgate, keyspace: d.vtgate.defaultKeyspace}, nil
}

type sessionVtgateConn struct {
	vtgate *sessionVtgate

	mu       sync.Mutex
	keyspace string // the session keyspace; USE changes it
}

func (c *sessionVtgateConn) Prepare(string) (driver.Stmt, error) { return nil, io.EOF }
func (c *sessionVtgateConn) Close() error                        { return nil }
func (c *sessionVtgateConn) Begin() (driver.Tx, error)           { return nil, io.EOF }

var (
	useStatement             = regexp.MustCompile("^USE `([^`]+)`$")
	showMigrationsFromClause = regexp.MustCompile("^SHOW VITESS_MIGRATIONS FROM `((?:[^`]|``)+)`")
)

func (c *sessionVtgateConn) ExecContext(_ context.Context, query string, _ []driver.NamedValue) (driver.Result, error) {
	m := useStatement.FindStringSubmatch(query)
	if m == nil {
		return nil, fmt.Errorf("session vtgate: unsupported statement %q", query)
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	c.keyspace = m[1]
	return driver.RowsAffected(0), nil
}

func (c *sessionVtgateConn) QueryContext(_ context.Context, query string, _ []driver.NamedValue) (driver.Rows, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	switch {
	case query == "SELECT DATABASE()":
		return &valueRows{columns: []string{"DATABASE()"}, values: [][]driver.Value{{c.keyspace}}}, nil
	case strings.HasPrefix(query, "SHOW VITESS_MIGRATIONS"):
		keyspace := c.keyspace
		if m := showMigrationsFromClause.FindStringSubmatch(query); m != nil {
			keyspace = strings.ReplaceAll(m[1], "``", "`")
		}
		table, ok := c.vtgate.tables[keyspace]
		if !ok {
			return nil, fmt.Errorf("session vtgate: unknown keyspace %q", keyspace)
		}
		return &valueRows{
			columns: []string{"migration_uuid", "migration_context", "keyspace", "shard", "mysql_table", "migration_status"},
			values:  [][]driver.Value{{"uuid-" + keyspace, "vtctl:" + keyspace, keyspace, "0", table, "running"}},
		}, nil
	}
	return nil, fmt.Errorf("session vtgate: unsupported query %q", query)
}

type valueRows struct {
	columns []string
	values  [][]driver.Value
}

func (r *valueRows) Columns() []string { return r.columns }
func (r *valueRows) Close() error      { return nil }

func (r *valueRows) Next(dest []driver.Value) error {
	if len(r.values) == 0 {
		return io.EOF
	}
	copy(dest, r.values[0])
	r.values = r.values[1:]
	return nil
}

// A vtgate DSN whose default database is itself a keyspace shares one pool
// between two readers: per-keyspace progress, and the plan fallback that loads
// that keyspace's live schema through getVtgateKeyspaceDB. Progress reading a
// different keyspace must not hand a connection back to that pool switched to
// it, or the next live-schema read would list the wrong keyspace's tables and
// diff the plan against them.
func TestShowVitessMigrationsForKeyspaceLeavesThePooledSessionUnchanged(t *testing.T) {
	ctx := t.Context()

	cfg := mysql.NewConfig()
	cfg.User = "schemabot"
	cfg.Net = "tcp"
	cfg.Addr = "vtgate.internal:3306"
	cfg.DBName = "commerce"
	dsn := cfg.FormatDSN()

	vtgate := &sessionVtgate{
		defaultKeyspace: "commerce",
		tables:          map[string]string{"commerce": "orders", "customers": "profiles"},
	}
	eng := New(slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelError})))
	eng.vtgateDBs[dsn] = openSessionVtgate(t, vtgate)

	// The plan fallback for the DSN's own keyspace resolves to the very pool
	// progress reads from, which is what makes leaked session state reachable.
	planPool, err := eng.getVtgateKeyspaceDB(ctx, dsn, "commerce")
	require.NoError(t, err)
	require.Same(t, eng.vtgateDBs[dsn], planPool, "the plan fallback must share the pool for this test to prove anything")

	rows, err := eng.showVitessMigrationsForKeyspace(ctx, dsn, "customers", "")
	require.NoError(t, err)
	require.Len(t, rows, 1)
	assert.Equal(t, "customers", rows[0].Keyspace, "progress must read the keyspace it was asked about")
	assert.Equal(t, "profiles", rows[0].Table)

	var sessionKeyspace string
	require.NoError(t, planPool.QueryRowContext(ctx, "SELECT DATABASE()").Scan(&sessionKeyspace))
	assert.Equal(t, "commerce", sessionKeyspace,
		"the pooled connection came back switched to another keyspace; a live-schema read on it would list that keyspace's tables")
}
