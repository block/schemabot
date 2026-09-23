package localscale

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"fmt"
	"io"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/block/spirit/pkg/utils"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// connProbe records, per step of getting a shard-targeted connection, how much
// time that step was given.
type connProbe struct {
	mu    sync.Mutex
	steps map[string]time.Duration
	seen  map[string]bool
}

func newConnProbe() *connProbe {
	return &connProbe{steps: map[string]time.Duration{}, seen: map[string]bool{}}
}

func (p *connProbe) record(step string, ctx context.Context) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.seen[step] = true
	if deadline, ok := ctx.Deadline(); ok {
		p.steps[step] = time.Until(deadline)
	}
}

// budget reports the time the step was given, and whether it was given any.
func (p *connProbe) budget(t *testing.T, step string) (time.Duration, bool) {
	t.Helper()
	p.mu.Lock()
	defer p.mu.Unlock()
	require.True(t, p.seen[step], "%s never ran, so the probe proves nothing", step)
	d, ok := p.steps[step]
	return d, ok
}

type probeDriver struct{ probe *connProbe }

func (d probeDriver) Open(string) (driver.Conn, error) { return probeConn(d), nil }

func (d probeDriver) OpenConnector(string) (driver.Connector, error) {
	return probeConnector{probe: d.probe, driver: d}, nil
}

type probeConnector struct {
	probe  *connProbe
	driver driver.Driver
}

func (c probeConnector) Connect(ctx context.Context) (driver.Conn, error) {
	c.probe.record("connect", ctx)
	return probeConn{c.probe}, nil
}

func (c probeConnector) Driver() driver.Driver { return c.driver }

type probeConn struct{ probe *connProbe }

func (c probeConn) Prepare(string) (driver.Stmt, error) { return nil, io.EOF }
func (c probeConn) Close() error                        { return nil }
func (c probeConn) Begin() (driver.Tx, error)           { return nil, io.EOF }

func (c probeConn) ExecContext(ctx context.Context, query string, _ []driver.NamedValue) (driver.Result, error) {
	c.probe.record("use", ctx)
	return driver.RowsAffected(0), nil
}

var probeDriverSeq atomic.Int64

// probeDB registers a driver of its own, since a test's recorder has to be the
// one its own connections report to and driver names are process-wide.
func probeDB(t *testing.T) (*sql.DB, *connProbe) {
	t.Helper()
	probe := newConnProbe()
	name := fmt.Sprintf("localscale-conn-probe-%d", probeDriverSeq.Add(1))
	sql.Register(name, probeDriver{probe})

	db, err := sql.Open(name, "")
	require.NoError(t, err)
	t.Cleanup(func() { utils.CloseAndLog(db) })
	return db, probe
}

// Getting a shard-targeted connection talks to vtgate twice before the caller
// has a statement to time out: once to take a connection from the pool, once to
// run USE keyspace:shard. A vtgate that accepts a connection and then stops
// answering would otherwise hold the single deploy processor for the life of
// the server, and a cancel recorded against that deploy could never be reached.
func TestVtgateTargetConnBoundsItsOwnRoundTrips(t *testing.T) {
	db, probe := probeDB(t)
	s := &Server{logger: quietLogger()}
	backend := &databaseBackend{unscopedVtgateDB: db}

	conn, cleanup, err := s.vtgateTargetConn(t.Context(), backend, "testapp_sharded", "-80")
	require.NoError(t, err)
	t.Cleanup(cleanup)
	require.NotNil(t, conn)

	for _, step := range []string{"connect", "use"} {
		budget, bounded := probe.budget(t, step)
		assert.True(t, bounded, "%s ran without a deadline", step)
		assert.Positive(t, budget)
		assert.LessOrEqual(t, budget, vitessQueryTimeout, "%s exceeded the step budget", step)
	}
}

// A caller resolving a cancel has a deadline of its own and spends what is left
// of it on the engine. The connection setup must not extend that.
func TestVtgateTargetConnKeepsTheShorterCallerDeadline(t *testing.T) {
	db, probe := probeDB(t)
	s := &Server{logger: quietLogger()}
	backend := &databaseBackend{unscopedVtgateDB: db}

	const remaining = 250 * time.Millisecond
	ctx, cancel := context.WithTimeout(t.Context(), remaining)
	defer cancel()

	conn, cleanup, err := s.vtgateTargetConn(ctx, backend, "testapp_sharded", "-80")
	require.NoError(t, err)
	t.Cleanup(cleanup)
	require.NotNil(t, conn)

	for _, step := range []string{"connect", "use"} {
		budget, bounded := probe.budget(t, step)
		assert.True(t, bounded, "%s ran without a deadline", step)
		assert.LessOrEqual(t, budget, remaining, "%s outlived the caller's own deadline", step)
	}
}
