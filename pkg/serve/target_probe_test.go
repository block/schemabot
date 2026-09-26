package serve

import (
	"bytes"
	"context"
	"log/slog"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/block/schemabot/pkg/drain"
	"github.com/block/schemabot/pkg/inventory"
	"github.com/block/schemabot/pkg/storage"
)

const probeJoinTimeout = 5 * time.Second

// blockingResolver enumerates one target whose resolution waits on the probe
// context, standing in for an inventory lookup that has not answered when the
// server is asked to shut down.
type blockingResolver struct {
	resolving chan struct{}
}

type panickingEnumerator struct{}

func (panickingEnumerator) ResolveTarget(context.Context, inventory.Request) (*inventory.Target, error) {
	return nil, assert.AnError
}

func (panickingEnumerator) Enumerate(context.Context) ([]inventory.ProbeRequest, error) {
	panic("enumeration failed unexpectedly")
}

func (panickingEnumerator) UnenumerableDatabaseTypes() []string { return nil }

func (r *blockingResolver) Enumerate(context.Context) ([]inventory.ProbeRequest, error) {
	return []inventory.ProbeRequest{{Target: "target-a", DatabaseType: storage.DatabaseTypeMySQL}}, nil
}

func (r *blockingResolver) UnenumerableDatabaseTypes() []string { return nil }

func (r *blockingResolver) ResolveTarget(ctx context.Context, _ inventory.Request) (*inventory.Target, error) {
	close(r.resolving)
	<-ctx.Done()
	return nil, ctx.Err()
}

// Closing the server while a startup probe is still resolving a target cancels
// the probe and waits for its goroutine to exit, so the probe never runs on
// past the resolver and clients the close tears down behind it.
func TestStopTargetProbeCancelsAndJoinsInFlightProbe(t *testing.T) {
	resolver := &blockingResolver{resolving: make(chan struct{})}
	var logs bytes.Buffer
	srv := &Server{logger: slog.New(slog.NewTextHandler(&logs, nil)), targetResolver: resolver}

	srv.startTargetProbe(t.Context())

	waitCtx, cancel := context.WithTimeout(t.Context(), probeJoinTimeout)
	defer cancel()
	select {
	case <-resolver.resolving:
	case <-waitCtx.Done():
		require.NoError(t, waitCtx.Err(), "waiting for the probe to start resolving")
	}

	stopped := make(chan struct{})
	go func() {
		srv.stopTargetProbe(nil)
		close(stopped)
	}()
	select {
	case <-stopped:
	case <-waitCtx.Done():
		require.NoError(t, waitCtx.Err(), "waiting for the probe to stop")
	}
	select {
	case <-srv.probeDone:
	default:
		require.Fail(t, "stopTargetProbe returned before the probe goroutine exited")
	}
	assert.Contains(t, logs.String(), "target probe stopped")
	assert.NotContains(t, logs.String(), "outcome=", "a probe cut short by shutdown records no outcome")
}

// A probe that does not observe its cancellation — a target reachable but not
// answering holds a pool worker, and with it the goroutine feeding that pool —
// must not hold the close open. This stage runs first, so an unbounded wait
// here would be a wait every bounded stage after it sits behind, on a probe of
// a database the process is on its way to stopping using.
func TestStopTargetProbeGivesUpOnAProbeThatIgnoresItsCancellation(t *testing.T) {
	var logs bytes.Buffer
	srv := &Server{logger: slog.New(slog.NewTextHandler(&logs, nil))}

	// The state startTargetProbe leaves behind: a cancel to call, and a
	// goroutine that has not closed probeDone and never will.
	_, cancel := context.WithCancel(t.Context())
	srv.probeCancel = cancel
	srv.probeDone = make(chan struct{})

	budget := drain.NewBudget(time.Nanosecond)
	require.True(t, budget.Spent())

	start := time.Now()
	srv.stopTargetProbe(budget)
	elapsed := time.Since(start)

	assert.Less(t, elapsed, targetProbeDrainTimeout, "a spent budget leaves the join less than its own bound")
	assert.Contains(t, logs.String(), "target probe did not exit within the shutdown drain")
	assert.Contains(t, logs.String(), "budget_spent=true")
	assert.NotContains(t, logs.String(), "target probe stopped",
		"the close must not report a probe stopped while its goroutine is still running")
}

// A server whose Start launched no probe closes without waiting on one.
func TestStopTargetProbeWithoutProbeIsNoOp(t *testing.T) {
	var logs bytes.Buffer
	srv := &Server{logger: slog.New(slog.NewTextHandler(&logs, nil))}
	srv.stopTargetProbe(nil)
	assert.NotContains(t, logs.String(), "target probe stopped")
}

// Enumeration runs outside the per-target pool, so the server's outer probe
// boundary contains its panic and preserves both diagnostic fields.
func TestStartTargetProbeContainsEnumerationPanic(t *testing.T) {
	var logs bytes.Buffer
	srv := &Server{
		logger:         slog.New(slog.NewTextHandler(&logs, nil)),
		targetResolver: panickingEnumerator{},
	}

	srv.startTargetProbe(t.Context())
	waitCtx, cancel := context.WithTimeout(t.Context(), probeJoinTimeout)
	defer cancel()
	select {
	case <-srv.probeDone:
	case <-waitCtx.Done():
		require.NoError(t, waitCtx.Err(), "waiting for the contained enumeration panic")
	}

	assert.Contains(t, logs.String(), `panic="enumeration failed unexpectedly"`)
	assert.Contains(t, logs.String(), "stack=")
}
