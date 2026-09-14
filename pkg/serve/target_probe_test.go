package serve

import (
	"bytes"
	"context"
	"log/slog"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

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
		srv.stopTargetProbe()
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

// A server whose Start launched no probe closes without waiting on one.
func TestStopTargetProbeWithoutProbeIsNoOp(t *testing.T) {
	var logs bytes.Buffer
	srv := &Server{logger: slog.New(slog.NewTextHandler(&logs, nil))}
	srv.stopTargetProbe()
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
