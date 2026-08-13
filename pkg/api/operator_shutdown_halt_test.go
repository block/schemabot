package api

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// haltingTernClient records whether shutdown halted it, and how many operator
// drivers were still running when it did.
type haltingTernClient struct {
	*mockTernClient
	haltErr     error
	halts       atomic.Int64
	driversSeen atomic.Int64
	svc         *Service
}

func (c *haltingTernClient) HaltForShutdown(context.Context) error {
	c.halts.Add(1)
	if c.svc != nil {
		c.driversSeen.Store(c.svc.driversBusy.Load())
	}
	return c.haltErr
}

// An engine that runs its schema change inside this process keeps the target's
// lock for as long as its work lives, and that work outlives the drive that
// started it. Shutting down without bringing it down leaves the target locked
// while the process no longer renews the apply's lease, so every driver that
// reclaims the apply is refused and burns a recovery attempt. Stopping the
// operator must halt those engines, and only after the drives it cancelled have
// returned — a halt racing a live drive would land while the drive is still
// reading engine state.
func TestStopOperatorHaltsInProcessEnginesAfterDrivesReturn(t *testing.T) {
	client := &haltingTernClient{mockTernClient: &mockTernClient{}}
	svc, _ := newQueueApplyTestService(trustedQueueApplyTestPlan(), client, &capturingApplyStore{})
	client.svc = svc
	require.NoError(t, svc.SetOperatorPollInterval(time.Hour))

	svc.StartOperator(t.Context())
	svc.StopOperator()

	assert.Equal(t, int64(1), client.halts.Load(), "the client's engine is halted exactly once on shutdown")
	assert.Equal(t, int64(0), client.driversSeen.Load(),
		"the halt runs after the cancelled drives have returned, not alongside them")
}

// Shutdown must not stop at the first engine that will not come down: every
// other target would stay locked behind it. Each failure is reported and the
// remaining clients are still halted.
func TestStopOperatorHaltsEveryClientDespiteFailures(t *testing.T) {
	failing := &haltingTernClient{mockTernClient: &mockTernClient{}, haltErr: errors.New("runner still copying")}
	healthy := &haltingTernClient{mockTernClient: &mockTernClient{}}
	svc, _ := newQueueApplyTestService(trustedQueueApplyTestPlan(), failing, &capturingApplyStore{})
	svc.RegisterTernClient("west", "staging", healthy)
	require.NoError(t, svc.SetOperatorPollInterval(time.Hour))

	svc.StartOperator(t.Context())
	svc.StopOperator()

	assert.Equal(t, int64(1), failing.halts.Load())
	assert.Equal(t, int64(1), healthy.halts.Load(), "a client that will not come down does not leave its peers held")
}

// A deployment can mix targets driven in this process with targets served by a
// separate Tern service. A client that delegates to that service owns no engines
// here — it halts its own on its own shutdown — so shutdown skips it and still
// halts the local ones rather than failing on the one it cannot halt.
func TestStopOperatorSkipsClientsWithoutInProcessEngines(t *testing.T) {
	remote := &mockTernClient{}
	local := &haltingTernClient{mockTernClient: &mockTernClient{}}
	svc, _ := newQueueApplyTestService(trustedQueueApplyTestPlan(), remote, &capturingApplyStore{})
	svc.RegisterTernClient("west", "staging", local)
	require.NoError(t, svc.SetOperatorPollInterval(time.Hour))

	svc.StartOperator(t.Context())
	svc.StopOperator()

	assert.Equal(t, int64(1), local.halts.Load(), "the in-process target is still halted")
}
