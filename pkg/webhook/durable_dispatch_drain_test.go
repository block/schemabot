package webhook

import (
	"bytes"
	"log/slog"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

// A delivery that never returns must not hold the process open. Stopping the
// pool cancels it and waits, and a delivery still running when that wait is
// spent is abandoned: its inbox row stays claimed, goes stale, and is
// redelivered by the next process to run the pool — which only happens once
// this one has managed to exit.
func TestStopDurableWebhookDispatchAbandonsADeliveryThatIgnoresItsContext(t *testing.T) {
	h := newDurableDriverHandler(t, newScriptedWebhookEventStore(), nil, nil)
	h.durableWebhookPollInterval = time.Hour

	var logs bytes.Buffer
	h.logger = slog.New(slog.NewTextHandler(&logs, &slog.HandlerOptions{Level: slog.LevelDebug}))

	h.StartDurableWebhookDispatch(t.Context())

	// A delivery that observes neither its cancelled context nor the stop
	// channel. Releasing it at the end of the test keeps the goroutine from
	// outliving the run.
	stuck := make(chan struct{})
	t.Cleanup(func() { close(stuck) })
	h.durableWebhookWg.Go(func() { <-stuck })

	start := time.Now()
	h.StopDurableWebhookDispatch()
	elapsed := time.Since(start)

	assert.GreaterOrEqual(t, elapsed, durableWebhookDrainTimeout, "the delivery gets its full drain before being abandoned")
	assert.Less(t, elapsed, durableWebhookDrainTimeout+5*time.Second, "stopping returns on the drain rather than on the delivery")
	assert.Contains(t, logs.String(), "did not return within the shutdown drain")
}
