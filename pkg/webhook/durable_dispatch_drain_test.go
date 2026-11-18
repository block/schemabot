package webhook

import (
	"bytes"
	"log/slog"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/block/schemabot/pkg/storage"
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

// A drain that expires names the inbox rows it walked away from. A count alone
// tells an operator that deliveries were abandoned but not which ones, so the
// redelivery that lands on the next process cannot be tied back to the shutdown
// that caused it.
func TestStopDurableWebhookDispatchNamesTheDeliveriesItAbandons(t *testing.T) {
	h := newDurableDriverHandler(t, newScriptedWebhookEventStore(), nil, nil)
	h.durableWebhookPollInterval = time.Hour

	var logs bytes.Buffer
	h.logger = slog.New(slog.NewTextHandler(&logs, &slog.HandlerOptions{Level: slog.LevelDebug}))

	h.StartDurableWebhookDispatch(t.Context())

	release := h.registerDurableWebhookClaim(3, &storage.WebhookEvent{
		Provider:    "github",
		DeliveryID:  "delivery-abandoned",
		Event:       "pull_request",
		Action:      "synchronize",
		Repository:  "acme/orders",
		PullRequest: 41,
		HeadSHA:     "9f2c1ab",
		Attempts:    2,
	})
	t.Cleanup(release)

	stuck := make(chan struct{})
	t.Cleanup(func() { close(stuck) })
	h.durableWebhookWg.Go(func() { <-stuck })

	h.StopDurableWebhookDispatch()

	output := logs.String()
	assert.Contains(t, output, "abandoned_deliveries=1")
	assert.Contains(t, output, "abandoned a claimed webhook delivery whose driver did not return")
	assert.Contains(t, output, "delivery-abandoned")
	assert.Contains(t, output, "acme/orders")
}

// A delivery that finishes is not reported as abandoned: the claim it registered
// is dropped when its drive returns, so a later drain that expires for some
// other delivery does not name it too.
func TestDurableWebhookClaimsClearWhenTheirDriveReturns(t *testing.T) {
	h := newDurableDriverHandler(t, newScriptedWebhookEventStore(), nil, nil)

	release := h.registerDurableWebhookClaim(1, &storage.WebhookEvent{
		Provider:   "github",
		DeliveryID: "delivery-finished",
	})
	assert.Len(t, h.durableWebhookClaimsSnapshot(), 1)

	release()
	assert.Empty(t, h.durableWebhookClaimsSnapshot())
}
