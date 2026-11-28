package storage

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

// Every webhook inbox state is either terminal, so retention may delete it and
// Redeliver reopens it, or still owned by a driver or a sweep. A new state has
// to be placed on one side here before CI passes, so retention and the reopen
// statement never miss it.
func TestWebhookEventStatesAreTerminalOrInFlight(t *testing.T) {
	inFlight := []string{WebhookEventPending, WebhookEventProcessing, WebhookEventFailedRetryable}
	assert.ElementsMatch(t, WebhookEventStatesAll, append(append([]string{}, WebhookEventStatesTerminal...), inFlight...))
}
