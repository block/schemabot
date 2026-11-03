package tern

import (
	"log/slog"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/block/schemabot/pkg/storage"
)

// A drive hands a deferred apply back after delivering its pending request only
// when it runs for a driver that skips deferred work. Every other drive, and any
// drive of an apply that does not defer its cutover, carries on.
func TestHandBackAfterControlDelivery(t *testing.T) {
	deferred := &storage.Apply{ApplyIdentifier: "apply-deferred", Options: []byte(`{"defer_cutover":true}`)}
	immediate := &storage.Apply{ApplyIdentifier: "apply-immediate", Options: []byte(`{}`)}
	marked := WithControlDeliveryOnlyForDeferredCutover(t.Context())

	assert.True(t, handBackAfterControlDelivery(marked, deferred, slog.Default()),
		"a delivery-only drive of a deferred apply hands it back")
	assert.False(t, handBackAfterControlDelivery(marked, immediate, slog.Default()),
		"a delivery-only drive of an apply that does not defer its cutover drives it")
	assert.False(t, handBackAfterControlDelivery(t.Context(), deferred, slog.Default()),
		"a drive for a driver that runs deferred work drives the deferred apply")
}
