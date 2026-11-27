package templates

import (
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

// The requester wrote the reason, so a mention in it notifies nobody.
func TestRenderFreezeOverrideApprovedDefusesMentionsInTheReason(t *testing.T) {
	body := RenderFreezeOverrideApproved(FreezeOverrideApprovedData{
		ID: "ovr-1a2b3c4d5e6f7a8b", Approver: "dave", Action: "apply", Database: "orders", Environment: "production",
		RequestedBy: "carol", Reason: "SEV-1234 ping @acme/db-admins", ExpiresAt: time.Date(2026, 11, 22, 10, 0, 0, 0, time.UTC),
	})
	assert.True(t, strings.HasPrefix(body, "✅ @dave approved `ovr-1a2b3c4d5e6f7a8b` for apply on `orders` in production. Requested by @carol: "), body)
	assert.Contains(t, body, "It expires at 2026-11-22 10:00:00 UTC. @carol can now re-run the command.")
	mapOutsideCodeSpans(body, false, func(prose string) string {
		assert.NotContains(t, prose, "@acme", "a mention in the reason must not notify")
		return prose
	})
}
