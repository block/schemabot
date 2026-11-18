package templates

import (
	"strings"
	"testing"

	"github.com/block/schemabot/pkg/state"
	"github.com/stretchr/testify/assert"
)

// A comment offers the support channel only when its render function declared
// it, via an invisible HTML-comment marker. The webhook layer keys the footer
// on the marker, never on human-visible copy.
func TestOffersSupportChannel(t *testing.T) {
	assert.False(t, OffersSupportChannel("## Schema Change Plan\n\nplan body\n"))
	assert.True(t, OffersSupportChannel(offerSupportChannel("## Apply Not Found\n\nbody\n")))
}

// The marker counts only as a standalone line, the way the render helpers
// emit it. User-controlled content that happens to contain the marker text
// mid-line neither triggers the footer nor suppresses a render function's
// own declaration.
func TestSupportChannelMarkerRequiresStandaloneLine(t *testing.T) {
	spoofed := "## Schema Change Plan\n\nALTER TABLE `t` COMMENT 'x <!-- schemabot:offer-support-channel --> y';\n"

	assert.False(t, OffersSupportChannel(spoofed))
	assert.True(t, OffersSupportChannel(offerSupportChannel(spoofed)))
}

func TestOfferSupportChannelIdempotent(t *testing.T) {
	once := offerSupportChannel("## Rollback Blocked\n\nbody\n")
	twice := offerSupportChannel(once)

	assert.Equal(t, once, twice)
	assert.Equal(t, 1, strings.Count(twice, supportChannelOfferMarker))
}

func TestWriteSupportChannelOfferDedupes(t *testing.T) {
	var sb strings.Builder
	sb.WriteString("## ❌ Schema Change Failed\n\n")
	writeSupportChannelOffer(&sb)
	writeSupportChannelOffer(&sb)

	assert.Equal(t, 1, strings.Count(sb.String(), supportChannelOfferMarker))
}

// The apply refusal for unsafe changes offers support; the plan comment's
// advisory unsafe-change summary is informational, appears on routine plans,
// and does not.
func TestUnsafeChangesSupportOffer(t *testing.T) {
	data := PlanCommentData{
		Database:    "orders",
		SchemaName:  "orders",
		Environment: "staging",
		IsMySQL:     true,
		Changes: []KeyspaceChangeData{{
			Keyspace:   "orders",
			Statements: []string{"ALTER TABLE `users` DROP INDEX `idx_email`;"},
		}},
		HasUnsafeChanges: true,
		UnsafeChanges: []UnsafeChangeData{
			{Table: "users", Reason: "DROP INDEX idx_email"},
		},
	}

	assert.True(t, OffersSupportChannel(RenderUnsafeChangesBlocked(data)))
	assert.False(t, OffersSupportChannel(RenderPlanComment(data)))
}

// A failed apply offers support; an in-progress apply does not.
func TestApplyStatusSupportOffer(t *testing.T) {
	failed := RenderApplyStatusComment(ApplyStatusCommentData{
		Database: "orders",
		State:    state.Apply.Failed,
	})
	running := RenderApplyStatusComment(ApplyStatusCommentData{
		Database: "orders",
		State:    state.Apply.Running,
	})

	assert.True(t, OffersSupportChannel(failed))
	assert.False(t, OffersSupportChannel(running))
}
