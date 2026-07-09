package commands

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/block/schemabot/pkg/apitypes"
)

// The classification core: a PR with a failed check-creating delivery in the
// crawl is redriveable (grouped under its App); everything else synthesizes.
// A crawl that stopped early marks the search incomplete so the report can
// say why some PRs synthesize instead of redriving.
func TestIndexRedriveableDeliveries(t *testing.T) {
	redriveable, complete := indexRedriveableDeliveries(&apitypes.WebhookRedriveResponse{
		Results: []apitypes.WebhookRedriveResult{
			{
				AppName:            "production",
				ReachedWindowStart: true,
				Selected: []apitypes.WebhookRedriveSelection{
					{ID: 11, PR: 5},
					{ID: 12, PR: 5},
					{ID: 13, PR: 9},
					{ID: 14, PR: 0},
				},
			},
		},
	})

	require.True(t, complete)
	require.Len(t, redriveable, 2)
	assert.Equal(t, redriveableDeliveries{app: "production", ids: []int64{11, 12}}, redriveable[5])
	assert.Equal(t, redriveableDeliveries{app: "production", ids: []int64{13}}, redriveable[9])

	_, complete = indexRedriveableDeliveries(&apitypes.WebhookRedriveResponse{
		Results: []apitypes.WebhookRedriveResult{
			{AppName: "production", NextCursor: "c1"},
		},
	})
	assert.False(t, complete, "a crawl stopped mid-window cannot prove which PRs are redriveable")

	_, complete = indexRedriveableDeliveries(&apitypes.WebhookRedriveResponse{
		Results: []apitypes.WebhookRedriveResult{
			{AppName: "production", HistoryExhausted: true},
		},
	})
	assert.True(t, complete, "exhausted history covered everything GitHub retains")
}
