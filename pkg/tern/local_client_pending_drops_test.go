package tern

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

// The pending drops quarantine is opt-in, and the metadata key is how a server
// states that intent to the data plane. An embedder that builds a LocalConfig
// itself never states it, so the absent key must drop tables outright: a
// deployment that quarantines without reaping leaves tables on its targets that
// nothing will ever remove.
func TestPendingDropsDisabledRequiresAnExplicitOptIn(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name     string
		metadata map[string]string
		want     bool
	}{
		{name: "nil metadata", metadata: nil, want: true},
		{name: "key absent", metadata: map[string]string{"organization": "acme"}, want: true},
		{name: "explicitly disabled", metadata: map[string]string{"pending_drops": "false"}, want: true},
		{name: "unrecognized value", metadata: map[string]string{"pending_drops": "yes"}, want: true},
		{name: "explicitly enabled", metadata: map[string]string{"pending_drops": "true"}, want: false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, tt.want, pendingDropsDisabled(tt.metadata))
		})
	}
}
