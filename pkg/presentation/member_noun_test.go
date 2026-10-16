package presentation

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

// A coverage phrase reads "all" when it covers every member, "N of M" for a
// subset, and a bare count, singular for one member, when the total is
// unknown.
func TestCoveragePhrase(t *testing.T) {
	for _, tc := range []struct {
		count, total int
		want         string
	}{
		{count: 32, total: 32, want: "all 32 shards"},
		{count: 12, total: 32, want: "12 of 32 shards"},
		{count: 3, total: 0, want: "3 shards"},
		{count: 1, total: 0, want: "1 shard"},
	} {
		assert.Equal(t, tc.want, CoveragePhrase(ShardNoun, tc.count, tc.total))
	}
}
