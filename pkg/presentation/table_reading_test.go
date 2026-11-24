package presentation

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

// A sharded table's figures name the shards they cover only while some of
// its shards have not reported, counted against the plan's shard count when
// the plan recorded more shards than are listed.
func TestWithShardCoverage(t *testing.T) {
	cases := []struct {
		name                 string
		planned              int
		listed, reporting    int
		wantReporting, total int
	}{
		{name: "one shard listed", listed: 1, reporting: 1},
		{name: "every listed shard reporting", listed: 4, reporting: 4},
		{name: "no shard reporting yet", listed: 4},
		{name: "later shards not reporting", listed: 4, reporting: 1, wantReporting: 1, total: 4},
		{name: "plan spans more shards than listed", planned: 8, listed: 4, reporting: 4, wantReporting: 4, total: 8},
		{name: "more shards listed than planned", planned: 2, listed: 4, reporting: 3, wantReporting: 3, total: 4},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			f := TableFacts{PlannedShards: tc.planned}.WithShardCoverage(tc.listed, tc.reporting)
			assert.Equal(t, tc.wantReporting, f.ShardsReporting, "shards reporting")
			assert.Equal(t, tc.total, f.ShardsTotal, "shards total")
		})
	}
}
