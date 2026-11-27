package presentation

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/block/schemabot/pkg/apitypes"
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

// A step waiting outside the lock queue for another session's lock names
// that session and how long the step has waited, instead of reading as a
// statement that is running.
func TestReadInFlightTableNamesTheLockHolderAStepWaitsOn(t *testing.T) {
	r := ReadInFlightTable(TableFacts{Status: "running", ChangeType: "alter", BuildWork: apitypes.BuildWork{
		HolderWait: apitypes.LockHolderWait{PID: 9433, Table: "public.t", Waited: 7944 * time.Millisecond, Limit: 30 * time.Second},
	}})
	assert.Equal(t, "⏳ Waiting for session 9433 to release its lock (7s of 30s)", r.Headline())
	assert.Equal(t, []TableDetail{{Text: "Reads and writes continue. The step runs once session 9433 commits or rolls back, or when the wait runs out.", Note: true}}, r.Details)
}
