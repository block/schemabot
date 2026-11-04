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

// A noun reads singular for one member and plural for any other count, bare
// as a label or after the count.
func TestNounLabelAndCount(t *testing.T) {
	for _, tc := range []struct {
		count          int
		label, counted string
	}{
		{count: 0, label: "targets", counted: "0 targets"},
		{count: 1, label: "target", counted: "1 target"},
		{count: 2, label: "targets", counted: "2 targets"},
	} {
		assert.Equal(t, tc.label, TargetNoun.Label(tc.count))
		assert.Equal(t, tc.counted, TargetNoun.Count(tc.count))
	}
}
