package storage

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

// A log read for one target of a rollout resolves to that target's
// operations, stays unscoped when the apply runs on that target alone, and
// reports a target the apply never ran on rather than an empty set.
func TestLogOperationsOnTarget(t *testing.T) {
	rollout := []*ApplyOperation{
		{ID: 1, Target: "shop-001"},
		{ID: 2, Target: "shop-002"},
		{ID: 3, Target: "shop-002"},
	}

	ids, ok := LogOperationsOnTarget(rollout, "shop-002")
	assert.True(t, ok)
	assert.Equal(t, []int64{2, 3}, ids)

	ids, ok = LogOperationsOnTarget([]*ApplyOperation{{ID: 1, Target: "shop-001"}}, "shop-001")
	assert.True(t, ok)
	assert.Nil(t, ids, "a single-target apply needs no scoping")

	ids, ok = LogOperationsOnTarget(rollout, "shop-003")
	assert.False(t, ok)
	assert.Nil(t, ids)
}
