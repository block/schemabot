package storage

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

// A log read for one target of a rollout resolves to that target's
// operations, names them even when the apply runs on that target alone, and
// finds none for a target the apply never ran on.
func TestLogOperationsOnTarget(t *testing.T) {
	rollout := []*ApplyOperation{
		{ID: 1, Target: "shop-001"},
		{ID: 2, Target: "shop-002"},
		{ID: 3, Target: "shop-002"},
	}

	assert.Equal(t, []int64{2, 3}, LogOperationsOnTarget(rollout, "shop-002"))
	assert.Equal(t, []int64{1}, LogOperationsOnTarget([]*ApplyOperation{{ID: 1, Target: "shop-001"}}, "shop-001"),
		"a sibling can join between listing and reading, so the only target still names its operations")
	assert.Nil(t, LogOperationsOnTarget(rollout, "shop-003"))
	assert.Equal(t, []string{"shop-001", "shop-002"}, DistinctOperationTargets(append(rollout, &ApplyOperation{ID: 4})))
}
