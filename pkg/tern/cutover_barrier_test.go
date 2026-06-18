package tern

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/block/schemabot/pkg/storage"
)

func barrierOp() *storage.ApplyOperation {
	return &storage.ApplyOperation{CutoverPolicy: storage.CutoverPolicyBarrier}
}

func rollingOp() *storage.ApplyOperation {
	return &storage.ApplyOperation{CutoverPolicy: storage.CutoverPolicyRolling}
}

func TestShouldAutoDeferCutover(t *testing.T) {
	tests := []struct {
		name           string
		multiOperation bool
		op             *storage.ApplyOperation
		want           bool
	}{
		{"multi-op barrier parks", true, barrierOp(), true},
		{"single-op barrier does not park", false, barrierOp(), false},
		{"multi-op rolling does not park", true, rollingOp(), false},
		{"multi-op nil op does not park", true, nil, false},
		{"single-op rolling does not park", false, rollingOp(), false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, shouldAutoDeferCutover(tt.multiOperation, tt.op))
		})
	}
}

func TestShouldReleaseAtCutoverBarrier(t *testing.T) {
	tests := []struct {
		name           string
		manualDefer    bool
		multiOperation bool
		op             *storage.ApplyOperation
		want           bool
	}{
		{"multi-op barrier auto-releases", false, true, barrierOp(), true},
		{"manual defer holds the claim", true, true, barrierOp(), false},
		{"single-op barrier does not release", false, false, barrierOp(), false},
		{"multi-op rolling does not release", false, true, rollingOp(), false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			apply := &storage.Apply{}
			apply.SetOptions(storage.ApplyOptions{DeferCutover: tt.manualDefer})
			assert.Equal(t, tt.want, shouldReleaseAtCutoverBarrier(apply, tt.multiOperation, tt.op))
		})
	}
}

func TestEffectiveCopyDriveOptions(t *testing.T) {
	t.Run("multi-op barrier turns on defer cutover", func(t *testing.T) {
		apply := &storage.Apply{}
		opts := effectiveCopyDriveOptions(apply, true, barrierOp())
		assert.True(t, opts.DeferCutover)
	})

	t.Run("single-op barrier leaves defer cutover off", func(t *testing.T) {
		apply := &storage.Apply{}
		opts := effectiveCopyDriveOptions(apply, false, barrierOp())
		assert.False(t, opts.DeferCutover)
	})

	t.Run("manual defer cutover is preserved for non-barrier multi-op", func(t *testing.T) {
		apply := &storage.Apply{}
		apply.SetOptions(storage.ApplyOptions{DeferCutover: true})
		opts := effectiveCopyDriveOptions(apply, true, rollingOp())
		assert.True(t, opts.DeferCutover)
	})

	t.Run("does not mutate the apply's stored options", func(t *testing.T) {
		apply := &storage.Apply{}
		_ = effectiveCopyDriveOptions(apply, true, barrierOp())
		assert.False(t, apply.GetOptions().DeferCutover)
	})
}

// The grouped-apply gating reads DeferCutover from the effective options map, so
// a MySQL operation that must park at the barrier takes the atomic-cutover path
// even though the apply's stored options have defer_cutover unset.
func TestGroupedApplyHonoursEffectiveOptions(t *testing.T) {
	c := &LocalClient{}
	apply := &storage.Apply{DatabaseType: storage.DatabaseTypeMySQL}

	stored := apply.GetOptions().Map()
	assert.False(t, c.usesGroupedApply(apply, stored))
	assert.Equal(t, "grouped_engine_apply", groupedApplyMode(apply, stored))

	effective := effectiveCopyDriveOptions(apply, true, barrierOp()).Map()
	assert.True(t, c.usesGroupedApply(apply, effective))
	assert.Equal(t, "spirit_atomic_cutover", groupedApplyMode(apply, effective))
}
