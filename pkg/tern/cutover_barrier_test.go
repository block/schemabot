package tern

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/block/schemabot/pkg/state"
	"github.com/block/schemabot/pkg/storage"
)

func barrierOp() *storage.ApplyOperation {
	return &storage.ApplyOperation{CutoverPolicy: storage.CutoverPolicyBarrier}
}

func rollingOp() *storage.ApplyOperation {
	return &storage.ApplyOperation{CutoverPolicy: storage.CutoverPolicyRolling}
}

func parallelOp() *storage.ApplyOperation {
	return &storage.ApplyOperation{CutoverPolicy: storage.CutoverPolicyParallel}
}

func TestShouldAutoDeferCutover(t *testing.T) {
	tests := []struct {
		name           string
		multiOperation bool
		op             *storage.ApplyOperation
		want           bool
	}{
		{"multi-op barrier parks", true, barrierOp(), true},
		{"multi-op parallel parks", true, parallelOp(), true},
		{"single-op barrier does not park", false, barrierOp(), false},
		{"single-op parallel does not park", false, parallelOp(), false},
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
		{"multi-op parallel auto-releases", false, true, parallelOp(), true},
		{"manual defer holds the claim", true, true, barrierOp(), false},
		{"manual defer holds the claim (parallel)", true, true, parallelOp(), false},
		{"single-op barrier does not release", false, false, barrierOp(), false},
		{"single-op parallel does not release", false, false, parallelOp(), false},
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

// The cutover drive may resume an operation parked at the barrier or already
// mid-cutover (stale-lease recovery), but never a copy-phase or terminal one.
func TestIsCutoverDriveState(t *testing.T) {
	tests := []struct {
		state string
		want  bool
	}{
		{state.Apply.WaitingForCutover, true},
		{state.Apply.CuttingOver, true},
		{state.Apply.RevertWindow, true},
		{state.Apply.Pending, false},
		{state.Apply.Running, false},
		{state.Apply.WaitingForDeploy, false},
		{state.Apply.Completed, false},
		{state.Apply.Failed, false},
		{state.Apply.Stopped, false},
	}
	for _, tt := range tests {
		t.Run(tt.state, func(t *testing.T) {
			assert.Equal(t, tt.want, isCutoverDriveState(tt.state))
		})
	}
}

// An ordered-cutover drive (forceCutoverResume) must load the parked engine
// checkpoint even though the barrier park deliberately leaves DeferCutover unset
// on the shared apply, while still requiring the apply to actually be parked.
func TestShouldInspectCutoverSignalForResume(t *testing.T) {
	parked := func(opts storage.ApplyOptions) *storage.Apply {
		return &storage.Apply{State: state.Apply.WaitingForCutover, Options: storage.MarshalApplyOptions(opts)}
	}

	tests := []struct {
		name               string
		apply              *storage.Apply
		forceCutoverResume bool
		want               bool
	}{
		{"manual defer recovery inspects without force", parked(storage.ApplyOptions{DeferCutover: true}), false, true},
		{"barrier park needs force to inspect", parked(storage.ApplyOptions{}), false, false},
		{"forced cutover drive inspects parked op", parked(storage.ApplyOptions{}), true, true},
		{"force ignored when not parked", &storage.Apply{State: state.Apply.Running}, true, false},
		{"nil apply never inspects", nil, true, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, shouldInspectCutoverSignalForResume(tt.apply, tt.forceCutoverResume))
		})
	}
}

// A pending cutover request waits for its member's turn only where the
// automatic cutover claim orders the swaps: an operation-scoped drive of a
// multi-operation apply under an ordered policy. Every other drive takes the
// request on apply-wide readiness, as it always has.
func TestTakesCutoverRequestInOrder(t *testing.T) {
	tests := []struct {
		name  string
		scope applyTaskScope
		want  bool
	}{
		{"multi-op barrier waits its turn", applyTaskScope{applyOperationID: 7, operation: barrierOp(), multiOperation: true}, true},
		{"multi-op parallel waits its turn", applyTaskScope{applyOperationID: 7, operation: parallelOp(), multiOperation: true}, true},
		{"multi-op rolling is unordered", applyTaskScope{applyOperationID: 7, operation: rollingOp(), multiOperation: true}, false},
		{"single-op barrier has no siblings", applyTaskScope{applyOperationID: 7, operation: barrierOp()}, false},
		{"whole-apply drive has no member", wholeApplyTaskScope(), false},
		{"operation not loaded", applyTaskScope{applyOperationID: 7, multiOperation: true}, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, tt.scope.takesCutoverRequestInOrder())
		})
	}
}
