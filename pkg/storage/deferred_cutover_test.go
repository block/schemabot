package storage

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/block/schemabot/pkg/state"
)

// Settled work and finalizers never wait at a cutover, so they do not widen
// the apply.
func TestDeferredCutoverHeldAtOnceCountsOnlyWorkStillToDrive(t *testing.T) {
	ops := make([]*ApplyOperation, 0, 4)
	for _, opState := range []string{state.ApplyOperation.Completed, state.ApplyOperation.Pending, state.ApplyOperation.Pending} {
		ops = append(ops, &ApplyOperation{OperationKind: ApplyOperationKindWork, State: opState})
	}
	ops = append(ops, &ApplyOperation{OperationKind: ApplyOperationKindGroupFinalizer, State: state.ApplyOperation.Pending})
	assert.Equal(t, 2, DeferredCutoverHeldAtOnce(ops, 0))
}

// A deferred apply holds at its cutover only the operations the claim lets run
// together (ST-10): one member at a time under rolling, every deployment at
// once under barrier and parallel, and never more than max_drivers_per_apply.
func TestDeferredCutoverHeldAtOnceCountsOperationsHeldAtOnce(t *testing.T) {
	shards := func(policy, deployment string, count int) []*ApplyOperation {
		ops := make([]*ApplyOperation, 0, count)
		for range count {
			ops = append(ops, &ApplyOperation{
				OperationKind: ApplyOperationKindWork, State: state.ApplyOperation.Pending,
				CutoverPolicy: policy, Deployment: deployment, Target: deployment,
			})
		}
		return ops
	}
	for _, tc := range []struct {
		name               string
		ops                []*ApplyOperation
		maxDriversPerApply int
		want               int
	}{
		{
			name: "rolling holds one deployment at a time",
			ops:  append(append(shards(CutoverPolicyRolling, "eu", 4), shards(CutoverPolicyRolling, "us", 2)...), shards(CutoverPolicyRolling, "ap", 3)...),
			want: 4,
		},
		{
			name: "barrier holds every deployment together",
			ops:  append(shards(CutoverPolicyBarrier, "eu", 4), shards(CutoverPolicyBarrier, "us", 2)...),
			want: 6,
		},
		{
			name: "parallel holds every deployment together",
			ops:  append(shards(CutoverPolicyParallel, "eu", 4), shards(CutoverPolicyParallel, "us", 2)...),
			want: 6,
		},
		{
			name:               "a fan-out wider than max_drivers_per_apply holds the cap",
			ops:                shards("", "eu", 128),
			maxDriversPerApply: 2,
			want:               2,
		},
		{
			name:               "barrier wider than max_drivers_per_apply holds the cap",
			ops:                append(shards(CutoverPolicyBarrier, "eu", 4), shards(CutoverPolicyBarrier, "us", 2)...),
			maxDriversPerApply: 5,
			want:               5,
		},
		{
			name:               "a cap above the width leaves it",
			ops:                shards("", "eu", 3),
			maxDriversPerApply: 8,
			want:               3,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.want, DeferredCutoverHeldAtOnce(tc.ops, tc.maxDriversPerApply))
		})
	}
}
