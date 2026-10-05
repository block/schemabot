package state

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

// rolloutShard builds payments-a's work on one shard of orders for target.
func rolloutShard(target, shard, opState string) RolloutOperation {
	return RolloutOperation{
		Deployment:   "payments-a",
		OperationKey: target + OperationKeyDelimiter + "orders" + OperationKeyDelimiter + shard + OperationKeyDelimiter + "orders",
		Work:         true,
		State:        opState,
	}
}

// rolloutFinalizer builds payments-a's orders finalizer for target.
func rolloutFinalizer(target, opState string) RolloutOperation {
	return RolloutOperation{
		Deployment:   "payments-a",
		OperationKey: target + OperationKeyDelimiter + "orders" + OperationKeyDelimiter + "group_finalizer",
		Finalizer:    true,
		State:        opState,
	}
}

// TestRolloutChildren_Orphaned verifies which finalizers every rollout
// projection treats as dead. payments-001's orders finalizer is orphaned once
// one of its own shards has failed, whether it is still pending or a stop
// moved it to stopped. It is not orphaned while its shards are only parked,
// once it has started, or when the failed shard belongs to payments-002, and
// a work row is never an orphan.
func TestRolloutChildren_Orphaned(t *testing.T) {
	cases := []struct {
		name      string
		candidate RolloutOperation
		siblings  []RolloutOperation
		want      bool
	}{
		{
			name:      "pending finalizer behind its own failed shard",
			candidate: rolloutFinalizer("payments-001", ApplyOperation.Pending),
			siblings:  []RolloutOperation{rolloutShard("payments-001", "-80", ApplyOperation.Failed), rolloutShard("payments-001", "80-", ApplyOperation.Completed)},
			want:      true,
		},
		{
			name:      "stopped finalizer behind its own failed shard",
			candidate: rolloutFinalizer("payments-001", ApplyOperation.Stopped),
			siblings:  []RolloutOperation{rolloutShard("payments-001", "-80", ApplyOperation.Failed)},
			want:      true,
		},
		{
			name:      "pending finalizer behind parked shards",
			candidate: rolloutFinalizer("payments-001", ApplyOperation.Pending),
			siblings:  []RolloutOperation{rolloutShard("payments-001", "-80", ApplyOperation.WaitingForCutover)},
		},
		{
			name:      "running finalizer",
			candidate: rolloutFinalizer("payments-001", ApplyOperation.Running),
			siblings:  []RolloutOperation{rolloutShard("payments-001", "-80", ApplyOperation.Failed)},
		},
		{
			name:      "failed shard of another target",
			candidate: rolloutFinalizer("payments-001", ApplyOperation.Pending),
			siblings:  []RolloutOperation{rolloutShard("payments-002", "-80", ApplyOperation.Failed)},
		},
		{
			name:      "failed shard of another deployment",
			candidate: rolloutFinalizer("payments-001", ApplyOperation.Pending),
			siblings: []RolloutOperation{func() RolloutOperation {
				op := rolloutShard("payments-001", "-80", ApplyOperation.Failed)
				op.Deployment = "payments-b"
				return op
			}()},
		},
		{
			name:      "work row",
			candidate: rolloutShard("payments-001", "80-", ApplyOperation.Pending),
			siblings:  []RolloutOperation{rolloutShard("payments-001", "-80", ApplyOperation.Failed)},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			ops := append([]RolloutOperation{tc.candidate}, tc.siblings...)
			assert.Equal(t, tc.want, RolloutChildren(ops)[0].Orphaned)
		})
	}
}

// TestDeriveRolloutApplyState_NeverStartedStopped verifies that a row a stop
// caught before any driver claimed it counts as pending. region-a's work
// failed; region-b's row is stopped. When region-b never started, halt
// settles the rollout failed and an unreleased pause settles it paused, as
// they would with region-b still pending; continue keeps it running_degraded,
// since start resumes region-b past the failure. When region-b had started
// before the stop, halt keeps the rollout running_degraded until it settles.
func TestDeriveRolloutApplyState_NeverStartedStopped(t *testing.T) {
	regionA := func(policy func(*RolloutOperation)) RolloutOperation {
		op := RolloutOperation{Deployment: "region-a", Work: true, State: ApplyOperation.Failed}
		policy(&op)
		return op
	}
	regionB := func(policy func(*RolloutOperation), finalizer, neverStarted bool) RolloutOperation {
		op := RolloutOperation{Deployment: "region-b", Work: !finalizer, Finalizer: finalizer, State: ApplyOperation.Stopped, NeverStarted: neverStarted}
		policy(&op)
		return op
	}
	halt := func(*RolloutOperation) {}
	cont := func(op *RolloutOperation) { op.ContinueOnFailure = true }
	pause := func(op *RolloutOperation) { op.PauseOnFailure = true }

	cases := []struct {
		name string
		ops  []RolloutOperation
		want string
	}{
		{name: "halt, work never started", ops: []RolloutOperation{regionA(halt), regionB(halt, false, true)}, want: Apply.Failed},
		{name: "halt, finalizer never started", ops: []RolloutOperation{regionA(halt), regionB(halt, true, true)}, want: Apply.Failed},
		{name: "halt, work had started", ops: []RolloutOperation{regionA(halt), regionB(halt, false, false)}, want: Apply.RunningDegraded},
		{name: "pause, work never started", ops: []RolloutOperation{regionA(pause), regionB(pause, false, true)}, want: Apply.Paused},
		{name: "continue, work never started", ops: []RolloutOperation{regionA(cont), regionB(cont, false, true)}, want: Apply.RunningDegraded},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.want, DeriveRolloutApplyState(RolloutChildren(tc.ops)))
		})
	}

	// NeverStarted describes a row that has not moved past pending or stopped;
	// on a row a driver is running it is ignored, so a stale flag cannot
	// settle a rollout over live work.
	running := regionB(halt, false, true)
	running.State = ApplyOperation.Running
	assert.Equal(t, Apply.RunningDegraded, DeriveRolloutApplyState(RolloutChildren([]RolloutOperation{regionA(halt), running})))
}

// TestRolloutChildren_FinalizerScopedToItsNamespace verifies that a
// namespace's finalizer is orphaned only by its own namespace's failed work,
// in both key shapes. Target orders-001 of a targets list runs ns_0 and ns_1:
// when ns_0's shard fails, ns_0's finalizer is dead and ns_1's is still owed,
// and the same holds the other way round. A single-target apply keys the same
// rows without the target and reaches the same answer.
func TestRolloutChildren_FinalizerScopedToItsNamespace(t *testing.T) {
	for _, shape := range []struct {
		name   string
		prefix string
	}{
		{name: "targets list", prefix: "orders-001" + OperationKeyDelimiter},
		{name: "single target", prefix: ""},
	} {
		work := func(namespace, opState string) RolloutOperation {
			return RolloutOperation{
				Deployment:   "orders",
				OperationKey: shape.prefix + namespace + OperationKeyDelimiter + "-80" + OperationKeyDelimiter + "orders",
				Work:         true,
				State:        opState,
			}
		}
		finalizer := func(namespace string) RolloutOperation {
			return RolloutOperation{
				Deployment:   "orders",
				OperationKey: shape.prefix + namespace + OperationKeyDelimiter + GroupFinalizerKeySegment,
				Finalizer:    true,
				State:        ApplyOperation.Pending,
			}
		}
		for _, failing := range []string{"ns_0", "ns_1"} {
			succeeding := "ns_1"
			if failing == "ns_1" {
				succeeding = "ns_0"
			}
			t.Run(shape.name+"/"+failing+" fails", func(t *testing.T) {
				ops := []RolloutOperation{
					work(failing, ApplyOperation.Failed),
					work(succeeding, ApplyOperation.Completed),
					finalizer(failing),
					finalizer(succeeding),
				}
				children := RolloutChildren(ops)
				assert.True(t, children[2].Orphaned, "%s's finalizer is dead behind its own failed shard", failing)
				assert.False(t, children[3].Orphaned, "%s's finalizer is still owed: its own work completed", succeeding)
			})
		}
	}
}

// TestFinalizerFinalizesWork verifies which work a finalizer finalizes in
// each key shape: the work under its own namespace, and under its own target
// when a targets list qualifies the keys.
func TestFinalizerFinalizesWork(t *testing.T) {
	cases := []struct {
		name      string
		finalizer string
		work      string
		want      bool
	}{
		{name: "single target, own namespace", finalizer: "ns_0/group_finalizer", work: "ns_0/-80/orders", want: true},
		{name: "single target, other namespace", finalizer: "ns_0/group_finalizer", work: "ns_1/-80/orders"},
		{name: "single target, namespace sharing a prefix", finalizer: "ns_0/group_finalizer", work: "ns_01/-80/orders"},
		{name: "targets list, own target and namespace", finalizer: "orders-001/ns_0/group_finalizer", work: "orders-001/ns_0/-80/orders", want: true},
		{name: "targets list, other namespace on the same target", finalizer: "orders-001/ns_0/group_finalizer", work: "orders-001/ns_1/-80/orders"},
		{name: "targets list, same namespace on another target", finalizer: "orders-001/ns_0/group_finalizer", work: "orders-002/ns_0/-80/orders"},
		{name: "deployment-scoped finalizer", finalizer: "group_finalizer", work: "group_finalizer/-80/orders"},
		{name: "work keyed by the scope itself", finalizer: "ns_0/group_finalizer", work: "ns_0", want: true},
		{name: "target-scoped finalizer, other target", finalizer: "orders-001/group_finalizer", work: "orders-002"},
		{name: "not a finalizer key", finalizer: "ns_0/-80/orders", work: "ns_0/-80/orders"},
		{name: "namespace differing only by case", finalizer: "orders/group_finalizer", work: "Orders/-80/orders"},
		{name: "namespace differing only by accent", finalizer: "cafe/group_finalizer", work: "café/-80/orders"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.want, FinalizerFinalizesWork(tc.finalizer, tc.work))
		})
	}
}
