package presentation

import "github.com/block/schemabot/pkg/state"

// MemberOutcomes counts how the members of a settled rollout ended, by the
// members the PR comment lists, so a message that counts them agrees with the
// comment beside it.
type MemberOutcomes struct {
	// Noun is what the PR comment calls the members.
	Noun Noun
	// Cancelled counts the members that ended cancelled with no row failed.
	Cancelled int
	// Failed counts the members with a failed row.
	Failed int
}

// SettledMemberOutcomes counts how the members of a settled rollout ended. ops
// are the rollout's work rows in resolved order, without its finalizers, which
// are not members.
//
// The members are the ones the PR comment lists: the shards of a sharded apply
// (IsShardedApply), each covering every table on one (namespace, shard), or
// the members Derive folds any other rollout's rows into, named the way
// MemberNoun names them.
//
// A member with a failed row counts as failed, even when its other rows were
// cancelled, because failed outranks cancelled for a member as it does for the
// apply. A member counts as cancelled when one of its rows ended cancelled and
// none failed. A table step that never started while a failed earlier step of
// its deployment held it for good is not counted as cancelled: nothing could
// ever have started it, so it is what the failure left behind, not work a
// cancel stopped.
func SettledMemberOutcomes(ops []Operation) MemberOutcomes {
	members, noun := rolloutMembers(ops)
	outcomes := MemberOutcomes{Noun: noun}
	for _, rows := range members {
		cancelled, failed := false, false
		for _, i := range rows {
			switch {
			case state.IsState(ops[i].State, state.ApplyOperation.Failed):
				failed = true
			case state.IsState(ops[i].State, state.ApplyOperation.Cancelled) && !heldByFailedEarlierStep(ops, i):
				cancelled = true
			}
		}
		switch {
		case failed:
			outcomes.Failed++
		case cancelled:
			outcomes.Cancelled++
		}
	}
	return outcomes
}

// rolloutMembers partitions ops into the members the PR comment lists, each a
// list of row indexes, and returns what the comment calls them.
func rolloutMembers(ops []Operation) ([][]int, Noun) {
	keyed := make([]KeyedOperation, len(ops))
	for i, op := range ops {
		keyed[i] = KeyedOperation{Deployment: op.Deployment, OperationKey: op.OperationKey}
	}
	if IsShardedApply(keyed) {
		return shardMembers(ops), ShardNoun
	}
	return memberRows(ops), MemberNoun(Derive(ops).Groups())
}

// shardMembers partitions the rows of a sharded apply by shard: every table on
// one (namespace, shard) is one shard, and a shard name that repeats across
// namespaces names two shards. A row without a shard key is not shard work, so
// it belongs to no shard.
func shardMembers(ops []Operation) [][]int {
	var members [][]int
	memberOf := make(map[string]int, len(ops))
	for i, op := range ops {
		namespace, shard, _, ok := state.ShardWorkKey(op.OperationKey)
		if !ok {
			continue
		}
		id := namespace + "/" + shard
		j, seen := memberOf[id]
		if !seen {
			j = len(members)
			memberOf[id] = j
			members = append(members, nil)
		}
		members[j] = append(members[j], i)
	}
	return members
}

// heldByFailedEarlierStep reports whether row i is a table step that never
// started because an earlier step of its deployment failed. A step starts only
// once every earlier step of its deployment has completed, so such a row could
// never have run, whatever else happened to the apply.
func heldByFailedEarlierStep(ops []Operation, i int) bool {
	op := ops[i]
	if op.RolloutStep <= 0 || !op.NeverStarted {
		return false
	}
	for _, earlier := range ops {
		if earlier.Deployment != op.Deployment || earlier.RolloutStep <= 0 || earlier.RolloutStep >= op.RolloutStep {
			continue
		}
		if state.IsState(earlier.State, state.ApplyOperation.Failed) {
			return true
		}
	}
	return false
}
