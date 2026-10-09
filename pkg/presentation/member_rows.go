package presentation

import (
	"github.com/block/schemabot/pkg/routing"
	"github.com/block/schemabot/pkg/state"
)

// memberRows partitions ops into rollout members, each a list of the rows of
// its work in input order. A target of a deployment that addresses several
// targets is one member however many rows its work spans, one per table when
// the rollout runs table by table, so the rollup counts and labels targets
// rather than rows. Every other row is a member of its own: a deployment that
// addresses one target, or several members dividing one target's work, keeps
// one member per row.
//
// The sharded fan-out is left row by row. It keys each row by shard and
// table, and its group finalizers publish per namespace, so its rows are
// rendered as shards rather than folded into targets.
func memberRows(ops []Operation) [][]int {
	if !foldsTargetRows(ops) {
		return eachRow(len(ops))
	}
	targets := make([]routing.ExecutionTarget, len(ops))
	for i, op := range ops {
		targets[i] = routing.ExecutionTarget{Deployment: op.Deployment, Target: op.Target}
	}
	multiTarget := routing.MultiTargetDeployments(targets)
	var members [][]int
	memberOf := make(map[string]int, len(ops))
	for i, op := range ops {
		if !multiTarget[op.Deployment] || op.Target == "" {
			members = append(members, []int{i})
			continue
		}
		id := targets[i].MemberID()
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

// foldsTargetRows reports whether ops fold by target: no row is a group
// finalizer, which only the sharded fan-out creates.
func foldsTargetRows(ops []Operation) bool {
	for _, op := range ops {
		if op.Finalizer {
			return false
		}
	}
	return true
}

func eachRow(n int) [][]int {
	members := make([][]int, n)
	for i := range n {
		members[i] = []int{i}
	}
	return members
}

// foldMember is the one Operation a member's rows read as. The lead row, the
// one most in need of an operator, speaks for the member's error and
// identifiers. Its state is the apply state over its rows: a target runs its
// tables one after another, so the rule that reads a single apply's tasks
// reads a target's tables, and a failure, retry or stop on any table is the
// target's. The rollout's on_failure policy governs other targets, never the
// failed target's own later tables, so it plays no part here.
func foldMember(ops []Operation, rows []int) Operation {
	lead := ops[leadRow(ops, rows)]
	if len(rows) == 1 {
		return lead
	}
	member := lead
	states := make([]string, len(rows))
	member.NeverStarted = true
	member.AlreadyConverged = true
	for k, i := range rows {
		states[k] = ops[i].State
		member.NeverStarted = member.NeverStarted && ops[i].NeverStarted
		member.AlreadyConverged = member.AlreadyConverged && ops[i].AlreadyConverged
	}
	member.State = state.DeriveApplyState(states)
	return member
}

// leadRow is the row among rows most in need of an operator: a failure, then
// a retry, a stop, a cutover, work under way, work still queued, and settled
// work last, the earliest row first among equals.
func leadRow(ops []Operation, rows []int) int {
	lead := rows[0]
	for _, i := range rows[1:] {
		if rowLeadRank(ops[i].State) < rowLeadRank(ops[lead].State) {
			lead = i
		}
	}
	return lead
}

// rowLeadRank orders row states for leadRow. An active state that is not
// named ranks with work under way.
func rowLeadRank(s string) int {
	switch {
	case state.IsState(s, state.ApplyOperation.Failed):
		return 0
	case state.IsState(s, state.ApplyOperation.FailedRetryable):
		return 1
	case state.IsState(s, state.ApplyOperation.Stopped):
		return 2
	case state.IsState(s, state.ApplyOperation.WaitingForCutover, state.ApplyOperation.CuttingOver):
		return 3
	case state.IsState(s, state.ApplyOperation.Pending):
		return 5
	case state.IsState(s, state.ApplyOperation.Cancelled, state.ApplyOperation.Reverted):
		return 6
	case state.IsState(s, state.ApplyOperation.Completed):
		return 7
	}
	return 4
}
