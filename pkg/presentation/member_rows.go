package presentation

import (
	"github.com/block/schemabot/pkg/routing"
	"github.com/block/schemabot/pkg/state"
)

// memberRows partitions ops into rollout members, each a list of the rows of
// its work in input order. A target's rows of a rollout run table by table,
// each stamped with its table step, are one member however many tables they
// run, so the rollup counts and labels targets rather than rows. Every other
// row is a member of its own: a row that runs its member's whole change
// carries no step, and that includes several members dividing one target's
// work and the sharded fan-out, which renders its rows as shards.
//
// Input order is the resolved order the claim's ordering gates read, so a
// member's rows are listed in the order its work is claimed. A table step
// marks where one table ends and the next begins; it is not a second order to
// sort a member's rows by.
func memberRows(ops []Operation) [][]int {
	var members [][]int
	memberOf := make(map[string]int, len(ops))
	for i, op := range ops {
		if op.RolloutStep == 0 || op.Target == "" {
			members = append(members, []int{i})
			continue
		}
		id := routing.ExecutionTarget{Deployment: op.Deployment, Target: op.Target}.MemberID()
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

// orderFoldedMembersByRow labels each member of a rollout run table by table
// that waits its turn by its lead row's place among every row, rather than its
// own place among the members. A target runs its next table only once the
// earlier rows allow it, and those can belong to targets the member order puts
// after it: orders-001 waiting on `docks` is held by orders-002's failure on
// `bikes`, though orders-001 is the first member. That holds for a target with
// one table too, when its table is a later step. Settled and active members
// read as their folded state already says. A row with no step is a member of
// its own (memberRows), sharded fan-out rows included, so its place among the
// members is its place among the rows and the member keeps its own label.
func orderFoldedMembersByRow(ops, members []Operation, names []string, memberOf []int, deployments []Deployment) {
	rowNames := make([]string, len(ops))
	for i := range ops {
		rowNames[i] = names[memberOf[i]]
	}
	for j := range deployments {
		d := &deployments[j]
		if ops[d.Row].RolloutStep == 0 || !waitsItsTurn(ops[d.Row].State, members[j].State) {
			continue
		}
		row := deriveDeployment(ops, rowNames, d.Row)
		d.set(row.Presentation, row.Label, row.Emoji, row.Open)
	}
}

// waitsItsTurn reports whether a folded member whose lead row is in rowState
// and whose rows fold to memberState is waiting on earlier rows: pending, or
// copied and parked for cutover, with the member reading the same.
func waitsItsTurn(rowState, memberState string) bool {
	if rowState != memberState {
		return false
	}
	return state.IsState(rowState, state.ApplyOperation.Pending, state.ApplyOperation.WaitingForCutover)
}

// foldMember is the one Operation a member's rows read as, and the index of
// its lead row. The lead row, the one most in need of an operator, speaks for
// the member's error and identifiers, and Deployment.Row points at it. Its
// state is the apply state over its rows: a target runs its tables one after
// another, so the rule that reads a single apply's tasks reads a target's
// tables, and a failure, retry or stop on any table is the target's. The
// rollout's on_failure policy governs other targets, never the failed
// target's own later tables, so it plays no part here.
func foldMember(ops []Operation, rows []int) (Operation, int) {
	lead := leadRow(ops, rows)
	if len(rows) == 1 {
		return ops[lead], lead
	}
	member := ops[lead]
	states := make([]string, len(rows))
	member.NeverStarted = true
	member.AlreadyConverged = true
	for k, i := range rows {
		states[k] = ops[i].State
		member.NeverStarted = member.NeverStarted && ops[i].NeverStarted
		member.AlreadyConverged = member.AlreadyConverged && ops[i].AlreadyConverged
	}
	member.State = state.DeriveApplyState(states)
	if member.State == state.ApplyOperation.Pending && parkedAtEarliestUnfinishedTable(ops, rows) {
		member.State = state.ApplyOperation.WaitingForCutover
	}
	return member, lead
}

// parkedAtEarliestUnfinishedTable reports whether the earliest of a member's
// rows that has not completed is parked for cutover. The apply rule reads a
// parked table beside queued ones as queued, since one apply's tables copy
// together. A target reaches its tables one after another, so its queued
// later tables are work it has not reached, and the parked table is where it
// stands: its cutover is what lets the target go on.
func parkedAtEarliestUnfinishedTable(ops []Operation, rows []int) bool {
	for _, i := range rows {
		if state.IsState(ops[i].State, state.ApplyOperation.Completed) {
			continue
		}
		return state.IsState(ops[i].State, state.ApplyOperation.WaitingForCutover)
	}
	return false
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
