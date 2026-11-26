package presentation

import (
	"fmt"

	"github.com/block/schemabot/pkg/state"
)

// WithLimit ends a rollout's progress phrase with how many of its operations
// SchemaBot runs at once: "32 running, 96 queued (32 at a time)". The limit
// is what keeps a queue behind the running members waiting, so stating it
// next to the counts tells the reader the queue is waiting its turn and has
// not stalled. The phrase is returned unchanged when QueueLimitPhrase is
// empty.
func WithLimit(phrase string, units, operations, competing, limit int) string {
	limitPhrase := QueueLimitPhrase(units, operations, competing, limit)
	if limitPhrase == "" {
		return phrase
	}
	return fmt.Sprintf("%s (%s)", phrase, limitPhrase)
}

// LimitPhrase states how many of a planned rollout's operations SchemaBot
// runs at once, "32 at a time", or "" when the limit cannot hold any
// operation back: no limit is known (limit is zero), or the rollout has no
// more operations than the limit.
//
// units is how many members the rollout counts (shards or targets), and
// operations is how many operations the limit applies to. A shard with
// several tables to change runs one operation per table, so when operations
// outnumber units the limit is stated in table changes rather than in units.
func LimitPhrase(units, operations, limit int) string {
	return QueueLimitPhrase(units, operations, operations, limit)
}

// QueueLimitPhrase is LimitPhrase for a rollout under way, where competing
// of its unfinished operations hold a slot or are queued for one. The limit
// holds work back only while more operations compete for a slot than it
// allows at once, so the phrase is "" once the rollout's remaining work fits
// within the limit, however many operations it ran before. Work that waits on
// rollout order, a failure, or an operator does not compete, since the limit
// is not what holds it back.
func QueueLimitPhrase(units, operations, competing, limit int) string {
	if limit <= 0 || competing <= limit {
		return ""
	}
	if operations > units {
		return fmt.Sprintf("%d table changes at a time", limit)
	}
	return fmt.Sprintf("%d at a time", limit)
}

// OperationUnfinished reports whether an operation in s still has work ahead
// of it, and so either holds one of the apply's slots or waits for one. s may
// be an operation state or, as the PR comment reads a shard's table change,
// the state of the task behind it.
func OperationUnfinished(s string) bool {
	return !state.IsApplyOperationTerminal(s) && !state.IsTerminalTaskState(s)
}

// CompetesForSlot reports whether a member read as p holds one of the
// apply's operation slots or is queued for one, so the limit is what decides
// when it runs. A member waiting for an earlier member, halted, or paused
// behind a failure waits on the rollout's order or on an operator instead,
// and the limit holds nothing back from it. That includes a copy parked at
// its cutover behind an earlier member: the barrier releases its claim, and a
// manual deferred cutover is refused at creation unless every operation can
// wait at once, so its parked copies never exceed the limit.
func (p PresentationState) CompetesForSlot() bool {
	switch p {
	case StateRunningCopy, StateReadyForCutoverNext, StateCuttingOver,
		StateRetrying, StateResuming, StateQueuedNext:
		return true
	default:
		return false
	}
}

// CompetingShardOperations counts the unfinished operations of the shards
// whose derived status competes for a slot (CompetesForSlot). statuses is
// DeriveShards over work; a shard held behind another member or a failure
// waits on that instead of on the limit, so its operations do not count.
func CompetingShardOperations(work []ShardWork, statuses []ShardStatus) int {
	type keyspaceShard struct{ keyspace, shard string }
	competes := make(map[keyspaceShard]bool, len(statuses))
	for _, s := range statuses {
		competes[keyspaceShard{s.Keyspace, s.Shard}] = s.Presentation.CompetesForSlot()
	}
	competing := 0
	for _, sw := range work {
		if !competes[keyspaceShard{sw.Keyspace, sw.Shard}] {
			continue
		}
		for _, op := range sw.Operations {
			if OperationUnfinished(op.State) {
				competing++
			}
		}
	}
	return competing
}

// LimitPhrase is QueueLimitPhrase for the rollout's members that get the
// change, each of which runs one operation, while the rollout runs. Only the
// members that compete for a slot count as unfinished, so a rolling rollout
// whose later members wait for the earlier ones, or a paused one, never reads
// as held back by the limit. It is "" once the apply is terminal, when
// nothing is left to wait its turn.
func (a Apply) LimitPhrase() string {
	if state.IsTerminalApplyState(a.State) {
		return ""
	}
	changing, competing := 0, 0
	for _, d := range a.Deployments {
		if d.AlreadyApplied() {
			continue
		}
		changing++
		if d.Presentation.CompetesForSlot() {
			competing++
		}
	}
	return QueueLimitPhrase(changing, changing, competing, a.OperationsAtOnce)
}

// CountsWithLimit ends phrase, which counts the rollout's members, with
// Apply.LimitPhrase, the way WithLimit does.
func (a Apply) CountsWithLimit(phrase string) string {
	if limitPhrase := a.LimitPhrase(); limitPhrase != "" {
		return fmt.Sprintf("%s (%s)", phrase, limitPhrase)
	}
	return phrase
}
