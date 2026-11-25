package presentation

import (
	"fmt"

	"github.com/block/schemabot/pkg/state"
)

// WithLimit ends a rollout's progress phrase with how many of its operations
// SchemaBot runs at once: "32 running, 96 queued (32 at a time)". The limit
// is what keeps a queue behind the running members waiting, so stating it
// next to the counts tells the reader the queue is waiting its turn and has
// not stalled. The phrase is returned unchanged when LimitPhrase is empty.
func WithLimit(phrase string, units, operations, limit int) string {
	limitPhrase := LimitPhrase(units, operations, limit)
	if limitPhrase == "" {
		return phrase
	}
	return fmt.Sprintf("%s (%s)", phrase, limitPhrase)
}

// LimitPhrase states how many of a rollout's operations SchemaBot runs at
// once, "32 at a time", or "" when the limit cannot hold any operation back:
// no limit is known (limit is zero), or the rollout has no more operations
// than the limit.
//
// units is how many members the rollout counts (shards or targets), and
// operations is how many operations the limit applies to. A shard with
// several tables to change runs one operation per table, so when operations
// outnumber units the limit is stated in table changes rather than in units.
func LimitPhrase(units, operations, limit int) string {
	if limit <= 0 || operations <= limit {
		return ""
	}
	if operations > units {
		return fmt.Sprintf("%d table changes at a time", limit)
	}
	return fmt.Sprintf("%d at a time", limit)
}

// LimitPhrase is LimitPhrase for the rollout's members that get the change,
// each of which runs one operation, while the rollout runs. It is "" once the
// apply is terminal, when nothing is left to wait its turn.
func (a Apply) LimitPhrase() string {
	if state.IsTerminalApplyState(a.State) {
		return ""
	}
	changing := 0
	for _, d := range a.Deployments {
		if !d.AlreadyApplied() {
			changing++
		}
	}
	return LimitPhrase(changing, changing, a.OperationsAtOnce)
}

// CountsWithLimit ends phrase, which counts the rollout's members, with
// Apply.LimitPhrase, the way WithLimit does.
func (a Apply) CountsWithLimit(phrase string) string {
	if limitPhrase := a.LimitPhrase(); limitPhrase != "" {
		return fmt.Sprintf("%s (%s)", phrase, limitPhrase)
	}
	return phrase
}
