package presentation

import (
	"fmt"
	"log/slog"
	"strings"

	"github.com/block/schemabot/pkg/state"
)

// ShardWork is one keyspace shard's work in a sharded apply: the (shard,
// table) operations it runs, in resolved order. The shard is the identity a
// status names; each operation's Deployment and OperationKey only place the
// row an EndedBy names.
type ShardWork struct {
	Keyspace   string
	Shard      string
	Operations []Operation
}

// ShardStatus is one keyspace shard's derived status.
type ShardStatus struct {
	Keyspace string
	Shard    string
	Deployment
}

// DeriveShards projects each keyspace shard of a sharded apply to one status,
// the way the PR comment and the CLI both read a shard: its operations
// aggregate to the most attention-worthy one's state and error, then every
// shard, across all keyspaces, is derived in one pass so ordering labels
// ("waiting for -40", "halted — -40 failed") name sibling shards. A shard's
// identity is its name, qualified by keyspace ("keyspace/shard") when the
// apply spans more than one keyspace, since shard names repeat across
// keyspaces and a label naming a bare duplicate would be ambiguous. The
// shards of an apply in applyState read as the rollout's members do
// (ResumeClaimed), so a start under way never counts its shards as stopped.
func DeriveShards(applyState string, shards []ShardWork) []ShardStatus {
	keyspaces := make(map[string]bool)
	for _, sw := range shards {
		keyspaces[sw.Keyspace] = true
	}
	qualify := len(keyspaces) > 1
	shardOf := shardIdentities(shards, qualify)
	inputs := make([]Operation, 0, len(shards))
	for _, sw := range shards {
		if len(sw.Operations) == 0 {
			continue
		}
		best := 0
		for k, op := range sw.Operations[1:] {
			if ShardStateRank(op.State) > ShardStateRank(sw.Operations[best].State) {
				best = k + 1
			}
		}
		first := sw.Operations[0]
		inputs = append(inputs, Operation{
			Deployment:        shardIdentity(sw, qualify),
			State:             sw.Operations[best].State,
			Barrier:           first.Barrier,
			Parallel:          first.Parallel,
			ContinueOnFailure: first.ContinueOnFailure,
			PauseOnFailure:    first.PauseOnFailure,
			Released:          first.Released,
			Error:             sw.Operations[best].Error,
			EndedBy:           shardEndedBy(sw.Operations, best, shardOf),
		})
	}
	byIdentity := make(map[string]Deployment, len(inputs))
	for _, d := range Derive(ResumeClaimed(applyState, inputs)).Deployments {
		byIdentity[d.Deployment] = d
	}
	out := make([]ShardStatus, 0, len(inputs))
	for _, sw := range shards {
		identity := shardIdentity(sw, qualify)
		d, ok := byIdentity[identity]
		if !ok {
			// Derive returns one deployment per input with its identity
			// preserved; a missing identity means that contract broke. Omit the
			// shard rather than show some other shard's status under its name.
			slog.Warn("sharded apply will omit a shard's status: presentation returned no status for its identity",
				"identity", identity, "keyspace", sw.Keyspace, "shard", sw.Shard)
			continue
		}
		out = append(out, ShardStatus{Keyspace: sw.Keyspace, Shard: sw.Shard, Deployment: d})
	}
	return out
}

// shardIdentity is the name a shard's status goes by: the shard, qualified by
// its keyspace when the apply spans more than one.
func shardIdentity(sw ShardWork, qualify bool) string {
	if qualify {
		return sw.Keyspace + "/" + sw.Shard
	}
	return sw.Shard
}

// shardIdentities maps each operation row of the shards to the identity of
// the shard that runs it.
func shardIdentities(shards []ShardWork, qualify bool) map[OperationRef]string {
	shardOf := make(map[OperationRef]string)
	for _, sw := range shards {
		for _, op := range sw.Operations {
			shardOf[OperationRef{Deployment: op.Deployment, OperationKey: op.OperationKey}] = shardIdentity(sw, qualify)
		}
	}
	return shardOf
}

// shardEndedBy is the EndedBy of the shard whose status its operation at best
// speaks for, rewritten to name the shard its row belongs to, since a shard's
// status is derived with the shard as its identity. It is set only when every
// failed operation of the shard was failed for another row. A row no shard
// runs, such as a finalizer, keeps an EndedBy that resolves to no name.
func shardEndedBy(ops []Operation, best int, shardOf map[OperationRef]string) *OperationRef {
	rows := make([]int, len(ops))
	for k := range ops {
		rows[k] = k
	}
	ref := endedByAll(ops, rows, best)
	if ref == nil {
		return nil
	}
	return &OperationRef{Deployment: shardOf[*ref]}
}

// ShardCounts is the per-status count of a sharded apply's shards, given each
// shard's status label, in the order each status first appears ("1 running
// table copy, 3 waiting for -40"): the shard-unit analogue of a rollout's
// deployment counts. A label counts by its leading state words, without the
// reason after " — ", so shards halted by the same failure count together.
func ShardCounts(labels []string) string {
	var order []string
	counts := make(map[string]int, len(labels))
	for _, label := range labels {
		if i := strings.Index(label, " — "); i >= 0 {
			label = label[:i]
		}
		if _, seen := counts[label]; !seen {
			order = append(order, label)
		}
		counts[label]++
	}
	parts := make([]string, 0, len(order))
	for _, label := range order {
		parts = append(parts, fmt.Sprintf("%d %s", counts[label], label))
	}
	return strings.Join(parts, ", ")
}

// ShardStateRank orders operation states by how much they demand attention,
// so a shard surfaces its most actionable operation. Failure ranks highest;
// completed lowest.
func ShardStateRank(s string) int {
	switch s {
	case state.ApplyOperation.Failed:
		return 12
	case state.ApplyOperation.FailedRetryable:
		return 11
	case state.ApplyOperation.Running:
		return 10
	case state.ApplyOperation.CuttingOver:
		return 9
	case state.ApplyOperation.WaitingForCutover:
		return 8
	case state.ApplyOperation.Recovering:
		return 7
	case state.ApplyOperation.Resuming:
		return 6
	case state.ApplyOperation.Stopped:
		return 5
	case state.ApplyOperation.RevertWindow:
		return 4
	case state.ApplyOperation.Pending:
		return 3
	case state.ApplyOperation.Cancelled:
		return 2
	case state.ApplyOperation.Reverted:
		return 1
	case state.ApplyOperation.Completed:
		return 0
	default:
		return 3
	}
}
