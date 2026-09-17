package tern

import (
	"fmt"
	"sort"

	ternv1 "github.com/block/schemabot/pkg/proto/ternv1"
	"github.com/block/schemabot/pkg/schema"
	"github.com/block/schemabot/pkg/storage"
)

// StoredPlanFingerprint keys a stored plan by the work it would run, so a caller
// holding plan rows can tell members that run the same change set from members
// that run different ones.
//
// It is ChangeSetFingerprint applied to the plan's own stored changes, not a
// second comparison. A plan row is the persisted form of the change set the
// differ produced, so rebuilding that change set and keying it the usual way is
// what keeps one keying rule behind every caller. A caller that compared stored
// plans some other way would be free to disagree with the review-time rollup
// about which members run the same work, which is the disagreement this exists
// to prevent.
//
// Fingerprints are comparable with each other, not with a value from a different
// release or a different side of the store: like ChangeSetFingerprint, this is a
// grouping key for one rollout, never something to persist or show an operator.
//
// Errors on what ChangeSetFingerprint errors on, plus a plan whose stored shape
// cannot be rebuilt — so a caller that cannot key a plan treats it as
// unclassifiable rather than as matching one it was never compared against.
func StoredPlanFingerprint(dialect schema.Dialect, plan *storage.Plan) (string, error) {
	if plan == nil {
		return "", fmt.Errorf("fingerprint stored plan: no plan")
	}
	cs, err := changeSetFromStoredPlan(plan)
	if err != nil {
		return "", err
	}
	return ChangeSetFingerprint(dialect, cs)
}

// changeSetFromStoredPlan rebuilds the change set a plan row was stored from.
//
// Both representations are emitted, exactly as the engine returned them: the
// namespace-collapsed table changes and the per-shard changes. The multiset
// keying decides which of the two is authoritative for a namespace, and it can
// only do that if it is handed both — dropping either here would key a sharded
// namespace off the lossy view, or off nothing.
//
// Namespaces are rebuilt in sorted order. The fingerprint does not depend on
// order, but a deterministic rebuild means a malformed plan reports the same
// failure every time rather than whichever namespace the map happened to yield
// first.
func changeSetFromStoredPlan(plan *storage.Plan) (ChangeSet, error) {
	namespaces := make([]string, 0, len(plan.Namespaces))
	for ns := range plan.Namespaces {
		namespaces = append(namespaces, ns)
	}
	sort.Strings(namespaces)

	cs := ChangeSet{}
	for _, ns := range namespaces {
		nsData := plan.Namespaces[ns]
		if nsData == nil {
			return ChangeSet{}, fmt.Errorf("rebuild change set for plan %s: namespace %q has no plan data", plan.PlanIdentifier, ns)
		}
		cs.Changes = append(cs.Changes, &ternv1.SchemaChange{
			Namespace:    ns,
			TableChanges: protoTableChangesFromStorage(nsData.Tables),
			Metadata:     nsData.Metadata,
		})
	}
	for _, shard := range plan.Shards {
		cs.Shards = append(cs.Shards, &ternv1.ShardPlan{
			Shard:     shard.Shard,
			Namespace: shard.Namespace,
			Changes:   protoTableChangesFromStorage(shard.Changes),
		})
	}
	return cs, nil
}

// protoTableChangesFromStorage converts stored table changes back to the form
// the change set keys on.
//
// The stored operation is carried across rather than left for the keying to
// re-derive from the DDL. It is the round trip of the change type the engine
// reported, which is what the live side keys on in preference to the DDL — so
// carrying it is what makes a plan read back from storage key the way the change
// set it was stored from did. Dropping it would key off the DDL instead, and the
// two sides would agree only for as long as the engine's classification and the
// parser's never diverged.
func protoTableChangesFromStorage(changes []storage.TableChange) []*ternv1.TableChange {
	if len(changes) == 0 {
		return nil
	}
	out := make([]*ternv1.TableChange, 0, len(changes))
	for _, change := range changes {
		out = append(out, &ternv1.TableChange{
			TableName:  change.Table,
			Ddl:        change.DDL,
			ChangeType: ddlActionToProtoChangeType(change.Operation),
		})
	}
	return out
}
