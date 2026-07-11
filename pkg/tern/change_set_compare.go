package tern

import (
	"fmt"
	"sort"
	"strings"

	ternv1 "github.com/block/schemabot/pkg/proto/ternv1"
)

// ChangeSet is a deployment's proto plan change set: the namespace-collapsed
// table changes and the authoritative per-shard changes a Plan or PlanDiff
// returns. It is the unit the review-time rollup compares across a database's
// deployments to detect drift before approval.
type ChangeSet struct {
	Changes []*ternv1.SchemaChange
	Shards  []*ternv1.ShardPlan
}

// ChangeSetDiffItem is one table DDL change that differs between two change sets.
// The DDL is the canonicalized form the comparison keyed on, so two items for
// the same namespace/shard/table/operation that differ only in DDL render
// distinctly instead of looking identical.
type ChangeSetDiffItem struct {
	Namespace string
	Shard     string
	Table     string
	Operation string
	DDL       string
}

// ChangeSetDiff describes how a candidate change set differs from a baseline.
// An empty diff means the two change sets are canonically identical.
type ChangeSetDiff struct {
	// MissingFromCandidate are changes the baseline would plan that the candidate
	// would not.
	MissingFromCandidate []ChangeSetDiffItem
	// UnexpectedInCandidate are changes the candidate would plan that the baseline
	// would not.
	UnexpectedInCandidate []ChangeSetDiffItem
	// MissingVSchema are namespaces the baseline changes the vschema for that the
	// candidate does not.
	MissingVSchema []string
	// UnexpectedVSchema are namespaces the candidate changes the vschema for that
	// the baseline does not.
	UnexpectedVSchema []string
}

// Empty reports whether the candidate matches the baseline exactly.
func (d ChangeSetDiff) Empty() bool {
	return len(d.MissingFromCandidate) == 0 &&
		len(d.UnexpectedInCandidate) == 0 &&
		len(d.MissingVSchema) == 0 &&
		len(d.UnexpectedVSchema) == 0
}

// CompareChangeSets reports how candidate differs from baseline, comparing table
// DDL by canonicalized form and vschema by per-namespace parity.
//
// It fails closed: malformed proto (nil entries, empty shard/table names, a
// vschema change carrying table DDL, an inconsistent sharded/non-sharded shape)
// and DDL that cannot be canonicalized (unparseable, multi-statement, or
// non-DDL) return an error, so a caller can treat the deployment as blocking
// rather than mistake a comparison it could not perform for agreement.
func CompareChangeSets(baseline, candidate ChangeSet) (ChangeSetDiff, error) {
	baseMS, baseVS, err := changeSetMultiset(baseline)
	if err != nil {
		return ChangeSetDiff{}, fmt.Errorf("baseline change set: %w", err)
	}
	candMS, candVS, err := changeSetMultiset(candidate)
	if err != nil {
		return ChangeSetDiff{}, fmt.Errorf("candidate change set: %w", err)
	}

	diff := ChangeSetDiff{}
	for key, want := range baseMS {
		if candMS[key] < want {
			diff.MissingFromCandidate = append(diff.MissingFromCandidate, itemFromDriftKey(key))
		}
	}
	for key, have := range candMS {
		if have > baseMS[key] {
			diff.UnexpectedInCandidate = append(diff.UnexpectedInCandidate, itemFromDriftKey(key))
		}
	}
	for ns := range baseVS {
		if !candVS[ns] {
			diff.MissingVSchema = append(diff.MissingVSchema, ns)
		}
	}
	for ns := range candVS {
		if !baseVS[ns] {
			diff.UnexpectedVSchema = append(diff.UnexpectedVSchema, ns)
		}
	}

	sortDiffItems(diff.MissingFromCandidate)
	sortDiffItems(diff.UnexpectedInCandidate)
	sort.Strings(diff.MissingVSchema)
	sort.Strings(diff.UnexpectedVSchema)
	return diff, nil
}

// changeSetMultiset builds the table DDL multiset and the set of vschema-changed
// namespaces for a proto change set.
//
// Table changes are counted from their authoritative representation: a sharded
// namespace's changes live per shard on Shards and the namespace-collapsed
// Changes view of them is lossy (it dedupes tables across shards), so for a
// namespace carried by shard rows only the shard rows are counted. A non-sharded
// namespace's changes live only on Changes and are counted there. VSchema
// carries no table DDL and is compared by per-namespace parity.
func changeSetMultiset(cs ChangeSet) (driftChangeMultiset, map[string]bool, error) {
	ms := driftChangeMultiset{}
	vschema := map[string]bool{}

	// nsInShards: namespace has at least one shard row (possibly empty).
	// nsShardChanges: namespace has a shard row carrying table changes, so the
	// shard rows are the authoritative representation for it.
	nsInShards := map[string]bool{}
	nsShardChanges := map[string]bool{}
	for _, sp := range cs.Shards {
		if sp == nil {
			return nil, nil, fmt.Errorf("nil shard plan")
		}
		shard := strings.TrimSpace(sp.Shard)
		if shard == "" {
			return nil, nil, fmt.Errorf("shard plan for namespace %q has an empty shard name", sp.Namespace)
		}
		nsInShards[sp.Namespace] = true
		if len(sp.Changes) > 0 {
			nsShardChanges[sp.Namespace] = true
		}
		for _, tc := range sp.Changes {
			key, err := driftKeyForTableChange(sp.Namespace, shard, tc)
			if err != nil {
				return nil, nil, fmt.Errorf("shard %q: %w", shard, err)
			}
			ms[key]++
		}
	}

	for _, sc := range cs.Changes {
		if sc == nil {
			return nil, nil, fmt.Errorf("nil schema change")
		}
		ns := sc.Namespace
		if sc.Metadata["vschema_changed"] == "true" {
			vschema[ns] = true
		}
		hasTableChanges := false
		for _, tc := range sc.TableChanges {
			if tc == nil {
				return nil, nil, fmt.Errorf("nil table change in namespace %q", ns)
			}
			// In the plan/proto representation a vschema change is signalled via
			// Metadata["vschema_changed"] and carries no table DDL. A vschema table
			// change indicates malformed input (e.g. a change set built from an
			// apply request's DdlChanges), so fail closed rather than skip it and
			// risk a false match. Checked before the shard skip so a sharded
			// namespace cannot smuggle one through.
			if tc.ChangeType == ternv1.ChangeType_CHANGE_TYPE_VSCHEMA {
				return nil, nil, fmt.Errorf("namespace %q table change %q carries a vschema change; vschema must be represented via metadata, not table DDL", ns, tc.TableName)
			}
			hasTableChanges = true
			// A namespace carried by shard rows is counted authoritatively there;
			// its collapsed view here would double-count and is lossy.
			if nsShardChanges[ns] {
				continue
			}
			key, err := driftKeyForTableChange(ns, "", tc)
			if err != nil {
				return nil, nil, fmt.Errorf("namespace %q: %w", ns, err)
			}
			ms[key]++
		}
		// A namespace with collapsed table changes but only empty shard rows is an
		// inconsistent shape: neither representation carries the change, so fail
		// closed rather than silently pick one.
		if hasTableChanges && nsInShards[ns] && !nsShardChanges[ns] {
			return nil, nil, fmt.Errorf("namespace %q has collapsed table changes but no shard carries them", ns)
		}
	}
	return ms, vschema, nil
}

// driftKeyForTableChange builds the multiset key for a proto table change,
// deriving the operation the same way a materialized apply does and
// canonicalizing the DDL with the fail-closed drift canonicalizer.
func driftKeyForTableChange(namespace, shard string, tc *ternv1.TableChange) (driftChangeKey, error) {
	if tc == nil {
		return driftChangeKey{}, fmt.Errorf("nil table change")
	}
	if strings.TrimSpace(tc.TableName) == "" {
		return driftChangeKey{}, fmt.Errorf("table change has an empty table name")
	}
	if tc.ChangeType == ternv1.ChangeType_CHANGE_TYPE_VSCHEMA {
		return driftChangeKey{}, fmt.Errorf("table %q carries a vschema change; vschema is namespace-level, not table DDL", tc.TableName)
	}
	op, err := materializedTableChangeOperation(tc)
	if err != nil {
		return driftChangeKey{}, err
	}
	canon, err := canonicalDDLForDrift(tc.Ddl)
	if err != nil {
		return driftChangeKey{}, fmt.Errorf("table %q: %w", tc.TableName, err)
	}
	return driftChangeKey{namespace, shard, tc.TableName, op, canon}, nil
}

func itemFromDriftKey(k driftChangeKey) ChangeSetDiffItem {
	return ChangeSetDiffItem{
		Namespace: k.namespace,
		Shard:     k.shard,
		Table:     k.table,
		Operation: k.operation,
		DDL:       k.ddl,
	}
}

func sortDiffItems(items []ChangeSetDiffItem) {
	sort.Slice(items, func(i, j int) bool {
		a, b := items[i], items[j]
		if a.Namespace != b.Namespace {
			return a.Namespace < b.Namespace
		}
		if a.Shard != b.Shard {
			return a.Shard < b.Shard
		}
		if a.Table != b.Table {
			return a.Table < b.Table
		}
		if a.Operation != b.Operation {
			return a.Operation < b.Operation
		}
		return a.DDL < b.DDL
	})
}
