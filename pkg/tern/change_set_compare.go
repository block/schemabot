package tern

import (
	"fmt"
	"sort"
	"strings"

	"github.com/block/schemabot/pkg/ddl"
	ternv1 "github.com/block/schemabot/pkg/proto/ternv1"
	"github.com/block/schemabot/pkg/schema"
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

// CanonicalChangeSet is a change set reduced to the form every question about it
// is actually answered from: the multiset of table DDL changes keyed by their
// canonicalized text, and the set of namespaces whose vschema changes. Parsing
// and canonicalizing the DDL is the expensive part of comparing or keying a
// change set, so a caller with more than one question to ask canonicalizes once
// and asks them of the result.
//
// It remembers the dialect it was canonicalized under, because its content only
// means what it means under that grammar. The zero value is not a canonicalized
// change set and is not a change set that plans nothing: it carries no dialect,
// so CompareTo refuses it and Fingerprint keys it apart from every change set
// Canonicalize produced.
type CanonicalChangeSet struct {
	dialect schema.Dialect
	changes driftChangeMultiset
	vschema map[string]bool
}

// Canonicalize reduces a change set to the form comparisons and grouping keys
// are built from. The dialect selects the grammar the statements are classified
// and canonicalized with, so a PostgreSQL deployment's DDL is never judged by
// the MySQL parser.
//
// It fails closed: an unregistered dialect, malformed proto (nil entries, empty
// shard/table names, a vschema change carrying table DDL, an inconsistent
// sharded/non-sharded shape) and DDL that cannot be canonicalized (unparseable,
// multi-statement, or non-DDL) return an error, so a caller can treat the
// deployment as blocking rather than mistake a change set it could not read for
// one that agrees with another.
func Canonicalize(dialect schema.Dialect, cs ChangeSet) (CanonicalChangeSet, error) {
	parser, err := ddl.ParserForDialect(dialect)
	if err != nil {
		return CanonicalChangeSet{}, err
	}
	return canonicalizeWith(dialect, parser, cs)
}

// canonicalizeWith is Canonicalize with the dialect's parser already resolved,
// so a caller canonicalizing several change sets under one dialect resolves it
// once and keeps an unregistered dialect reportable as its own cause rather than
// as a failure of whichever change set happened to be read first.
func canonicalizeWith(dialect schema.Dialect, parser ddl.StatementParser, cs ChangeSet) (CanonicalChangeSet, error) {
	ms, vschema, err := changeSetMultiset(parser, cs)
	if err != nil {
		return CanonicalChangeSet{}, err
	}
	return CanonicalChangeSet{dialect: dialect, changes: ms, vschema: vschema}, nil
}

// CompareTo reports how candidate differs from the receiver, which is the
// baseline: table DDL by canonicalized form, vschema by per-namespace parity.
// Both sides are already canonicalized, so it parses nothing.
//
// Both sides must have been canonicalized under the same dialect. DDL
// canonicalized under different grammars proves nothing about agreement — and
// the zero value was canonicalized under none — so a mismatched pair is an
// error rather than a diff a caller would read as drift or as a match.
func (c CanonicalChangeSet) CompareTo(candidate CanonicalChangeSet) (ChangeSetDiff, error) {
	if c.dialect != candidate.dialect {
		return ChangeSetDiff{}, fmt.Errorf("cannot compare a change set canonicalized as dialect %q to one canonicalized as dialect %q", c.dialect, candidate.dialect)
	}
	if c.dialect == "" {
		return ChangeSetDiff{}, fmt.Errorf("change set carries no dialect; it was never canonicalized")
	}

	diff := ChangeSetDiff{}
	for key, want := range c.changes {
		if candidate.changes[key] < want {
			diff.MissingFromCandidate = append(diff.MissingFromCandidate, itemFromDriftKey(key))
		}
	}
	for key, have := range candidate.changes {
		if have > c.changes[key] {
			diff.UnexpectedInCandidate = append(diff.UnexpectedInCandidate, itemFromDriftKey(key))
		}
	}
	for ns := range c.vschema {
		if !candidate.vschema[ns] {
			diff.MissingVSchema = append(diff.MissingVSchema, ns)
		}
	}
	for ns := range candidate.vschema {
		if !c.vschema[ns] {
			diff.UnexpectedVSchema = append(diff.UnexpectedVSchema, ns)
		}
	}

	sortDiffItems(diff.MissingFromCandidate)
	sortDiffItems(diff.UnexpectedInCandidate)
	sort.Strings(diff.MissingVSchema)
	sort.Strings(diff.UnexpectedVSchema)
	return diff, nil
}

// CompareChangeSets reports how candidate differs from baseline, canonicalizing
// both under the dialect's grammar and comparing the results. Both sides of a
// comparison are always the same dialect, since comparing DDL canonicalized
// under different grammars proves nothing.
//
// It is the one-shot form of Canonicalize plus CompareTo, and fails closed on
// everything they do. A caller that also needs a grouping key for either side,
// or that compares one baseline to several candidates, should canonicalize once
// and compare the values instead of parsing the same change set again here.
func CompareChangeSets(dialect schema.Dialect, baseline, candidate ChangeSet) (ChangeSetDiff, error) {
	parser, err := ddl.ParserForDialect(dialect)
	if err != nil {
		return ChangeSetDiff{}, err
	}
	base, err := canonicalizeWith(dialect, parser, baseline)
	if err != nil {
		return ChangeSetDiff{}, fmt.Errorf("baseline change set: %w", err)
	}
	cand, err := canonicalizeWith(dialect, parser, candidate)
	if err != nil {
		return ChangeSetDiff{}, fmt.Errorf("candidate change set: %w", err)
	}
	return base.CompareTo(cand)
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
func changeSetMultiset(parser ddl.StatementParser, cs ChangeSet) (driftChangeMultiset, map[string]bool, error) {
	ms := driftChangeMultiset{}
	vschema := map[string]bool{}

	// nsInShards: namespace has at least one shard row (possibly empty).
	// nsShardChanges: namespace has a shard row carrying table changes, so the
	// shard rows are the authoritative representation for it.
	nsInShards := map[string]bool{}
	nsShardChanges := namespacesCarriedByShards(cs.Shards)
	for _, sp := range cs.Shards {
		if sp == nil {
			return nil, nil, fmt.Errorf("nil shard plan")
		}
		shard := strings.TrimSpace(sp.Shard)
		if shard == "" {
			return nil, nil, fmt.Errorf("shard plan for namespace %q has an empty shard name", sp.Namespace)
		}
		nsInShards[sp.Namespace] = true
		for _, tc := range sp.Changes {
			key, err := driftKeyForTableChange(parser, sp.Namespace, shard, tc)
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
			key, err := driftKeyForTableChange(parser, ns, "", tc)
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

// namespacesCarriedByShards reports the namespaces with at least one shard row
// carrying table changes. For those namespaces the shard rows are the
// authoritative representation and the namespace-collapsed Changes view is a
// lossy duplicate of them, so any walk over a change set that must count each
// change once skips the collapsed view for exactly this set.
func namespacesCarriedByShards(shards []*ternv1.ShardPlan) map[string]bool {
	carried := map[string]bool{}
	for _, sp := range shards {
		if sp != nil && len(sp.Changes) > 0 {
			carried[sp.Namespace] = true
		}
	}
	return carried
}

// AuthoritativeTableChanges returns every table change in the change set once,
// read from its authoritative representation: the shard rows for a namespace
// they carry, the collapsed Changes view for every other namespace. This is
// the same representation rule the drift comparison counts by, so a total
// derived from this walk agrees with the change count drift reports. Nil rows
// are skipped; malformed shapes are the comparison's concern and fail there.
func (cs ChangeSet) AuthoritativeTableChanges() []*ternv1.TableChange {
	carried := namespacesCarriedByShards(cs.Shards)
	var out []*ternv1.TableChange
	for _, sp := range cs.Shards {
		if sp == nil {
			continue
		}
		out = append(out, sp.Changes...)
	}
	for _, sc := range cs.Changes {
		if sc == nil || carried[sc.Namespace] {
			continue
		}
		out = append(out, sc.TableChanges...)
	}
	return out
}

// driftKeyForTableChange builds the multiset key for a proto table change,
// deriving the operation the same way a materialized apply does and
// canonicalizing the DDL with the fail-closed drift canonicalizer.
func driftKeyForTableChange(parser ddl.StatementParser, namespace, shard string, tc *ternv1.TableChange) (driftChangeKey, error) {
	if tc == nil {
		return driftChangeKey{}, fmt.Errorf("nil table change")
	}
	if strings.TrimSpace(tc.TableName) == "" {
		return driftChangeKey{}, fmt.Errorf("table change has an empty table name")
	}
	if tc.ChangeType == ternv1.ChangeType_CHANGE_TYPE_VSCHEMA {
		return driftChangeKey{}, fmt.Errorf("table %q carries a vschema change; vschema is namespace-level, not table DDL", tc.TableName)
	}
	op, err := materializedTableChangeOperation(parser, tc)
	if err != nil {
		return driftChangeKey{}, err
	}
	canon, err := canonicalDDLForDrift(parser, tc.Ddl)
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
