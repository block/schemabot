package vschema

import (
	"fmt"
	"sort"
	"strings"

	vschemapb "vitess.io/vitess/go/vt/proto/vschema"
)

// Mutation kinds. Vindex kinds are reported per vindex in the order listed,
// then the keyspace's sharded flag, then table kinds per table in the order
// listed.
const (
	MutationKindVindexType   = "vindex_type"
	MutationKindVindexParams = "vindex_params"
	MutationKindVindexOwner  = "vindex_owner"

	MutationKindKeyspaceSharded = "keyspace_sharded"

	MutationKindTableType          = "table_type"
	MutationKindTablePrimaryVindex = "table_primary_vindex"
	MutationKindTableAutoIncrement = "table_auto_increment"
)

// Mutation describes an in-place change between a current and desired
// VSchema that alters how Vitess routes rows or generates ids without
// structurally removing anything: a vindex definition's type, one of its
// params, or its owner; the keyspace's sharded flag; or a table's type,
// primary vindex, or auto-increment sequence. Vitess starts routing queries,
// maintaining lookups, or issuing ids differently the moment the VSchema is
// applied — the blast radius matches removing the entry outright — so callers
// treat mutations as unsafe changes requiring the same explicit operator
// opt-in as removals.
//
// Name is the vindex name for vindex kinds, the table name for table kinds,
// and empty for the keyspace kind.
type Mutation struct {
	Kind   string `json:"kind"`
	Name   string `json:"name"`
	Reason string `json:"reason"`
}

// Mutations returns the in-place changes between the current and desired
// VSchema. A vindex, table, or column-vindex association that disappears
// entirely is a removal, reported by Deletions and never duplicated here;
// additions (a new vindex, table, or secondary vindex) are not mutations. A
// keyspace with no VSchema yet has nothing to mutate. Both documents must
// parse as VSchema keyspace JSON; a document that cannot be parsed returns an
// error so callers can fail closed rather than miss a mutation.
func Mutations(current, desired string) ([]Mutation, error) {
	if hasNoVSchema(current) {
		return nil, nil
	}

	currentKs, err := parseKeyspace(current)
	if err != nil {
		return nil, fmt.Errorf("parse current VSchema: %w", err)
	}
	desiredKs, err := parseKeyspace(desired)
	if err != nil {
		return nil, fmt.Errorf("parse desired VSchema: %w", err)
	}

	var mutations []Mutation
	for _, name := range sortedKeys(currentKs.Vindexes) {
		desiredVindex, ok := desiredKs.Vindexes[name]
		if !ok {
			continue
		}
		currentVindex := currentKs.Vindexes[name]

		if currentVindex.GetType() != desiredVindex.GetType() {
			mutations = append(mutations, Mutation{
				Kind: MutationKindVindexType,
				Name: name,
				Reason: fmt.Sprintf("vindex %q changes type from %q to %q: every row's keyspace id is computed differently the moment the VSchema is applied, so queries routed through it can miss rows or scatter",
					name, currentVindex.GetType(), desiredVindex.GetType()),
			})
		}

		mutations = append(mutations, paramMutations(name, currentVindex, desiredVindex)...)

		if currentVindex.GetOwner() != desiredVindex.GetOwner() {
			mutations = append(mutations, Mutation{
				Kind: MutationKindVindexOwner,
				Name: name,
				Reason: fmt.Sprintf("vindex %q changes owner from %q to %q: a different table's writes now maintain its lookup rows, so rows written through the old owner stop being maintained",
					name, currentVindex.GetOwner(), desiredVindex.GetOwner()),
			})
		}
	}

	if currentKs.GetSharded() != desiredKs.GetSharded() {
		mutations = append(mutations, Mutation{
			Kind: MutationKindKeyspaceSharded,
			Reason: fmt.Sprintf("keyspace changes from %s to %s: Vitess routes every table in it differently the moment the VSchema is applied, while the rows stay where they are, so queries can miss rows or fail",
				shardedLabel(currentKs.GetSharded()), shardedLabel(desiredKs.GetSharded())),
		})
	}

	for _, table := range sortedKeys(currentKs.Tables) {
		desiredTable, ok := desiredKs.Tables[table]
		if !ok {
			continue
		}
		mutations = append(mutations, tableMutations(table, currentKs.Tables[table], desiredTable)...)
	}

	return mutations, nil
}

// tableMutations reports in-place changes to a table present in both the
// current and desired VSchema.
//
// The primary vindex is the table's first column vindex: Vitess computes
// every row's keyspace id from it, so replacing or reordering it re-keys the
// whole table even when every association is still present. It is reported
// whenever both sides have one and they differ, including when the old
// primary's association is also removed — Deletions discloses the lost
// association, this discloses that every row now routes by a different
// vindex. A table that had no column vindexes gaining one is an addition.
//
// Adding, removing, or re-pointing an auto-increment changes where new ids
// come from. Vitess uses the configured sequence in both sharded and
// unsharded keyspaces; without it, inserts pass through to the database, whose
// backing column is not guaranteed to generate an id. A different source can
// also hand out ids already in use.
func tableMutations(table string, current, desired *vschemapb.Table) []Mutation {
	var mutations []Mutation

	if current.GetType() != desired.GetType() {
		mutations = append(mutations, Mutation{
			Kind: MutationKindTableType,
			Name: table,
			Reason: fmt.Sprintf("table %q changes type from %s to %s: Vitess routes its queries differently the moment the VSchema is applied, so queries can miss rows or fail",
				table, tableTypeLabel(current.GetType()), tableTypeLabel(desired.GetType())),
		})
	}

	currentPrimary, desiredPrimary := primaryColumnVindex(current), primaryColumnVindex(desired)
	if currentPrimary != nil && desiredPrimary != nil && columnVindexKey(currentPrimary) != columnVindexKey(desiredPrimary) {
		mutations = append(mutations, Mutation{
			Kind: MutationKindTablePrimaryVindex,
			Name: table,
			Reason: fmt.Sprintf("table %q changes its primary vindex from %q on (%s) to %q on (%s): every row's keyspace id is computed differently the moment the VSchema is applied, while the rows stay on their current shards, so queries can miss rows and new rows land on different shards",
				table, currentPrimary.GetName(), strings.Join(columnVindexColumns(currentPrimary), ", "),
				desiredPrimary.GetName(), strings.Join(columnVindexColumns(desiredPrimary), ", ")),
		})
	}

	currentAuto, desiredAuto := current.GetAutoIncrement(), desired.GetAutoIncrement()
	if currentAuto != nil || desiredAuto != nil {
		switch {
		case currentAuto == nil:
			mutations = append(mutations, Mutation{
				Kind: MutationKindTableAutoIncrement,
				Name: table,
				Reason: fmt.Sprintf("table %q starts using sequence %q for column %q: new ids come from a different source the moment the VSchema is applied, so they can collide with ids already in use",
					table, desiredAuto.GetSequence(), desiredAuto.GetColumn()),
			})
		case desiredAuto == nil:
			mutations = append(mutations, Mutation{
				Kind: MutationKindTableAutoIncrement,
				Name: table,
				Reason: fmt.Sprintf("table %q stops using sequence %q for column %q: inserts pass through without sequence-generated ids the moment the VSchema is applied, and the backing column is not guaranteed to generate them",
					table, currentAuto.GetSequence(), currentAuto.GetColumn()),
			})
		case currentAuto.GetColumn() != desiredAuto.GetColumn() || currentAuto.GetSequence() != desiredAuto.GetSequence():
			mutations = append(mutations, Mutation{
				Kind: MutationKindTableAutoIncrement,
				Name: table,
				Reason: fmt.Sprintf("table %q changes its auto-increment from sequence %q on column %q to sequence %q on column %q: new ids come from a different source the moment the VSchema is applied, so they can collide with ids already in use",
					table, currentAuto.GetSequence(), currentAuto.GetColumn(), desiredAuto.GetSequence(), desiredAuto.GetColumn()),
			})
		}
	}

	return mutations
}

// primaryColumnVindex returns a table's primary vindex association — its
// first column vindex — or nil when the table has none.
func primaryColumnVindex(t *vschemapb.Table) *vschemapb.ColumnVindex {
	if cvs := t.GetColumnVindexes(); len(cvs) > 0 {
		return cvs[0]
	}
	return nil
}

func shardedLabel(sharded bool) string {
	if sharded {
		return "sharded"
	}
	return "unsharded"
}

func tableTypeLabel(t string) string {
	if t == "" {
		return "the default table type"
	}
	return fmt.Sprintf("%q", t)
}

// paramMutations reports each changed, added, or removed param on a same-name
// vindex. Presence is part of a param's identity — a key set to the empty
// string is not the same as an absent key — so add and remove are detected by
// presence, not by comparing zero values. The backing-table param gets its
// own reason when both definitions are lookup-family: repointing it makes
// Vitess read and write lookup rows in a different table immediately, which
// is the highest-impact param change.
func paramMutations(name string, current, desired *vschemapb.Vindex) []Mutation {
	keys := make(map[string]struct{}, len(current.GetParams())+len(desired.GetParams()))
	for k := range current.GetParams() {
		keys[k] = struct{}{}
	}
	for k := range desired.GetParams() {
		keys[k] = struct{}{}
	}
	sorted := make([]string, 0, len(keys))
	for k := range keys {
		sorted = append(sorted, k)
	}
	sort.Strings(sorted)

	bothLookup := strings.Contains(current.GetType(), "lookup") && strings.Contains(desired.GetType(), "lookup")

	var mutations []Mutation
	for _, key := range sorted {
		currentValue, inCurrent := current.GetParams()[key]
		desiredValue, inDesired := desired.GetParams()[key]
		if inCurrent == inDesired && currentValue == desiredValue {
			continue
		}
		var reason string
		switch {
		case key == "table" && bothLookup && inCurrent && inDesired:
			reason = fmt.Sprintf("lookup vindex %q repoints its backing table from %q to %q: Vitess immediately reads and writes lookup rows in the new table, the old table's rows go stale, and lookups can fail until the new table is populated",
				name, currentValue, desiredValue)
		case !inCurrent:
			reason = fmt.Sprintf("vindex %q adds parameter %q with value %q: routing and lookup behavior changes the moment the VSchema is applied",
				name, key, desiredValue)
		case !inDesired:
			reason = fmt.Sprintf("vindex %q removes parameter %q (was %q): routing and lookup behavior changes the moment the VSchema is applied",
				name, key, currentValue)
		default:
			reason = fmt.Sprintf("vindex %q changes parameter %q from %q to %q: routing and lookup behavior changes the moment the VSchema is applied",
				name, key, currentValue, desiredValue)
		}
		mutations = append(mutations, Mutation{
			Kind:   MutationKindVindexParams,
			Name:   name,
			Reason: reason,
		})
	}
	return mutations
}
