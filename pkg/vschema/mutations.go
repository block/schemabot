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

	MutationKindKeyspaceSharded                = "keyspace_sharded"
	MutationKindKeyspaceRequireExplicitRouting = "keyspace_require_explicit_routing"

	MutationKindTableType          = "table_type"
	MutationKindTablePinned        = "table_pinned"
	MutationKindTablePrimaryVindex = "table_primary_vindex"
	MutationKindTableSource        = "table_source"
	MutationKindTableAutoIncrement = "table_auto_increment"
)

// Mutation describes an in-place change between a current and desired
// VSchema that alters how Vitess routes rows or generates ids without
// structurally removing anything: a vindex definition's type, one of its
// params, or its owner; the keyspace's sharded or require_explicit_routing
// flag; or a table's type, pin, primary vindex, reference source, or
// auto-increment sequence. Vitess starts routing queries, maintaining
// lookups, or issuing ids differently the moment the VSchema is applied —
// the blast radius matches removing the entry outright — so callers treat
// mutations as unsafe changes requiring the same explicit operator opt-in as
// removals.
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
// additions (a new vindex, a new table entry, or a secondary vindex) are not
// mutations, with one exception. liveTables names the tables that exist on
// the target: an unsharded keyspace routes every one of them whether or not
// its VSchema lists it, so a new entry that gives a live table a sequence
// changes where that table's ids come from and is reported even though the
// entry itself is new. A keyspace whose current document is absent (see
// hasNoVSchema) is compared as an empty keyspace, with only the sharded flag
// exempt. Both documents must parse as VSchema keyspace JSON; a document that
// cannot be parsed returns an error so callers can fail closed rather than
// miss a mutation.
func Mutations(current, desired string, liveTables []string) ([]Mutation, error) {
	desiredKs, err := parseKeyspace(desired)
	if err != nil {
		return nil, fmt.Errorf("parse desired VSchema: %w", err)
	}
	currentKs := &vschemapb.Keyspace{}
	firstVSchema := hasNoVSchema(current)
	if !firstVSchema {
		currentKs, err = parseKeyspace(current)
		if err != nil {
			return nil, fmt.Errorf("parse current VSchema: %w", err)
		}
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

	// A sharded keyspace's shards exist in the topology before its first
	// VSchema, so that document saying sharded is how every sharded keyspace
	// is onboarded, not a flip; and a keyspace that still has a single shard
	// routes every keyspace id to it, so declaring it sharded moves no query
	// off the shard that holds its rows. The flip is compared only once a
	// VSchema exists to flip from.
	if !firstVSchema && currentKs.GetSharded() != desiredKs.GetSharded() {
		mutations = append(mutations, Mutation{
			Kind: MutationKindKeyspaceSharded,
			Reason: fmt.Sprintf("keyspace changes from %s to %s: Vitess routes every table in it differently the moment the VSchema is applied, while the rows stay where they are, so queries can miss rows or fail",
				shardedLabel(currentKs.GetSharded()), shardedLabel(desiredKs.GetSharded())),
		})
	}

	if currentKs.GetRequireExplicitRouting() != desiredKs.GetRequireExplicitRouting() {
		mutations = append(mutations, Mutation{
			Kind:   MutationKindKeyspaceRequireExplicitRouting,
			Reason: explicitRoutingReason(desiredKs.GetRequireExplicitRouting()),
		})
	}

	live := make(map[string]struct{}, len(liveTables))
	for _, name := range liveTables {
		live[name] = struct{}{}
	}
	for _, table := range sortedKeys(desiredKs.Tables) {
		desiredTable := desiredKs.Tables[table]
		if currentTable, ok := currentKs.Tables[table]; ok {
			mutations = append(mutations, tableMutations(table, currentTable, desiredTable)...)
			continue
		}
		_, isLive := live[table]
		if desiredKs.GetSharded() || !isLive || desiredTable.GetAutoIncrement() == nil {
			continue
		}
		auto := desiredTable.GetAutoIncrement()
		mutations = append(mutations, Mutation{
			Kind: MutationKindTableAutoIncrement,
			Name: table,
			Reason: fmt.Sprintf("table %q already exists in this unsharded keyspace and starts using sequence %q for column %q: new ids come from a different source the moment the VSchema is applied, so they can collide with ids already in use",
				table, auto.GetSequence(), auto.GetColumn()),
		})
	}

	return mutations, nil
}

// explicitRoutingReason explains a require_explicit_routing flip. With the
// flag on, none of the keyspace's tables take part in global routing, so a
// query that names a table without its keyspace stops resolving; turning it
// off puts them back, where a same-named table elsewhere makes the name
// ambiguous.
func explicitRoutingReason(desired bool) string {
	if desired {
		return "keyspace starts requiring explicit routing: its tables leave global routing the moment the VSchema is applied, so queries that name a table without its keyspace stop resolving"
	}
	return "keyspace stops requiring explicit routing: its tables join global routing the moment the VSchema is applied, so a table name shared with another keyspace becomes ambiguous and queries that relied on it fail"
}

// tableMutations reports in-place changes to a table present in both the
// current and desired VSchema.
//
// A table's primary routing is its pin when it has one — Vitess sends every
// query for a pinned table to the shard owning the pinned keyspace id, ahead
// of any vindex — and otherwise its first column vindex, from which Vitess
// computes every row's keyspace id. Changing either, or swapping one for the
// other in either direction, re-keys the whole table while the rows stay on
// their current shards. A table that had neither and gains a primary vindex
// is reported too: in a sharded keyspace such a table could not route, so the
// change re-keys rows that were only reachable some other way. The swap and
// the gained primary are reported even when Deletions also discloses a lost
// association — Deletions discloses what is gone, this discloses that every
// row now routes by something else.
//
// A reference table's source names the keyspace its rows are copied from
// and its writes are sent to; re-pointing it changes both at once.
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

	mutations = append(mutations, primaryRoutingMutations(table, current, desired)...)

	if current.GetSource() != desired.GetSource() {
		mutations = append(mutations, Mutation{
			Kind: MutationKindTableSource,
			Name: table,
			Reason: fmt.Sprintf("table %q changes its reference source from %q to %q: Vitess copies its rows from and sends its writes to a different keyspace the moment the VSchema is applied, so reads can return another keyspace's rows and writes land away from the current source",
				table, current.GetSource(), desired.GetSource()),
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

// primaryRoutingMutations compares how a table's rows are keyed: by its pin
// when it has one, otherwise by its primary vindex. A pin that moves is a
// table_pinned mutation; every other change to the key — a different primary
// vindex, a pin swapped for a vindex or a vindex for a pin, or a first
// primary vindex on a table that had no key — is a table_primary_vindex
// mutation, because each one computes every row's keyspace id differently.
func primaryRoutingMutations(table string, current, desired *vschemapb.Table) []Mutation {
	currentPin, desiredPin := current.GetPinned(), desired.GetPinned()
	currentPrimary, desiredPrimary := primaryColumnVindex(current), primaryColumnVindex(desired)

	if currentPin != "" && desiredPin != "" {
		if currentPin == desiredPin {
			return nil
		}
		return []Mutation{{
			Kind: MutationKindTablePinned,
			Name: table,
			Reason: fmt.Sprintf("table %q moves its pin from keyspace id %q to %q: every query for it goes to the shard owning the new keyspace id the moment the VSchema is applied, while its rows stay on the shard owning the old one",
				table, currentPin, desiredPin),
		}}
	}

	currentKey, desiredKey := primaryRoutingKey(currentPin, currentPrimary), primaryRoutingKey(desiredPin, desiredPrimary)
	if currentKey == desiredKey || desiredKey == "" {
		return nil
	}
	const consequence = "every row's keyspace id is computed differently the moment the VSchema is applied, while the rows stay on their current shards, so queries can miss rows and new rows land on different shards"
	reason := fmt.Sprintf("table %q changes how its rows are keyed from %s to %s: %s",
		table, primaryRoutingLabel(currentPin, currentPrimary), primaryRoutingLabel(desiredPin, desiredPrimary), consequence)
	if currentPin == "" && desiredPin == "" && currentPrimary != nil {
		reason = fmt.Sprintf("table %q changes its primary vindex from %q on (%s) to %q on (%s): %s",
			table, currentPrimary.GetName(), strings.Join(columnVindexColumns(currentPrimary), ", "),
			desiredPrimary.GetName(), strings.Join(columnVindexColumns(desiredPrimary), ", "), consequence)
	}
	return []Mutation{{Kind: MutationKindTablePrimaryVindex, Name: table, Reason: reason}}
}

// primaryRoutingKey identifies a table's row key: the pin, the primary
// vindex association, or "" when the table has neither.
func primaryRoutingKey(pin string, primary *vschemapb.ColumnVindex) string {
	switch {
	case pin != "":
		return "pinned:" + pin
	case primary != nil:
		return "vindex:" + columnVindexKey(primary)
	}
	return ""
}

func primaryRoutingLabel(pin string, primary *vschemapb.ColumnVindex) string {
	switch {
	case pin != "":
		return fmt.Sprintf("a pin to keyspace id %q", pin)
	case primary != nil:
		return fmt.Sprintf("primary vindex %q on (%s)", primary.GetName(), strings.Join(columnVindexColumns(primary), ", "))
	}
	return "no primary vindex"
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
