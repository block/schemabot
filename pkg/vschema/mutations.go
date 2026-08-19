package vschema

import (
	"fmt"
	"sort"
	"strings"

	vschemapb "vitess.io/vitess/go/vt/proto/vschema"
)

// Mutation kinds, in the order they are reported per vindex.
const (
	MutationKindVindexType   = "vindex_type"
	MutationKindVindexParams = "vindex_params"
	MutationKindVindexOwner  = "vindex_owner"
)

// Mutation describes an in-place change to a vindex definition between a
// current and desired VSchema: its type, one of its params, or its owner. The
// vindex keeps its name, so nothing is structurally removed, but Vitess
// starts routing queries and maintaining lookups differently the moment the
// VSchema is applied — the blast radius matches removing the vindex outright
// — so callers treat mutations as unsafe changes requiring the same explicit
// operator opt-in as removals.
type Mutation struct {
	Kind   string `json:"kind"`
	Name   string `json:"name"`
	Reason string `json:"reason"`
}

// Mutations returns the in-place vindex definition changes between the
// current and desired VSchema. A vindex that disappears entirely is a
// removal, reported by Deletions and never duplicated here. An empty or blank
// current VSchema (a new keyspace) has nothing to mutate. Both documents must
// parse as VSchema keyspace JSON; a document that cannot be parsed returns an
// error so callers can fail closed rather than miss a mutation.
func Mutations(current, desired string) ([]Mutation, error) {
	if strings.TrimSpace(current) == "" || strings.TrimSpace(current) == "{}" {
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

	return mutations, nil
}

// paramMutations reports each changed, added, or removed param on a same-name
// vindex. The backing-table param of a lookup-family vindex gets its own
// reason: repointing it makes Vitess read and write lookup rows in a
// different table immediately, which is the highest-impact param change.
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

	var mutations []Mutation
	for _, key := range sorted {
		currentValue := current.GetParams()[key]
		desiredValue := desired.GetParams()[key]
		if currentValue == desiredValue {
			continue
		}
		reason := fmt.Sprintf("vindex %q changes parameter %q from %q to %q: routing and lookup behavior changes the moment the VSchema is applied",
			name, key, currentValue, desiredValue)
		if key == "table" && strings.Contains(current.GetType(), "lookup") {
			reason = fmt.Sprintf("lookup vindex %q repoints its backing table from %q to %q: Vitess immediately reads and writes lookup rows in the new table, the old table's rows go stale, and lookups can fail until the new table is populated",
				name, currentValue, desiredValue)
		}
		mutations = append(mutations, Mutation{
			Kind:   MutationKindVindexParams,
			Name:   name,
			Reason: reason,
		})
	}
	return mutations
}
