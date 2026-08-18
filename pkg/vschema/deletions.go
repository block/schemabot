package vschema

import (
	"fmt"
	"sort"
	"strings"

	"google.golang.org/protobuf/encoding/protojson"

	vschemapb "vitess.io/vitess/go/vt/proto/vschema"
)

// Deletion kinds, in the order they are reported.
const (
	DeletionKindVindex       = "vindex"
	DeletionKindTable        = "table"
	DeletionKindColumnVindex = "column_vindex"
)

// Deletion describes a structural removal between a current and desired
// VSchema: a vindex definition, a table routing entry, or a table's
// column-vindex association. Removals take effect the moment the VSchema is
// applied — Vitess stops using the removed entry for routing, lookup vindexes
// stop being maintained, and queries that depended on it can fail or scatter —
// so callers treat them as unsafe changes requiring explicit operator opt-in.
type Deletion struct {
	Kind   string `json:"kind"`
	Name   string `json:"name"`
	Reason string `json:"reason"`
}

// Deletions returns the structural removals needed to go from the current
// VSchema to the desired one. An empty or blank current VSchema (a new
// keyspace) has nothing to remove. Both documents must parse as VSchema
// keyspace JSON; a document that cannot be parsed returns an error so callers
// can fail closed rather than miss a removal.
func Deletions(current, desired string) ([]Deletion, error) {
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

	var deletions []Deletion

	for _, name := range sortedKeys(currentKs.Vindexes) {
		if _, ok := desiredKs.Vindexes[name]; !ok {
			deletions = append(deletions, Deletion{
				Kind:   DeletionKindVindex,
				Name:   name,
				Reason: vindexRemovalReason(name, currentKs.Vindexes[name]),
			})
		}
	}

	for _, table := range sortedKeys(currentKs.Tables) {
		desiredTable, ok := desiredKs.Tables[table]
		if !ok {
			deletions = append(deletions, Deletion{
				Kind:   DeletionKindTable,
				Name:   table,
				Reason: fmt.Sprintf("table %q is removed from the VSchema: Vitess loses its routing entry and queries against it can fail", table),
			})
			continue
		}
		currentTable := currentKs.Tables[table]
		desiredVindexes := columnVindexNames(desiredTable)
		for _, cv := range currentTable.GetColumnVindexes() {
			if _, ok := desiredVindexes[cv.GetName()]; !ok {
				deletions = append(deletions, Deletion{
					Kind:   DeletionKindColumnVindex,
					Name:   table + "." + cv.GetName(),
					Reason: fmt.Sprintf("table %q no longer uses vindex %q: routing for queries on its columns changes immediately and lookup rows stop being maintained", table, cv.GetName()),
				})
			}
		}
	}

	return deletions, nil
}

// vindexRemovalReason explains the operational impact of removing a vindex
// definition. Lookup-family vindexes own rows in a backing table, so their
// removal additionally stops that table from being maintained; functional
// vindexes (hash etc.) lose routing only.
func vindexRemovalReason(name string, v *vschemapb.Vindex) string {
	if strings.Contains(v.GetType(), "lookup") {
		if backing := v.GetParams()["table"]; backing != "" {
			return fmt.Sprintf("lookup vindex %q is removed: Vitess immediately stops maintaining its rows in backing table %q, queries routed through it can fail or scatter, and the lookup data goes stale", name, backing)
		}
		return fmt.Sprintf("lookup vindex %q is removed: Vitess immediately stops maintaining its lookup rows and queries routed through it can fail or scatter", name)
	}
	return fmt.Sprintf("vindex %q is removed: Vitess immediately stops using it for routing and lookups, and queries that depend on it can fail or scatter", name)
}

// parseKeyspace decodes a VSchema keyspace JSON document. Unlike Normalize,
// which is a best-effort canonicalizer for display comparison, this parse is
// strict: deletion detection is a safety input and must not silently treat an
// unreadable document as empty. Unknown fields are tolerated so a VSchema
// served by a newer Vitess than the vendored proto still parses; malformed
// JSON still fails.
func parseKeyspace(s string) (*vschemapb.Keyspace, error) {
	s = strings.TrimSpace(s)
	if s == "" {
		s = "{}"
	}
	var ks vschemapb.Keyspace
	if err := (protojson.UnmarshalOptions{DiscardUnknown: true}).Unmarshal([]byte(s), &ks); err != nil {
		return nil, err
	}
	return &ks, nil
}

func columnVindexNames(t *vschemapb.Table) map[string]struct{} {
	names := make(map[string]struct{}, len(t.GetColumnVindexes()))
	for _, cv := range t.GetColumnVindexes() {
		names[cv.GetName()] = struct{}{}
	}
	return names
}

func sortedKeys[V any](m map[string]V) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}
