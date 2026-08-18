package apitypes

import (
	"encoding/json"
	"fmt"
)

// VSchemaChangeType is the ChangeType recorded on unsafe changes that stem
// from a VSchema removal rather than table DDL.
const VSchemaChangeType = "vschema"

// VSchemaChangesMetadataKey is the progress display-metadata key under which the
// engine projects per-keyspace VSchema application state as a JSON-encoded
// []VSchemaChange. The CLI and PR comment both decode it via ParseVSchemaChanges
// so they render VSchema identically.
const VSchemaChangesMetadataKey = "vschema_changes"

// Plan change-metadata keys under which engines annotate a namespace's VSchema
// work: a rendered diff, or a flag when the work is known without a rendered
// diff. Either one marks the namespace as carrying a VSchema change.
const (
	VSchemaDiffMetadataKey    = "vschema"
	VSchemaChangedMetadataKey = "vschema_changed"
)

// VSchemaDeletionsMetadataKey is the plan change-metadata key under which
// engines record structural removals in a namespace's VSchema change as a
// JSON-encoded []VSchemaDeletion. A removal changes Vitess query routing the
// moment the VSchema is applied — a deleted vindex stops being used for
// routing and lookups, and queries that depended on it can fail — so any
// recorded deletion makes the plan's VSchema change an unsafe change requiring
// the same operator opt-in as destructive DDL.
const VSchemaDeletionsMetadataKey = "vschema_deletions"

// VSchemaDeletion is one structural removal in a namespace's VSchema change.
// It mirrors the engine-side deletion type (pkg/vschema); apitypes keeps its
// own copy so this package stays dependency-free.
type VSchemaDeletion struct {
	Kind   string `json:"kind"`   // "vindex", "table", or "column_vindex"
	Name   string `json:"name"`   // vindex name, table name, or "table.vindex"
	Reason string `json:"reason"` // operator-facing explanation of the risk
}

// EncodeVSchemaDeletions marshals VSchema deletions for plan change metadata.
// Returns "" for an empty list so the metadata key is omitted.
func EncodeVSchemaDeletions(deletions []VSchemaDeletion) (string, error) {
	if len(deletions) == 0 {
		return "", nil
	}
	b, err := json.Marshal(deletions)
	if err != nil {
		return "", fmt.Errorf("encode %d VSchema deletions: %w", len(deletions), err)
	}
	return string(b), nil
}

// ParseVSchemaDeletions decodes the VSchema deletions recorded in a
// namespace's plan change metadata. Returns nil when the change carries none.
func ParseVSchemaDeletions(metadata map[string]string) ([]VSchemaDeletion, error) {
	raw := metadata[VSchemaDeletionsMetadataKey]
	if raw == "" {
		return nil, nil
	}
	var deletions []VSchemaDeletion
	if err := json.Unmarshal([]byte(raw), &deletions); err != nil {
		return nil, fmt.Errorf("decode VSchema deletions metadata: %w", err)
	}
	return deletions, nil
}

// VSchemaUnsafeChanges returns the unsafe-change view of this namespace's
// recorded VSchema deletions. Metadata that cannot be decoded fails closed:
// the namespace reports a single unsafe change explaining that deletions are
// present but unreadable, so a corrupt record can never bypass the opt-in
// gate.
func (sc *SchemaChangeResponse) VSchemaUnsafeChanges() []UnsafeChange {
	deletions, err := ParseVSchemaDeletions(sc.Metadata)
	if err != nil {
		return []UnsafeChange{{
			Table:      sc.Namespace + "/vschema.json",
			Reason:     "VSchema deletions were recorded on this plan but could not be decoded; treating the VSchema change as unsafe",
			ChangeType: VSchemaChangeType,
		}}
	}
	result := make([]UnsafeChange, 0, len(deletions))
	for _, d := range deletions {
		result = append(result, UnsafeChange{
			Table:      sc.Namespace + "/vschema.json",
			Reason:     d.Reason,
			ChangeType: VSchemaChangeType,
		})
	}
	return result
}

// HasVSchemaChange reports whether this namespace's change carries VSchema work.
func (sc *SchemaChangeResponse) HasVSchemaChange() bool {
	return sc.Metadata[VSchemaDiffMetadataKey] != "" || sc.Metadata[VSchemaChangedMetadataKey] == "true"
}

// VSchemaChange is one keyspace's VSchema application state for display. Each
// keyspace that changes its VSchema carries its own status and diff so a
// multi-keyspace deploy renders each keyspace independently.
type VSchemaChange struct {
	Namespace string `json:"namespace"`
	Status    string `json:"status"` // "applying", "applied", or "" (pending)
	Diff      string `json:"diff"`   // VSchema diff (not SQL); empty when unavailable
}

// EncodeVSchemaChanges marshals VSchema changes for the progress display
// metadata. Returns "" for an empty list so the metadata key is omitted.
func EncodeVSchemaChanges(changes []VSchemaChange) (string, error) {
	if len(changes) == 0 {
		return "", nil
	}
	b, err := json.Marshal(changes)
	if err != nil {
		return "", err
	}
	return string(b), nil
}

// ParseVSchemaChanges decodes the VSchema changes carried in progress display
// metadata. Returns nil when the apply carries no VSchema change.
func ParseVSchemaChanges(metadata map[string]string) ([]VSchemaChange, error) {
	raw := metadata[VSchemaChangesMetadataKey]
	if raw == "" {
		return nil, nil
	}
	var changes []VSchemaChange
	if err := json.Unmarshal([]byte(raw), &changes); err != nil {
		return nil, err
	}
	return changes, nil
}
