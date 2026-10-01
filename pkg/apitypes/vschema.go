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

// VSchemaMutationsMetadataKey is the plan change-metadata key under which
// engines record in-place routing changes in a namespace's VSchema change as a
// JSON-encoded []VSchemaMutation. A mutation removes nothing but changes how
// Vitess routes rows or issues ids — a vindex type change or a new primary
// vindex re-computes every row's keyspace id, a repointed lookup backing
// table moves lookup rows, and an auto-increment change switches the source
// of generated ids — so any recorded mutation makes the plan's VSchema
// change an unsafe change requiring the same operator opt-in as a removal.
const VSchemaMutationsMetadataKey = "vschema_mutations"

// VSchemaMutation is one in-place routing change in a namespace's VSchema
// change. It mirrors the engine-side mutation type (pkg/vschema); apitypes
// keeps its own copy so this package stays dependency-free.
type VSchemaMutation struct {
	Kind   string `json:"kind"`   // "vindex_type", "vindex_params", "vindex_owner", "keyspace_sharded", "table_type", "table_primary_vindex", or "table_auto_increment"
	Name   string `json:"name"`   // vindex name, table name, or empty for the keyspace
	Reason string `json:"reason"` // operator-facing explanation of the risk
}

// EncodeVSchemaMutations marshals VSchema mutations for plan change metadata.
// Returns "" for an empty list so the metadata key is omitted.
func EncodeVSchemaMutations(mutations []VSchemaMutation) (string, error) {
	if len(mutations) == 0 {
		return "", nil
	}
	b, err := json.Marshal(mutations)
	if err != nil {
		return "", fmt.Errorf("encode %d VSchema mutations: %w", len(mutations), err)
	}
	return string(b), nil
}

// ParseVSchemaMutations decodes the VSchema mutations recorded in a
// namespace's plan change metadata. Returns nil when the change carries none.
func ParseVSchemaMutations(metadata map[string]string) ([]VSchemaMutation, error) {
	raw := metadata[VSchemaMutationsMetadataKey]
	if raw == "" {
		return nil, nil
	}
	var mutations []VSchemaMutation
	if err := json.Unmarshal([]byte(raw), &mutations); err != nil {
		return nil, fmt.Errorf("decode VSchema mutations metadata: %w", err)
	}
	return mutations, nil
}

// VSchemaUnsafeChanges returns the unsafe-change view of this namespace's
// recorded VSchema deletions and mutations. Metadata that cannot be decoded
// fails closed: the namespace reports an unsafe change explaining that the
// record is present but unreadable, so a corrupt record can never bypass the
// opt-in gate. Each record fails closed independently so one corrupt key
// never hides the other's disclosures.
func (sc *SchemaChangeResponse) VSchemaUnsafeChanges() []UnsafeChange {
	var result []UnsafeChange
	deletions, err := ParseVSchemaDeletions(sc.Metadata)
	if err != nil {
		result = append(result, UnsafeChange{
			Table:      sc.Namespace + "/vschema.json",
			Reason:     "VSchema deletions were recorded on this plan but could not be decoded, so the VSchema change is treated as unsafe",
			ChangeType: VSchemaChangeType,
		})
	}
	for _, d := range deletions {
		result = append(result, UnsafeChange{
			Table:      sc.Namespace + "/vschema.json",
			Reason:     d.Reason,
			ChangeType: VSchemaChangeType,
		})
	}
	mutations, err := ParseVSchemaMutations(sc.Metadata)
	if err != nil {
		result = append(result, UnsafeChange{
			Table:      sc.Namespace + "/vschema.json",
			Reason:     "VSchema mutations were recorded on this plan but could not be decoded, so the VSchema change is treated as unsafe",
			ChangeType: VSchemaChangeType,
		})
	}
	for _, m := range mutations {
		result = append(result, UnsafeChange{
			Table:      sc.Namespace + "/vschema.json",
			Reason:     m.Reason,
			ChangeType: VSchemaChangeType,
		})
	}
	return result
}

// HasVSchemaWork reports whether a plan change's metadata records VSchema work.
//
// Either key alone means work, because they are two annotations of the same
// thing: an engine records a rendered diff when it has one and the flag when the
// work is known without one.
//
// Review reads the annotations here: the surface that renders a namespace's
// work, the comparison that judges it, the grouping key derived from that
// comparison, and the count of members with work that the check reads cannot
// come to different answers about the same change. Storage and
// apply still test the flag directly, which agrees with this for every plan an
// engine produces today, since an engine that records a diff records the flag
// with it. An engine that recorded only the diff would be described as changing
// its VSchema and persisted as not changing it, so widening those callers is
// what keeps the two halves from splitting.
func HasVSchemaWork(metadata map[string]string) bool {
	return metadata[VSchemaDiffMetadataKey] != "" || metadata[VSchemaChangedMetadataKey] == "true"
}

// HasVSchemaChange reports whether this namespace's change carries VSchema work.
func (sc *SchemaChangeResponse) HasVSchemaChange() bool {
	return HasVSchemaWork(sc.Metadata)
}

// VSchemaGeneratedOnlyMetadataKey is the plan change-metadata key ("true")
// under which an engine says a namespace's VSchema change is made up entirely
// of what the engine generates from the plan's DDL, so it has no hand-written
// diff to show. It mirrors engine.MetadataVSchemaGeneratedOnly; apitypes keeps
// its own copy so this package stays dependency-free.
const VSchemaGeneratedOnlyMetadataKey = "vschema_generated_only"

// ShowsVSchemaChange reports whether plan surfaces show this namespace's
// VSchema work as a VSchema change. A change the engine generates entirely
// from the plan's DDL, with no diff to review, is left to the DDL and the
// finalize that writes it, the same as any other keyspace the engine
// finalizes after its DDL. It is shown as a VSchema change whenever it
// records a deletion or mutation, so an unsafe VSchema change is never
// hidden, and when there is no finalize to write it, so the namespace's work
// never drops out of the plan.
func (sc *SchemaChangeResponse) ShowsVSchemaChange() bool {
	if !sc.HasVSchemaChange() {
		return false
	}
	return !sc.vschemaChangeGeneratedFromDDL()
}

// vschemaChangeGeneratedFromDDL reports whether the engine marked this
// namespace's VSchema change generated from the DDL, sent no diff and no
// deletion or mutation record for it, and scheduled the finalize that writes
// it.
func (sc *SchemaChangeResponse) vschemaChangeGeneratedFromDDL() bool {
	generatedOnly := sc.Metadata[VSchemaGeneratedOnlyMetadataKey] == "true"
	noDiff := sc.Metadata[VSchemaDiffMetadataKey] == ""
	noUnsafeRecord := sc.Metadata[VSchemaDeletionsMetadataKey] == "" && sc.Metadata[VSchemaMutationsMetadataKey] == ""
	return generatedOnly && noDiff && noUnsafeRecord && sc.NeedsFinalizer()
}

// NeedsFinalizerMetadataKey is the plan change-metadata key ("true") under
// which an engine asks for a namespace's group finalizer to run once its DDL
// lands, independent of a VSchema change. It mirrors
// engine.MetadataNeedsFinalizer; apitypes keeps its own copy so this package
// stays dependency-free.
const NeedsFinalizerMetadataKey = "needs_finalizer"

// NeedsFinalizer reports whether the engine asked for this namespace to be
// finalized after its DDL. The finalizer is work an apply runs, so a plan whose
// only work is a finalizer still has changes.
func (sc *SchemaChangeResponse) NeedsFinalizer() bool {
	return sc.Metadata[NeedsFinalizerMetadataKey] == "true"
}

// VSchemaChange is one keyspace's VSchema application state for display. Each
// keyspace that changes its VSchema carries its own status and diff so a
// multi-keyspace deploy renders each keyspace independently.
type VSchemaChange struct {
	Namespace string `json:"namespace"`
	Status    string `json:"status"` // "applying", "applied", "failed", "cancelled", "stopped", or "" (pending)
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
