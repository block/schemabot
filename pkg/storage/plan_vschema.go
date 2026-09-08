package storage

import (
	"encoding/json"
	"sort"
)

// Plan change-metadata keys persisted into NamespacePlanData.Metadata. These
// mirror the keys engines annotate plan changes with (and pkg/apitypes exposes
// on the live-plan API): the wire names must match so a value recorded at plan
// time reads back identically from the stored plan.
const (
	// PlanMetadataVSchemaChanged is "true" when the namespace's plan change
	// carries VSchema work.
	PlanMetadataVSchemaChanged = "vschema_changed"

	// PlanMetadataVSchemaDiff holds the rendered VSchema diff the engine
	// annotated on the namespace's plan change. Persisting it lets apply-time
	// surfaces (PR comments) show the diff the operator approved at plan time,
	// rather than re-diffing against live state that changes as the apply
	// lands. Display-only: no safety gate reads it.
	PlanMetadataVSchemaDiff = "vschema"

	// PlanMetadataVSchemaDeletions holds the structural removals in the
	// namespace's VSchema change as a JSON-encoded list of {kind, name,
	// reason} records. A removal changes Vitess query routing the moment the
	// VSchema is applied, so any recorded deletion makes the stored plan's
	// VSchema change an unsafe change requiring the same operator opt-in as
	// destructive DDL.
	PlanMetadataVSchemaDeletions = "vschema_deletions"

	// PlanMetadataVSchemaMutations holds the in-place vindex definition
	// changes in the namespace's VSchema change as a JSON-encoded list of
	// {kind, name, reason} records. A mutation keeps the vindex's name but
	// changes how Vitess routes through it, so it requires the same opt-in
	// as a removal.
	PlanMetadataVSchemaMutations = "vschema_mutations"
)

// VSchemaPlanMetadata extracts the subset of an engine's plan change-metadata
// that must survive plan persistence: the keys apply-time safety gates read
// (the VSchema-changed flag and the recorded structural deletions and vindex
// mutations) plus the rendered diff apply-time display reads. Every plan
// persistence site uses this helper so stored plans carry the same metadata
// regardless of which plane persisted them. Returns nil for a change without
// VSchema work, so such namespaces store no metadata.
func VSchemaPlanMetadata(metadata map[string]string) map[string]string {
	if metadata[PlanMetadataVSchemaChanged] != "true" {
		return nil
	}
	persisted := map[string]string{PlanMetadataVSchemaChanged: "true"}
	for _, key := range []string{PlanMetadataVSchemaDeletions, PlanMetadataVSchemaMutations, PlanMetadataVSchemaDiff} {
		if raw := metadata[key]; raw != "" {
			persisted[key] = raw
		}
	}
	return persisted
}

// VSchemaUnsafeChange is one namespace's VSchema change that requires explicit
// unsafe opt-in before queueing operator work: a recorded structural removal
// or vindex mutation, or a VSchema change whose record is missing or
// unreadable.
type VSchemaUnsafeChange struct {
	Namespace string
	Reason    string
}

// vschemaChangeRecord mirrors the engine-side deletion and mutation records
// persisted under PlanMetadataVSchemaDeletions / PlanMetadataVSchemaMutations.
// Storage keeps its own copy so reading stored plans does not depend on
// engine packages.
type vschemaChangeRecord struct {
	Kind   string `json:"kind"`
	Name   string `json:"name"`
	Reason string `json:"reason"`
}

// UnsafeVSchemaChanges returns the stored plan's VSchema changes that require
// explicit unsafe opt-in. It fails closed on uncertainty: a namespace that
// changes its VSchema without persisted change-metadata has no deletion or
// mutation record to consult, a namespace carrying change-metadata without a
// desired VSchema document is a divergent record whose disclosures cannot be
// trusted, and a record that cannot be decoded may hide one — all are
// reported as unsafe rather than skipped, so neither an old stored plan, a
// divergent one, nor a corrupt record can bypass the opt-in gate. Each record
// key fails closed independently so one corrupt key never hides the other's
// disclosures. Re-planning records fresh metadata and clears the ambiguity.
func (p *Plan) UnsafeVSchemaChanges() []VSchemaUnsafeChange {
	if p == nil {
		return nil
	}
	var result []VSchemaUnsafeChange
	for _, namespace := range p.vschemaGateNamespaces() {
		nsData := p.Namespaces[namespace]
		if !nsData.ChangesVSchema() {
			result = append(result, VSchemaUnsafeChange{
				Namespace: namespace,
				Reason:    "the stored plan records VSchema change-metadata for this namespace but no desired VSchema document, so it is treated as unsafe; re-plan to record both",
			})
			continue
		}
		meta := nsData.Metadata
		if meta[PlanMetadataVSchemaChanged] != "true" {
			result = append(result, VSchemaUnsafeChange{
				Namespace: namespace,
				Reason:    "the stored plan does not record whether this VSchema change removes or mutates vindexes or routing entries, so it is treated as unsafe; re-plan to record it",
			})
			continue
		}
		result = append(result, unsafeVSchemaRecords(namespace, meta, PlanMetadataVSchemaDeletions,
			"VSchema deletions were recorded on this plan but could not be decoded, so the VSchema change is treated as unsafe; re-plan to record them")...)
		result = append(result, unsafeVSchemaRecords(namespace, meta, PlanMetadataVSchemaMutations,
			"VSchema mutations were recorded on this plan but could not be decoded, so the VSchema change is treated as unsafe; re-plan to record them")...)
	}
	return result
}

// vschemaGateNamespaces returns, in sorted order, every namespace the unsafe
// gate must inspect: those changing their VSchema (carrying the desired
// document) and those carrying VSchema change-metadata. The union matters —
// a namespace with metadata but no document is a divergent record the gate
// must fail closed on, not skip.
func (p *Plan) vschemaGateNamespaces() []string {
	var namespaces []string
	for namespace, nsData := range p.Namespaces {
		if nsData.ChangesVSchema() || len(nsData.vschemaMetadata()) > 0 {
			namespaces = append(namespaces, namespace)
		}
	}
	sort.Strings(namespaces)
	return namespaces
}

// vschemaMetadata returns the namespace's persisted VSchema change-metadata,
// nil-safe for absent namespaces.
func (n *NamespacePlanData) vschemaMetadata() map[string]string {
	if n == nil {
		return nil
	}
	return n.Metadata
}

// unsafeVSchemaRecords decodes one persisted VSchema record key into unsafe
// changes, one per record with its operator-facing reason. A record that
// cannot be decoded fails closed as a single unsafe change carrying
// undecodableReason.
func unsafeVSchemaRecords(namespace string, meta map[string]string, key, undecodableReason string) []VSchemaUnsafeChange {
	raw := meta[key]
	if raw == "" {
		return nil
	}
	var records []vschemaChangeRecord
	if err := json.Unmarshal([]byte(raw), &records); err != nil {
		return []VSchemaUnsafeChange{{Namespace: namespace, Reason: undecodableReason}}
	}
	result := make([]VSchemaUnsafeChange, 0, len(records))
	for _, r := range records {
		result = append(result, VSchemaUnsafeChange{Namespace: namespace, Reason: r.Reason})
	}
	return result
}
