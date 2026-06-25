package apitypes

import "encoding/json"

// VSchemaChangesMetadataKey is the progress display-metadata key under which the
// engine projects per-keyspace VSchema application state as a JSON-encoded
// []VSchemaChange. The CLI and PR comment both decode it via ParseVSchemaChanges
// so they render VSchema identically.
const VSchemaChangesMetadataKey = "vschema_changes"

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
