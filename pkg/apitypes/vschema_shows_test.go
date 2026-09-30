package apitypes

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

// Plan surfaces show a namespace's VSchema work as a VSchema change unless the
// engine generated all of it from the plan's DDL, sent no diff and no deletion
// or mutation record, and finalizes the namespace, in which case the finalize
// is what they show.
func TestShowsVSchemaChange(t *testing.T) {
	cases := []struct {
		name     string
		metadata map[string]string
		want     bool
	}{
		{"DDL only", nil, false},
		{"changed flag", map[string]string{VSchemaChangedMetadataKey: "true"}, true},
		{"rendered diff", map[string]string{VSchemaDiffMetadataKey: "+    \"refunds\": {}"}, true},
		{"generated and finalized", map[string]string{VSchemaChangedMetadataKey: "true", VSchemaGeneratedOnlyMetadataKey: "true", NeedsFinalizerMetadataKey: "true"}, false},
		{"generated with no finalize to show", map[string]string{VSchemaChangedMetadataKey: "true", VSchemaGeneratedOnlyMetadataKey: "true"}, true},
		{"generated but a diff was rendered", map[string]string{VSchemaDiffMetadataKey: "+    \"refunds\": {}", VSchemaGeneratedOnlyMetadataKey: "true", NeedsFinalizerMetadataKey: "true"}, true},
		{"finalize only", map[string]string{NeedsFinalizerMetadataKey: "true"}, false},
		{"changed and finalized without the marker", map[string]string{VSchemaChangedMetadataKey: "true", NeedsFinalizerMetadataKey: "true"}, true},
		{"generated with a recorded deletion", map[string]string{VSchemaChangedMetadataKey: "true", VSchemaGeneratedOnlyMetadataKey: "true", NeedsFinalizerMetadataKey: "true", VSchemaDeletionsMetadataKey: `[{"kind":"table","name":"refund_notes","reason":"r"}]`}, true},
		{"generated with a recorded mutation", map[string]string{VSchemaChangedMetadataKey: "true", VSchemaGeneratedOnlyMetadataKey: "true", NeedsFinalizerMetadataKey: "true", VSchemaMutationsMetadataKey: `[{"kind":"vindex_type","name":"hash","reason":"r"}]`}, true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			sc := &SchemaChangeResponse{Namespace: "payments_001", Metadata: tc.metadata}
			assert.Equal(t, tc.want, sc.ShowsVSchemaChange())
		})
	}
}
