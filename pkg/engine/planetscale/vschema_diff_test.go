package planetscale

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/encoding/protojson"
	vschemapb "vitess.io/vitess/go/vt/proto/vschema"
)

func TestVSchemaChanged(t *testing.T) {
	tests := []struct {
		name     string
		current  string
		desired  string
		expected bool
	}{
		{
			name:     "identical",
			current:  `{"sharded": true, "tables": {"users": {}}}`,
			desired:  `{"sharded": true, "tables": {"users": {}}}`,
			expected: false,
		},
		{
			name:     "different key order same content",
			current:  `{"tables": {"users": {}}, "sharded": true}`,
			desired:  `{"sharded": true, "tables": {"users": {}}}`,
			expected: false,
		},
		{
			name:     "whitespace differences",
			current:  `{ "sharded" :  true }`,
			desired:  `{"sharded":true}`,
			expected: false,
		},
		{
			name:     "both empty",
			current:  "",
			desired:  "",
			expected: false,
		},
		{
			name:     "empty vs empty object",
			current:  "",
			desired:  "{}",
			expected: false,
		},
		{
			name:     "empty vs empty tables",
			current:  "{}",
			desired:  `{"tables": {}}`,
			expected: false,
		},
		{
			name:     "empty string vs empty tables",
			current:  "",
			desired:  `{"tables": {}}`,
			expected: false,
		},
		{
			name:     "empty vs empty vindexes",
			current:  "{}",
			desired:  `{"vindexes": {}}`,
			expected: false,
		},
		{
			name:     "value changed",
			current:  `{"sharded": true}`,
			desired:  `{"sharded": false}`,
			expected: true,
		},
		{
			name:     "field added",
			current:  `{"sharded": true}`,
			desired:  `{"sharded": true, "tables": {"users": {}}}`,
			expected: true,
		},
		{
			name:     "field removed",
			current:  `{"sharded": true, "tables": {"users": {}}}`,
			desired:  `{"sharded": true}`,
			expected: true,
		},
		{
			name:     "empty to populated",
			current:  "",
			desired:  `{"sharded": true, "tables": {"users": {}}}`,
			expected: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := VSchemaChanged(tt.current, tt.desired)
			assert.Equal(t, tt.expected, got)
		})
	}
}

func TestVSchemaDiff(t *testing.T) {
	tests := []struct {
		name           string
		current        string
		desired        string
		expectEmpty    bool
		expectContains []string
	}{
		{
			name:        "identical returns empty",
			current:     `{"sharded": true}`,
			desired:     `{"sharded": true}`,
			expectEmpty: true,
		},
		{
			name:    "sharded true to false shows diff (false is proto default, stripped)",
			current: `{"sharded": true}`,
			desired: `{"sharded": false}`,
			expectContains: []string{
				"-  \"sharded\": true",
			},
		},
		{
			name:    "field added",
			current: `{"sharded": true}`,
			desired: `{"sharded": true, "tables": {"users": {}}}`,
			expectContains: []string{
				"+  \"tables\"",
				"+    \"users\"",
			},
		},
		{
			name:    "empty to populated",
			current: "",
			desired: `{"sharded": true, "vindexes": {"hash": {"type": "hash"}}}`,
			expectContains: []string{
				"+  \"sharded\": true",
				"+  \"vindexes\"",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			diff := VSchemaDiff(tt.current, tt.desired)
			if tt.expectEmpty {
				assert.Empty(t, diff, "expected empty diff")
				return
			}
			require.NotEmpty(t, diff, "expected non-empty diff")
			for _, s := range tt.expectContains {
				assert.Contains(t, diff, s)
			}
		})
	}
}

func TestNormalizeVSchemaJSON_Deterministic(t *testing.T) {
	// Same VSchema with different key ordering should normalize identically
	a := `{"tables":{"users":{},"orders":{}},"vindexes":{"hash":{"type":"hash"}},"sharded":true}`
	b := `{"sharded":true,"vindexes":{"hash":{"type":"hash"}},"tables":{"orders":{},"users":{}}}`

	assert.Equal(t, normalizeVSchemaJSON(a), normalizeVSchemaJSON(b),
		"same VSchema with different key order should normalize identically")
}

func TestNormalizeVSchemaJSON_RepeatedCalls(t *testing.T) {
	// Normalizing the same string multiple times should always produce the same result
	vs := `{"sharded":true,"vindexes":{"hash":{"type":"hash"}},"tables":{"users":{"column_vindexes":[{"column":"id","name":"hash"}]}}}`
	first := normalizeVSchemaJSON(vs)
	for range 10 {
		assert.Equal(t, first, normalizeVSchemaJSON(vs),
			"repeated normalization should be stable")
	}
}

func TestVSchemaChanged_RoundTrip(t *testing.T) {
	// Normalize, then compare against the normalized form — should not detect a change
	vs := `{"sharded":true,"vindexes":{"hash":{"type":"hash"}},"tables":{"users":{"column_vindexes":[{"column":"id","name":"hash"}]},"orders":{"column_vindexes":[{"column":"user_id","name":"hash"}]}}}`
	normalized := normalizeVSchemaJSON(vs)
	assert.False(t, VSchemaChanged(vs, normalized),
		"original vs normalized should not be detected as changed")
	assert.False(t, VSchemaChanged(normalized, vs),
		"normalized vs original should not be detected as changed")
}

func TestVSchemaChanged_SequenceTablesUnsharded(t *testing.T) {
	// Simulates the testapp keyspace: sequence tables only, unsharded
	fileVSchema := `{"tables":{"users_seq":{"type":"sequence"},"orders_seq":{"type":"sequence"},"products_seq":{"type":"sequence"}}}`
	// API might return with different formatting or field ordering
	apiVSchema := `{"tables": {"orders_seq": {"type": "sequence"}, "products_seq": {"type": "sequence"}, "users_seq": {"type": "sequence"}}}`

	assert.False(t, VSchemaChanged(apiVSchema, fileVSchema),
		"sequence tables VSchema should match regardless of key ordering or whitespace")
}

func TestVSchemaChanged_ShardedWithVindexes(t *testing.T) {
	// Simulates testapp_sharded: full VSchema with vindexes and column_vindexes
	fileVSchema := `{"sharded":true,"vindexes":{"hash":{"type":"hash"}},"tables":{"users":{"column_vindexes":[{"column":"id","name":"hash"}],"auto_increment":{"column":"id","sequence":"users_seq"}},"orders":{"column_vindexes":[{"column":"user_id","name":"hash"}],"auto_increment":{"column":"id","sequence":"orders_seq"}},"products":{"column_vindexes":[{"column":"id","name":"hash"}],"auto_increment":{"column":"id","sequence":"products_seq"}}}}`
	// API returns same content, different ordering
	apiVSchema := `{"vindexes":{"hash":{"type":"hash"}},"sharded":true,"tables":{"products":{"auto_increment":{"sequence":"products_seq","column":"id"},"column_vindexes":[{"name":"hash","column":"id"}]},"users":{"auto_increment":{"sequence":"users_seq","column":"id"},"column_vindexes":[{"name":"hash","column":"id"}]},"orders":{"auto_increment":{"sequence":"orders_seq","column":"id"},"column_vindexes":[{"name":"hash","column":"user_id"}]}}}`

	assert.False(t, VSchemaChanged(apiVSchema, fileVSchema),
		"same sharded VSchema with different key ordering should not be detected as changed")
}

func TestVSchemaChanged_EmptyTables(t *testing.T) {
	// Some tables have empty config {} — should be identical
	fileVSchema := `{"tables":{"users":{}}}`
	apiVSchema := `{"tables":{"users":{}}}`
	assert.False(t, VSchemaChanged(apiVSchema, fileVSchema))
}

func TestVSchemaChanged_ProtoDefaultStripping(t *testing.T) {
	// "sharded": false is a proto default — normalizer should strip it
	withShardedFalse := `{"sharded":false,"tables":{"users":{"type":"sequence"}}}`
	withoutSharded := `{"tables":{"users":{"type":"sequence"}}}`
	assert.False(t, VSchemaChanged(withShardedFalse, withoutSharded),
		"sharded:false (proto default) vs absent should not be detected as changed")
}

func TestVSchemaChanged_RealDifference(t *testing.T) {
	// Actual VSchema change: adding a table entry
	before := `{"tables":{"users":{"type":"sequence"}}}`
	after := `{"tables":{"users":{"type":"sequence"},"orders":{"type":"sequence"}}}`
	assert.True(t, VSchemaChanged(before, after),
		"adding a table should be detected as a real change")
}

func TestVSchemaChanged_RealDifference_RemoveTable(t *testing.T) {
	before := `{"tables":{"users":{"type":"sequence"},"orders":{"type":"sequence"}}}`
	after := `{"tables":{"users":{"type":"sequence"}}}`
	assert.True(t, VSchemaChanged(before, after),
		"removing a table should be detected as a real change")
}

func TestVSchemaChanged_ProtojsonRoundTrip(t *testing.T) {
	fileVSchema := `{"tables":{"users_seq":{"type":"sequence"},"orders_seq":{"type":"sequence"},"products_seq":{"type":"sequence"}}}`
	var ks vschemapb.Keyspace
	require.NoError(t, protojson.Unmarshal([]byte(fileVSchema), &ks))
	apiBytes, err := protojson.Marshal(&ks)
	require.NoError(t, err)
	apiVSchema := string(apiBytes)
	t.Logf("file normalized: %s", normalizeVSchemaJSON(fileVSchema))
	t.Logf("api normalized:  %s", normalizeVSchemaJSON(apiVSchema))
	assert.False(t, VSchemaChanged(apiVSchema, fileVSchema),
		"protojson round-trip should not produce a false positive")
}

func TestVSchemaChanged_ProtojsonRoundTrip_Sharded(t *testing.T) {
	fileVSchema := `{"sharded":true,"vindexes":{"hash":{"type":"hash"}},"tables":{"users":{"column_vindexes":[{"column":"id","name":"hash"}],"auto_increment":{"column":"id","sequence":"users_seq"}},"orders":{"column_vindexes":[{"column":"user_id","name":"hash"}],"auto_increment":{"column":"id","sequence":"orders_seq"}}}}`
	var ks vschemapb.Keyspace
	require.NoError(t, protojson.Unmarshal([]byte(fileVSchema), &ks))
	apiBytes, err := protojson.Marshal(&ks)
	require.NoError(t, err)
	apiVSchema := string(apiBytes)
	t.Logf("file normalized: %s", normalizeVSchemaJSON(fileVSchema))
	t.Logf("api normalized:  %s", normalizeVSchemaJSON(apiVSchema))
	assert.False(t, VSchemaChanged(apiVSchema, fileVSchema),
		"protojson round-trip of sharded VSchema should not produce a false positive")
}
