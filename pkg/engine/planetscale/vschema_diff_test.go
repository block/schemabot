package planetscale

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
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
