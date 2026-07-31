package spirit

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Absent or explicitly disabled metadata resolves to the fail-closed zero
// policy: refused statements stay blocked.
func TestDirectPolicyFromMetadata_Disabled(t *testing.T) {
	for name, md := range map[string]map[string]string{
		"nil metadata":    nil,
		"empty metadata":  {},
		"explicit false":  {"direct_execution": "false"},
		"numeric false":   {"direct_execution": "0"},
		"bound but off":   {"direct_execution": "false", "direct_execution_max_table_rows": "1000"},
		"bound alone off": {"direct_execution_max_table_rows": "1000"},
	} {
		t.Run(name, func(t *testing.T) {
			policy, err := directPolicyFromMetadata(md)
			require.NoError(t, err)
			assert.False(t, policy.Enabled)
			assert.Zero(t, policy.MaxTableRows)
		})
	}
}

// Enabling direct execution with a positive row bound resolves the policy,
// accepting any standard boolean spelling of the enable flag.
func TestDirectPolicyFromMetadata_Enabled(t *testing.T) {
	for _, enable := range []string{"true", "True", "1"} {
		t.Run(enable, func(t *testing.T) {
			policy, err := directPolicyFromMetadata(map[string]string{
				"direct_execution":                enable,
				"direct_execution_max_table_rows": "500000",
			})
			require.NoError(t, err)
			assert.True(t, policy.Enabled)
			assert.Equal(t, int64(500000), policy.MaxTableRows)
		})
	}
}

// A malformed policy is a hard error, never a silent fallback to disabled:
// enabling without a bound, a non-numeric or non-positive bound, and an
// unrecognized enable value are all rejected with the offending key named.
func TestDirectPolicyFromMetadata_Malformed(t *testing.T) {
	cases := map[string]struct {
		md      map[string]string
		wantErr string
	}{
		"enabled without bound": {
			md:      map[string]string{"direct_execution": "true"},
			wantErr: "direct_execution_max_table_rows is not set",
		},
		"non-numeric bound": {
			md:      map[string]string{"direct_execution": "true", "direct_execution_max_table_rows": "lots"},
			wantErr: `parse direct_execution_max_table_rows metadata value "lots"`,
		},
		"zero bound": {
			md:      map[string]string{"direct_execution": "true", "direct_execution_max_table_rows": "0"},
			wantErr: "must be positive",
		},
		"negative bound": {
			md:      map[string]string{"direct_execution": "true", "direct_execution_max_table_rows": "-5"},
			wantErr: "must be positive",
		},
		"unrecognized enable value": {
			md:      map[string]string{"direct_execution": "yes"},
			wantErr: `invalid direct_execution metadata value "yes"`,
		},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			_, err := directPolicyFromMetadata(tc.md)
			require.Error(t, err)
			assert.Contains(t, err.Error(), tc.wantErr)
		})
	}
}
