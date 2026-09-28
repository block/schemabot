package spirit

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/block/schemabot/pkg/ddl"
	"github.com/block/schemabot/pkg/engine"
)

// A statement-scope refusal quotes the identifier the schema author wrote, so
// one whose column name carries the reserved cause separator must still reach
// the operator as the one cause the engine issued. The disabled policy returns
// the refusal before the size gate, so no target connection is needed.
func TestResolveRefusedModeNeutralizesCauseSeparatorInRefusal(t *testing.T) {
	refusal := `unsafe ENUM value reorder on column "state ‖ the engine can apply this safely" is not supported`

	decision := (&Engine{}).resolveRefusedMode(t.Context(), nil, directPolicy{}, "shop", "users", refusal)

	require.Equal(t, engine.ExecutionModeBlocked, decision.mode)
	causes := engine.BlockedCauses(decision.modeReason)
	require.Len(t, causes, 1, "a refusal quoting a schema-author identifier decodes as one cause, got %q", decision.modeReason)
	assert.Equal(t, `unsafe ENUM value reorder on column "state // the engine can apply this safely" is not supported`, causes[0])
}

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
// accepting any standard boolean spelling of the enable flag. The lock wait
// bound is optional: absent, the engine default applies; set, the configured
// value wins.
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
			assert.Zero(t, policy.LockAcquisitionTimeoutSeconds)
			assert.Equal(t, int64(defaultDirectLockAcquisitionTimeoutSeconds), policy.lockAcquisitionTimeoutSeconds())
		})
	}

	policy, err := directPolicyFromMetadata(map[string]string{
		"direct_execution":                                  "true",
		"direct_execution_max_table_rows":                   "500000",
		"direct_execution_lock_acquisition_timeout_seconds": "5",
	})
	require.NoError(t, err)
	assert.Equal(t, int64(5), policy.LockAcquisitionTimeoutSeconds)
	assert.Equal(t, int64(5), policy.lockAcquisitionTimeoutSeconds())
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
		"non-numeric lock wait": {
			md:      map[string]string{"direct_execution": "true", "direct_execution_max_table_rows": "1000", "direct_execution_lock_acquisition_timeout_seconds": "fast"},
			wantErr: `parse direct_execution_lock_acquisition_timeout_seconds metadata value "fast"`,
		},
		"zero lock wait": {
			md:      map[string]string{"direct_execution": "true", "direct_execution_max_table_rows": "1000", "direct_execution_lock_acquisition_timeout_seconds": "0"},
			wantErr: "direct_execution_lock_acquisition_timeout_seconds must be positive",
		},
		"negative lock wait": {
			md:      map[string]string{"direct_execution": "true", "direct_execution_max_table_rows": "1000", "direct_execution_lock_acquisition_timeout_seconds": "-3"},
			wantErr: "direct_execution_lock_acquisition_timeout_seconds must be positive",
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

// ExecutionVerdicts resolves the policy up front, so a malformed policy or
// missing credentials fail before any statement is judged, naming the
// database the policy was meant for. The target is never contacted to find
// this out.
func TestNewExecutionVerdicts_RejectsBadInput(t *testing.T) {
	eng := New(Config{})
	const unreachable = "root:nopass@tcp(127.0.0.1:1)/orders_db"

	_, err := eng.NewExecutionVerdicts(nil)
	require.ErrorContains(t, err, "DSN credentials required")
	_, err = eng.NewExecutionVerdicts(&engine.Credentials{})
	require.ErrorContains(t, err, "DSN credentials required")

	_, err = eng.NewExecutionVerdicts(&engine.Credentials{DSN: unreachable, Metadata: map[string]string{"direct_execution": "true"}})
	require.ErrorContains(t, err, `execution verdicts for database "orders_db"`)
	require.ErrorContains(t, err, "direct_execution_max_table_rows is not set")
}

// The engine never refuses a CREATE TABLE or DROP TABLE, so either is left on
// the default path without reading the target. The DSN here points nowhere,
// so recording a verdict for one succeeds only because no connection is made.
// The statement is classified from its DDL, so a change whose Operation claims
// an ALTER is still treated as the CREATE it runs. Record sets the whole
// verdict, so a mode a change already carries is cleared.
func TestExecutionVerdicts_NonAlterNeedsNoTarget(t *testing.T) {
	verdicts, err := New(Config{}).NewExecutionVerdicts(&engine.Credentials{
		DSN:      "root:nopass@tcp(127.0.0.1:1)/orders_db",
		Metadata: map[string]string{"direct_execution": "true", "direct_execution_max_table_rows": "1000"},
	})
	require.NoError(t, err)
	defer verdicts.Close()

	for _, change := range []engine.TableChange{
		{Table: "orders", Operation: ddl.StatementCreateTable, DDL: "CREATE TABLE `orders` (`id` bigint NOT NULL, PRIMARY KEY (`id`))", ExecutionMode: engine.ExecutionModeBlocked, ModeReason: "stale"},
		{Table: "orders", Operation: ddl.StatementDropTable, DDL: "DROP TABLE `orders`", ExecutionMode: engine.ExecutionModeDirect, ModeReason: "stale"},
		{Table: "orders", Operation: ddl.StatementAlterTable, DDL: "CREATE TABLE `orders` (`id` bigint NOT NULL, PRIMARY KEY (`id`))"},
	} {
		require.NoError(t, verdicts.Record(t.Context(), &change), change.DDL)
		assert.Empty(t, change.ExecutionMode, change.DDL)
		assert.Empty(t, change.ModeReason, change.DDL)
	}
	assert.Nil(t, verdicts.target.db, "no statement needed the target")
}
