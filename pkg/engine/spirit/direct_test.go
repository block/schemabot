package spirit

import (
	"database/sql"
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
	assert.Zero(t, policy.MaxTableBytes, "a policy without the byte key sets no byte bound")
}

// A byte bound is a complete policy on its own.
func TestDirectPolicyFromMetadata_ByteBound(t *testing.T) {
	policy, err := directPolicyFromMetadata(map[string]string{
		"direct_execution":                 "true",
		"direct_execution_max_table_bytes": "104857600",
	})
	require.NoError(t, err)
	assert.Equal(t, directPolicy{Enabled: true, MaxTableBytes: 104857600}, policy)
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
			wantErr: "neither direct_execution_max_table_rows nor direct_execution_max_table_bytes is set",
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
		"both bounds": {
			md:      map[string]string{"direct_execution": "true", "direct_execution_max_table_rows": "175000", "direct_execution_max_table_bytes": "104857600"},
			wantErr: "sets both direct_execution_max_table_rows and direct_execution_max_table_bytes: a policy sets exactly one size bound",
		},
		"empty row bound": {
			md:      map[string]string{"direct_execution": "true", "direct_execution_max_table_rows": "", "direct_execution_max_table_bytes": "104857600"},
			wantErr: `parse direct_execution_max_table_rows metadata value ""`,
		},
		"negative row bound beside a byte bound": {
			md:      map[string]string{"direct_execution": "true", "direct_execution_max_table_rows": "-1", "direct_execution_max_table_bytes": "104857600"},
			wantErr: "direct_execution_max_table_rows must be positive",
		},
		"non-numeric byte bound": {
			md:      map[string]string{"direct_execution": "true", "direct_execution_max_table_bytes": "100MiB"},
			wantErr: `parse direct_execution_max_table_bytes metadata value "100MiB"`,
		},
		"empty byte bound": {
			md:      map[string]string{"direct_execution": "true", "direct_execution_max_table_bytes": ""},
			wantErr: `parse direct_execution_max_table_bytes metadata value ""`,
		},
		"zero byte bound": {
			md:      map[string]string{"direct_execution": "true", "direct_execution_max_table_bytes": "0"},
			wantErr: "direct_execution_max_table_bytes must be positive",
		},
		"negative byte bound": {
			md:      map[string]string{"direct_execution": "true", "direct_execution_max_table_bytes": "-1"},
			wantErr: "direct_execution_max_table_bytes must be positive",
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
	require.ErrorContains(t, err, "neither direct_execution_max_table_rows nor direct_execution_max_table_bytes is set")
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

// The size gate compares only figures information_schema actually reported:
// a NULL or negative statistic is unavailable, never a zero that would slip
// under any bound. Zero itself is a real figure, as for an empty table.
func TestUsableStatistic(t *testing.T) {
	v, err := usableStatistic(sql.NullInt64{Int64: 45_800_000, Valid: true}, "DATA_LENGTH", "shop", "orders")
	require.NoError(t, err)
	assert.Equal(t, int64(45_800_000), v)

	v, err = usableStatistic(sql.NullInt64{Int64: 0, Valid: true}, "INDEX_LENGTH", "shop", "orders")
	require.NoError(t, err)
	assert.Zero(t, v)

	_, err = usableStatistic(sql.NullInt64{}, "DATA_LENGTH", "shop", "orders")
	require.Error(t, err)
	assert.Equal(t, "DATA_LENGTH for `shop`.`orders` is unavailable", err.Error())

	_, err = usableStatistic(sql.NullInt64{Int64: -1, Valid: true}, "INDEX_LENGTH", "shop", "orders")
	require.Error(t, err)
	assert.Equal(t, "INDEX_LENGTH for `shop`.`orders` is negative (-1), treating it as unavailable", err.Error())

	_, err = usableStatistic(sql.NullInt64{}, "TABLE_ROWS", "shop", "ord`ers")
	require.Error(t, err)
	assert.Equal(t, "TABLE_ROWS for `shop`.`ord``ers` is unavailable", err.Error(),
		"the name is quoted the way MySQL reads it, so the message names one table unambiguously")
}

// The bounded row count caps its scan at one row past the policy bound and
// escapes schema and table as identifiers, so a name containing a backtick
// still counts the table it names instead of changing the statement.
func TestBoundedRowCountQuery(t *testing.T) {
	assert.Equal(t,
		"SELECT COUNT(*) FROM (SELECT 1 FROM `shop`.`orders` LIMIT 1001) bounded",
		boundedRowCountQuery("shop", "orders", 1000))
	assert.Equal(t,
		"SELECT COUNT(*) FROM (SELECT 1 FROM `sh``op`.`ord``ers` LIMIT 11) bounded",
		boundedRowCountQuery("sh`op", "ord`ers", 10))
}
