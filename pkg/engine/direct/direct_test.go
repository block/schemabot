package direct

import (
	"context"
	"errors"
	"log/slog"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/block/schemabot/pkg/engine"
)

// Absent or explicitly disabled metadata resolves to the fail-closed zero
// policy: refused statements stay blocked.
func TestPolicyFromMetadata_Disabled(t *testing.T) {
	for name, md := range map[string]map[string]string{
		"nil metadata":    nil,
		"empty metadata":  {},
		"explicit false":  {"direct_execution": "false"},
		"numeric false":   {"direct_execution": "0"},
		"bound but off":   {"direct_execution": "false", "direct_execution_max_table_rows": "1000"},
		"bound alone off": {"direct_execution_max_table_rows": "1000"},
	} {
		t.Run(name, func(t *testing.T) {
			policy, err := PolicyFromMetadata(md)
			require.NoError(t, err)
			assert.False(t, policy.Enabled)
			assert.Zero(t, policy.MaxTableRows)
		})
	}
}

// Enabling direct execution with a positive row bound resolves the policy,
// accepting any standard boolean spelling of the enable flag. The lock wait
// bound is optional: absent, the engine's default applies; set, the
// configured value wins.
func TestPolicyFromMetadata_Enabled(t *testing.T) {
	const engineDefault = 10
	for _, enable := range []string{"true", "True", "1"} {
		t.Run(enable, func(t *testing.T) {
			policy, err := PolicyFromMetadata(map[string]string{
				"direct_execution":                enable,
				"direct_execution_max_table_rows": "500000",
			})
			require.NoError(t, err)
			assert.True(t, policy.Enabled)
			assert.Equal(t, int64(500000), policy.MaxTableRows)
			assert.Zero(t, policy.LockAcquisitionTimeoutSeconds)
			assert.Equal(t, int64(engineDefault), policy.EffectiveLockAcquisitionTimeoutSeconds(engineDefault))
		})
	}

	policy, err := PolicyFromMetadata(map[string]string{
		"direct_execution":                                  "true",
		"direct_execution_max_table_rows":                   "500000",
		"direct_execution_lock_acquisition_timeout_seconds": "5",
	})
	require.NoError(t, err)
	assert.Equal(t, int64(5), policy.LockAcquisitionTimeoutSeconds)
	assert.Equal(t, int64(5), policy.EffectiveLockAcquisitionTimeoutSeconds(engineDefault))
}

// A malformed policy is a hard error, never a silent fallback to disabled:
// enabling without a bound, a non-numeric or non-positive bound, and an
// unrecognized enable value are all rejected with the offending key named.
func TestPolicyFromMetadata_Malformed(t *testing.T) {
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
			_, err := PolicyFromMetadata(tc.md)
			require.Error(t, err)
			assert.Contains(t, err.Error(), tc.wantErr)
		})
	}
}

// fakeSizer scripts the size gate's two measurements so each resolver branch
// can be exercised without a database.
type fakeSizer struct {
	estimate    int64
	estimateErr error
	exact       int64
	exactErr    error
}

func (f *fakeSizer) EstimatedRows(_ context.Context, _ string) (int64, error) {
	return f.estimate, f.estimateErr
}

func (f *fakeSizer) ExactRowsWithin(_ context.Context, _ string, _ int64) (int64, error) {
	return f.exact, f.exactErr
}

func testResolver(sizer SizeEstimator) Resolver {
	return Resolver{
		Logger:    slog.Default(),
		Estimator: sizer,
		RunsAs:    "native test DDL",
	}
}

const testRefusal = "dropping primary key is not supported"

// A disabled policy blocks without touching the size gate, and the mode
// reason is the engine's refusal verbatim — there is no size context to add.
func TestResolveRefusedMode_PolicyDisabledBlocks(t *testing.T) {
	sizer := &fakeSizer{estimateErr: errors.New("size gate must not run")}
	decision, err := testResolver(sizer).ResolveRefusedMode(t.Context(), Policy{}, "orders", "users", testRefusal)
	require.NoError(t, err)
	assert.Equal(t, engine.ExecutionModeBlocked, decision.Mode)
	assert.Equal(t, testRefusal, decision.ModeReason)
	assert.Equal(t, OutcomeBlockedPolicyDisabled, decision.Outcome)
}

// An estimator that cannot produce an estimate blocks: unknown size is never
// assumed small, and the verdict says the row count is unavailable.
func TestResolveRefusedMode_EstimateUnavailableBlocks(t *testing.T) {
	sizer := &fakeSizer{estimateErr: errors.New("connect to the target: connection refused")}
	policy := Policy{Enabled: true, MaxTableRows: 1000}
	decision, err := testResolver(sizer).ResolveRefusedMode(t.Context(), policy, "orders", "users", testRefusal)
	require.NoError(t, err)
	assert.Equal(t, engine.ExecutionModeBlocked, decision.Mode)
	assert.Contains(t, decision.ModeReason, testRefusal)
	assert.Contains(t, decision.ModeReason, "row count is unavailable")
	assert.Equal(t, OutcomeBlockedSizeUnknown, decision.Outcome)
}

// An estimate above the bound blocks without running the exact count, and the
// reason names the configured limit — not the measured count — so identical
// verdicts on different shards collapse into one row in PR-facing summaries.
func TestResolveRefusedMode_EstimateAboveBoundBlocks(t *testing.T) {
	sizer := &fakeSizer{estimate: 5000, exactErr: errors.New("exact count must not run")}
	policy := Policy{Enabled: true, MaxTableRows: 1000}
	decision, err := testResolver(sizer).ResolveRefusedMode(t.Context(), policy, "orders", "users", testRefusal)
	require.NoError(t, err)
	assert.Equal(t, engine.ExecutionModeBlocked, decision.Mode)
	assert.Contains(t, decision.ModeReason, "above the configured limit of 1,000 rows")
	assert.NotContains(t, decision.ModeReason, "5,000")
	assert.Equal(t, OutcomeBlockedSizeLimit, decision.Outcome)
	assert.Equal(t, int64(5000), decision.Rows)
}

// A failed exact count blocks even when the estimate was within bound: the
// estimate is trusted to block, never to approve on its own.
func TestResolveRefusedMode_ExactCountUnavailableBlocks(t *testing.T) {
	sizer := &fakeSizer{estimate: 500, exactErr: errors.New("count rows: table lock timeout")}
	policy := Policy{Enabled: true, MaxTableRows: 1000}
	decision, err := testResolver(sizer).ResolveRefusedMode(t.Context(), policy, "orders", "users", testRefusal)
	require.NoError(t, err)
	assert.Equal(t, engine.ExecutionModeBlocked, decision.Mode)
	assert.Contains(t, decision.ModeReason, "row count is unavailable")
	assert.Equal(t, OutcomeBlockedSizeUnknown, decision.Outcome)
}

// An exact count above the bound blocks even when the estimate was under it —
// the corroboration step exists exactly because the estimate can undercount.
func TestResolveRefusedMode_ExactCountAboveBoundBlocks(t *testing.T) {
	sizer := &fakeSizer{estimate: 500, exact: 1001}
	policy := Policy{Enabled: true, MaxTableRows: 1000}
	decision, err := testResolver(sizer).ResolveRefusedMode(t.Context(), policy, "orders", "users", testRefusal)
	require.NoError(t, err)
	assert.Equal(t, engine.ExecutionModeBlocked, decision.Mode)
	assert.Contains(t, decision.ModeReason, "above the configured limit of 1,000 rows")
	assert.Equal(t, OutcomeBlockedSizeLimit, decision.Outcome)
}

// A table within the bound on both measurements resolves to direct, and the
// reason carries the engine's execution phrase and the corroborated count.
func TestResolveRefusedMode_WithinBoundResolvesDirect(t *testing.T) {
	sizer := &fakeSizer{estimate: 900, exact: 950}
	policy := Policy{Enabled: true, MaxTableRows: 1000}
	decision, err := testResolver(sizer).ResolveRefusedMode(t.Context(), policy, "orders", "users", testRefusal)
	require.NoError(t, err)
	assert.Equal(t, engine.ExecutionModeDirect, decision.Mode)
	assert.Contains(t, decision.ModeReason, testRefusal)
	assert.Contains(t, decision.ModeReason, "runs as native test DDL on a table with ~950 rows")
	assert.Equal(t, int64(950), decision.Rows)
}

// A context cancelled while the size gate runs is an error, not a blocked
// verdict: reporting it as "row count unavailable" would misdiagnose an
// operator stop as an unmeasurable table.
func TestResolveRefusedMode_CancelledContextIsAnErrorNotAVerdict(t *testing.T) {
	policy := Policy{Enabled: true, MaxTableRows: 1000}

	t.Run("during the estimate", func(t *testing.T) {
		ctx, cancel := context.WithCancel(t.Context())
		cancel()
		sizer := &fakeSizer{estimateErr: context.Canceled}
		_, err := testResolver(sizer).ResolveRefusedMode(ctx, policy, "orders", "users", testRefusal)
		require.Error(t, err)
		assert.ErrorIs(t, err, context.Canceled)
		assert.NotContains(t, err.Error(), "row count is unavailable")
	})

	t.Run("during the exact count", func(t *testing.T) {
		ctx, cancel := context.WithCancel(t.Context())
		cancel()
		sizer := &fakeSizer{estimate: 500, exactErr: context.Canceled}
		_, err := testResolver(sizer).ResolveRefusedMode(ctx, policy, "orders", "users", testRefusal)
		require.Error(t, err)
		assert.ErrorIs(t, err, context.Canceled)
		assert.NotContains(t, err.Error(), "row count is unavailable")
	})
}
