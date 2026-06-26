package storage

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestApplyOptionsFromMapRoundTrip verifies that user-facing apply options can
// be decoded from API-style strings and encoded back without losing fields.
func TestApplyOptionsFromMapRoundTrip(t *testing.T) {
	options := ApplyOptionsFromMap(map[string]string{
		"allow_unsafe":  "true",
		"branch":        "schema-change-branch",
		"defer_cutover": "true",
		"defer_deploy":  "true",
		"skip_revert":   "true",
		"volume":        "7",
	})

	assert.Equal(t, ApplyOptions{
		AllowUnsafe:  true,
		Branch:       "schema-change-branch",
		DeferCutover: true,
		DeferDeploy:  true,
		SkipRevert:   true,
		Volume:       7,
	}, options)

	assert.Equal(t, map[string]string{
		"allow_unsafe":  "true",
		"branch":        "schema-change-branch",
		"defer_cutover": "true",
		"defer_deploy":  "true",
		"skip_revert":   "true",
		"volume":        "7",
	}, options.Map())
}

func TestPlanUnsafeDDLChanges(t *testing.T) {
	plan := &Plan{Namespaces: map[string]*NamespacePlanData{
		"testdb": {Tables: []TableChange{
			{Namespace: "testdb", Table: "users", Operation: "alter", IsUnsafe: true, UnsafeReason: "DROP COLUMN removes data"},
			{Namespace: "testdb", Table: "orders", Operation: "drop"},
			{Namespace: "testdb", Table: "products", Operation: "alter"},
		}},
	}, Shards: []ShardPlan{{
		Namespace: "testdb",
		Shard:     "-80",
		Changes:   []TableChange{{Table: "accounts", Operation: "alter", IsUnsafe: true, UnsafeReason: "DROP PRIMARY KEY rebuilds the table"}},
	}}}

	changes := plan.UnsafeDDLChanges()

	require.Len(t, changes, 3)
	assert.Equal(t, "users", changes[0].Table)
	assert.Equal(t, "DROP COLUMN removes data", changes[0].UnsafeOptInReason())
	assert.Equal(t, "orders", changes[1].Table)
	assert.Equal(t, "DROP TABLE removes all data", changes[1].UnsafeOptInReason())
	assert.Equal(t, "accounts", changes[2].Table)
	assert.Equal(t, "testdb", changes[2].Namespace)
	assert.Equal(t, "DROP PRIMARY KEY rebuilds the table", changes[2].UnsafeOptInReason())
}

// TestReleasesPausedRollout verifies the one-way release latch semantics: a
// pending or completed release request releases a paused rollout, while a
// failed release, any non-release operation, and a nil request do not (the
// rollout stays paused — fail-closed).
func TestReleasesPausedRollout(t *testing.T) {
	tests := []struct {
		name string
		req  *ApplyControlRequest
		want bool
	}{
		{name: "nil request", req: nil, want: false},
		{
			name: "pending release latches",
			req:  &ApplyControlRequest{Operation: ControlOperationRelease, Status: ControlRequestPending},
			want: true,
		},
		{
			name: "completed release latches",
			req:  &ApplyControlRequest{Operation: ControlOperationRelease, Status: ControlRequestCompleted},
			want: true,
		},
		{
			name: "failed release does not latch",
			req:  &ApplyControlRequest{Operation: ControlOperationRelease, Status: ControlRequestFailed},
			want: false,
		},
		{
			name: "pending start is not a release",
			req:  &ApplyControlRequest{Operation: ControlOperationStart, Status: ControlRequestPending},
			want: false,
		},
		{
			name: "completed stop is not a release",
			req:  &ApplyControlRequest{Operation: ControlOperationStop, Status: ControlRequestCompleted},
			want: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, tt.req.ReleasesPausedRollout())
		})
	}
}

// TestApplyOptionsFromMapIgnoresInvalidVolume verifies that malformed numeric
// options and out-of-range volumes are ignored instead of being persisted back
// into apply metadata.
func TestApplyOptionsFromMapIgnoresInvalidVolume(t *testing.T) {
	for _, volume := range []string{"fast", "0", "-1", "12"} {
		options := ApplyOptionsFromMap(map[string]string{"volume": volume})

		assert.Zero(t, options.Volume)
		assert.Empty(t, options.Map())
	}
}
