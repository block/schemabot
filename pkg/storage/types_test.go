package storage

import (
	"testing"

	"github.com/stretchr/testify/assert"
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
