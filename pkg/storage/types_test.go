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
