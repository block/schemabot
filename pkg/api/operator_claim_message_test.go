package api

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

// A driver's claim names the deployment it will drive, so the claims of a
// multi-deployment apply can be told apart in its log, and reads the same
// whether the drive is the apply's first or one that follows a failure.
func TestApplyClaimMessage(t *testing.T) {
	assert.Equal(t, "Driver 3 claimed the apply (apply state running)",
		applyClaimMessage(3, "running", 0, "us-west"))
	assert.Equal(t, "Driver 3 claimed deployment us-west (apply state running)",
		applyClaimMessage(3, "running", 42, "us-west"))
}
