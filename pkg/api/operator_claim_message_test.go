package api

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

// A driver's claim names the work it will drive, so the claims of a
// multi-deployment apply can be told apart in its log, and reads the same
// whether the drive is the apply's first or one that follows a failure. The
// operations of a sharded target share one deployment, so their claims also
// name the operation key that tells the shards apart.
func TestApplyClaimMessage(t *testing.T) {
	assert.Equal(t, "Driver 3 claimed the apply (apply state running)",
		applyClaimMessage(3, "running", 0, "us-west", ""))
	assert.Equal(t, "Driver 3 claimed deployment us-west (apply state running)",
		applyClaimMessage(3, "running", 42, "us-west", ""))
	assert.Equal(t, "Driver 3 claimed operation commerce/-80/users on deployment commerce-deployment (apply state running)",
		applyClaimMessage(3, "running", 42, "commerce-deployment", "commerce/-80/users"))
	assert.Equal(t, "Driver 4 claimed operation commerce/80-/users on deployment commerce-deployment (apply state running)",
		applyClaimMessage(4, "running", 43, "commerce-deployment", "commerce/80-/users"))
}
