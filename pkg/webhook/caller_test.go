package webhook

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestActorFromCaller(t *testing.T) {
	assert.Equal(t, "morgo", actorFromCaller("github:morgo@block/example#11890"), "extract the username from the structured caller")
	assert.Equal(t, "", actorFromCaller("morgo"), "an unrecognised caller yields no actor")
	assert.Equal(t, "", actorFromCaller(""), "empty caller yields no actor")
	assert.Equal(t, "", actorFromCaller("github:"), "missing @ yields no actor")
}

// formatGitHubCaller and actorFromCaller are inverses: the actor round-trips.
func TestGitHubCallerRoundTrip(t *testing.T) {
	assert.Equal(t, "morgo", actorFromCaller(formatGitHubCaller("morgo", "block/example", 11890)))
}
