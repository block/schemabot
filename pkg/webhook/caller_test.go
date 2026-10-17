package webhook

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestActorFromCaller(t *testing.T) {
	assert.Equal(t, "morgo", actorFromCaller("github:morgo@block/example#11890"), "extract the username from the structured caller")
	assert.Equal(t, "", actorFromCaller("morgo"), "an unrecognised caller yields no actor")
	assert.Equal(t, "", actorFromCaller(""), "empty caller yields no actor")
	assert.Equal(t, "", actorFromCaller("github:"), "missing @ yields no actor")
	assert.Equal(t, "dependabot[bot]", actorFromCaller("github:dependabot[bot]@block/example#1"), "an app account keeps its [bot] suffix")
	assert.Equal(t, "", actorFromCaller("github:alice\n\n## Forged status\ntext@block/example#1"), "a username carrying markup yields no actor")
	assert.Equal(t, "", actorFromCaller("github:alice*bold*@block/example#1"), "a username carrying emphasis yields no actor")
	assert.Equal(t, "", actorFromCaller("github:"+strings.Repeat("a", 40)+"@block/example#1"), "a username longer than GitHub allows yields no actor")
}

// formatGitHubCaller and actorFromCaller are inverses: the actor round-trips.
func TestGitHubCallerRoundTrip(t *testing.T) {
	assert.Equal(t, "morgo", actorFromCaller(formatGitHubCaller("morgo", "block/example", 11890)))
}
