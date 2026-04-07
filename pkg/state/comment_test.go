package state

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestCommentConstants(t *testing.T) {
	assert.Equal(t, "progress", Comment.Progress)
	assert.Equal(t, "cutover", Comment.Cutover)
	assert.Equal(t, "summary", Comment.Summary)
}

func TestCommentConstantsAreDistinct(t *testing.T) {
	states := []string{Comment.Progress, Comment.Cutover, Comment.Summary}
	seen := make(map[string]bool)
	for _, s := range states {
		assert.False(t, seen[s], "duplicate comment state: %s", s)
		seen[s] = true
	}
}
