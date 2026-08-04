package templates

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestShortCaller(t *testing.T) {
	t.Run("CLI caller drops the machine", func(t *testing.T) {
		assert.Equal(t, "cli:jdoe", shortCaller("cli:jdoe@macbook.local"))
	})

	t.Run("email-shaped user keeps its domain and drops only the machine", func(t *testing.T) {
		assert.Equal(t, "cli:jdoe@example.com", shortCaller("cli:jdoe@example.com@macbook.local"))
	})

	t.Run("webhook caller drops the repo and PR", func(t *testing.T) {
		assert.Equal(t, "github:jdoe", shortCaller("github:jdoe@acme/repo#42"))
	})

	t.Run("caller without a location passes through unchanged", func(t *testing.T) {
		assert.Equal(t, "cli:jdoe", shortCaller("cli:jdoe"))
	})
}
