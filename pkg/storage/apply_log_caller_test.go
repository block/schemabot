package storage

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestApplyLogCaller(t *testing.T) {
	t.Run("CLI caller drops the hostname", func(t *testing.T) {
		assert.Equal(t, "cli:bob", ApplyLogCaller("cli:bob@laptop.example.com"))
	})

	t.Run("CLI caller without a hostname passes through", func(t *testing.T) {
		assert.Equal(t, "cli:bob", ApplyLogCaller("cli:bob"))
	})

	t.Run("webhook caller passes through unchanged", func(t *testing.T) {
		assert.Equal(t, "github:alice@acme/repo#42", ApplyLogCaller("github:alice@acme/repo#42"))
	})

	t.Run("bare subject passes through unchanged", func(t *testing.T) {
		assert.Equal(t, "bob@example.com", ApplyLogCaller("bob@example.com"))
	})

	t.Run("empty caller reads as unknown", func(t *testing.T) {
		assert.Equal(t, "unknown", ApplyLogCaller(""))
	})
}
