package client

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestResolveToken(t *testing.T) {
	t.Run("flag takes precedence over env", func(t *testing.T) {
		t.Setenv("SCHEMABOT_TOKEN", "from-env")
		assert.Equal(t, "from-flag", ResolveToken("from-flag"))
	})

	t.Run("falls back to env when flag is empty", func(t *testing.T) {
		t.Setenv("SCHEMABOT_TOKEN", "from-env")
		assert.Equal(t, "from-env", ResolveToken(""))
	})

	t.Run("empty when neither is set", func(t *testing.T) {
		t.Setenv("SCHEMABOT_TOKEN", "")
		assert.Empty(t, ResolveToken(""))
	})
}
