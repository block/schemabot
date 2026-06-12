package api

import (
	"errors"
	"fmt"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
)

// TestIsInternalControlError verifies that control operation errors are
// classified so callers log internal failures at error severity and
// operator-actionable rejections at warning severity.
func TestIsInternalControlError(t *testing.T) {
	t.Run("conflict rejection is not internal", func(t *testing.T) {
		assert.False(t, IsInternalControlError(controlConflictf("schema change is already terminal (current state: %s)", "completed")))
	})

	t.Run("client-facing status is not internal", func(t *testing.T) {
		assert.False(t, IsInternalControlError(controlHTTPErrorf(http.StatusNotFound, "apply not found: %s", "apply-123")))
		assert.False(t, IsInternalControlError(controlHTTPErrorf(http.StatusBadRequest, "environment is required")))
	})

	t.Run("wrapped rejection keeps its classification", func(t *testing.T) {
		wrapped := fmt.Errorf("execute start: %w", controlConflictf("schema change is still running; stop it before starting it again"))
		assert.False(t, IsInternalControlError(wrapped))
	})

	t.Run("unclassified error is internal", func(t *testing.T) {
		assert.True(t, IsInternalControlError(errors.New("control request store is not available")))
		assert.True(t, IsInternalControlError(fmt.Errorf("record start control request for apply %s: %w", "apply-123", errors.New("storage unavailable"))))
	})

	t.Run("explicit server status is internal", func(t *testing.T) {
		assert.True(t, IsInternalControlError(controlHTTPErrorf(http.StatusBadGateway, "tern unavailable")))
	})
}
