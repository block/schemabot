package storage

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// attrsToMap converts a flat slog key/value attr slice into a map for
// order-independent assertions.
func attrsToMap(t *testing.T, attrs []any) map[string]any {
	t.Helper()
	require.Zero(t, len(attrs)%2, "attrs must be key/value pairs")
	m := make(map[string]any, len(attrs)/2)
	for i := 0; i < len(attrs); i += 2 {
		key, ok := attrs[i].(string)
		require.True(t, ok, "attr key at %d must be a string", i)
		_, dup := m[key]
		require.False(t, dup, "duplicate attr key %q", key)
		m[key] = attrs[i+1]
	}
	return m
}

func TestApplyIdentityLogAttrs(t *testing.T) {
	t.Run("nil receiver returns nil", func(t *testing.T) {
		var a *Apply
		assert.Nil(t, a.IdentityLogAttrs())
	})

	t.Run("carries identity and provenance, never mutable attrs", func(t *testing.T) {
		a := &Apply{
			ApplyIdentifier: "apply-abc123",
			Database:        "orders",
			DatabaseType:    "mysql",
			Environment:     "staging",
			Deployment:      "primary",
			State:           "running",
			Repository:      "org/repo",
			PullRequest:     1090,
			ExternalID:      "apply-remote456",
			Caller:          "webhook:org/repo#1090",
		}
		m := attrsToMap(t, a.IdentityLogAttrs())
		assert.Equal(t, "apply-abc123", m["apply_id"])
		assert.Equal(t, "orders", m["database"])
		assert.Equal(t, "mysql", m["database_type"])
		assert.Equal(t, "staging", m["environment"])
		assert.Equal(t, "org/repo", m["repo"])
		assert.Equal(t, 1090, m["pr"])
		assert.Equal(t, "webhook:org/repo#1090", m["caller"])
		assert.NotContains(t, m, "state", "state is mutable and must not be bound at claim time")
		assert.NotContains(t, m, "deployment", "the driven deployment may differ from the apply row's")
		assert.NotContains(t, m, "external_id", "external_id is assigned mid-drive and must not be bound at claim time")
	})

	t.Run("omits unset optional attrs", func(t *testing.T) {
		a := &Apply{
			ApplyIdentifier: "apply-abc123",
			Database:        "orders",
			DatabaseType:    "mysql",
			Environment:     "staging",
		}
		m := attrsToMap(t, a.IdentityLogAttrs())
		assert.NotContains(t, m, "repo")
		assert.NotContains(t, m, "pr")
		assert.NotContains(t, m, "external_id")
		assert.NotContains(t, m, "caller")
	})
}

func TestApplyLogAttrs(t *testing.T) {
	t.Run("nil receiver returns nil", func(t *testing.T) {
		var a *Apply
		assert.Nil(t, a.LogAttrs())
	})

	t.Run("extends identity with deployment, current state, and external_id", func(t *testing.T) {
		a := &Apply{
			ApplyIdentifier: "apply-abc123",
			Database:        "orders",
			DatabaseType:    "mysql",
			Environment:     "staging",
			Deployment:      "primary",
			State:           "running",
			Repository:      "org/repo",
			PullRequest:     1090,
		}
		m := attrsToMap(t, a.LogAttrs())
		assert.Equal(t, "apply-abc123", m["apply_id"])
		assert.Equal(t, "org/repo", m["repo"])
		assert.Equal(t, 1090, m["pr"])
		assert.Equal(t, "primary", m["deployment"])
		assert.Equal(t, "running", m["state"])
		assert.NotContains(t, m, "external_id", "unset external_id is omitted")

		a.State = "completed"
		a.ExternalID = "apply-remote456"
		m = attrsToMap(t, a.LogAttrs())
		assert.Equal(t, "completed", m["state"], "LogAttrs snapshots the state at each call")
		assert.Equal(t, "apply-remote456", m["external_id"],
			"LogAttrs snapshots the external_id at each call once the data plane assigns it")
	})
}
