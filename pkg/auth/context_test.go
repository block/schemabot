package auth_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/block/schemabot/pkg/auth"
)

func TestWithUserAndUserFromContext(t *testing.T) {
	user := &auth.User{
		Subject: "user@example.com",
		Groups:  []string{"admins", "developers"},
	}

	ctx := auth.WithUser(t.Context(), user)
	got := auth.UserFromContext(ctx)
	require.NotNil(t, got)
	assert.Equal(t, "user@example.com", got.Subject)
	assert.Equal(t, []string{"admins", "developers"}, got.Groups)
}

func TestUserFromContextReturnsNilWhenNotSet(t *testing.T) {
	got := auth.UserFromContext(t.Context())
	assert.Nil(t, got)
}

func TestAuthenticatedSubject(t *testing.T) {
	t.Run("real identity is authenticated", func(t *testing.T) {
		ctx := auth.WithUser(t.Context(), &auth.User{Subject: "user@example.com"})
		subject, ok := auth.AuthenticatedSubject(ctx)
		assert.True(t, ok)
		assert.Equal(t, "user@example.com", subject)
	})

	t.Run("no user is not authenticated", func(t *testing.T) {
		subject, ok := auth.AuthenticatedSubject(t.Context())
		assert.False(t, ok)
		assert.Empty(t, subject)
	})

	t.Run("anonymous placeholder is not authenticated", func(t *testing.T) {
		ctx := auth.WithUser(t.Context(), &auth.User{Subject: auth.AnonymousSubject})
		subject, ok := auth.AuthenticatedSubject(ctx)
		assert.False(t, ok)
		assert.Empty(t, subject)
	})

	t.Run("empty subject is not authenticated", func(t *testing.T) {
		ctx := auth.WithUser(t.Context(), &auth.User{Subject: ""})
		subject, ok := auth.AuthenticatedSubject(ctx)
		assert.False(t, ok)
		assert.Empty(t, subject)
	})
}
