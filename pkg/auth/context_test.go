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
