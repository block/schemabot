package api

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/block/schemabot/pkg/auth"
)

// A control operation or apply is attributed to the authenticated caller when
// the request carries a real identity; otherwise the client-supplied request
// caller is used, and the synthetic anonymous user (auth disabled) never
// overrides it.
func TestResolveCaller(t *testing.T) {
	t.Run("authenticated identity wins over the request caller", func(t *testing.T) {
		ctx := auth.WithUser(t.Context(), &auth.User{Subject: "bob@example.com"})
		assert.Equal(t, "bob@example.com", resolveCaller(ctx, "client-supplied"))
	})

	t.Run("no authenticated user falls back to the request caller", func(t *testing.T) {
		assert.Equal(t, "client-supplied", resolveCaller(t.Context(), "client-supplied"))
	})

	t.Run("anonymous user falls back to the request caller", func(t *testing.T) {
		ctx := auth.WithUser(t.Context(), &auth.User{Subject: auth.AnonymousSubject})
		assert.Equal(t, "client-supplied", resolveCaller(ctx, "client-supplied"))
	})
}
