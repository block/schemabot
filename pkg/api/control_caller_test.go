package api

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/block/schemabot/pkg/auth"
	"github.com/block/schemabot/pkg/storage"
)

// A control operation or apply is attributed to the authenticated caller when
// the request carries a real identity; otherwise the client-supplied request
// caller is used, and the synthetic anonymous user (auth disabled) never
// overrides it. A CLI-shaped request caller keeps its channel prefix and
// hostname around the verified subject, so status surfaces show how and from
// which machine a change was driven.
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

	t.Run("CLI caller keeps its channel and hostname around the verified subject", func(t *testing.T) {
		ctx := auth.WithUser(t.Context(), &auth.User{Subject: "bob"})
		assert.Equal(t, "cli:bob@laptop.example.com", resolveCaller(ctx, "cli:mallory@laptop.example.com"))
	})

	t.Run("email-shaped subject keeps its domain in the composite", func(t *testing.T) {
		ctx := auth.WithUser(t.Context(), &auth.User{Subject: "bob@example.com"})
		assert.Equal(t, "cli:bob@example.com@laptop.example.com",
			resolveCaller(ctx, "cli:mallory@laptop.example.com"))
	})

	t.Run("CLI caller without a hostname is attributed to the subject alone", func(t *testing.T) {
		ctx := auth.WithUser(t.Context(), &auth.User{Subject: "bob"})
		assert.Equal(t, "bob", resolveCaller(ctx, "cli:mallory"))
		assert.Equal(t, "bob", resolveCaller(ctx, "cli:mallory@"))
	})

	t.Run("non-CLI channel is attributed to the subject alone", func(t *testing.T) {
		ctx := auth.WithUser(t.Context(), &auth.User{Subject: "bob"})
		assert.Equal(t, "bob", resolveCaller(ctx, "github:alice@acme/repo#42"))
	})

	t.Run("hostname with unsafe characters is attributed to the subject alone", func(t *testing.T) {
		ctx := auth.WithUser(t.Context(), &auth.User{Subject: "bob"})
		assert.Equal(t, "bob", resolveCaller(ctx, "cli:mallory@host with spaces"))
		assert.Equal(t, "bob", resolveCaller(ctx, "cli:mallory@host\x1b[2Kescape"))
		assert.Equal(t, "bob", resolveCaller(ctx, "cli:mallory@host\nnewline"))
	})

	t.Run("composite that exceeds the stored caller width is attributed to the subject alone", func(t *testing.T) {
		ctx := auth.WithUser(t.Context(), &auth.User{Subject: strings.Repeat("s", 200) + "@example.com"})
		host := strings.Repeat("h", 100) + ".example.com"
		assert.Equal(t, strings.Repeat("s", 200)+"@example.com", resolveCaller(ctx, "cli:mallory@"+host))
	})

	t.Run("unauthenticated CLI caller passes through unchanged", func(t *testing.T) {
		assert.Equal(t, "cli:bob@laptop.example.com", resolveCaller(t.Context(), "cli:bob@laptop.example.com"))
	})
}

// The resolved composite must survive apply-log compaction with the subject
// intact: the stored caller carries the machine for the detail view, while the
// apply-log message keeps the full verified identity and drops only the host.
func TestResolveCallerApplyLogRoundTrip(t *testing.T) {
	ctx := auth.WithUser(t.Context(), &auth.User{Subject: "bob@example.com"})

	resolved := resolveCaller(ctx, "cli:mallory@laptop.example.com")

	assert.Equal(t, "cli:bob@example.com", storage.ApplyLogCaller(resolved))
}

func TestCLICallerHostValidation(t *testing.T) {
	t.Run("hostname-shaped values are accepted", func(t *testing.T) {
		host, ok := cliCallerHost("cli:bob@build-agent_7.example.com")
		assert.True(t, ok)
		assert.Equal(t, "build-agent_7.example.com", host)
	})

	t.Run("hostname is the segment after the last at-sign", func(t *testing.T) {
		host, ok := cliCallerHost("cli:bob@example.com@laptop.example.com")
		assert.True(t, ok)
		assert.Equal(t, "laptop.example.com", host)
	})

	t.Run("hostname over the DNS length limit is rejected", func(t *testing.T) {
		_, ok := cliCallerHost("cli:bob@" + strings.Repeat("h", 254))
		assert.False(t, ok)
	})
}
