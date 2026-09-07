package client

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Both grants must schedule renewal from the credential sent to SchemaBot,
// even when the provider assigns the access token a different lifetime.
func TestIDTokenExpiryForGrants(t *testing.T) {
	future := time.Now().Add(2 * time.Hour).Unix()
	for _, grant := range []string{"login", "refresh"} {
		for _, tc := range []struct {
			name, token, wantErr string
			expiry               int64
		}{
			{name: "shorter ID lifetime", token: testIDToken(fmt.Sprintf(`{"exp":%d}`, future)), expiry: future},
			{name: "missing exp", token: testIDToken(`{}`), wantErr: "positive exp"},
			{name: "null exp", token: testIDToken(`{"exp":null}`), wantErr: "positive exp"},
			{name: "zero exp", token: testIDToken(`{"exp":0}`), wantErr: "positive exp"},
			{name: "negative exp", token: testIDToken(`{"exp":-1}`), wantErr: "positive exp"},
			{name: "expired", token: testIDToken(`{"exp":1}`), wantErr: "already expired"},
			{name: "string exp", token: testIDToken(`{"exp":"secret-value"}`), wantErr: "integer exp"},
			{name: "fractional exp", token: testIDToken(`{"exp":123.5}`), wantErr: "integer exp"},
			{name: "overflow", token: testIDToken(`{"exp":9223372036854775808}`), wantErr: "integer exp"},
			{name: "invalid JSON", token: testIDToken(`{`), wantErr: "integer exp"},
			{name: "invalid encoding", token: "header.%%%.signature", wantErr: "base64url"},
			{name: "invalid JWT", token: "opaque-token", wantErr: "three JWT segments"},
			{name: "missing ID token", wantErr: "did not include an id_token"},
		} {
			t.Run(grant+"/"+tc.name, func(t *testing.T) {
				f := newFakeOIDC(t)
				f.idToken, f.refreshedIDToken = tc.token, tc.token
				f.accessExpires = 4 * 60 * 60
				ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
				defer cancel()
				cfg := LoginConfig{Issuer: f.issuer(), ClientID: "cli-client", RedirectPort: freeLoopbackPort(t)}
				var result *LoginResult
				var err error
				if grant == "login" {
					result, err = Login(ctx, cfg, browserVisitor(ctx))
				} else {
					result, err = RefreshToken(ctx, cfg, "refresh")
				}
				if tc.wantErr != "" {
					require.ErrorContains(t, err, tc.wantErr)
					assert.NotContains(t, err.Error(), "secret-value")
					assert.Nil(t, result)
					return
				}
				require.NoError(t, err)
				assert.Equal(t, tc.expiry, result.Expiry.Unix())
			})
		}
	}
}

// Cache the login result exactly as the login command does, then resolve twice.
// Renewal follows the ID lifetime and the rotated expiry survives a config reload.
func TestCachedIDTokenLifetimes(t *testing.T) {
	for _, tc := range []struct {
		name           string
		idLifetime     time.Duration
		accessLifetime int
		refresh        bool
	}{
		{"ID expires first", 30 * time.Second, 3600, true},
		{"access expires first", 2 * time.Hour, 1, false},
		{"access expiry omitted", 2 * time.Hour, 0, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Setenv("SCHEMABOT_TOKEN", "")
			t.Setenv("SCHEMABOT_ENDPOINT", "")
			t.Setenv("SCHEMABOT_PROFILE", "")
			f := newFakeOIDC(t)
			expiry := time.Now().Add(tc.idLifetime).Unix()
			refreshedExpiry := time.Now().Add(3 * time.Hour).Unix()
			f.idToken = testIDToken(fmt.Sprintf(`{"exp":%d}`, expiry))
			f.refreshedIDToken = testIDToken(fmt.Sprintf(`{"exp":%d}`, refreshedExpiry))
			f.accessExpires = tc.accessLifetime
			ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
			defer cancel()
			result, err := Login(ctx, LoginConfig{Issuer: f.issuer(), ClientID: "cli-client", RedirectPort: freeLoopbackPort(t)}, browserVisitor(ctx))
			require.NoError(t, err)
			writeConfig(t, &Config{Profiles: map[string]Profile{"default": {
				Endpoint: "https://schemabot.example", Token: result.IDToken, RefreshToken: result.RefreshToken, TokenExpiry: unixExpiry(result.Expiry),
				OIDC: &OIDCLogin{Issuer: f.issuer(), ClientID: "cli-client"},
			}}}, 0o600)
			want := f.idToken
			if tc.refresh {
				want = f.refreshedIDToken
				expiry = refreshedExpiry
			}
			for range 2 {
				token, err := ResolveBearerToken(ctx, "", "", "")
				require.NoError(t, err)
				assert.Equal(t, want, token)
				cfg, err := LoadConfig()
				require.NoError(t, err)
				assert.Equal(t, expiry, cfg.Profiles["default"].TokenExpiry)
			}
			f.mu.Lock()
			defer f.mu.Unlock()
			expected := 0
			if tc.refresh {
				expected = 1
			}
			assert.Equal(t, expected, f.refreshCalls)
		})
	}
}

func TestMalformedRefreshPreservesCache(t *testing.T) {
	t.Setenv("SCHEMABOT_TOKEN", "")
	f := newFakeOIDC(t)
	f.refreshedIDToken = testIDToken(`{}`)
	profile := Profile{Endpoint: "https://schemabot.example", Token: "old-token", RefreshToken: "old-refresh", TokenExpiry: 1, OIDC: &OIDCLogin{Issuer: f.issuer(), ClientID: "cli-client"}}
	writeConfig(t, &Config{Profiles: map[string]Profile{"default": profile}}, 0o600)
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	token, err := ResolveBearerToken(ctx, "", "", "default")
	require.ErrorContains(t, err, "positive exp")
	assert.Equal(t, profile.Token, token)
	cfg, err := LoadConfig()
	require.NoError(t, err)
	assert.Equal(t, profile, cfg.Profiles["default"])
}
