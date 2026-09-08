package client

import (
	"context"
	"encoding/base64"
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
			{name: "local clock ahead of expiry", token: testIDToken(`{"exp":1}`), expiry: 1},
			{name: "string exp", token: testIDToken(`{"exp":"secret-value"}`), wantErr: "numeric exp"},
			{name: "fractional exp", token: testIDToken(fmt.Sprintf(`{"exp":%d.5}`, future)), expiry: future},
			{name: "decimal exp", token: testIDToken(fmt.Sprintf(`{"exp":%d.0}`, future)), expiry: future},
			{name: "exponent exp", token: testIDToken(fmt.Sprintf(`{"exp":%de0}`, future)), expiry: future},
			{name: "overflow", token: testIDToken(`{"exp":9223372036854775808}`), wantErr: "supported time range"},
			{name: "invalid JSON", token: testIDToken(`{`), wantErr: "numeric exp"},
			{name: "invalid encoding", token: "header.%%%.signature", wantErr: "base64url"},
			{name: "numeric overflow", token: testIDToken(`{"exp":987654321e999}`), wantErr: "numeric exp"},
			{name: "time overflow", token: testIDToken(`{"exp":9223371974719179008}`), wantErr: "supported time range"},
			{name: "two segments", token: "header." + base64.RawURLEncoding.EncodeToString([]byte(`{"exp":1234}`)), wantErr: "three JWT segments"},
			{name: "five segments", token: "a.b.c.d.e", wantErr: "three JWT segments"},
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
					assert.NotContains(t, err.Error(), "987654321")
					assert.NotContains(t, err.Error(), "922337")
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

// A malformed refresh response must not replace the previously cached session.
func TestMalformedRefreshPreservesCache(t *testing.T) {
	t.Setenv("SCHEMABOT_TOKEN", "")
	f := newFakeOIDC(t)
	f.refreshedIDToken = testIDToken(`{}`)
	profile := Profile{Endpoint: "https://schemabot.example", Token: testIDToken(`{"exp":1}`), RefreshToken: "old-refresh", TokenExpiry: 1, OIDC: &OIDCLogin{Issuer: f.issuer(), ClientID: "cli-client"}}
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

// Existing profiles may have cached either a shorter or longer access-token
// lifetime. The ID token controls renewal even before the cache is rewritten.
func TestExistingOIDCProfileExpiry(t *testing.T) {
	for _, tc := range []struct {
		name                       string
		idLifetime, cachedLifetime time.Duration
		refresh                    bool
	}{
		{"expired ID with future cached expiry", -time.Hour, 2 * time.Hour, true},
		{"live ID with expired cached expiry", time.Hour, -time.Hour, false},
		{"ID inside refresh window", 30 * time.Second, time.Hour, true},
		{"ID outside refresh window", 2 * time.Minute, -time.Hour, false},
		{"expired ID with unknown cached expiry", -time.Hour, 0, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Setenv("SCHEMABOT_TOKEN", "")
			t.Setenv("SCHEMABOT_ENDPOINT", "")
			f := newFakeOIDC(t)
			id := testIDToken(fmt.Sprintf(`{"exp":%d}`, time.Now().Add(tc.idLifetime).Unix()))
			var cached int64
			if tc.cachedLifetime != 0 {
				cached = time.Now().Add(tc.cachedLifetime).Unix()
			}
			writeConfig(t, &Config{Profiles: map[string]Profile{"default": {Endpoint: "https://schemabot.example", Token: id, TokenExpiry: cached, RefreshToken: "old-refresh", OIDC: &OIDCLogin{Issuer: f.issuer(), ClientID: "cli-client"}}}}, 0o600)
			ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
			defer cancel()
			token, err := ResolveBearerToken(ctx, "", "", "default")
			require.NoError(t, err)
			want := id
			calls := 0
			if tc.refresh {
				want = f.refreshedIDToken
				calls = 1
			}
			assert.Equal(t, want, token)
			f.mu.Lock()
			defer f.mu.Unlock()
			assert.Equal(t, calls, f.refreshCalls)
		})
	}
}

func TestTokenRefreshWindow(t *testing.T) {
	now := time.Unix(1700000000, 0)
	for _, tc := range []struct {
		name    string
		expiry  time.Time
		refresh bool
	}{
		{"unknown", time.Time{}, false},
		{"expired", now.Add(-time.Second), true},
		{"inside", now.Add(59 * time.Second), true},
		{"boundary", now.Add(60 * time.Second), true},
		{"outside", now.Add(61 * time.Second), false},
		{"epoch", time.Unix(0, 0), true},
	} {
		t.Run(tc.name, func(t *testing.T) { assert.Equal(t, tc.refresh, tokenNeedsRefresh(tc.expiry, now)) })
	}
}

// A provider may omit refresh_token when it does not rotate the credential.
// Both the refresh result and the saved session must retain the previous token.
func TestRefreshTokenRetention(t *testing.T) {
	t.Setenv("SCHEMABOT_TOKEN", "")
	t.Setenv("SCHEMABOT_ENDPOINT", "")
	f := newFakeOIDC(t)
	f.omitRefreshToken = true
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	result, err := RefreshToken(ctx, LoginConfig{Issuer: f.issuer(), ClientID: "cli-client"}, "old-refresh")
	require.NoError(t, err)
	assert.Equal(t, "old-refresh", result.RefreshToken)
	writeConfig(t, &Config{Profiles: map[string]Profile{"default": {Endpoint: "https://schemabot.example", Token: testIDToken(`{"exp":1}`), RefreshToken: "old-refresh", OIDC: &OIDCLogin{Issuer: f.issuer(), ClientID: "cli-client"}}}}, 0o600)
	token, err := ResolveBearerToken(ctx, "", "", "default")
	require.NoError(t, err)
	assert.Equal(t, result.IDToken, token)
	cfg, err := LoadConfig()
	require.NoError(t, err)
	assert.Equal(t, "old-refresh", cfg.Profiles["default"].RefreshToken)
	assert.Equal(t, f.refreshedIDExpiry, cfg.Profiles["default"].TokenExpiry)
}

// Bad cached expiry hints produce an actionable warning without contacting the
// provider; explicit credentials and other endpoints bypass the cached token.
func TestCachedOIDCTokenBoundaries(t *testing.T) {
	t.Setenv("SCHEMABOT_TOKEN", "")
	t.Setenv("SCHEMABOT_ENDPOINT", "")
	f := newFakeOIDC(t)
	profile := Profile{Endpoint: "https://schemabot.example", Token: testIDToken(`{}`), TokenExpiry: 1, RefreshToken: "old-refresh", OIDC: &OIDCLogin{Issuer: f.issuer(), ClientID: "cli-client"}}
	writeConfig(t, &Config{Profiles: map[string]Profile{"default": profile}}, 0o600)
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	token, err := ResolveBearerToken(ctx, "", "", "default")
	require.ErrorContains(t, err, "read cached ID token expiry")
	assert.Contains(t, err.Error(), "schemabot login")
	assert.Equal(t, profile.Token, token)
	cfg, err := LoadConfig()
	require.NoError(t, err)
	assert.Equal(t, profile, cfg.Profiles["default"])
	token, err = ResolveBearerToken(ctx, "explicit-token", "", "default")
	require.NoError(t, err)
	assert.Equal(t, "explicit-token", token)
	token, err = ResolveBearerToken(ctx, "", "https://other.example", "default")
	require.NoError(t, err)
	assert.Empty(t, token)
	f.mu.Lock()
	defer f.mu.Unlock()
	assert.Zero(t, f.refreshCalls)
}
