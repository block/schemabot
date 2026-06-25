package client

import (
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"net"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// fakeOIDC is an in-process OIDC provider: it serves discovery, an
// authorization endpoint that auto-approves and redirects back with a code, and
// a token endpoint that verifies the PKCE verifier against the challenge it was
// issued. It lets the login flow be exercised end-to-end without a browser or a
// real provider, and proves the PKCE S256 round-trip is correct.
type fakeOIDC struct {
	server *httptest.Server

	mu         sync.Mutex
	challenges map[string]string // authorization code -> code_challenge
	lastMethod string            // code_challenge_method seen at the authz endpoint

	// Knobs for negative cases.
	stateOverride string // when set, echo this state instead of the request's
	authError     string // when set, redirect with ?error=<authError>

	idToken      string
	accessToken  string
	refreshToken string
	omitIDToken  bool // when true, the token response omits the id_token
}

func newFakeOIDC(t *testing.T) *fakeOIDC {
	t.Helper()
	f := &fakeOIDC{
		challenges:   map[string]string{},
		idToken:      "header.payload.signature",
		accessToken:  "test-access-token",
		refreshToken: "test-refresh-token",
	}

	mux := http.NewServeMux()
	mux.HandleFunc("/.well-known/openid-configuration", func(w http.ResponseWriter, _ *http.Request) {
		base := f.server.URL
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{
			"issuer":                                base,
			"authorization_endpoint":                base + "/auth",
			"token_endpoint":                        base + "/token",
			"jwks_uri":                              base + "/keys",
			"id_token_signing_alg_values_supported": []string{"RS256"},
			"response_types_supported":              []string{"code"},
			"scopes_supported":                      []string{"openid", "email", "groups", "offline_access"},
		})
	})
	mux.HandleFunc("/auth", f.handleAuth)
	mux.HandleFunc("/token", f.handleToken)

	f.server = httptest.NewServer(mux)
	t.Cleanup(f.server.Close)
	return f
}

func (f *fakeOIDC) issuer() string { return f.server.URL }

func (f *fakeOIDC) handleAuth(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	redirectURI := q.Get("redirect_uri")

	f.mu.Lock()
	f.lastMethod = q.Get("code_challenge_method")
	const code = "test-auth-code"
	f.challenges[code] = q.Get("code_challenge")
	f.mu.Unlock()

	state := q.Get("state")
	if f.stateOverride != "" {
		state = f.stateOverride
	}

	loc := redirectURI + "?state=" + state
	if f.authError != "" {
		loc += "&error=" + f.authError + "&error_description=" + "denied+by+test"
	} else {
		loc += "&code=test-auth-code"
	}
	http.Redirect(w, r, loc, http.StatusFound)
}

func (f *fakeOIDC) handleToken(w http.ResponseWriter, r *http.Request) {
	if err := r.ParseForm(); err != nil {
		http.Error(w, "bad form", http.StatusBadRequest)
		return
	}
	code := r.Form.Get("code")
	verifier := r.Form.Get("code_verifier")

	f.mu.Lock()
	challenge := f.challenges[code]
	f.mu.Unlock()

	sum := sha256.Sum256([]byte(verifier))
	if challenge == "" || base64.RawURLEncoding.EncodeToString(sum[:]) != challenge {
		http.Error(w, `{"error":"invalid_grant"}`, http.StatusBadRequest)
		return
	}

	body := map[string]any{
		"access_token":  f.accessToken,
		"token_type":    "Bearer",
		"refresh_token": f.refreshToken,
		"expires_in":    3600,
	}
	if !f.omitIDToken {
		body["id_token"] = f.idToken
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(body)
}

// freeLoopbackPort returns a currently-free loopback port for the login
// redirect listener.
func freeLoopbackPort(t *testing.T) int {
	t.Helper()
	l, err := (&net.ListenConfig{}).Listen(t.Context(), "tcp", "127.0.0.1:0")
	require.NoError(t, err)
	port := l.Addr().(*net.TCPAddr).Port
	require.NoError(t, l.Close())
	return port
}

// browserVisitor returns an opener that fetches the authorization URL the way a
// browser would, following the redirect back to the loopback callback server.
// It runs synchronously and returns any transport error so a broken flow fails
// the test fast instead of hanging until the test deadline. The callback
// handler delivers its result on a buffered channel, so the visit completes
// without waiting for Login to read it.
func browserVisitor(ctx context.Context) BrowserOpener {
	return func(u string) error {
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, u, nil)
		if err != nil {
			return err
		}
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			return err
		}
		defer func() { _ = resp.Body.Close() }()
		return nil
	}
}

func TestLogin(t *testing.T) {
	f := newFakeOIDC(t)
	ctx := t.Context()

	result, err := Login(ctx, LoginConfig{
		Issuer:       f.issuer(),
		ClientID:     "schemabot-cli",
		RedirectPort: freeLoopbackPort(t),
	}, browserVisitor(ctx))
	require.NoError(t, err)

	assert.Equal(t, "header.payload.signature", result.IDToken)
	assert.Equal(t, "test-access-token", result.AccessToken)
	assert.Equal(t, "test-refresh-token", result.RefreshToken)
	assert.False(t, result.Expiry.IsZero(), "token expiry should be populated from expires_in")

	// The flow must use PKCE with S256; the token endpoint already rejected any
	// verifier that didn't hash to the challenge, so reaching here proves the
	// round-trip, and the method is asserted explicitly.
	f.mu.Lock()
	method := f.lastMethod
	f.mu.Unlock()
	assert.Equal(t, "S256", method)
}

func TestLoginRejectsStateMismatch(t *testing.T) {
	f := newFakeOIDC(t)
	f.stateOverride = "tampered-state"
	ctx := t.Context()

	_, err := Login(ctx, LoginConfig{
		Issuer:       f.issuer(),
		ClientID:     "schemabot-cli",
		RedirectPort: freeLoopbackPort(t),
	}, browserVisitor(ctx))
	require.Error(t, err)
	assert.Contains(t, err.Error(), "state")
}

func TestLoginSurfacesProviderError(t *testing.T) {
	f := newFakeOIDC(t)
	f.authError = "access_denied"
	ctx := t.Context()

	_, err := Login(ctx, LoginConfig{
		Issuer:       f.issuer(),
		ClientID:     "schemabot-cli",
		RedirectPort: freeLoopbackPort(t),
	}, browserVisitor(ctx))
	require.Error(t, err)
	assert.Contains(t, err.Error(), "access_denied")
}

func TestLoginRequiresIDToken(t *testing.T) {
	f := newFakeOIDC(t)
	f.omitIDToken = true
	ctx := t.Context()

	_, err := Login(ctx, LoginConfig{
		Issuer:       f.issuer(),
		ClientID:     "schemabot-cli",
		RedirectPort: freeLoopbackPort(t),
	}, browserVisitor(ctx))
	require.Error(t, err)
	assert.Contains(t, err.Error(), "id_token")
}

func TestLoginStopsWhenContextCancelled(t *testing.T) {
	f := newFakeOIDC(t)
	ctx, cancel := context.WithCancel(t.Context())

	// Cancel at the moment the browser would open, before any callback arrives,
	// so Login is waiting on the redirect when the context goes away.
	opener := func(string) error {
		cancel()
		return nil
	}

	_, err := Login(ctx, LoginConfig{
		Issuer:       f.issuer(),
		ClientID:     "schemabot-cli",
		RedirectPort: freeLoopbackPort(t),
	}, opener)
	require.Error(t, err)
	assert.ErrorIs(t, err, context.Canceled)
}

func TestLoginValidatesConfig(t *testing.T) {
	noop := func(string) error { return nil }
	cases := map[string]LoginConfig{
		"missing issuer":    {ClientID: "c", RedirectPort: 1},
		"missing client ID": {Issuer: "https://issuer.example.com", RedirectPort: 1},
		"missing port":      {Issuer: "https://issuer.example.com", ClientID: "c"},
	}
	for name, cfg := range cases {
		t.Run(name, func(t *testing.T) {
			_, err := Login(t.Context(), cfg, noop)
			require.Error(t, err)
		})
	}

	t.Run("nil opener", func(t *testing.T) {
		_, err := Login(t.Context(), LoginConfig{Issuer: "https://issuer.example.com", ClientID: "c", RedirectPort: 1}, nil)
		require.Error(t, err)
	})
}
