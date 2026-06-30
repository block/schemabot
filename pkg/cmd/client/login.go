package client

import (
	"context"
	"crypto/rand"
	"encoding/base64"
	"errors"
	"fmt"
	"net"
	"net/http"
	"time"

	"github.com/coreos/go-oidc/v3/oidc"
	"golang.org/x/oauth2"
)

// defaultLoginScopes are requested during login. "openid" is required for an
// id_token; "email" and "groups" carry the identity and authorization claims
// the server checks; "offline_access" asks the provider for a refresh token so
// the session can be renewed without another browser round-trip.
var defaultLoginScopes = []string{oidc.ScopeOpenID, "email", "groups", oidc.ScopeOfflineAccess}

// loginCallbackPath is the loopback path the provider redirects back to after
// the user authenticates.
const loginCallbackPath = "/callback"

// LoginConfig configures an interactive OIDC authorization-code login.
type LoginConfig struct {
	// Issuer is the OIDC issuer URL; its discovery document supplies the
	// authorization and token endpoints.
	Issuer string
	// ClientID identifies SchemaBot as a public OAuth client (no secret; the
	// PKCE verifier proves possession instead).
	ClientID string
	// RedirectPort is the loopback port the browser is redirected back to. It
	// must match a redirect URI registered with the provider, so it is a fixed
	// input rather than an ephemeral port.
	RedirectPort int
	// Scopes overrides defaultLoginScopes when non-empty.
	Scopes []string
}

// LoginResult holds the tokens returned by a successful login.
type LoginResult struct {
	IDToken      string
	AccessToken  string
	RefreshToken string
	Expiry       time.Time
}

// BrowserOpener opens the system browser at the given URL. Login calls it with
// the authorization URL; the CLI wires a real opener while tests inject a
// programmatic visitor.
type BrowserOpener func(url string) error

// callbackResult carries the outcome of the loopback redirect from the handler
// goroutine back to Login.
type callbackResult struct {
	code string
	err  error
}

// Login runs the OIDC authorization-code flow with PKCE for a public client:
//
//   - discover the provider's authorization and token endpoints,
//   - start a loopback HTTP server on RedirectPort,
//   - open the browser at the authorization URL (PKCE S256 challenge + state),
//   - receive the code on the loopback redirect, verifying the state matches,
//   - exchange the code (with the PKCE verifier) for tokens.
//
// ctx bounds the whole flow; cancelling it (or giving it a deadline) stops Login
// waiting for the user. The loopback server is always shut down before Login
// returns.
func Login(ctx context.Context, cfg LoginConfig, open BrowserOpener) (*LoginResult, error) {
	if cfg.Issuer == "" {
		return nil, errors.New("login: issuer is required")
	}
	if cfg.ClientID == "" {
		return nil, errors.New("login: client ID is required")
	}
	if cfg.RedirectPort == 0 {
		return nil, errors.New("login: redirect port is required (it must match a registered redirect URI)")
	}
	if open == nil {
		return nil, errors.New("login: browser opener is required")
	}

	provider, err := oidc.NewProvider(ctx, cfg.Issuer)
	if err != nil {
		return nil, fmt.Errorf("discover OIDC provider %s: %w", cfg.Issuer, err)
	}

	scopes := cfg.Scopes
	if len(scopes) == 0 {
		scopes = defaultLoginScopes
	}

	// Use the literal loopback IP for both the redirect URI and the listener
	// bind so the browser reaches the exact socket the callback server is on.
	// "localhost" can resolve to IPv6 (::1) while the listener is on IPv4,
	// which would strand the callback; RFC 8252 §7.3 recommends the literal IP.
	redirectURL := fmt.Sprintf("http://127.0.0.1:%d%s", cfg.RedirectPort, loginCallbackPath)
	oauthCfg := &oauth2.Config{
		ClientID:    cfg.ClientID,
		Endpoint:    provider.Endpoint(),
		RedirectURL: redirectURL,
		Scopes:      scopes,
	}

	verifier := oauth2.GenerateVerifier()
	state, err := randomURLSafe()
	if err != nil {
		return nil, fmt.Errorf("generate login state: %w", err)
	}

	listener, err := (&net.ListenConfig{}).Listen(ctx, "tcp", fmt.Sprintf("127.0.0.1:%d", cfg.RedirectPort))
	if err != nil {
		return nil, fmt.Errorf("listen on loopback port %d for the login redirect: %w", cfg.RedirectPort, err)
	}

	results := make(chan callbackResult, 1)
	mux := http.NewServeMux()
	mux.HandleFunc(loginCallbackPath, func(w http.ResponseWriter, r *http.Request) {
		code, err := readCallback(r, state)
		if err != nil {
			http.Error(w, "Login failed: "+err.Error()+". You can close this tab and return to the terminal.", http.StatusBadRequest)
		} else {
			w.Header().Set("Content-Type", "text/html; charset=utf-8")
			_, _ = w.Write([]byte("<html><body><h2>Login complete.</h2><p>You can close this tab and return to the terminal.</p></body></html>"))
		}
		// Deliver exactly one result; a duplicate or stray request is ignored.
		select {
		case results <- callbackResult{code: code, err: err}:
		default:
		}
	})

	srv := &http.Server{Handler: mux, ReadHeaderTimeout: 10 * time.Second}
	serveErr := make(chan error, 1)
	go func() {
		// ErrServerClosed is the normal result of the deferred Shutdown; anything
		// else means the callback server never came up, so surface it.
		if err := srv.Serve(listener); err != nil && !errors.Is(err, http.ErrServerClosed) {
			serveErr <- err
		}
	}()
	defer func() {
		// The request context has likely been cancelled by shutdown time; detach
		// it but keep a bound so a stuck connection can't block the return.
		shutdownCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
		defer cancel()
		_ = srv.Shutdown(shutdownCtx)
	}()

	authURL := oauthCfg.AuthCodeURL(state, oauth2.AccessTypeOffline, oauth2.S256ChallengeOption(verifier))
	if err := open(authURL); err != nil {
		return nil, fmt.Errorf("open browser for login: %w", err)
	}

	var code string
	select {
	case <-ctx.Done():
		return nil, fmt.Errorf("waiting for login callback: %w", ctx.Err())
	case err := <-serveErr:
		return nil, fmt.Errorf("login callback server failed on port %d: %w", cfg.RedirectPort, err)
	case res := <-results:
		if res.err != nil {
			return nil, res.err
		}
		code = res.code
	}

	token, err := oauthCfg.Exchange(ctx, code, oauth2.VerifierOption(verifier))
	if err != nil {
		return nil, fmt.Errorf("exchange authorization code for token: %w", err)
	}

	rawID, ok := token.Extra("id_token").(string)
	if !ok || rawID == "" {
		return nil, errors.New("token response did not include an id_token")
	}

	return &LoginResult{
		IDToken:      rawID,
		AccessToken:  token.AccessToken,
		RefreshToken: token.RefreshToken,
		Expiry:       token.Expiry,
	}, nil
}

// readCallback validates the loopback redirect and returns the authorization
// code. A provider-reported error or a state mismatch (a sign the response did
// not originate from the request this flow started) is surfaced as an error.
func readCallback(r *http.Request, wantState string) (string, error) {
	q := r.URL.Query()
	if errCode := q.Get("error"); errCode != "" {
		if desc := q.Get("error_description"); desc != "" {
			return "", fmt.Errorf("authorization failed: %s: %s", errCode, desc)
		}
		return "", fmt.Errorf("authorization failed: %s", errCode)
	}
	if got := q.Get("state"); got != wantState {
		return "", errors.New("authorization response state did not match the request")
	}
	code := q.Get("code")
	if code == "" {
		return "", errors.New("authorization response did not include a code")
	}
	return code, nil
}

// randomURLSafe returns 32 bytes of cryptographic randomness as a URL-safe
// string, used for the unguessable OAuth state value.
func randomURLSafe() (string, error) {
	b := make([]byte, 32)
	if _, err := rand.Read(b); err != nil {
		return "", err
	}
	return base64.RawURLEncoding.EncodeToString(b), nil
}
