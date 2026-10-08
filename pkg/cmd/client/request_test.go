package client

import (
	"bytes"
	"crypto/tls"
	"crypto/x509"
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/block/schemabot/pkg/apitypes"
)

// captureAuthServer returns a test server that records the Authorization header
// of the request it receives and replies with an empty JSON object.
func captureAuthServer(t *testing.T, gotAuth *string) *httptest.Server {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		*gotAuth = r.Header.Get("Authorization")
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{}`))
	}))
	t.Cleanup(srv.Close)
	return srv
}

// setTokenForTest sets the shared auth token and restores it after the test so
// the package-level transport state does not leak between tests.
func setTokenForTest(t *testing.T, token string) {
	t.Helper()
	prev := authTransport.token
	t.Cleanup(func() { authTransport.token = prev })
	SetAuthToken(token)
}

// sendThroughTransport drives a request through the shared httpClient (and thus
// the bearer transport), tying it to the test lifecycle via t.Context(). The
// optional preset header is set before the transport runs, to verify the
// transport does not clobber caller-provided headers.
func sendThroughTransport(t *testing.T, rawURL string, presetAuth string) (*http.Response, error) {
	t.Helper()
	req, err := http.NewRequestWithContext(t.Context(), http.MethodGet, rawURL, nil)
	require.NoError(t, err)
	if presetAuth != "" {
		req.Header.Set("Authorization", presetAuth)
	}
	return httpClient.Do(req)
}

// controlStatusServer returns a test server that answers every request with
// the given status code and JSON body, for exercising the client's status
// handling.
func controlStatusServer(t *testing.T, statusCode int, body string) *httptest.Server {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(statusCode)
		_, _ = w.Write([]byte(body))
	}))
	t.Cleanup(srv.Close)
	return srv
}

// A 202 Accepted from a control endpoint means the request was durably
// recorded and the apply owner completes it asynchronously — the operator's
// command succeeded. The client must decode the response body as a success,
// not surface the status code as an error.
func TestDoPostIntoAcceptsAccepted(t *testing.T) {
	srv := controlStatusServer(t, http.StatusAccepted, `{"accepted": true, "status": "already_requested"}`)

	var result struct {
		Accepted bool   `json:"accepted"`
		Status   string `json:"status"`
	}
	err := doPostInto(srv.URL, "/api/skip-revert", map[string]string{"apply_id": "apply-abc"}, &result)

	require.NoError(t, err)
	assert.True(t, result.Accepted)
	assert.Equal(t, "already_requested", result.Status)
}

func TestDoPostIntoAcceptsOK(t *testing.T) {
	srv := controlStatusServer(t, http.StatusOK, `{"accepted": true}`)

	var result struct {
		Accepted bool `json:"accepted"`
	}
	err := doPostInto(srv.URL, "/api/skip-revert", map[string]string{"apply_id": "apply-abc"}, &result)

	require.NoError(t, err)
	assert.True(t, result.Accepted)
}

func TestDoPostIntoErrorStatusReturnsAPIError(t *testing.T) {
	srv := controlStatusServer(t, http.StatusConflict, `{"error": "schema change is not in its revert window", "error_code": "conflict"}`)

	var result struct{}
	err := doPostInto(srv.URL, "/api/skip-revert", map[string]string{"apply_id": "apply-abc"}, &result)

	var apiErr *APIError
	require.ErrorAs(t, err, &apiErr)
	assert.Equal(t, http.StatusConflict, apiErr.Status)
	assert.Equal(t, "conflict", apiErr.ErrorCode)
	assert.Contains(t, apiErr.Message, "not in its revert window")
}

// A refused request carries both facts a client automating against the API
// needs: that the refusal is transient, and the wait the server expects it to
// observe before trying again.
func TestDoPostIntoCarriesTheServersRetryDelay(t *testing.T) {
	srv := controlStatusServer(t, http.StatusTooManyRequests, `{"error": "too many pull requests for this database and environment; retry in 4s", "error_code": "rate_limited", "retry_after_seconds": 4}`)

	var result struct{}
	err := doPostInto(srv.URL, "/api/pull", map[string]string{"database": "orders"}, &result)

	var apiErr *APIError
	require.ErrorAs(t, err, &apiErr)
	assert.Equal(t, http.StatusTooManyRequests, apiErr.Status)
	assert.Equal(t, apitypes.ErrCodeRateLimited, apiErr.ErrorCode)
	assert.Equal(t, 4, apiErr.RetryAfterSeconds)

	retry, after := apiErr.RetryAfter()
	assert.True(t, retry)
	assert.Equal(t, 4*time.Second, after)
}

// A permanent refusal reports no retry, so a client reading only RetryAfter
// never schedules one against an error that will never succeed.
func TestDoPostIntoReportsNoRetryForPermanentErrors(t *testing.T) {
	srv := controlStatusServer(t, http.StatusConflict, `{"error": "schema change is not in its revert window", "error_code": "conflict"}`)

	var result struct{}
	err := doPostInto(srv.URL, "/api/skip-revert", map[string]string{"apply_id": "apply-abc"}, &result)

	var apiErr *APIError
	require.ErrorAs(t, err, &apiErr)
	retry, after := apiErr.RetryAfter()
	assert.False(t, retry)
	assert.Zero(t, after)
}

func TestAuthTokenAttachedAsBearer(t *testing.T) {
	var gotAuth string
	srv := captureAuthServer(t, &gotAuth)
	setTokenForTest(t, "tok-abc123")

	resp, err := sendThroughTransport(t, srv.URL+"/api/status", "")
	require.NoError(t, err)
	_ = resp.Body.Close()
	assert.Equal(t, "Bearer tok-abc123", gotAuth)
}

func TestNoAuthTokenSendsNoHeader(t *testing.T) {
	var gotAuth string
	srv := captureAuthServer(t, &gotAuth)
	setTokenForTest(t, "")

	resp, err := sendThroughTransport(t, srv.URL+"/api/status", "")
	require.NoError(t, err)
	_ = resp.Body.Close()
	assert.Empty(t, gotAuth)
}

func TestAuthTokenTrimmed(t *testing.T) {
	var gotAuth string
	srv := captureAuthServer(t, &gotAuth)
	setTokenForTest(t, "  tok-padded\n")

	resp, err := sendThroughTransport(t, srv.URL+"/api/status", "")
	require.NoError(t, err)
	_ = resp.Body.Close()
	assert.Equal(t, "Bearer tok-padded", gotAuth)
}

func TestExistingAuthorizationHeaderPreserved(t *testing.T) {
	var gotAuth string
	srv := captureAuthServer(t, &gotAuth)
	setTokenForTest(t, "tok-abc123")

	resp, err := sendThroughTransport(t, srv.URL+"/api/status", "Basic dXNlcjpwYXNz")
	require.NoError(t, err)
	_ = resp.Body.Close()
	assert.Equal(t, "Basic dXNlcjpwYXNz", gotAuth)
}

func TestAuthTokenAllowedOverLoopbackHTTP(t *testing.T) {
	// httptest serves plaintext on 127.0.0.1, which is loopback and therefore
	// safe to send a token to during local development.
	var gotAuth string
	srv := captureAuthServer(t, &gotAuth)
	setTokenForTest(t, "tok-local")

	resp, err := sendThroughTransport(t, srv.URL+"/api/status", "")
	require.NoError(t, err)
	_ = resp.Body.Close()
	assert.Equal(t, "Bearer tok-local", gotAuth)
}

func TestAuthTokenRefusedOverInsecureRemote(t *testing.T) {
	setTokenForTest(t, "tok-abc123")

	var out map[string]any
	err := doGetIntoCtx(t.Context(), "http://schemabot.example.com", "/api/status", &out)
	require.Error(t, err)
	assert.ErrorIs(t, err, ErrInsecureTokenTransport)
}

// redirectServer returns a test server that answers every request with a 307
// to target+path, so a redirected request keeps its method and body.
func redirectServer(t *testing.T, newServer func(http.Handler) *httptest.Server, target func() string) *httptest.Server {
	t.Helper()
	srv := newServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, target()+r.URL.Path, http.StatusTemporaryRedirect)
	}))
	t.Cleanup(srv.Close)
	return srv
}

// tokenClient returns a client whose bearer transport carries token over base,
// so a test can exercise the transport against TLS servers without touching
// the package-level client.
func tokenClient(base http.RoundTripper, token string) *http.Client {
	return &http.Client{Transport: &bearerTransport{base: base, token: token}}
}

// getThrough sends a GET to rawURL with client and closes any response body.
func getThrough(t *testing.T, client *http.Client, rawURL string) error {
	t.Helper()
	req, err := http.NewRequestWithContext(t.Context(), http.MethodGet, rawURL, nil)
	require.NoError(t, err)
	resp, err := client.Do(req)
	if resp != nil {
		require.NoError(t, resp.Body.Close())
	}
	return err
}

// A SchemaBot server that redirects /api/status to /api/v2/status on itself is
// still the server the token was issued for, so the redirected request arrives
// authenticated.
func TestAuthTokenFollowsSameOriginRedirect(t *testing.T) {
	var gotAuth, gotPath string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/api/status" {
			http.Redirect(w, r, "/api/v2/status", http.StatusTemporaryRedirect)
			return
		}
		gotPath = r.URL.Path
		gotAuth = r.Header.Get("Authorization")
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{}`))
	}))
	t.Cleanup(srv.Close)
	setTokenForTest(t, "tok-abc123")

	var out map[string]any
	require.NoError(t, doGetIntoCtx(t.Context(), srv.URL, "/api/status", &out))
	assert.Equal(t, "/api/v2/status", gotPath)
	assert.Equal(t, "Bearer tok-abc123", gotAuth)
}

// A SchemaBot server on https that redirects to a different https server — here
// the same address on another port, which net/http alone would still treat as
// the same host — hands the request to a server the token was not issued for.
// The redirect is followed, but the second server never sees the token.
func TestAuthTokenWithheldFromCrossOriginRedirect(t *testing.T) {
	var gotAuth string
	var reached bool
	other := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		reached = true
		gotAuth = r.Header.Get("Authorization")
	}))
	t.Cleanup(other.Close)
	source := redirectServer(t, httptest.NewTLSServer, func() string { return other.URL })
	roots := x509.NewCertPool()
	roots.AddCert(source.Certificate())
	roots.AddCert(other.Certificate())
	base := http.DefaultTransport.(*http.Transport).Clone()
	base.TLSClientConfig = &tls.Config{RootCAs: roots, MinVersion: tls.VersionTLS12}

	err := getThrough(t, tokenClient(base, "tok-abc123"), source.URL+"/api/status")
	require.NoError(t, err)
	assert.True(t, reached, "the cross-origin redirect is followed")
	assert.Empty(t, gotAuth)
}

// A chain that leaves the token's origin and bounces back to it stays
// unauthenticated: the path it returns to was chosen by a server the token was
// never meant for.
func TestAuthTokenWithheldAfterChainLeavesOrigin(t *testing.T) {
	var gotAuth, gotPath string
	var home *httptest.Server
	elsewhere := redirectServer(t, httptest.NewServer, func() string { return home.URL + "/api/landing" })
	home = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/api/status" {
			http.Redirect(w, r, elsewhere.URL+"/bounce", http.StatusTemporaryRedirect)
			return
		}
		gotPath = r.URL.Path
		gotAuth = r.Header.Get("Authorization")
	}))
	t.Cleanup(home.Close)

	err := getThrough(t, tokenClient(http.DefaultTransport, "tok-abc123"), home.URL+"/api/status")
	require.NoError(t, err)
	assert.Equal(t, "/api/landing/bounce", gotPath)
	assert.Empty(t, gotAuth)
}

// dropsResponseRequest is a RoundTripper that, unlike http.Transport, does not
// record which request produced each response, so a redirected request cannot
// be traced back to the request the command sent.
type dropsResponseRequest struct{ base http.RoundTripper }

func (d dropsResponseRequest) RoundTrip(req *http.Request) (*http.Response, error) {
	resp, err := d.base.RoundTrip(req)
	if resp != nil {
		resp.Request = nil
	}
	return resp, err
}

// When the base transport leaves the redirect chain untraceable, the transport
// cannot tell a same-origin redirect from a cross-origin one, so it withholds
// the token from both rather than let the redirect target pass for the origin
// the token was issued for.
func TestAuthTokenWithheldWhenRedirectProvenanceIsUnknown(t *testing.T) {
	var otherAuth string
	other := captureAuthServer(t, &otherAuth)
	crossOrigin := redirectServer(t, httptest.NewServer, func() string { return other.URL })

	var homeAuth string
	home := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/api/status" {
			http.Redirect(w, r, "/api/v2/status", http.StatusTemporaryRedirect)
			return
		}
		homeAuth = r.Header.Get("Authorization")
	}))
	t.Cleanup(home.Close)

	client := tokenClient(dropsResponseRequest{http.DefaultTransport}, "tok-abc123")
	require.NoError(t, getThrough(t, client, crossOrigin.URL+"/api/status"))
	assert.Empty(t, otherAuth, "the token never reaches a server the chain cannot be traced to")

	require.NoError(t, getThrough(t, client, home.URL+"/api/status"))
	assert.Empty(t, homeAuth, "an untraceable same-origin redirect is treated as having left the origin")
}

// captureWarnings redirects the transport's warning stream into a buffer for
// the test and restores the process stream afterwards.
func captureWarnings(t *testing.T) *bytes.Buffer {
	t.Helper()
	prev := warnWriter
	t.Cleanup(func() { warnWriter = prev })
	var buf bytes.Buffer
	warnWriter = &buf
	return &buf
}

// An https SchemaBot server that redirects to plaintext http has sent the
// request to another origin: the redirect is followed, as any cross-origin
// redirect is, and the plaintext server never sees the token. The operator is
// told on stderr why the request went on unauthenticated.
func TestAuthTokenWithheldFromHTTPSDowngradeRedirect(t *testing.T) {
	var gotAuth string
	var reached bool
	plaintext := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		reached = true
		gotAuth = r.Header.Get("Authorization")
	}))
	t.Cleanup(plaintext.Close)
	source := redirectServer(t, httptest.NewTLSServer, func() string { return plaintext.URL })
	warnings := captureWarnings(t)

	err := getThrough(t, tokenClient(source.Client().Transport, "tok-abc123"), source.URL+"/api/status")
	require.NoError(t, err)
	assert.True(t, reached, "the downgrade redirect is followed")
	assert.Empty(t, gotAuth)
	sourceURL, err := url.Parse(source.URL)
	require.NoError(t, err)
	plaintextURL, err := url.Parse(plaintext.URL)
	require.NoError(t, err)
	assert.Equal(t, "Warning: not sending the auth token for "+RequestOrigin(sourceURL)+
		" to redirect target "+RequestOrigin(plaintextURL)+
		" because it is on another origin; the request continues unauthenticated\n", warnings.String())
}

// An Authorization header the caller set itself is withheld across the same
// downgrade redirect, so a credential the transport did not attach is not sent
// somewhere the transport's own token would not go.
func TestCallerAuthorizationWithheldFromHTTPSDowngradeRedirect(t *testing.T) {
	var gotAuth string
	plaintext := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotAuth = r.Header.Get("Authorization")
	}))
	t.Cleanup(plaintext.Close)
	source := redirectServer(t, httptest.NewTLSServer, func() string { return plaintext.URL })
	req, err := http.NewRequestWithContext(t.Context(), http.MethodGet, source.URL+"/api/status", nil)
	require.NoError(t, err)
	req.Header.Set("Authorization", "Bearer caller-token")
	resp, err := tokenClient(source.Client().Transport, "").Do(req)
	require.NoError(t, err)
	require.NoError(t, resp.Body.Close())
	assert.Equal(t, http.StatusOK, resp.StatusCode)
	assert.Empty(t, gotAuth)
}

// Two loopback servers on different ports are two origins. The token may be
// sent to either over plaintext, since both are local, but it belongs to the one
// the command addressed and does not follow a redirect to the other.
func TestAuthTokenWithheldBetweenLoopbackPorts(t *testing.T) {
	var gotAuth string
	var reached bool
	other := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		reached = true
		gotAuth = r.Header.Get("Authorization")
	}))
	t.Cleanup(other.Close)
	source := redirectServer(t, httptest.NewServer, func() string { return other.URL })

	err := getThrough(t, tokenClient(http.DefaultTransport, "tok-abc123"), source.URL+"/api/status")
	require.NoError(t, err)
	assert.True(t, reached, "the redirect to the other loopback port is followed")
	assert.Empty(t, gotAuth)
}

// The local-runtime credential is bound to the endpoint's origin, so a request
// that spells the same server differently still carries it, and a request to
// any other server is refused before it is sent.
func TestLocalAuthBoundToNormalizedOrigin(t *testing.T) {
	var gotAuth string
	srv := captureAuthServer(t, &gotAuth)
	prevToken, prevOrigin := authTransport.token, authTransport.origin
	t.Cleanup(func() { authTransport.token, authTransport.origin = prevToken, prevOrigin })
	srvURL, err := url.Parse(srv.URL)
	require.NoError(t, err)
	SetLocalAuth("local-tok", "HTTP://"+srvURL.Host+"/")

	resp, err := sendThroughTransport(t, srv.URL+"/api/status", "")
	require.NoError(t, err)
	require.NoError(t, resp.Body.Close())
	assert.Equal(t, "Bearer local-tok", gotAuth)

	resp, err = sendThroughTransport(t, "http://127.0.0.1:1/api/status", "")
	if resp != nil {
		require.NoError(t, resp.Body.Close())
	}
	require.ErrorContains(t, err, "refusing to forward local runtime credentials to another endpoint")
}

func TestRequestOriginNormalizesDefaultPortAndCase(t *testing.T) {
	for _, tc := range []struct {
		raw  string
		want string
	}{
		{raw: "https://SchemaBot.example.com/api", want: "https://schemabot.example.com:443"},
		{raw: "https://schemabot.example.com:443/api", want: "https://schemabot.example.com:443"},
		{raw: "http://127.0.0.1:8080/api", want: "http://127.0.0.1:8080"},
		{raw: "http://[::1]/api", want: "http://[::1]:80"},
	} {
		u, err := url.Parse(tc.raw)
		require.NoError(t, err)
		assert.Equal(t, tc.want, RequestOrigin(u), tc.raw)
	}
}

// An empty redirect chain has sent nothing, so it has left no origin; a chain
// that changes port has.
func TestStayedOnFirstOrigin(t *testing.T) {
	assert.True(t, StayedOnFirstOrigin(nil))
	request := func(raw string) *http.Request {
		r, err := http.NewRequestWithContext(t.Context(), http.MethodGet, raw, nil)
		require.NoError(t, err)
		return r
	}
	assert.True(t, StayedOnFirstOrigin([]*http.Request{request("https://a.example/x"), request("https://A.example:443/y")}))
	assert.False(t, StayedOnFirstOrigin([]*http.Request{request("https://a.example/x"), request("https://a.example:8443/y")}))
}

func TestNoTokenAllowedOverInsecureRemote(t *testing.T) {
	// Without a token there is nothing to leak, so the insecure-transport guard
	// must not block ordinary unauthenticated requests: the request proceeds
	// past the guard and fails on connection, not on the token guard.
	setTokenForTest(t, "")

	var out map[string]any
	err := doGetIntoCtx(t.Context(), "http://schemabot.example.com", "/api/status", &out)
	require.Error(t, err)
	assert.NotErrorIs(t, err, ErrInsecureTokenTransport)
}
