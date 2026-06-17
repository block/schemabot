//go:build integration

package auth_test

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"net/http/cookiejar"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/docker/docker/api/types/container"
	"github.com/docker/go-connections/nat"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"

	"github.com/block/schemabot/pkg/auth"
)

// This exercises the OIDC authorizer end-to-end against a real Dex container:
// a real provider issues a real, signed, groups-bearing token, and we assert
// the tier policy holds — read open to any valid token, write gated on an admin
// group, and missing/garbage tokens rejected. It complements the unit tests
// (which use an in-process JWKS provider) by proving compatibility with an
// actual OIDC server: discovery, JWKS, and claim shape.
//
// Dex's static-password connector does not emit a groups claim, so we use the
// mockCallback connector — a fixed identity in group "authors" — and drive the
// auth-code flow to get a token that actually carries groups.

const dexImage = "ghcr.io/dexidp/dex:v2.41.1"

// dexHTTPTimeout bounds every HTTP call in this test so a hung Dex or network
// fails fast instead of blocking until the overall test timeout.
const dexHTTPTimeout = 30 * time.Second

// reserveHostPort grabs a free TCP port and releases it. The OIDC issuer URL
// must be known before Dex starts (it is baked into Dex's config and must match
// what clients discover), so we pin Dex's published port to this value.
func reserveHostPort(t *testing.T) int {
	t.Helper()
	l, err := (&net.ListenConfig{}).Listen(t.Context(), "tcp", "127.0.0.1:0")
	require.NoError(t, err)
	defer func() { _ = l.Close() }()
	return l.Addr().(*net.TCPAddr).Port
}

// startDex runs Dex and returns its issuer URL. Because the issuer port must be
// reserved before Dex binds it, there is an inherent TOCTOU window where another
// process can claim the port first; we retry on start failure with a freshly
// reserved port to absorb that race.
func startDex(t *testing.T) string {
	t.Helper()
	const attempts = 3
	var lastErr error
	for i := range attempts {
		issuer, err := tryStartDex(t)
		if err == nil {
			return issuer
		}
		lastErr = err
		t.Logf("dex start attempt %d/%d failed, retrying with a new port: %v", i+1, attempts, err)
	}
	require.NoError(t, lastErr, "dex failed to start after %d attempts", attempts)
	return ""
}

// tryStartDex reserves a port, pins Dex's published port to it, and starts the
// container. It returns an error (rather than failing the test) so startDex can
// retry on a port-binding race.
func tryStartDex(t *testing.T) (string, error) {
	t.Helper()
	hostPort := reserveHostPort(t)
	issuer := fmt.Sprintf("http://127.0.0.1:%d/dex", hostPort)

	cfg := fmt.Sprintf(`issuer: %s
storage:
  type: memory
web:
  http: 0.0.0.0:5556
oauth2:
  skipApprovalScreen: true
staticClients:
  - id: schemabot-test
    secret: test-secret
    name: SchemaBot Test
    redirectURIs:
      - http://localhost/callback
connectors:
  - type: mockCallback
    id: mock
    name: Mock
`, issuer)

	req := testcontainers.ContainerRequest{
		Image:        dexImage,
		ExposedPorts: []string{"5556/tcp"},
		Cmd:          []string{"dex", "serve", "/etc/dex/config.yaml"},
		Files: []testcontainers.ContainerFile{{
			Reader:            strings.NewReader(cfg),
			ContainerFilePath: "/etc/dex/config.yaml",
			FileMode:          0o444,
		}},
		HostConfigModifier: func(hc *container.HostConfig) {
			hc.PortBindings = nat.PortMap{
				"5556/tcp": []nat.PortBinding{{HostIP: "127.0.0.1", HostPort: fmt.Sprintf("%d", hostPort)}},
			}
		},
		WaitingFor: wait.ForHTTP("/dex/.well-known/openid-configuration").
			WithPort("5556/tcp").
			WithStartupTimeout(90 * time.Second),
	}
	ctr, err := testcontainers.GenericContainer(t.Context(), testcontainers.GenericContainerRequest{
		ContainerRequest: req,
		Started:          true,
	})
	if err != nil {
		return "", err
	}
	// t.Context() is already cancelled by the time cleanup runs; WithoutCancel
	// detaches that cancellation so the container is reliably torn down.
	t.Cleanup(func() { _ = ctr.Terminate(context.WithoutCancel(t.Context())) })
	return issuer, nil
}

// dexToken drives Dex's auth-code flow. The mockCallback connector
// auto-authenticates as a fixed user in group "authors", so a single request
// (following Dex's internal redirects, stopping at the client redirect_uri)
// yields an auth code we exchange for an id_token carrying that group.
func dexToken(t *testing.T, issuer string) string {
	t.Helper()
	jar, err := cookiejar.New(nil)
	require.NoError(t, err)

	var code string
	hc := &http.Client{
		Jar:     jar,
		Timeout: dexHTTPTimeout,
		CheckRedirect: func(r *http.Request, _ []*http.Request) error {
			if r.URL.Host == "localhost" { // the client redirect_uri — flow complete
				code = r.URL.Query().Get("code")
				return http.ErrUseLastResponse
			}
			return nil
		},
	}

	authURL := issuer + "/auth?" + url.Values{
		"client_id":     {"schemabot-test"},
		"redirect_uri":  {"http://localhost/callback"},
		"response_type": {"code"},
		"scope":         {"openid email groups"},
		"state":         {"teststate"},
	}.Encode()
	authReq, err := http.NewRequestWithContext(t.Context(), http.MethodGet, authURL, nil)
	require.NoError(t, err)
	resp, err := hc.Do(authReq)
	require.NoError(t, err)
	_ = resp.Body.Close()
	require.NotEmpty(t, code, "no auth code captured from Dex auth-code flow")

	form := url.Values{
		"grant_type":    {"authorization_code"},
		"code":          {code},
		"redirect_uri":  {"http://localhost/callback"},
		"client_id":     {"schemabot-test"},
		"client_secret": {"test-secret"},
	}
	tokReq, err := http.NewRequestWithContext(t.Context(), http.MethodPost, issuer+"/token", strings.NewReader(form.Encode()))
	require.NoError(t, err)
	tokReq.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	tokResp, err := hc.Do(tokReq)
	require.NoError(t, err)
	defer func() { _ = tokResp.Body.Close() }()
	require.Equal(t, http.StatusOK, tokResp.StatusCode)

	var tok struct {
		IDToken string `json:"id_token"`
	}
	require.NoError(t, json.NewDecoder(tokResp.Body).Decode(&tok))
	require.NotEmpty(t, tok.IDToken, "Dex returned no id_token")
	return tok.IDToken
}

func TestOIDCAuthorizerDexEndToEnd(t *testing.T) {
	issuer := startDex(t)
	token := dexToken(t, issuer)

	client := &http.Client{Timeout: dexHTTPTimeout}
	ok := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusOK) })

	serverWithAdminGroups := func(t *testing.T, adminGroups []string) *httptest.Server {
		authz, err := auth.NewOIDCAuthorizer(t.Context(), auth.OIDCConfig{
			Issuer:      issuer,
			Audience:    "schemabot-test", // Dex sets aud = client_id
			AdminGroups: adminGroups,
		}, testLogger())
		require.NoError(t, err)
		srv := httptest.NewServer(authz.Middleware(ok))
		t.Cleanup(srv.Close)
		return srv
	}

	status := func(t *testing.T, srv *httptest.Server, method, path, bearer string) int {
		req, err := http.NewRequestWithContext(t.Context(), method, srv.URL+path, nil)
		require.NoError(t, err)
		if bearer != "" {
			req.Header.Set("Authorization", "Bearer "+bearer)
		}
		resp, err := client.Do(req)
		require.NoError(t, err)
		_ = resp.Body.Close()
		return resp.StatusCode
	}

	t.Run("token in admin group: read and write allowed", func(t *testing.T) {
		srv := serverWithAdminGroups(t, []string{"authors"})
		assert.Equal(t, http.StatusOK, status(t, srv, http.MethodGet, "/api/status", token))
		assert.Equal(t, http.StatusOK, status(t, srv, http.MethodPost, "/api/apply", token))
	})

	t.Run("valid token not in admin group: read allowed, write 403", func(t *testing.T) {
		srv := serverWithAdminGroups(t, []string{"some-other-team"})
		assert.Equal(t, http.StatusOK, status(t, srv, http.MethodGet, "/api/status", token))
		assert.Equal(t, http.StatusForbidden, status(t, srv, http.MethodPost, "/api/apply", token))
	})

	t.Run("no token: 401", func(t *testing.T) {
		srv := serverWithAdminGroups(t, []string{"authors"})
		assert.Equal(t, http.StatusUnauthorized, status(t, srv, http.MethodGet, "/api/status", ""))
	})

	t.Run("garbage token: 401", func(t *testing.T) {
		srv := serverWithAdminGroups(t, []string{"authors"})
		assert.Equal(t, http.StatusUnauthorized, status(t, srv, http.MethodGet, "/api/status", "not-a-jwt"))
	})
}
