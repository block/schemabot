package psclient

import (
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Every PlanetScale API call runs through an HTTP client with a bounded
// response timeout so a hung PlanetScale endpoint cannot block a drive
// goroutine forever.
func TestNewPSClientBoundsHTTPRequests(t *testing.T) {
	client, err := NewPSClient("token-name", "token-value")
	require.NoError(t, err)

	wrapper, ok := client.(*psClientWrapper)
	require.True(t, ok, "NewPSClient must return the wrapper around the SDK client")

	require.NotNil(t, wrapper.httpClient)
	assert.Equal(t, planetScaleHTTPTimeout, wrapper.httpClient.Timeout)
	assert.Equal(t, "https://api.planetscale.com", wrapper.baseURL)
}

func TestNewPSClientWithBaseURLBoundsHTTPRequests(t *testing.T) {
	client, err := NewPSClientWithBaseURL("token-name", "token-value", "https://ps.example.com")
	require.NoError(t, err)

	wrapper, ok := client.(*psClientWrapper)
	require.True(t, ok, "NewPSClientWithBaseURL must return the wrapper around the SDK client")

	require.NotNil(t, wrapper.httpClient)
	assert.Equal(t, planetScaleHTTPTimeout, wrapper.httpClient.Timeout)
	assert.Equal(t, "https://ps.example.com", wrapper.baseURL)
}

// The SDK's service-token option wraps the transport of the client installed
// before it. The wrapper's HTTP client must be non-nil-transport and distinct
// from the SDK's, so the throttle endpoint's per-request Authorization header
// is the only one sent.
func TestThrottleClientHasOwnTransport(t *testing.T) {
	httpClient := newPlanetScaleHTTPClient()
	require.NotNil(t, httpClient.Transport)
	assert.Equal(t, planetScaleHTTPTimeout, httpClient.Timeout)
}

// A throttle call against an endpoint that accepts the request but never
// responds must return once the HTTP client's timeout fires instead of
// blocking the drive goroutine indefinitely.
func TestThrottleDeployRequestReturnsWhenServerHangs(t *testing.T) {
	release := make(chan struct{})
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		<-release
	}))
	// Cleanups run last-registered-first: the handler must be released before
	// server.Close waits for active connections to finish.
	t.Cleanup(server.Close)
	t.Cleanup(func() { close(release) })

	wrapper := &psClientWrapper{
		httpClient: &http.Client{Timeout: 100 * time.Millisecond},
		baseURL:    server.URL,
		tokenName:  "token-name",
		tokenValue: "token-value",
	}

	start := time.Now()
	err := wrapper.ThrottleDeployRequest(t.Context(), &ThrottleDeployRequestRequest{
		Organization:  "org",
		Database:      "db",
		Number:        1,
		ThrottleRatio: 0.5,
	})

	require.Error(t, err)
	assert.Contains(t, err.Error(), "throttle deploy request")
	assert.Less(t, time.Since(start), 10*time.Second, "call must return by the client timeout, not hang")
}

// The throttle endpoint authenticates with a service-token Authorization
// header and reports non-200 responses as errors.
func TestThrottleDeployRequestSendsAuthAndBody(t *testing.T) {
	var gotAuth []string
	var gotPath string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotAuth = r.Header.Values("Authorization")
		gotPath = r.URL.Path
		w.WriteHeader(http.StatusOK)
	}))
	t.Cleanup(server.Close)

	wrapper := &psClientWrapper{
		httpClient: newPlanetScaleHTTPClient(),
		baseURL:    server.URL,
		tokenName:  "token-name",
		tokenValue: "token-value",
	}

	err := wrapper.ThrottleDeployRequest(t.Context(), &ThrottleDeployRequestRequest{
		Organization:  "org",
		Database:      "db",
		Number:        42,
		ThrottleRatio: 0.5,
	})

	require.NoError(t, err)
	assert.Equal(t, []string{"token-name:token-value"}, gotAuth)
	assert.Equal(t, "/v1/organizations/org/databases/db/deploy-requests/42/throttle", gotPath)
}
