package client

import (
	"errors"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/block/spirit/pkg/utils"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/block/schemabot/pkg/cmd/cliname"
)

// shortTimeoutClient fails a request well before stalledServer answers it.
var shortTimeoutClient = &http.Client{Timeout: 100 * time.Millisecond}

// stalledServer reads each request and then holds it without answering until
// the client gives up or the test ends.
func stalledServer(t *testing.T) *httptest.Server {
	t.Helper()
	release := make(chan struct{})
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.Copy(io.Discard, r.Body)
		select {
		case <-r.Context().Done():
		case <-release:
		}
	}))
	t.Cleanup(srv.Close)
	t.Cleanup(func() { close(release) })
	return srv
}

// droppingServer reads each request and then closes the connection without
// answering, as a proxy or a crashing server does.
func droppingServer(t *testing.T) *httptest.Server {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.Copy(io.Discard, r.Body)
		conn, _, err := http.NewResponseController(w).Hijack()
		if err != nil {
			t.Errorf("hijack connection: %v", err)
			return
		}
		utils.CloseAndLog(conn)
	}))
	t.Cleanup(srv.Close)
	return srv
}

// closedPortEndpoint returns an endpoint on a loopback port nothing listens on.
func closedPortEndpoint(t *testing.T) string {
	t.Helper()
	var lc net.ListenConfig
	lis, err := lc.Listen(t.Context(), "tcp", "127.0.0.1:0")
	require.NoError(t, err)
	addr := lis.Addr().String()
	require.NoError(t, lis.Close())
	return "http://" + addr
}

func requireConnectionError(t *testing.T, err error) *ConnectionError {
	t.Helper()
	require.Error(t, err)
	var connErr *ConnectionError
	require.True(t, errors.As(err, &connErr), "want a ConnectionError, got %T: %v", err, err)
	return connErr
}

var statusHint = "Run '" + cliname.Command("status") + "' to check before you send it again."

// An apply request that times out after the server read it may still have
// started the apply. The operator sees that the request timed out, the
// client's own reason, and the command that shows whether the apply started,
// so they do not send a second apply blind.
func TestTimedOutPostNamesTheCauseAndWarnsItMayHaveTakenEffect(t *testing.T) {
	srv := stalledServer(t)

	var result map[string]any
	err := doPostIntoWithClient(t.Context(), shortTimeoutClient, srv.URL, "/api/apply", map[string]string{"database": "orders"}, &result)

	msg := requireConnectionError(t, err).Error()
	assert.Contains(t, msg, "request to "+srv.URL+" timed out: ")
	assert.Contains(t, msg, "Client.Timeout exceeded")
	assert.Contains(t, msg, "The server may already have acted on this request.")
	assert.Contains(t, msg, statusHint)
	assert.NotContains(t, msg, "/api/apply", "the cause should not repeat the request URL")
}

// A read that times out changed nothing on the server, so it names the cause
// without the warning.
func TestTimedOutGetNamesTheCauseWithoutTheWarning(t *testing.T) {
	srv := stalledServer(t)

	var result map[string]any
	err := doGetIntoWithClient(t.Context(), shortTimeoutClient, srv.URL, "/api/status", &result)

	msg := requireConnectionError(t, err).Error()
	assert.Contains(t, msg, "request to "+srv.URL+" timed out: ")
	assert.Contains(t, msg, "Client.Timeout exceeded")
	assert.NotContains(t, msg, "may already have acted")
}

// A server that drops the connection after reading a stop request may have
// recorded the stop, so the message names the cause and warns.
func TestDroppedPostNamesTheCauseAndWarnsItMayHaveTakenEffect(t *testing.T) {
	srv := droppingServer(t)

	err := doSendBody(srv.URL, http.MethodPost, "/api/stop", map[string]string{"apply_id": "apply-abc"})

	msg := requireConnectionError(t, err).Error()
	assert.Contains(t, msg, "connection failed: "+srv.URL+": EOF")
	assert.Contains(t, msg, statusHint)
}

// A refused connection never delivered the request, so the message stays the
// short "is the server running?" form with no warning.
func TestRefusedPostKeepsTheShortMessage(t *testing.T) {
	endpoint := closedPortEndpoint(t)

	var result map[string]any
	err := doPostIntoWithClient(t.Context(), shortTimeoutClient, endpoint, "/api/apply", map[string]string{"database": "orders"}, &result)

	msg := requireConnectionError(t, err).Error()
	assert.Equal(t, "cannot connect to "+endpoint+" (is the server running?)", msg)
}

// A host name that does not resolve never delivered the request either.
func TestUnresolvedHostKeepsTheShortMessage(t *testing.T) {
	err := FormatConnectionError("http://schemabot.invalid", &net.DNSError{
		Err: "no such host", Name: "schemabot.invalid", IsNotFound: true,
	})

	assert.Equal(t, "cannot resolve host: http://schemabot.invalid", err.Error())
}

// A cause relayed as text, with no errno or DNS error behind it, is still
// recognised as a refused connection.
func TestRefusedCauseWithoutAnErrnoKeepsTheShortMessage(t *testing.T) {
	err := FormatConnectionError("http://schemabot.test", errors.New("connection refused"))

	assert.Equal(t, "cannot connect to http://schemabot.test (is the server running?)", err.Error())
}
