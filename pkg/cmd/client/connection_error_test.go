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

// truncatingServer reads each request, starts answering it with status, and
// then cuts the answer off partway through its body, as a server shutting
// down mid-response does.
func truncatingServer(t *testing.T, status int) *httptest.Server {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.Copy(io.Discard, r.Body)
		w.Header().Set("Content-Type", "application/json")
		// A declared length the body never reaches makes the early end
		// visible to the client.
		w.Header().Set("Content-Length", "64")
		w.WriteHeader(status)
		_, _ = w.Write([]byte(`{"accepted":`))
		_ = http.NewResponseController(w).Flush()
		panic(http.ErrAbortHandler)
	}))
	t.Cleanup(srv.Close)
	return srv
}

// truncatedOK is a truncatingServer whose cut-off answer reports success.
func truncatedOK(t *testing.T) *httptest.Server {
	t.Helper()
	return truncatingServer(t, http.StatusOK)
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

const mayHaveActed = "The server may already have acted on this request."

// checkHint is the end of the warning when command shows what the server did.
func checkHint(command string) string {
	return "Run '" + cliname.Command(command) + "' to check before you send it again."
}

var statusHint = checkHint("status")

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

// A stop is recorded before the server acknowledges it. When the 202 answer is
// cut off partway through, the stop still takes effect, so the operator gets
// the same warning as when no answer arrived at all.
func TestTruncatedAcceptedStopWarnsItMayHaveTakenEffect(t *testing.T) {
	srv := truncatingServer(t, http.StatusAccepted)

	var result map[string]any
	err := doPostInto(srv.URL, "/api/stop", map[string]string{"apply_id": "apply-abc"}, &result)

	var readErr *ResponseReadError
	require.ErrorAs(t, err, &readErr)
	assert.Equal(t, "read response: unexpected EOF\n"+mayHaveActed+" "+statusHint, err.Error())
}

// A read whose answer is cut off changed nothing, so it carries no warning.
func TestTruncatedGetCarriesNoWarning(t *testing.T) {
	srv := truncatingServer(t, http.StatusOK)

	var result map[string]any
	err := doGetIntoWithClient(t.Context(), srv.Client(), srv.URL, "/api/status", &result)

	var readErr *ResponseReadError
	require.ErrorAs(t, err, &readErr)
	assert.Equal(t, "read response: unexpected EOF", err.Error())
}

// The warning sends the operator to the command that shows the thing they
// tried to change: the settings after a settings update, the locks after a
// lock change. It does so whether no answer arrived or the answer was cut off.
func TestWarningNamesTheCommandThatShowsWhatTheRequestChanged(t *testing.T) {
	releaseLock := func(endpoint string) error {
		return ReleaseLock(endpoint, "orders", "mysql", "cli:alice")
	}
	acquireLock := func(endpoint string) error {
		_, err := AcquireLock(endpoint, "orders", "mysql", "cli:alice", "", 0)
		return err
	}
	setSetting := func(endpoint string) error {
		return SetSetting(endpoint, "require_review", "true")
	}
	tests := []struct {
		name     string
		server   func(t *testing.T) *httptest.Server
		basePath string
		send     func(endpoint string) error
		wantHint string
	}{
		{name: "settings update, no answer", server: droppingServer, send: setSetting, wantHint: checkHint("settings")},
		{name: "settings update, answer cut off", server: truncatedOK, send: setSetting, wantHint: checkHint("settings")},
		{name: "lock release, no answer", server: droppingServer, send: releaseLock, wantHint: checkHint("locks")},
		{name: "lock release, answer cut off", server: truncatedOK, send: releaseLock, wantHint: checkHint("locks")},
		{name: "lock acquire, no answer", server: droppingServer, send: acquireLock, wantHint: checkHint("locks")},
		{name: "lock acquire, answer cut off", server: truncatedOK, send: acquireLock, wantHint: checkHint("locks")},
		{name: "endpoint with a path of its own", server: droppingServer, basePath: "/schemabot", send: setSetting, wantHint: checkHint("settings")},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			srv := tc.server(t)

			err := tc.send(srv.URL + tc.basePath)

			require.Error(t, err)
			assert.Contains(t, err.Error(), "\n"+mayHaveActed+" "+tc.wantHint)
		})
	}
}

// A request with no single command that shows its result still warns, without
// pointing the operator at a command that cannot answer the question.
func TestWarningNamesNoCommandWhenNoneShowsTheResult(t *testing.T) {
	srv := droppingServer(t)

	var result map[string]any
	err := doPostInto(srv.URL, "/api/checks/synthesize", map[string]string{"repository": "acme/orders"}, &result)

	msg := requireConnectionError(t, err).Error()
	assert.Contains(t, msg, "\n"+mayHaveActed+" Check what it did before you send it again.")
	assert.NotContains(t, msg, "Run '")
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
