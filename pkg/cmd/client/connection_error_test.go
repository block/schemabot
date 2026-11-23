package client

import (
	"errors"
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"io"
	"maps"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"slices"
	"strconv"
	"strings"
	"syscall"
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
		_, _, err := AcquireLock(endpoint, "orders", "mysql", "cli:alice", "", 0)
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

// A plan, a pull and a storage schema plan are POSTs that start nothing. One
// that fails is safe to send again, so its message names the cause and carries
// no warning, whether no answer arrived or the answer was cut off.
func TestFailedReadOnlyPostCarriesNoWarning(t *testing.T) {
	tests := []struct {
		name      string
		server    func(t *testing.T) *httptest.Server
		path      string
		wantCause string
	}{
		{name: "a plan that times out", server: stalledServer, path: "/api/plan", wantCause: "timed out: "},
		{name: "a pull whose connection is dropped", server: droppingServer, path: "/api/pull", wantCause: ": EOF"},
		{name: "a rollback plan whose answer is cut off", server: truncatedOK, path: "/api/rollback/plan", wantCause: "read response: unexpected EOF"},
		{name: "a storage schema plan whose answer is cut off", server: truncatedOK, path: "/api/storage/schema/plan", wantCause: "read response: unexpected EOF"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			srv := tc.server(t)

			var result map[string]any
			err := doPostIntoWithClient(t.Context(), shortTimeoutClient, srv.URL, tc.path, map[string]string{"database": "orders"}, &result)

			require.Error(t, err)
			assert.Contains(t, err.Error(), tc.wantCause)
			assert.NotContains(t, err.Error(), "\n", "a read fails with one line")
			assert.NotContains(t, err.Error(), "may already have acted")
		})
	}
}

// Every path the client sends with a method other than GET says what a failed
// request may have left behind: nothing, a change one command shows, or a
// change no single command shows. A path added to the client without an entry
// fails here, as does an entry for a path the client no longer sends.
func TestEveryNonGetRequestPathIsClassified(t *testing.T) {
	want := map[string]requestEffect{
		"/api/plan":                 {},
		"/api/pull":                 {},
		"/api/rollback/plan":        {},
		"/api/storage/schema/plan":  {},
		"/api/checks/scan":          {},
		"/api/checks/repos":         {},
		"/api/apply":                {changes: true, check: "status"},
		"/api/cutover":              {changes: true, check: "status"},
		"/api/stop":                 {changes: true, check: "status"},
		"/api/cancel":               {changes: true, check: "status"},
		"/api/start":                {changes: true, check: "status"},
		"/api/release":              {changes: true, check: "status"},
		"/api/revert":               {changes: true, check: "status"},
		"/api/skip-revert":          {changes: true, check: "status"},
		"/api/settings":             {changes: true, check: "settings"},
		"/api/locks":                {changes: true, check: "locks"},
		"/api/locks/acquire":        {changes: true, check: "locks"},
		"/api/freezes":              {changes: true, check: "freeze list"},
		"/api/freezes/":             {changes: true, check: "freeze list"},
		"/api/freezes/overrides/":   {changes: true, check: "freeze override <ovr-id>"},
		"/api/checks/synthesize":    {changes: true},
		"/api/storage/schema/apply": {changes: true},
	}

	assert.Equal(t, want, requestEffects)
	assert.Equal(t, slices.Sorted(maps.Keys(want)), nonGetPathsTheClientSends(t))
}

// nonGetPathsTheClientSends reads this package's source and returns, sorted,
// every API path written out in a call to one of the helpers that send a POST
// or a DELETE, or in a request built by hand with a method other than GET.
func nonGetPathsTheClientSends(t *testing.T) []string {
	t.Helper()
	entries, err := os.ReadDir(".")
	require.NoError(t, err)

	paths := map[string]bool{}
	collect := func(node ast.Node) {
		ast.Inspect(node, func(n ast.Node) bool {
			lit, ok := n.(*ast.BasicLit)
			if !ok || lit.Kind != token.STRING {
				return true
			}
			value, err := strconv.Unquote(lit.Value)
			require.NoError(t, err)
			if strings.HasPrefix(value, "/api/") {
				paths[value] = true
			}
			return true
		})
	}

	fset := token.NewFileSet()
	for _, entry := range entries {
		name := entry.Name()
		if !strings.HasSuffix(name, ".go") || strings.HasSuffix(name, "_test.go") {
			continue
		}
		file, err := parser.ParseFile(fset, name, nil, parser.SkipObjectResolution)
		require.NoError(t, err)
		ast.Inspect(file, func(n ast.Node) bool {
			call, ok := n.(*ast.CallExpr)
			if !ok {
				return true
			}
			switch calledFunction(call) {
			case "doPostInto", "doSlowPostIntoCtx", "doPostIntoWithClient", "doSendBody", "callControlAPI":
				for _, arg := range call.Args {
					collect(arg)
				}
			case "NewRequestWithContext":
				// The method is the second argument and the URL the third.
				if len(call.Args) == 4 && !namesMethodGet(call.Args[1]) {
					collect(call.Args[2])
				}
			}
			return true
		})
	}
	return slices.Sorted(maps.Keys(paths))
}

// calledFunction returns the name of the function a call invokes, without its
// package or type arguments.
func calledFunction(call *ast.CallExpr) string {
	fun := call.Fun
	if index, ok := fun.(*ast.IndexExpr); ok {
		fun = index.X
	}
	switch fun := fun.(type) {
	case *ast.Ident:
		return fun.Name
	case *ast.SelectorExpr:
		return fun.Sel.Name
	}
	return ""
}

func namesMethodGet(expr ast.Expr) bool {
	sel, ok := expr.(*ast.SelectorExpr)
	return ok && sel.Sel.Name == "MethodGet"
}

// The warning depends on what was sent and when it failed. A request that only
// reads never warns. A request that failed before any of it was sent never
// warns either: the server cannot have acted on what it never received.
func TestWarningDependsOnTheRequestAndWhenItFailed(t *testing.T) {
	const endpoint = "http://schemabot.test"
	tests := []struct {
		name        string
		err         *url.Error
		wantMessage string
	}{
		{
			name:        "a HEAD request that lost its connection",
			err:         &url.Error{Op: "Head", URL: endpoint + "/api/apply", Err: io.ErrUnexpectedEOF},
			wantMessage: "connection failed: " + endpoint + ": unexpected EOF",
		},
		{
			name:        "a DELETE that lost its connection",
			err:         &url.Error{Op: "Delete", URL: endpoint + "/api/locks", Err: io.ErrUnexpectedEOF},
			wantMessage: "connection failed: " + endpoint + ": unexpected EOF\n" + mayHaveActed + " " + checkHint("locks"),
		},
		{
			name:        "a request to a path nobody classified",
			err:         &url.Error{Op: "Post", URL: endpoint + "/api/reports", Err: io.ErrUnexpectedEOF},
			wantMessage: "connection failed: " + endpoint + ": unexpected EOF\n" + mayHaveActed + " Check what it did before you send it again.",
		},
		{
			name: "an apply whose connection took too long to open",
			err: &url.Error{Op: "Post", URL: endpoint + "/api/apply",
				Err: &net.OpError{Op: "dial", Net: "tcp", Err: os.ErrDeadlineExceeded}},
			wantMessage: "request to " + endpoint + " timed out: dial tcp: i/o timeout",
		},
		{
			name: "an apply whose proxy refused to open a tunnel",
			err: &url.Error{Op: "Post", URL: endpoint + "/api/apply",
				Err: &net.OpError{Op: "proxyconnect", Net: "tcp", Err: errors.New("Forbidden")}},
			wantMessage: "connection failed: " + endpoint + ": proxyconnect tcp: Forbidden",
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			err := FormatConnectionError(endpoint, tc.err)
			assert.Equal(t, tc.wantMessage, err.Error())
			assert.Equal(t, strings.Contains(tc.wantMessage, mayHaveActed), ServerMayHaveActed(err),
				"ServerMayHaveActed agrees with the warning in the message")
		})
	}
}

// ServerMayHaveActed answers for real failures the way their messages do: yes
// when a request that changes something got no complete answer, and no when
// the server answered, the request never reached it, or it only reads.
func TestServerMayHaveActedMatchesWhatTheFailureMessageSays(t *testing.T) {
	release := map[string]string{"database": "orders", "database_type": "mysql", "owner": "cli:alice@laptop"}
	refusing := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, `{"error":"storage unavailable"}`, http.StatusInternalServerError)
	}))
	t.Cleanup(refusing.Close)

	tests := []struct {
		name string
		err  error
		want bool
	}{
		{
			name: "a lock release the server read and never answered",
			err:  doSendBody(droppingServer(t).URL, http.MethodDelete, "/api/locks", release),
			want: true,
		},
		{
			name: "a lock release whose answer was cut off",
			err:  doSendBody(truncatedOK(t).URL, http.MethodDelete, "/api/locks", release),
			want: true,
		},
		{
			name: "a lock release wrapped by its caller",
			err:  fmt.Errorf("release lock for orders: %w", doSendBody(droppingServer(t).URL, http.MethodDelete, "/api/locks", release)),
			want: true,
		},
		{
			name: "a lock release the server answered with an error",
			err:  doSendBody(refusing.URL, http.MethodDelete, "/api/locks", release),
			want: false,
		},
		{
			name: "a lock release whose connection was refused",
			err:  doSendBody(closedPortEndpoint(t), http.MethodDelete, "/api/locks", release),
			want: false,
		},
		{
			name: "a read the server never answered",
			err:  doGetInto(droppingServer(t).URL, "/api/locks", &map[string]any{}),
			want: false,
		},
		{
			name: "an error that is not a failed request",
			err:  ErrLockNotFound,
			want: false,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			require.Error(t, tc.err)
			assert.Equal(t, tc.want, ServerMayHaveActed(tc.err), tc.err.Error())
			assert.Equal(t, tc.want, strings.Contains(tc.err.Error(), mayHaveActed), tc.err.Error())
		})
	}
}

// A certificate the client does not trust fails the TLS handshake, before the
// apply is sent. The message names the certificate problem and does not say
// the server may have acted.
func TestUntrustedCertificateCarriesNoWarning(t *testing.T) {
	srv := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		t.Errorf("the request reached the server: %s %s", r.Method, r.URL.Path)
	}))
	t.Cleanup(srv.Close)

	var result map[string]any
	err := doPostIntoWithClient(t.Context(), &http.Client{Timeout: 5 * time.Second}, srv.URL, "/api/apply", map[string]string{"database": "orders"}, &result)

	msg := requireConnectionError(t, err).Error()
	assert.Contains(t, msg, "connection failed: "+srv.URL+": tls: failed to verify certificate")
	assert.NotContains(t, msg, "may already have acted")
}

// An https endpoint that reaches a plain HTTP server fails the TLS handshake
// too, so the apply was never sent and the message carries no warning.
func TestPlainHTTPServerBehindAnHTTPSEndpointCarriesNoWarning(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		t.Errorf("the request reached the server: %s %s", r.Method, r.URL.Path)
	}))
	t.Cleanup(srv.Close)
	endpoint := "https://" + srv.Listener.Addr().String()

	var result map[string]any
	err := doPostIntoWithClient(t.Context(), &http.Client{Timeout: 5 * time.Second}, endpoint, "/api/apply", map[string]string{"database": "orders"}, &result)

	msg := requireConnectionError(t, err).Error()
	assert.Equal(t, "connection failed: "+endpoint+": http: server gave HTTP response to HTTPS client", msg)
}

// A plain HTTP server answering an https request is recognized by the error
// value the HTTP client returns, however deep it sits in the chain: the apply
// was never sent, so the message carries no warning. An error that only
// carries the same words says nothing about this request's own handshake, so
// it gets the warning like any other failure the client cannot place.
func TestPlainHTTPServerIsRecognizedByErrorValueNotByItsWords(t *testing.T) {
	const endpoint = "https://schemabot.test"
	const words = "http: server gave HTTP response to HTTPS client"
	tests := []struct {
		name        string
		cause       error
		wantMessage string
	}{
		{
			name:        "the client's own error value, wrapped again on the way up",
			cause:       fmt.Errorf("round trip: %w", http.ErrSchemeMismatch),
			wantMessage: "connection failed: " + endpoint + ": round trip: " + words,
		},
		{
			name:        "another error with the same words",
			cause:       errors.New(words),
			wantMessage: "connection failed: " + endpoint + ": " + words + "\n" + mayHaveActed + " " + statusHint,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			err := &url.Error{Op: "Post", URL: endpoint + "/api/apply", Err: tc.cause}
			assert.Equal(t, tc.wantMessage, FormatConnectionError(endpoint, err).Error())
		})
	}
}

// An https endpoint that reaches something that speaks neither TLS nor HTTP,
// such as a database port, fails the TLS handshake as well.
func TestNonTLSServerBehindAnHTTPSEndpointCarriesNoWarning(t *testing.T) {
	var lc net.ListenConfig
	lis, err := lc.Listen(t.Context(), "tcp", "127.0.0.1:0")
	require.NoError(t, err)
	t.Cleanup(func() { utils.CloseAndLog(lis) })
	go func() {
		conn, err := lis.Accept()
		if err != nil {
			return
		}
		// A server that greets first, the way a database does.
		_, _ = conn.Write([]byte("\x4a\x00\x00\x00\x0a8.0.36\x00"))
		utils.CloseAndLog(conn)
	}()
	endpoint := "https://" + lis.Addr().String()

	var result map[string]any
	err = doPostIntoWithClient(t.Context(), &http.Client{Timeout: 5 * time.Second}, endpoint, "/api/apply", map[string]string{"database": "orders"}, &result)

	msg := requireConnectionError(t, err).Error()
	assert.Contains(t, msg, "connection failed: "+endpoint+": tls: first record does not look like a TLS handshake")
	assert.NotContains(t, msg, "may already have acted")
}

// The hint is a command the operator can paste, so it carries the routing
// flags this run was given and reaches the same server.
func TestWarningHintKeepsTheRoutingFlags(t *testing.T) {
	cliname.SetRoute(cliname.Flag{Name: "--profile", Value: "prod"})
	t.Cleanup(func() { cliname.SetRoute() })
	srv := droppingServer(t)

	err := doSendBody(srv.URL, http.MethodPost, "/api/stop", map[string]string{"apply_id": "apply-abc"})

	msg := requireConnectionError(t, err).Error()
	assert.Contains(t, msg, "Run '"+cliname.Name()+" status --profile prod' to check before you send it again.")
}

// The cause comes from the network path, a proxy included, so control
// characters are removed before it reaches the operator's terminal.
func TestCauseIsStrippedOfControlCharacters(t *testing.T) {
	err := FormatConnectionError("http://schemabot.test", &url.Error{
		Op: "Post", URL: "http://schemabot.test/api/apply", Err: errors.New("proxy said \x1b[2Jno"),
	})

	assert.NotContains(t, err.Error(), "\x1b")
	assert.Contains(t, err.Error(), "connection failed: http://schemabot.test: proxy said ")
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

// errnoInOtherWords is a cause that carries an errno under wording of its own,
// as an operating system that words the errno differently does.
type errnoInOtherWords struct {
	text  string
	errno syscall.Errno
}

func (e errnoInOtherWords) Error() string { return e.text }
func (e errnoInOtherWords) Unwrap() error { return e.errno }

// A refused connection is recognised by its errno, whatever its wording.
func TestRefusedCauseInOtherWordsKeepsTheShortMessage(t *testing.T) {
	err := FormatConnectionError("http://schemabot.test", errnoInOtherWords{
		text:  "No connection could be made because the target machine actively refused it.",
		errno: syscall.ECONNREFUSED,
	})

	assert.Equal(t, "cannot connect to http://schemabot.test (is the server running?)", err.Error())
}

// A host name that does not resolve is recognised by the resolver's verdict,
// whatever its wording.
func TestUnresolvedHostInOtherWordsKeepsTheShortMessage(t *testing.T) {
	err := FormatConnectionError("http://schemabot.invalid", &net.DNSError{
		Err: "nodename nor servname provided, or not known", Name: "schemabot.invalid", IsNotFound: true,
	})

	assert.Equal(t, "cannot resolve host: http://schemabot.invalid", err.Error())
}

// A cause relayed as text, with no errno or DNS error behind it, is still
// recognised as a refused connection.
func TestRefusedCauseWithoutAnErrnoKeepsTheShortMessage(t *testing.T) {
	err := FormatConnectionError("http://schemabot.test", errors.New("connection refused"))

	assert.Equal(t, "cannot connect to http://schemabot.test (is the server running?)", err.Error())
}

// A freeze request carries an ID in its path, and is still classified by its
// route: setting or lifting a freeze points at the freeze list, and an
// override approval at the override request.
func TestEffectOfClassifiesFreezeRoutesWithIDs(t *testing.T) {
	assert.Equal(t, shownBy("freeze list"), effectOf(http.MethodPost, "/api/freezes"))
	assert.Equal(t, shownBy("freeze list"), effectOf(http.MethodPost, "/api/freezes/freeze-abc/lift"))
	assert.Equal(t, shownBy("freeze override <ovr-id>"), effectOf(http.MethodPost, "/schemabot/api/freezes/overrides/ovr-1/approve"))
	assert.Equal(t, readsOnly, effectOf(http.MethodGet, "/api/freezes/freeze-abc"))
}
