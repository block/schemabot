package client

import (
	"bytes"
	"context"
	"crypto/tls"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"net"
	"net/http"
	"net/url"
	"os"
	"slices"
	"strconv"
	"strings"
	"syscall"
	"time"

	"golang.org/x/net/html"

	"github.com/block/schemabot/pkg/apitypes"
	"github.com/block/schemabot/pkg/cmd/cliname"
	"github.com/block/schemabot/pkg/ui"
)

// authTransport injects a Bearer token on outbound requests when one is
// configured. It wraps the default transport so every CLI request — including
// those built outside the doGet/doPost helpers — is authenticated uniformly.
var authTransport = &bearerTransport{base: http.DefaultTransport}

// httpClient is the shared HTTP client for all CLI requests.
// Uses a 30s timeout to avoid hanging indefinitely on network stalls.
var httpClient = &http.Client{Timeout: 30 * time.Second, Transport: authTransport}

// operatorHTTPClient serves the operator endpoints whose server-side work
// routinely needs far longer than the default client timeout: the webhook
// operations that crawl GitHub delivery history or every open PR, and the
// storage schema convergence that runs a bootstrap against the storage
// database. Matches the server's own budget for these routes.
var operatorHTTPClient = &http.Client{Timeout: 15 * time.Minute, Transport: authTransport}

// clientForBudget builds a client for one request whose server-side work is
// bounded by a budget the caller named, rather than by a fixed one this package
// can know in advance.
//
// A client timeout below the server's budget is the worst of both: the server
// converges to completion while the client gives up, so the operator is told
// their command timed out and has no way to tell whether the DDL ran. The
// margin covers the round trip and the response either side of the work.
func clientForBudget(budget time.Duration) *http.Client {
	return &http.Client{Timeout: budget + operatorResponseMargin, Transport: authTransport}
}

// operatorResponseMargin is the room a bounded operator request needs beyond
// the server's own budget: the two diffs bracketing a convergence, plus the
// round trip and the response. It is deliberately generous — overshooting means
// waiting a little longer for an answer the server is still going to send,
// while undershooting means not getting it at all.
const operatorResponseMargin = 5 * time.Minute

// SetAuthToken configures the Bearer token attached to every CLI request. An
// empty token leaves requests unauthenticated, which is correct against a
// server with auth disabled. Surrounding whitespace is trimmed so a token
// sourced from an environment variable or file does not break the header.
func SetAuthToken(token string) {
	authTransport.token = strings.TrimSpace(token)
	authTransport.origin = ""
}

// bearerTransport sets "Authorization: Bearer <token>" on each request when a
// token is configured and the header is not already set.
//
// The token belongs to the server the command addressed, so across a redirect
// it travels only while every hop stays on the origin (scheme, host, and port)
// of the request the command sent. That is stricter than net/http, which keeps
// sensitive headers across a redirect to a subdomain, another port, or another
// scheme of the same host. A redirect to another origin is still followed, as
// net/http follows it, but without the token; once a chain has left the origin
// it stays unauthenticated, as net/http's own stripping of sensitive headers
// does. A redirect whose chain the base transport did not record in full is
// treated as having left the origin, since the token's origin cannot be
// established from what remains.
type bearerTransport struct {
	base  http.RoundTripper
	token string
	// origin, when set, is the one origin (as RequestOrigin renders it) the
	// private local-runtime credential may be sent to; every other request
	// is refused outright.
	origin string
}

func (t *bearerTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	if t.origin != "" && RequestOrigin(req.URL) != t.origin {
		return nil, fmt.Errorf("refusing to forward local runtime credentials to another endpoint")
	}
	chain, complete := redirectChain(req)
	if !complete {
		WarnUnauthenticatedRedirect("its origin cannot be established", "", RequestOrigin(req.URL))
		return t.base.RoundTrip(withoutAuthorization(req))
	}
	first := chain[0]
	if !StayedOnFirstOrigin(chain) {
		WarnUnauthenticatedRedirect("it is on another origin", RequestOrigin(first.URL), RequestOrigin(req.URL))
		return t.base.RoundTrip(withoutAuthorization(req))
	}
	if t.token != "" && req.Header.Get("Authorization") == "" {
		if err := GuardInsecureToken(req.URL); err != nil {
			return nil, err
		}
		// RoundTrip must not mutate the caller's request, so clone it.
		req = req.Clone(req.Context())
		req.Header.Set("Authorization", "Bearer "+t.token)
	}
	return t.base.RoundTrip(req)
}

// warnWriter receives the CLI's warning lines. Nil means the process's stderr
// as it is when the warning is written, the same stream the commands use for
// their own warnings, so an operator sees why a later 401 happened next to the
// command output rather than in a log format they filter out. It is resolved at
// write time rather than bound at package init, so a caller that redirects
// os.Stderr sees the warning too.
var warnWriter io.Writer

// WarnUnauthenticatedRedirect tells the operator that a redirect is being
// followed without the token and why, naming the origin the token belongs to
// (when known) and the origin the redirect points at.
func WarnUnauthenticatedRedirect(reason, tokenOrigin, redirectOrigin string) {
	subject := "the auth token"
	if tokenOrigin != "" {
		subject += " for " + tokenOrigin
	}
	// The warning is advisory. A stderr that refuses writes leaves nowhere to
	// report that, and the request itself is unaffected.
	out := warnWriter
	if out == nil {
		out = os.Stderr
	}
	_, _ = fmt.Fprintf(out, "Warning: not sending %s to redirect target %s because %s; the request continues unauthenticated\n",
		subject, redirectOrigin, reason)
}

// withoutAuthorization returns a copy of req with no Authorization header.
// RoundTrip must not mutate the caller's request, so the header is removed
// from a clone. The clone is the contract, not a sign the header is expected:
// by the time a cross-host redirect reaches this transport net/http has usually
// stripped it already, and the copy is then of a request with nothing to remove.
func withoutAuthorization(req *http.Request) *http.Request {
	stripped := req.Clone(req.Context())
	stripped.Header.Del("Authorization")
	return stripped
}

// redirectChain returns the requests net/http sent to arrive at req, oldest
// first and ending with req itself, and whether that chain is complete. A
// request that is not a redirect is a complete chain of one.
//
// net/http links each redirected request to the response that caused it, and
// the response to the request that produced it. A base RoundTripper that omits
// that second link leaves the earlier hops unknowable, so the chain is reported
// incomplete rather than presenting the hop it stopped at as the first request.
func redirectChain(req *http.Request) (chain []*http.Request, complete bool) {
	chain = []*http.Request{req}
	for hop := req; hop.Response != nil; hop = hop.Response.Request {
		if hop.Response.Request == nil {
			slices.Reverse(chain)
			return chain, false
		}
		chain = append(chain, hop.Response.Request)
	}
	slices.Reverse(chain)
	return chain, true
}

// StayedOnFirstOrigin reports whether every hop in a redirect chain targets the
// origin of the request the command sent. The chain is oldest first, so an
// http.Client CheckRedirect policy passes its via requests with the pending
// request appended. Any CLI client that sends a credential across redirects
// holds it to this rule, so the token stays with the origin it was sent to.
// An empty chain has sent nothing, so it has left no origin.
func StayedOnFirstOrigin(chain []*http.Request) bool {
	if len(chain) == 0 {
		return true
	}
	origin := RequestOrigin(chain[0].URL)
	for _, hop := range chain[1:] {
		if RequestOrigin(hop.URL) != origin {
			return false
		}
	}
	return true
}

// RequestOrigin renders a URL's origin as scheme://host:port, with the scheme
// and host lowercased and the scheme's default port made explicit, so the same
// server compares equal however a redirect spells it.
func RequestOrigin(u *url.URL) string {
	scheme := strings.ToLower(u.Scheme)
	port := u.Port()
	if port == "" {
		switch scheme {
		case "https":
			port = "443"
		case "http":
			port = "80"
		}
	}
	return scheme + "://" + net.JoinHostPort(strings.ToLower(u.Hostname()), port)
}

// ErrInsecureTokenTransport is returned when a Bearer token would be sent over
// a plaintext connection to a non-loopback host.
var ErrInsecureTokenTransport = errors.New("refusing to send auth token over an insecure connection")

// GuardInsecureToken refuses to attach a Bearer token to a plaintext connection
// unless it targets loopback. A token sent over http:// to a remote host is
// exposed to anyone on the network path, so this fails closed rather than
// leaking the credential; https and local (loopback) endpoints are allowed.
//
// It is exported because the rule belongs to the credential, not to this
// transport: any CLI path that attaches a bearer token to a URL an operator's
// environment supplied answers to it.
func GuardInsecureToken(u *url.URL) error {
	if u.Scheme == "https" || isLoopbackHost(u.Hostname()) {
		return nil
	}
	return fmt.Errorf("%w to %s: use an https:// endpoint (loopback is allowed for local testing)", ErrInsecureTokenTransport, u.Host)
}

func isLoopbackHost(host string) bool {
	if host == "localhost" {
		return true
	}
	if ip := net.ParseIP(host); ip != nil {
		return ip.IsLoopback()
	}
	return false
}

// APIError represents an error response from the API.
type APIError struct {
	Status    int    // HTTP status code (e.g., 404, 500)
	ErrorCode string // Error code from API response (e.g., "not_found", "storage_error")
	Message   string

	// Blockers, OverrideRequest and OverrideOnReason are set on a
	// change_gate_refused answer: what refused the change, the override
	// request SchemaBot recorded for the caller, if any, and whether a repeat
	// with an override reason runs with no approver.
	Blockers         []apitypes.ChangeGateBlocker
	OverrideRequest  *apitypes.FreezeOverrideRequest
	OverrideOnReason bool

	// LockHolder is set on a lock_in_use answer: the apply that kept the
	// lock.
	LockHolder *apitypes.LockHolder

	// RetryAfterSeconds is the delay the server asked the client to wait, set
	// only on responses that advertise one. Read it through RetryAfter rather
	// than on its own, so a retry is never scheduled without it.
	RetryAfterSeconds int

	// RetryAfterHeader is the delay the response's Retry-After header asked
	// for, zero when it sent none or one that could not be read. Unlike
	// RetryAfter it does not say whether to retry: a proxy in front of the
	// server can send it with no error code, so the caller decides from the
	// status whether the request is worth repeating.
	RetryAfterHeader time.Duration

	// RetryAfterHeaderUncapped is the delay the Retry-After header asked for
	// when it was longer than the CLI's bound, so RetryAfterHeader holds the
	// bound instead. Zero when the header asked for no more than the bound, so
	// a caller can tell an operator the wait was shortened.
	RetryAfterHeaderUncapped time.Duration
}

func (e *APIError) Error() string {
	return e.Message
}

// RetryAfter reports whether this request should be retried and how long to
// wait first, so a caller automating against the API gets both facts from one
// call instead of retrying on the code and ignoring the delay. Whether to retry
// comes from the error code alone; the delay is the longer of the body's and
// the Retry-After header's, so no caller retries sooner than either asked.
func (e *APIError) RetryAfter() (retry bool, after time.Duration) {
	retry, after = apitypes.ErrorResponse{ErrorCode: e.ErrorCode, RetryAfterSeconds: e.RetryAfterSeconds}.RetryAfter()
	if !retry {
		return false, 0
	}
	return true, max(after, e.RetryAfterHeader)
}

// IsNotFound reports whether the error is a 404 from the API.
func IsNotFound(err error) bool {
	var apiErr *APIError
	return errors.As(err, &apiErr) && apiErr.Status == http.StatusNotFound
}

// doGetInto sends a GET request and unmarshals the JSON response into result.
// Returns an *APIError for non-200 responses (use IsNotFound to check for 404).
func doGetInto(endpoint, path string, result any) error {
	return doGetIntoCtx(context.Background(), endpoint, path, result)
}

// doGetIntoCtx is like doGetInto but accepts a context for timeout/cancellation control.
func doGetIntoCtx(ctx context.Context, endpoint, path string, result any) error {
	return doGetIntoWithClient(ctx, httpClient, endpoint, path, result)
}

// doSlowGetIntoCtx is doGetIntoCtx with the long-running operator client, for
// read endpoints whose server-side work calls out to GitHub or reads a storage
// catalog, and so shares the operator budget rather than the default request
// timeout.
func doSlowGetIntoCtx(ctx context.Context, endpoint, path string, result any) error {
	return doGetIntoWithClient(ctx, operatorHTTPClient, endpoint, path, result)
}

func doGetIntoWithClient(ctx context.Context, client *http.Client, endpoint, path string, result any) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint+path, nil)
	if err != nil {
		return err
	}
	resp, err := client.Do(req)
	if err != nil {
		// Surface a canceled context as-is so callers can tell an operator
		// cancellation apart from a real connection failure.
		if ctxErr := ctx.Err(); ctxErr != nil {
			return ctxErr
		}
		return FormatConnectionError(endpoint, err)
	}
	defer func() { _ = resp.Body.Close() }()

	respBody, err := io.ReadAll(resp.Body)
	if err != nil {
		return &ResponseReadError{Err: err}
	}

	if resp.StatusCode != http.StatusOK {
		return parseAPIError(resp, respBody)
	}

	if err := json.Unmarshal(respBody, result); err != nil {
		return checkNonJSONResponse(resp, respBody, err)
	}
	return nil
}

// ResponseReadError means the server started answering a request and the
// response body then failed before it was complete: the connection was reset,
// an HTTP/2 stream was reset, or the read timed out. The server was reached,
// so this is not a ConnectionError, and no complete answer arrived, so it is
// not an APIError. The cause varies with the protocol in use, which is why
// callers that poll match this type instead of the cause.
//
// Method and Path name the request when it was not a GET. The server received
// it before its answer failed, so when the request changes something, such as
// a stop, the message carries the same warning a ConnectionError does.
type ResponseReadError struct {
	Method string
	Path   string
	Err    error
}

func (e *ResponseReadError) Error() string {
	msg := "read response: " + e.Err.Error()
	if warning := e.effect().warning(); warning != "" {
		msg += "\n" + warning
	}
	return msg
}

// effect returns what the request may have left behind on the server, which
// received it before the answer failed.
func (e *ResponseReadError) effect() requestEffect {
	return effectOf(e.Method, e.Path)
}

func (e *ResponseReadError) Unwrap() error {
	return e.Err
}

// doSendBody sends a request with a JSON body and checks for success.
// Used for POST/DELETE operations that don't need to parse the response.
func doSendBody(endpoint, method, path string, body any) error {
	jsonBody, err := json.Marshal(body)
	if err != nil {
		return fmt.Errorf("marshal request body: %w", err)
	}
	req, err := http.NewRequestWithContext(context.Background(), method, endpoint+path, bytes.NewReader(jsonBody))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := httpClient.Do(req)
	if err != nil {
		return FormatConnectionError(endpoint, err)
	}
	defer func() { _ = resp.Body.Close() }()

	respBody, err := io.ReadAll(resp.Body)
	if err != nil {
		return &ResponseReadError{Method: method, Path: path, Err: err}
	}

	if resp.StatusCode != http.StatusOK {
		return parseAPIError(resp, respBody)
	}
	return nil
}

// doPostInto sends a JSON POST to endpoint+path and unmarshals the JSON response into result.
// Returns an *APIError for error responses (use IsNotFound to check for 404).
func doPostInto(endpoint, path string, body any, result any) error {
	return doPostIntoWithClient(context.Background(), httpClient, endpoint, path, body, result)
}

// doSlowPostIntoCtx is doPostInto with the long-running operator client and
// a caller-supplied context, for operator endpoints whose server-side work
// legitimately outlives the default timeout and that must stop promptly when
// the operator cancels (Ctrl+C).
func doSlowPostIntoCtx(ctx context.Context, endpoint, path string, body any, result any) error {
	return doPostIntoWithClient(ctx, operatorHTTPClient, endpoint, path, body, result)
}

func doPostIntoWithClient(ctx context.Context, client *http.Client, endpoint, path string, body any, result any) error {
	jsonBody, err := json.Marshal(body)
	if err != nil {
		return fmt.Errorf("marshal request body: %w", err)
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint+path, bytes.NewReader(jsonBody))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := client.Do(req)
	if err != nil {
		// Surface a canceled context as-is so callers can tell an operator
		// cancellation apart from a real connection failure.
		if ctxErr := ctx.Err(); ctxErr != nil {
			return ctxErr
		}
		return FormatConnectionError(endpoint, err)
	}
	defer func() { _ = resp.Body.Close() }()

	respBody, err := io.ReadAll(resp.Body)
	if err != nil {
		return &ResponseReadError{Method: http.MethodPost, Path: path, Err: err}
	}

	// Control endpoints answer 202 Accepted when the request is durably
	// recorded and the apply owner completes it asynchronously; the body
	// carries the same JSON shape as a 200 and is equally a success. Creating a
	// freeze answers 201 Created with the new freeze in the body.
	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusAccepted && resp.StatusCode != http.StatusCreated {
		return parseAPIError(resp, respBody)
	}

	if err := json.Unmarshal(respBody, result); err != nil {
		return checkNonJSONResponse(resp, respBody, err)
	}
	return nil
}

// checkNonJSONResponse provides a clear error when the server returns non-JSON
// (e.g., an HTML auth page from a proxy). Falls back to the original parse error.
func checkNonJSONResponse(resp *http.Response, body []byte, parseErr error) error {
	ct := resp.Header.Get("Content-Type")
	if strings.Contains(ct, "html") || (len(body) > 0 && body[0] == '<') {
		// Extract visible text from HTML for context (strip tags).
		text := stripHTMLTags(string(body))
		text = strings.Join(strings.Fields(text), " ") // collapse whitespace
		// The tokenizer decodes character references, so a control rune can
		// arrive spelled out as one. They are removed before the text is cut to
		// length.
		text = ui.TerminalSafe(text)
		if len(text) > 200 {
			text = text[:200] + "..."
		}
		if text != "" {
			return fmt.Errorf("unexpected response from server (received HTML, expected JSON):\n  %s", text)
		}
		return fmt.Errorf("unexpected response from server (received HTML, expected JSON)")
	}
	return fmt.Errorf("parse response: %w", parseErr)
}

// stripHTMLTags extracts visible text from HTML using the x/net/html tokenizer.
// Skips <style> and <script> content.
func stripHTMLTags(s string) string {
	tokenizer := html.NewTokenizer(strings.NewReader(s))
	var b strings.Builder
	skip := false
	for {
		tt := tokenizer.Next()
		switch tt {
		case html.ErrorToken:
			return strings.TrimSpace(b.String())
		case html.StartTagToken:
			tn, _ := tokenizer.TagName()
			tag := string(tn)
			if tag == "style" || tag == "script" {
				skip = true
			}
		case html.EndTagToken:
			tn, _ := tokenizer.TagName()
			tag := string(tn)
			if tag == "style" || tag == "script" {
				skip = false
			}
		case html.TextToken:
			if !skip {
				b.Write(tokenizer.Text())
			}
		}
	}
}

// parseAPIError builds an APIError from a non-200 HTTP response, extracting
// the error_code and any advertised retry delay from the JSON body, and the
// Retry-After header, which a proxy or rate limiter in front of SchemaBot may
// send with a body that carries neither.
func parseAPIError(resp *http.Response, body []byte) *APIError {
	retryAfter, uncapped := parseRetryAfterHeader(resp.Header.Get("Retry-After"), time.Now())
	apiErr := &APIError{
		Status:                   resp.StatusCode,
		Message:                  FormatAPIError(resp.StatusCode, body),
		RetryAfterHeader:         retryAfter,
		RetryAfterHeaderUncapped: uncapped,
	}
	var errResp apitypes.ErrorResponse
	if json.Unmarshal(body, &errResp) == nil {
		apiErr.ErrorCode = errResp.ErrorCode
		apiErr.RetryAfterSeconds = errResp.RetryAfterSeconds
		apiErr.Blockers = errResp.Blockers
		apiErr.OverrideRequest = errResp.OverrideRequest
		apiErr.OverrideOnReason = errResp.OverrideOnReason
		apiErr.LockHolder = errResp.LockHolder
	}
	return apiErr
}

// maxRetryAfterHeader bounds the delay a Retry-After header can ask for. The
// header can come from any proxy or maintenance page in front of the server,
// which commonly asks for an hour, and a watch that honoured that would sit far
// past the few minutes of failed polls after which it is documented to give
// up. The server's own delay arrives in the response body and is not bounded
// here.
const maxRetryAfterHeader = 5 * time.Minute

// parseRetryAfterHeader reads a Retry-After header value in either form RFC
// 9110 allows: a count of seconds, or an HTTP date measured from now, bounded
// by maxRetryAfterHeader. An absent, malformed, or negative value, or a date
// already past, yields zero, so a header nobody can act on never delays a
// retry. A count of seconds too large to read is still a request to wait long,
// so it yields the bound. When the bound cuts the delay short, uncapped is the
// delay the header asked for, saturating at the longest time.Duration, and
// zero otherwise.
func parseRetryAfterHeader(value string, now time.Time) (after, uncapped time.Duration) {
	value = strings.TrimSpace(value)
	if value == "" {
		return 0, 0
	}
	if isDigits(value) {
		seconds, err := strconv.ParseInt(value, 10, 64)
		if err != nil || seconds > int64(math.MaxInt64/time.Second) {
			return maxRetryAfterHeader, time.Duration(math.MaxInt64)
		}
		after = time.Duration(seconds) * time.Second
	} else {
		at, err := http.ParseTime(value)
		if err != nil {
			return 0, 0
		}
		after = max(at.Sub(now), 0)
	}
	if after > maxRetryAfterHeader {
		return maxRetryAfterHeader, after
	}
	return after, 0
}

func isDigits(s string) bool {
	for _, r := range s {
		if r < '0' || r > '9' {
			return false
		}
	}
	return true
}

// ConnectionError represents a client-side failure to reach the server
// (connection refused, DNS resolution, timeout). Distinct from APIError,
// which means the server responded with a non-200 status.
type ConnectionError struct {
	Endpoint string
	Err      error
}

func (e *ConnectionError) Error() string {
	return e.message()
}

func (e *ConnectionError) Unwrap() error {
	return e.Err
}

// message names the failure and its cause. A refused connection or an
// unknown host never reached the server, so those keep their short messages.
// Any other failure of a request that changes something, such as an apply or
// a stop, may have happened after the server received it, so the message says
// so and names the command that shows what the server did with that request.
// A request that only reads, or one that failed before any of it was sent,
// gets no such line.
func (e *ConnectionError) message() string {
	if isConnectionRefused(e.Err) {
		return fmt.Sprintf("cannot connect to %s (is the server running?)", e.Endpoint)
	}
	if isHostNotFound(e.Err) {
		return fmt.Sprintf("cannot resolve host: %s", e.Endpoint)
	}
	cause := ui.TerminalSafe(requestFailureCause(e.Err))
	var msg string
	if isTimeout(e.Err) {
		msg = fmt.Sprintf("request to %s timed out: %s", e.Endpoint, cause)
	} else {
		msg = fmt.Sprintf("connection failed: %s: %s", e.Endpoint, cause)
	}
	if warning := e.effect().warning(); warning != "" {
		msg += "\n" + warning
	}
	return msg
}

// effect returns what the failed request may have left behind on the server.
// A request that never reached the server changed nothing: its connection was
// refused, its host did not resolve, or it failed before any of it was sent.
func (e *ConnectionError) effect() requestEffect {
	if isConnectionRefused(e.Err) || isHostNotFound(e.Err) || failedBeforeSending(e.Err) {
		return requestEffect{}
	}
	return effectOf(failedRequest(e.Err))
}

// ServerMayHaveActed reports whether err is the failure of a request that
// changes something and that the server may have received: the connection
// failed after some of the request could have been sent, or the answer was cut
// off. These are the failures whose message says the server may already have
// acted. It is false when the server answered, when the request never reached
// it, and when the request only reads.
func ServerMayHaveActed(err error) bool {
	if connErr, ok := errors.AsType[*ConnectionError](err); ok {
		return connErr.effect().changes
	}
	if readErr, ok := errors.AsType[*ResponseReadError](err); ok {
		return readErr.effect().changes
	}
	return false
}

// WithoutCheckAdvice returns err with the line that tells the operator to
// check what the server did with the failed request removed from its message.
// A command that has already looked and told the operator what it found uses
// it, so the error does not ask for a check the output has just answered. The
// returned error still wraps err, so ServerMayHaveActed and errors.Is see
// through it.
func WithoutCheckAdvice(err error) error {
	return checkedRequestError{err: err}
}

type checkedRequestError struct{ err error }

func (e checkedRequestError) Error() string {
	msg := e.err.Error()
	var warning string
	if connErr, ok := errors.AsType[*ConnectionError](e.err); ok {
		warning = connErr.effect().warning()
	} else if readErr, ok := errors.AsType[*ResponseReadError](e.err); ok {
		warning = readErr.effect().warning()
	}
	if warning == "" {
		return msg
	}
	return strings.Replace(msg, "\n"+warning, "", 1)
}

func (e checkedRequestError) Unwrap() error {
	return e.err
}

// requestEffect says what a failed request may have left behind on the
// server, which decides what the operator is told before sending it again.
type requestEffect struct {
	// changes is false for a request that only reads or computes an answer.
	// Sending it again is safe, so its failure carries no warning.
	changes bool
	// check is the command that shows what the server did with the request,
	// or "" when no single command shows it.
	check string
}

var (
	readsOnly             = requestEffect{}
	changesWithoutCommand = requestEffect{changes: true}
)

// shownBy is the effect of a request whose result command shows.
func shownBy(command string) requestEffect {
	return requestEffect{changes: true, check: command}
}

// requestEffects classifies every API path the client sends with a method
// other than GET. An apply or a control request shows up in status, a settings
// update in settings, and a lock change in locks.
var requestEffects = map[string]requestEffect{
	// Reads that are POSTs because their input travels in the body. A plan
	// stores a plan record, but sending it again starts nothing.
	"/api/plan":                readsOnly,
	"/api/pull":                readsOnly,
	"/api/rollback/plan":       readsOnly,
	"/api/storage/schema/plan": readsOnly,
	"/api/checks/scan":         readsOnly,
	"/api/checks/repos":        readsOnly,

	"/api/apply":         shownBy("status"),
	"/api/cutover":       shownBy("status"),
	"/api/stop":          shownBy("status"),
	"/api/cancel":        shownBy("status"),
	"/api/start":         shownBy("status"),
	"/api/release":       shownBy("status"),
	"/api/revert":        shownBy("status"),
	"/api/skip-revert":   shownBy("status"),
	"/api/settings":      shownBy("settings"),
	"/api/locks":         shownBy("locks"),
	"/api/locks/acquire": shownBy("locks"),

	// Freeze routes carry an ID, so effectOf looks them up by these prefixes.
	// Setting or lifting a freeze shows in the freeze list, and an override
	// approval in the override request itself.
	"/api/freezes":            shownBy("freeze list"),
	"/api/freezes/":           shownBy("freeze list"),
	"/api/freezes/overrides/": shownBy("freeze override <ovr-id>"),

	// Changes that no single command shows.
	"/api/checks/synthesize":    changesWithoutCommand,
	"/api/storage/schema/apply": changesWithoutCommand,
}

// effectOf returns what a request with this method to this path may change. A
// GET or a HEAD changes nothing, and neither does a request whose method is
// not known, since nothing says it changed anything. A path that is not
// listed is treated as a change that no command shows, so a request nobody
// classified still warns.
func effectOf(method, path string) requestEffect {
	if !changesServerState(method) {
		return readsOnly
	}
	// The endpoint may carry a path of its own ahead of the API path.
	if i := strings.LastIndex(path, "/api/"); i >= 0 {
		path = path[i:]
	}
	switch {
	case strings.HasPrefix(path, "/api/freezes/overrides/"):
		path = "/api/freezes/overrides/"
	case strings.HasPrefix(path, "/api/freezes/"):
		path = "/api/freezes/"
	}
	if effect, ok := requestEffects[path]; ok {
		return effect
	}
	return changesWithoutCommand
}

// warning is the line a failed request that changes something ends with, or
// "" for one that only reads. It names the command that shows what the server
// did, so the operator checks the thing they tried to change.
func (e requestEffect) warning() string {
	const warning = "The server may already have acted on this request."
	switch {
	case !e.changes:
		return ""
	case e.check == "":
		return warning + " Check what it did before you send it again."
	default:
		return fmt.Sprintf("%s Run '%s' to check before you send it again.", warning, cliname.Command(e.check))
	}
}

// changesServerState reports whether a request with this method can change
// something on the server. The HTTP client records the method of a failed
// request as "Post", not "POST", so the comparison ignores case.
func changesServerState(method string) bool {
	return method != "" && !strings.EqualFold(method, http.MethodGet) && !strings.EqualFold(method, http.MethodHead)
}

// failedRequest returns the method and URL path of the request the HTTP
// client failed to complete, or two empty strings when the cause does not
// name them.
func failedRequest(err error) (method, path string) {
	var urlErr *url.Error
	if !errors.As(err, &urlErr) {
		return "", ""
	}
	u, parseErr := url.Parse(urlErr.URL)
	if parseErr != nil {
		return urlErr.Op, ""
	}
	return urlErr.Op, u.Path
}

// failedBeforeSending reports whether the request failed before any of it was
// sent, so the server cannot have acted on it: while opening the connection to
// the server or to a proxy, or during the TLS handshake. The HTTP client
// replaces the handshake error with http.ErrSchemeMismatch when a plain HTTP
// server answers.
func failedBeforeSending(err error) bool {
	var opErr *net.OpError
	if errors.As(err, &opErr) && (opErr.Op == "dial" || opErr.Op == "proxyconnect") {
		return true
	}
	var certErr *tls.CertificateVerificationError
	if errors.As(err, &certErr) {
		return true
	}
	var recordErr tls.RecordHeaderError
	if errors.As(err, &recordErr) {
		return true
	}
	return errors.Is(err, http.ErrSchemeMismatch)
}

// isConnectionRefused reports whether the server's host refused the
// connection. The text match covers causes that carry no errno, such as one
// relayed by a proxy.
func isConnectionRefused(err error) bool {
	return errors.Is(err, syscall.ECONNREFUSED) || strings.Contains(err.Error(), "connection refused")
}

// isHostNotFound reports whether the server's host name did not resolve. The
// text match covers causes that carry no DNS error, such as one relayed by a
// proxy.
func isHostNotFound(err error) bool {
	var dnsErr *net.DNSError
	if errors.As(err, &dnsErr) && dnsErr.IsNotFound {
		return true
	}
	return strings.Contains(err.Error(), "no such host")
}

// isTimeout reports whether the request ran out of time, whether the client's
// own timeout or a network read or write deadline ended it.
func isTimeout(err error) bool {
	if errors.Is(err, context.DeadlineExceeded) {
		return true
	}
	var netErr net.Error
	return errors.As(err, &netErr) && netErr.Timeout()
}

// requestFailureCause returns the text of the underlying cause. The HTTP
// client wraps it with the method and full URL, which the message already
// names through the endpoint, so that wrapper is dropped.
func requestFailureCause(err error) string {
	var urlErr *url.Error
	if errors.As(err, &urlErr) && urlErr.Err != nil {
		return urlErr.Err.Error()
	}
	return err.Error()
}

// FormatConnectionError returns a ConnectionError wrapping the underlying cause.
// A token-transport refusal is surfaced verbatim rather than reframed as a
// network failure, so the operator sees the actual security reason.
func FormatConnectionError(endpoint string, err error) error {
	if errors.Is(err, ErrInsecureTokenTransport) {
		return err
	}
	return &ConnectionError{Endpoint: endpoint, Err: err}
}

// FormatAPIError returns a user-friendly error message from an API response.
// The message is printed to the operator's terminal and comes from whatever
// answered the request, a proxy included, so control runes are removed first.
func FormatAPIError(statusCode int, body []byte) string {
	return ui.TerminalSafe(apiErrorText(statusCode, body))
}

// apiErrorText picks the error message out of an API response body, falling
// back to the start of the raw body.
func apiErrorText(statusCode int, body []byte) string {
	var resp map[string]any
	if err := json.Unmarshal(body, &resp); err == nil {
		if msg, ok := resp["error"].(string); ok && msg != "" {
			return msg
		}
		if msg, ok := resp["message"].(string); ok && msg != "" {
			return msg
		}
		if msg, ok := resp["error_message"].(string); ok && msg != "" {
			return msg
		}
	}

	bodyStr := string(body)
	if len(bodyStr) > 100 {
		bodyStr = bodyStr[:100] + "..."
	}
	if bodyStr == "" {
		return fmt.Sprintf("HTTP %d", statusCode)
	}
	return fmt.Sprintf("HTTP %d: %s", statusCode, bodyStr)
}

// SetLocalAuth binds the private runtime credential to its verified endpoint.
// The endpoint is stored as the origin RoundTrip compares requests against, so
// the credential goes to the same server however a request spells it. An
// endpoint that does not parse as a URL is kept as given; nothing renders to
// it, so every request is refused rather than one slipping through.
func SetLocalAuth(token, endpoint string) {
	SetAuthToken(token)
	authTransport.origin = endpoint
	if u, err := url.Parse(endpoint); err == nil {
		authTransport.origin = RequestOrigin(u)
	}
}
