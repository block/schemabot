package client

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"os"
	"slices"
	"strings"
	"time"

	"golang.org/x/net/html"

	"github.com/block/schemabot/pkg/apitypes"
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

	// RetryAfterSeconds is the delay the server asked the client to wait, set
	// only on responses that advertise one. Read it through RetryAfter rather
	// than on its own, so a retry is never scheduled without it.
	RetryAfterSeconds int
}

func (e *APIError) Error() string {
	return e.Message
}

// RetryAfter reports whether this request should be retried and how long to
// wait first, so a caller automating against the API gets both facts from one
// call instead of retrying on the code and ignoring the delay.
func (e *APIError) RetryAfter() (retry bool, after time.Duration) {
	return apitypes.ErrorResponse{ErrorCode: e.ErrorCode, RetryAfterSeconds: e.RetryAfterSeconds}.RetryAfter()
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
		return fmt.Errorf("read response: %w", err)
	}

	if resp.StatusCode != http.StatusOK {
		return parseAPIError(resp.StatusCode, respBody)
	}

	if err := json.Unmarshal(respBody, result); err != nil {
		return checkNonJSONResponse(resp, respBody, err)
	}
	return nil
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
		return fmt.Errorf("read response: %w", err)
	}

	if resp.StatusCode != http.StatusOK {
		return parseAPIError(resp.StatusCode, respBody)
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
		return fmt.Errorf("read response: %w", err)
	}

	// Control endpoints answer 202 Accepted when the request is durably
	// recorded and the apply owner completes it asynchronously; the body
	// carries the same JSON shape as a 200 and is equally a success.
	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusAccepted {
		return parseAPIError(resp.StatusCode, respBody)
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
// the error_code and any advertised retry delay from the JSON body. The delay
// is read from the body rather than the Retry-After header because this client
// keeps error bodies and discards responses.
func parseAPIError(statusCode int, body []byte) *APIError {
	apiErr := &APIError{
		Status:  statusCode,
		Message: FormatAPIError(statusCode, body),
	}
	var resp apitypes.ErrorResponse
	if json.Unmarshal(body, &resp) == nil {
		apiErr.ErrorCode = resp.ErrorCode
		apiErr.RetryAfterSeconds = resp.RetryAfterSeconds
	}
	return apiErr
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

func (e *ConnectionError) message() string {
	msg := e.Err.Error()
	if strings.Contains(msg, "connection refused") {
		return fmt.Sprintf("cannot connect to %s (is the server running?)", e.Endpoint)
	}
	if strings.Contains(msg, "no such host") {
		return fmt.Sprintf("cannot resolve host: %s", e.Endpoint)
	}
	if strings.Contains(msg, "timeout") {
		return fmt.Sprintf("connection timeout: %s", e.Endpoint)
	}
	return fmt.Sprintf("connection failed: %s", e.Endpoint)
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
func FormatAPIError(statusCode int, body []byte) string {
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
