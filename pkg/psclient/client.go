package psclient

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/hashicorp/go-cleanhttp"
	ps "github.com/planetscale/planetscale-go/planetscale"
)

// planetScaleHTTPTimeout bounds every PlanetScale API request, the raw-HTTP
// endpoints the SDK does not cover included, so a driver can never block
// forever on a hung API call. It is generous because deploy request creation and branch schema
// diffs can legitimately take minutes.
const planetScaleHTTPTimeout = 5 * time.Minute

// newPlanetScaleHTTPClient returns the SDK's default HTTP client with the
// request timeout set. Its Transport is non-nil, which ps.WithServiceToken
// requires: that option wraps the installed transport in an auth RoundTripper.
func newPlanetScaleHTTPClient() *http.Client {
	client := cleanhttp.DefaultClient()
	client.Timeout = planetScaleHTTPTimeout
	return client
}

// boundedClientOptions applies the caller's options first and then installs the
// bounded HTTP client and the service token, so no caller option can displace
// either. WithHTTPClient must precede WithServiceToken because the token option
// wraps whichever client is installed at that point.
func boundedClientOptions(tokenName, tokenValue string, opts []ps.ClientOption) []ps.ClientOption {
	return append(append([]ps.ClientOption{}, opts...),
		ps.WithHTTPClient(newPlanetScaleHTTPClient()),
		ps.WithServiceToken(tokenName, tokenValue),
	)
}

// PSClient defines the interface for PlanetScale API operations.
// The engine flow is: create branch → get credentials → MySQL-connect to
// branch → apply keyspace changes (DDL + VSchema) → create deploy request.
type PSClient interface {
	// Branch operations
	GetBranch(ctx context.Context, req *ps.GetDatabaseBranchRequest) (*ps.DatabaseBranch, error)
	CreateBranch(ctx context.Context, req *ps.CreateDatabaseBranchRequest) (*ps.DatabaseBranch, error)
	DeleteBranch(ctx context.Context, req *ps.DeleteDatabaseBranchRequest) error
	GetBranchSchema(ctx context.Context, req *ps.BranchSchemaRequest) ([]*ps.Diff, error)

	// RefreshSchema brings a branch's schema up to date with its parent (main).
	RefreshSchema(ctx context.Context, org, database, branch string) error

	// Branch credentials — returns a MySQL endpoint + credentials for connecting to a branch.
	// The engine uses this to MySQL-connect and run DDL on the branch directly.
	CreateBranchPassword(ctx context.Context, req *ps.DatabaseBranchPasswordRequest) (*ps.DatabaseBranchPassword, error)

	// Keyspace operations

	// ListKeyspaces returns every keyspace the API reports for the branch,
	// across all of its pages, or an error; the pages already read are never
	// returned on their own. The pages are read one after another, so a branch
	// whose keyspaces change while the listing is in progress can be read
	// incompletely; the result is as of a read that may span pages, not a
	// snapshot.
	ListKeyspaces(ctx context.Context, req *ps.ListKeyspacesRequest) ([]*ps.Keyspace, error)
	GetKeyspaceVSchema(ctx context.Context, req *ps.GetKeyspaceVSchemaRequest) (*ps.VSchema, error)
	UpdateKeyspaceVSchema(ctx context.Context, req *ps.UpdateKeyspaceVSchemaRequest) (*ps.VSchema, error)

	// Deploy request operations
	CreateDeployRequest(ctx context.Context, req *ps.CreateDeployRequestRequest) (*ps.DeployRequest, error)
	DeployDeployRequest(ctx context.Context, req *ps.PerformDeployRequest) (*ps.DeployRequest, error)
	GetDeployRequest(ctx context.Context, req *ps.GetDeployRequestRequest) (*ps.DeployRequest, error)
	CancelDeployRequest(ctx context.Context, req *ps.CancelDeployRequestRequest) (*ps.DeployRequest, error)
	// CloseDeployRequest closes a deploy request that has not been deployed.
	// Cancel only reaches a deploy that is queued or running; an undeployed
	// deploy request is retired by closing it instead.
	CloseDeployRequest(ctx context.Context, req *ps.CloseDeployRequestRequest) (*ps.DeployRequest, error)
	ApplyDeployRequest(ctx context.Context, req *ps.ApplyDeployRequestRequest) (*ps.DeployRequest, error)
	RevertDeployRequest(ctx context.Context, req *ps.RevertDeployRequestRequest) (*ps.DeployRequest, error)
	SkipRevertDeployRequest(ctx context.Context, req *ps.SkipRevertDeployRequestRequest) (*ps.DeployRequest, error)

	// ListDeployRequests lists all deploy requests for a database.
	ListDeployRequests(ctx context.Context, req *ps.ListDeployRequestsRequest) ([]*ps.DeployRequest, error)

	// DeployRequestAutoCutover reports the cutover setting the backend actually
	// holds for a deploy request. The setting is settled when the deploy request
	// is created and no later call can change it, so reading it back is the only
	// way to know the request that was sent is the one being honoured. The SDK
	// models auto_cutover on the create request but on neither response, so this
	// uses raw HTTP.
	DeployRequestAutoCutover(ctx context.Context, org, database string, number uint64) (bool, error)
}

// psClientWrapper wraps the real PlanetScale client to implement PSClient.
type psClientWrapper struct {
	client     *ps.Client
	httpClient *http.Client // for endpoints not in the SDK
	baseURL    string       // the SDK client's base URL, reused for endpoints not in the SDK
	tokenName  string
	tokenValue string
	// keyspaceListingTimeout bounds a whole ListKeyspaces call, every page
	// included. The constructors set it to defaultKeyspaceListingTimeout.
	keyspaceListingTimeout time.Duration
}

// APIError is a non-2xx response from a PlanetScale endpoint this package calls
// directly rather than through the SDK.
//
// A failed call travels up as the apply's failure message and is rendered into a
// PR comment, so Error() is written for that surface: it names the request, the
// status, and the API's own refusal when there is one. Path is the endpoint
// without the base URL, so the message says what was called without naming where
// it lives, and Body keeps the whole response for the server log.
type APIError struct {
	Method     string
	Path       string
	StatusCode int
	Body       string
}

func (e *APIError) Error() string {
	status := fmt.Sprintf("%s %s: %d %s", e.Method, e.Path, e.StatusCode, http.StatusText(e.StatusCode))
	if summary := e.summary(); summary != "" {
		return status + ": " + summary
	}
	return status
}

// maxAPIErrorSummaryLen bounds the refusal text rendered into a PR comment.
const maxAPIErrorSummaryLen = 200

// summary is the part of a failed response that can be put in front of an
// operator.
//
// A refusal is worth reading — "branch has no schema changes" answers the
// question without anyone reproducing the call — but only the message field is
// the API speaking to a human. The rest of a response body is text from whatever
// answered: a proxy's HTML, an upstream dump, addresses of hosts that have no
// business appearing in a PR comment. So the message is taken when the body is
// the API's own error shape and nothing is taken otherwise, and what is taken is
// still flattened and clamped for the markdown it lands in.
func (e *APIError) summary() string {
	var refusal struct {
		Message string `json:"message"`
	}
	if err := json.Unmarshal([]byte(e.Body), &refusal); err != nil || refusal.Message == "" {
		return ""
	}
	s := strings.ReplaceAll(refusal.Message, "\n", " ")
	s = strings.ReplaceAll(s, "\r", " ")
	s = strings.ReplaceAll(s, "|", "/")
	runes := []rune(s)
	if len(runes) > maxAPIErrorSummaryLen {
		return string(runes[:maxAPIErrorSummaryLen-1]) + "…"
	}
	return s
}

// DefaultBaseURL is the public PlanetScale API endpoint, used when no base URL
// is configured.
const DefaultBaseURL = "https://api.planetscale.com"

// NewPSClient creates a new PSClient for the public PlanetScale API. It takes
// no SDK options: a client for another endpoint, or with options of its own,
// is built with NewPSClientWithBaseURL, which names the endpoint explicitly.
func NewPSClient(tokenName, tokenValue string) (PSClient, error) {
	return NewPSClientWithBaseURL(tokenName, tokenValue, "")
}

// NewPSClientWithBaseURL creates a new PSClient that addresses the PlanetScale
// API at baseURL, or at DefaultBaseURL when baseURL is empty.
//
// SDK calls and the raw-HTTP calls for endpoints the SDK does not cover go to
// the same base URL. It is installed after the caller's options, so a
// ps.WithBaseURL among them cannot send SDK calls somewhere the raw calls do
// not go. The two resolve paths differently: the SDK resolves its relative
// endpoints against the URL, and the raw calls append an absolute path to it.
// So trailing slashes are trimmed once, the raw calls get the trimmed URL, and
// the SDK gets it with exactly one trailing slash. That way a base URL with a
// trailing slash or a path prefix reaches the same path root on both.
func NewPSClientWithBaseURL(tokenName, tokenValue, baseURL string, opts ...ps.ClientOption) (PSClient, error) {
	baseURL = strings.TrimRight(baseURL, "/")
	if baseURL == "" {
		baseURL = DefaultBaseURL
	}
	allOpts := boundedClientOptions(tokenName, tokenValue, append(append([]ps.ClientOption{}, opts...), ps.WithBaseURL(baseURL+"/")))
	client, err := ps.NewClient(allOpts...)
	if err != nil {
		return nil, fmt.Errorf("create PlanetScale client for %s: %w", baseURL, err)
	}
	return &psClientWrapper{
		client:     client,
		httpClient: newPlanetScaleHTTPClient(),
		baseURL:    baseURL,
		tokenName:  tokenName,
		tokenValue: tokenValue,

		keyspaceListingTimeout: defaultKeyspaceListingTimeout,
	}, nil
}

// Branch operations

func (w *psClientWrapper) GetBranch(ctx context.Context, req *ps.GetDatabaseBranchRequest) (*ps.DatabaseBranch, error) {
	return w.client.DatabaseBranches.Get(ctx, req)
}

func (w *psClientWrapper) CreateBranch(ctx context.Context, req *ps.CreateDatabaseBranchRequest) (*ps.DatabaseBranch, error) {
	return w.client.DatabaseBranches.Create(ctx, req)
}

func (w *psClientWrapper) DeleteBranch(ctx context.Context, req *ps.DeleteDatabaseBranchRequest) error {
	return w.client.DatabaseBranches.Delete(ctx, req)
}

func (w *psClientWrapper) RefreshSchema(ctx context.Context, org, database, branch string) error {
	return w.client.DatabaseBranches.RefreshSchema(ctx, &ps.RefreshSchemaRequest{
		Organization: org,
		Database:     database,
		Branch:       branch,
	})
}

func (w *psClientWrapper) GetBranchSchema(ctx context.Context, req *ps.BranchSchemaRequest) ([]*ps.Diff, error) {
	return w.client.DatabaseBranches.Schema(ctx, req)
}

func (w *psClientWrapper) CreateBranchPassword(ctx context.Context, req *ps.DatabaseBranchPasswordRequest) (*ps.DatabaseBranchPassword, error) {
	return w.client.Passwords.Create(ctx, req)
}

// Keyspace operations

// keyspacesPerPage is the page size requested when listing keyspaces. The
// loop follows the API's next_page rather than counting results, so a server
// that caps pages at a smaller size is still read to the end.
const keyspacesPerPage = 100

// maxKeyspacePages bounds how many pages a keyspace listing reads. It sits far
// above any real branch, so reaching it means the API keeps reporting another
// page, and the listing fails rather than loop or return a partial view.
const maxKeyspacePages = 100

// defaultKeyspaceListingTimeout bounds a keyspace listing as a whole. The page
// bound alone does not: each page request may run for the full per-request
// timeout, so a caller with no deadline of its own could otherwise wait for the
// page bound times that timeout. Two per-request timeouts lets one slow page use
// its whole budget and still leaves the rest of the listing room to finish, while
// a branch whose API answers every page just inside the per-request timeout ends
// after a couple of pages rather than after all of them.
const defaultKeyspaceListingTimeout = 2 * planetScaleHTTPTimeout

// ListKeyspaces returns every keyspace on the branch, reading each page the API
// reports.
//
// Callers treat the result as the whole branch: progress and failure detail are
// gathered keyspace by keyspace, so a keyspace missing from the list is one whose
// schema change nobody sees. The SDK's list call reads only the first page and
// takes no page options, so the listing uses raw HTTP.
//
// The pages are numbered offsets into a set that can change between requests.
// A keyspace added before a later page is read shifts the rest forward, so a
// name can appear on two pages; the first copy is kept and the repeat is
// logged, since it is the one visible sign that the set moved under the read.
// A keyspace removed shifts the rest back, and the name that slides off the
// start of the next page is not seen at all; nothing in the response reveals
// that, so the result is complete for a branch whose keyspaces held still and
// the next listing heals one that did not.
//
// The HTTP client's timeout applies to each page request. The listing as a whole
// reads at most maxKeyspacePages pages and ends by the wrapper's
// keyspaceListingTimeout whatever ctx carries; a ctx deadline that comes sooner
// ends it sooner. When the overall bound fires, the error says the listing as a
// whole timed out and how many pages it had read, which tells it apart from a
// single page timing out and from ctx itself ending.
func (w *psClientWrapper) ListKeyspaces(ctx context.Context, req *ps.ListKeyspacesRequest) ([]*ps.Keyspace, error) {
	listCtx, cancel := context.WithTimeout(ctx, w.keyspaceListingTimeout)
	defer cancel()

	// Each name is its own path segment, so a character that URL syntax gives
	// meaning to cannot retarget the request or swallow the page query.
	basePath := fmt.Sprintf("/v1/organizations/%s/databases/%s/branches/%s/keyspaces",
		url.PathEscape(req.Organization), url.PathEscape(req.Database), url.PathEscape(req.Branch))
	var keyspaces []*ps.Keyspace
	seen := make(map[string]int)
	page := 1
	for fetched := 1; ; fetched++ {
		if fetched > maxKeyspacePages {
			return nil, fmt.Errorf("list keyspaces for %s/%s branch %s: API still reports page %d after %d pages and %d keyspaces", req.Organization, req.Database, req.Branch, page, maxKeyspacePages, len(keyspaces))
		}
		query := url.Values{}
		query.Set("page", strconv.Itoa(page))
		query.Set("per_page", strconv.Itoa(keyspacesPerPage))
		respBody, err := w.doRawJSON(listCtx, http.MethodGet, basePath+"?"+query.Encode(), nil)
		if err != nil {
			if listingBoundEnded(ctx, listCtx) {
				return nil, fmt.Errorf("list keyspaces for %s/%s branch %s: listing as a whole timed out after %s (pages read: %d, waiting on page %d): %w",
					req.Organization, req.Database, req.Branch, w.keyspaceListingTimeout, fetched-1, page, err)
			}
			return nil, fmt.Errorf("list keyspaces for %s/%s branch %s page %d: %w", req.Organization, req.Database, req.Branch, page, err)
		}
		var payload struct {
			Data     []*ps.Keyspace `json:"data"`
			NextPage *int           `json:"next_page"`
		}
		if err := json.Unmarshal(respBody, &payload); err != nil {
			return nil, fmt.Errorf("decode keyspaces for %s/%s branch %s page %d: %w", req.Organization, req.Database, req.Branch, page, err)
		}
		for _, keyspace := range payload.Data {
			if firstPage, ok := seen[keyspace.Name]; ok {
				slog.Warn("keyspace listed on two pages; the branch's keyspaces changed while they were being listed, so a keyspace may have been skipped",
					"organization", req.Organization, "database", req.Database, "branch", req.Branch,
					"keyspace", keyspace.Name, "first_page", firstPage, "page", page)
				continue
			}
			seen[keyspace.Name] = page
			keyspaces = append(keyspaces, keyspace)
		}
		if payload.NextPage == nil || *payload.NextPage == 0 {
			return keyspaces, nil
		}
		if *payload.NextPage <= page {
			return nil, fmt.Errorf("list keyspaces for %s/%s branch %s: page %d reports next page %d, which does not advance", req.Organization, req.Database, req.Branch, page, *payload.NextPage)
		}
		page = *payload.NextPage
	}
}

// listingBoundEnded reports whether the listing's own overall bound ended
// listCtx while the caller's ctx is still live. When ctx has ended too, the
// caller's deadline or cancellation is the cause, and it is reported as such.
func listingBoundEnded(ctx, listCtx context.Context) bool {
	return listCtx.Err() != nil && ctx.Err() == nil
}

func (w *psClientWrapper) GetKeyspaceVSchema(ctx context.Context, req *ps.GetKeyspaceVSchemaRequest) (*ps.VSchema, error) {
	return w.client.Keyspaces.VSchema(ctx, req)
}

func (w *psClientWrapper) UpdateKeyspaceVSchema(ctx context.Context, req *ps.UpdateKeyspaceVSchemaRequest) (*ps.VSchema, error) {
	return w.client.Keyspaces.UpdateVSchema(ctx, req)
}

// Deploy request operations

// CreateDeployRequest creates a deploy request via a raw HTTP POST so the
// cutover setting is actually transmitted.
//
// The SDK's request struct tags auto_cutover and auto_delete_branch
// `omitempty`, and both are plain bools, so the zero value — false — is dropped
// from the body entirely. "Off" and "unspecified" serialize identically, and an
// unspecified deploy request falls to whatever default the database carries.
// For auto_cutover that default may be on, which would let PlanetScale swap the
// schema on its own: exactly the outcome SchemaBot's cutover ownership exists to
// prevent, and one no later call can undo, since the API exposes no way to
// change auto_cutover after creation.
//
// Marshalling the body here is what makes false expressible. The response is
// the deploy request object, the same shape the SDK decodes.
func (w *psClientWrapper) CreateDeployRequest(ctx context.Context, req *ps.CreateDeployRequestRequest) (*ps.DeployRequest, error) {
	body, err := json.Marshal(map[string]any{
		"branch":             req.Branch,
		"into_branch":        req.IntoBranch,
		"notes":              req.Notes,
		"auto_cutover":       req.AutoCutover,
		"auto_delete_branch": req.AutoDeleteBranch,
	})
	if err != nil {
		return nil, fmt.Errorf("marshal deploy request payload for %s/%s: %w", req.Organization, req.Database, err)
	}
	path := fmt.Sprintf("/v1/organizations/%s/databases/%s/deploy-requests", req.Organization, req.Database)
	respBody, err := w.doRawJSON(ctx, http.MethodPost, path, body)
	if err != nil {
		return nil, fmt.Errorf("create deploy request for %s/%s from branch %s: %w", req.Organization, req.Database, req.Branch, err)
	}
	dr := &ps.DeployRequest{}
	if err := json.Unmarshal(respBody, dr); err != nil {
		return nil, fmt.Errorf("decode created deploy request for %s/%s: %w", req.Organization, req.Database, err)
	}
	return dr, nil
}

// ErrDeploymentNotReported is returned when a deploy request carries no
// deployment object, so there is no cutover setting to read at all. A read that
// arrived before the backend filled the deployment in, or came back partial,
// looks like this.
var ErrDeploymentNotReported = errors.New("deploy request reports no deployment")

// ErrAutoCutoverNotReported is returned when a deploy request carries a
// deployment that does not report the cutover setting. The backend answered and
// the field this package reads was not in the answer, which is a different thing
// from having nothing to read: the shape being decoded no longer matches what
// the API sends.
var ErrAutoCutoverNotReported = errors.New("deployment reports no auto_cutover setting")

// DeployRequestAutoCutover reads back the cutover setting the backend holds for
// a deploy request.
//
// The setting is carried on the deployment rather than the deploy request
// itself, and the SDK models it on neither, so the response is decoded here
// against the one field this answer needs. A backend that reports no setting is
// an error rather than a default: the caller is asking precisely because it
// cannot assume one.
func (w *psClientWrapper) DeployRequestAutoCutover(ctx context.Context, org, database string, number uint64) (bool, error) {
	path := fmt.Sprintf("/v1/organizations/%s/databases/%s/deploy-requests/%d", org, database, number)
	respBody, err := w.doRawJSON(ctx, http.MethodGet, path, nil)
	if err != nil {
		return false, fmt.Errorf("read auto_cutover for %s/%s deploy request #%d: %w", org, database, number, err)
	}
	var payload struct {
		Deployment *struct {
			AutoCutover *bool `json:"auto_cutover"`
		} `json:"deployment"`
	}
	if err := json.Unmarshal(respBody, &payload); err != nil {
		return false, fmt.Errorf("decode auto_cutover for %s/%s deploy request #%d: %w", org, database, number, err)
	}
	if payload.Deployment == nil {
		return false, fmt.Errorf("read auto_cutover for %s/%s deploy request #%d: %w", org, database, number, ErrDeploymentNotReported)
	}
	if payload.Deployment.AutoCutover == nil {
		return false, fmt.Errorf("read auto_cutover for %s/%s deploy request #%d: %w", org, database, number, ErrAutoCutoverNotReported)
	}
	return *payload.Deployment.AutoCutover, nil
}

// doRawJSON sends a JSON request to an endpoint the SDK does not cover and
// returns the response body. The SDK owns authentication for everything else,
// so the service-token header is spelled out here in the one place these calls
// are made.
//
// path is the endpoint below the base URL, so a failure can name what was called
// without carrying the host into the returned error.
func (w *psClientWrapper) doRawJSON(ctx context.Context, method, path string, body []byte) ([]byte, error) {
	httpReq, err := http.NewRequestWithContext(ctx, method, w.baseURL+path, bytes.NewReader(body))
	if err != nil {
		return nil, fmt.Errorf("create %s %s request: %w", method, path, err)
	}
	httpReq.Header.Set("Content-Type", "application/json")
	httpReq.Header.Set("Authorization", w.tokenName+":"+w.tokenValue)
	resp, err := w.httpClient.Do(httpReq)
	if err != nil {
		return nil, fmt.Errorf("send %s %s request: %w", method, path, err)
	}
	defer resp.Body.Close()
	respBody, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("read %s %s response: %w", method, path, err)
	}
	if resp.StatusCode < 200 || resp.StatusCode > 299 {
		// The body is logged rather than returned: it is what explains a refusal,
		// and this is the only place it is still in hand.
		slog.Error("PlanetScale API request failed",
			"method", method,
			"path", path,
			"status_code", resp.StatusCode,
			"response_body", string(respBody),
		)
		return nil, &APIError{Method: method, Path: path, StatusCode: resp.StatusCode, Body: string(respBody)}
	}
	return respBody, nil
}

func (w *psClientWrapper) DeployDeployRequest(ctx context.Context, req *ps.PerformDeployRequest) (*ps.DeployRequest, error) {
	return w.client.DeployRequests.Deploy(ctx, req)
}

func (w *psClientWrapper) GetDeployRequest(ctx context.Context, req *ps.GetDeployRequestRequest) (*ps.DeployRequest, error) {
	return w.client.DeployRequests.Get(ctx, req)
}

func (w *psClientWrapper) CancelDeployRequest(ctx context.Context, req *ps.CancelDeployRequestRequest) (*ps.DeployRequest, error) {
	return w.client.DeployRequests.CancelDeploy(ctx, req)
}

func (w *psClientWrapper) CloseDeployRequest(ctx context.Context, req *ps.CloseDeployRequestRequest) (*ps.DeployRequest, error) {
	return w.client.DeployRequests.CloseDeploy(ctx, req)
}

func (w *psClientWrapper) ApplyDeployRequest(ctx context.Context, req *ps.ApplyDeployRequestRequest) (*ps.DeployRequest, error) {
	return w.client.DeployRequests.ApplyDeploy(ctx, req)
}

func (w *psClientWrapper) RevertDeployRequest(ctx context.Context, req *ps.RevertDeployRequestRequest) (*ps.DeployRequest, error) {
	return w.client.DeployRequests.RevertDeploy(ctx, req)
}

func (w *psClientWrapper) SkipRevertDeployRequest(ctx context.Context, req *ps.SkipRevertDeployRequestRequest) (*ps.DeployRequest, error) {
	return w.client.DeployRequests.SkipRevertDeploy(ctx, req)
}

func (w *psClientWrapper) ListDeployRequests(ctx context.Context, req *ps.ListDeployRequestsRequest) ([]*ps.DeployRequest, error) {
	return w.client.DeployRequests.List(ctx, req)
}
