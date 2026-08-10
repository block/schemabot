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
	"strings"

	ps "github.com/planetscale/planetscale-go/planetscale"
)

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
	ListKeyspaces(ctx context.Context, req *ps.ListKeyspacesRequest) ([]*ps.Keyspace, error)
	GetKeyspaceVSchema(ctx context.Context, req *ps.GetKeyspaceVSchemaRequest) (*ps.VSchema, error)
	UpdateKeyspaceVSchema(ctx context.Context, req *ps.UpdateKeyspaceVSchemaRequest) (*ps.VSchema, error)

	// Deploy request operations
	CreateDeployRequest(ctx context.Context, req *ps.CreateDeployRequestRequest) (*ps.DeployRequest, error)
	DeployDeployRequest(ctx context.Context, req *ps.PerformDeployRequest) (*ps.DeployRequest, error)
	GetDeployRequest(ctx context.Context, req *ps.GetDeployRequestRequest) (*ps.DeployRequest, error)
	CancelDeployRequest(ctx context.Context, req *ps.CancelDeployRequestRequest) (*ps.DeployRequest, error)
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
	// uses raw HTTP via baseURL; it returns an error if baseURL is not set.
	DeployRequestAutoCutover(ctx context.Context, org, database string, number uint64) (bool, error)

	// ThrottleDeployRequest sets the throttle ratio for a running deploy request.
	// This controls the speed of the online DDL copy phase (0.0 = full speed,
	// 0.95 = max throttle). The PlanetScale API supports this endpoint but the
	// Go SDK (planetscale-go) does not expose it, so we use raw HTTP via baseURL.
	// Requires NewPSClientWithBaseURL; returns an error if baseURL is not set.
	ThrottleDeployRequest(ctx context.Context, req *ThrottleDeployRequestRequest) error
}

// ThrottleDeployRequestRequest is the request for setting a deploy request's throttle ratio.
type ThrottleDeployRequestRequest struct {
	Organization  string
	Database      string
	Number        uint64
	ThrottleRatio float64 // 0.0 (full speed) to 0.95 (max throttle). PlanetScale API caps at 0.95.
}

// psClientWrapper wraps the real PlanetScale client to implement PSClient.
type psClientWrapper struct {
	client     *ps.Client
	baseURL    string // for endpoints not in the SDK (throttle)
	tokenName  string
	tokenValue string
	logger     *slog.Logger
}

func (w *psClientWrapper) log() *slog.Logger {
	if w.logger == nil {
		return slog.Default()
	}
	return w.logger
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

// NewPSClient creates a new PSClient using the real PlanetScale API.
// Use NewPSClientWithBaseURL for endpoints not yet in the SDK (throttle).
func NewPSClient(tokenName, tokenValue string, opts ...ps.ClientOption) (PSClient, error) {
	allOpts := append([]ps.ClientOption{ps.WithServiceToken(tokenName, tokenValue)}, opts...)
	client, err := ps.NewClient(allOpts...)
	if err != nil {
		return nil, err
	}
	return &psClientWrapper{
		client:     client,
		baseURL:    "https://api.planetscale.com",
		tokenName:  tokenName,
		tokenValue: tokenValue,
	}, nil
}

// NewPSClientWithBaseURL creates a new PSClient with a custom base URL.
// The base URL is used for endpoints not yet in the SDK (throttle).
func NewPSClientWithBaseURL(tokenName, tokenValue, baseURL string) (PSClient, error) {
	var opts []ps.ClientOption
	if baseURL != "" {
		opts = append(opts, ps.WithBaseURL(baseURL))
	}
	allOpts := append([]ps.ClientOption{ps.WithServiceToken(tokenName, tokenValue)}, opts...)
	client, err := ps.NewClient(allOpts...)
	if err != nil {
		return nil, err
	}
	return &psClientWrapper{
		client:     client,
		baseURL:    baseURL,
		tokenName:  tokenName,
		tokenValue: tokenValue,
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

func (w *psClientWrapper) ListKeyspaces(ctx context.Context, req *ps.ListKeyspacesRequest) ([]*ps.Keyspace, error) {
	return w.client.Keyspaces.List(ctx, req)
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
	if w.baseURL == "" {
		// Falling back to the SDK here would silently drop the cutover setting
		// and hand cutover control to the backend, so the deploy request is not
		// created at all.
		return nil, fmt.Errorf("create deploy request for %s/%s: cannot set auto_cutover without a PlanetScale API base URL", req.Organization, req.Database)
	}
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
	if w.baseURL == "" {
		return false, fmt.Errorf("read auto_cutover for %s/%s deploy request #%d: no PlanetScale API base URL", org, database, number)
	}
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
	resp, err := http.DefaultClient.Do(httpReq)
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
		w.log().Error("PlanetScale API request failed",
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

// ThrottleDeployRequest sets the throttle ratio via a raw HTTP PUT.
// Not yet available in the PlanetScale SDK.
func (w *psClientWrapper) ThrottleDeployRequest(ctx context.Context, req *ThrottleDeployRequestRequest) error {
	if w.baseURL == "" {
		return fmt.Errorf("throttle not supported without base URL")
	}
	path := fmt.Sprintf("/v1/organizations/%s/databases/%s/deploy-requests/%d/throttle",
		req.Organization, req.Database, req.Number)
	body, err := json.Marshal(map[string]float64{"throttle_ratio": req.ThrottleRatio})
	if err != nil {
		return fmt.Errorf("marshal throttle payload for %s/%s deploy request #%d: %w", req.Organization, req.Database, req.Number, err)
	}
	if _, err := w.doRawJSON(ctx, http.MethodPut, path, body); err != nil {
		return fmt.Errorf("throttle %s/%s deploy request #%d to %.2f: %w", req.Organization, req.Database, req.Number, req.ThrottleRatio, err)
	}
	return nil
}
