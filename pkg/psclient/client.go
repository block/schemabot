package psclient

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"

	ps "github.com/planetscale/planetscale-go/planetscale"
)

// PSClient defines the interface for PlanetScale API operations.
// The engine flow is: create branch → get credentials → MySQL-connect to
// branch → apply keyspace changes (DDL + VSchema) → create deploy request.
type PSClient interface {
	// Branch operations
	GetBranch(ctx context.Context, req *ps.GetDatabaseBranchRequest) (*ps.DatabaseBranch, error)
	CreateBranch(ctx context.Context, req *ps.CreateDatabaseBranchRequest) (*ps.DatabaseBranch, error)
	GetBranchSchema(ctx context.Context, req *ps.BranchSchemaRequest) ([]*ps.Diff, error)

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

func (w *psClientWrapper) CreateDeployRequest(ctx context.Context, req *ps.CreateDeployRequestRequest) (*ps.DeployRequest, error) {
	return w.client.DeployRequests.Create(ctx, req)
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

// ThrottleDeployRequest sets the throttle ratio via a raw HTTP PUT.
// Not yet available in the PlanetScale SDK.
func (w *psClientWrapper) ThrottleDeployRequest(ctx context.Context, req *ThrottleDeployRequestRequest) error {
	if w.baseURL == "" {
		return fmt.Errorf("throttle not supported without base URL")
	}
	url := fmt.Sprintf("%s/v1/organizations/%s/databases/%s/deploy-requests/%d/throttle",
		w.baseURL, req.Organization, req.Database, req.Number)
	body, err := json.Marshal(map[string]float64{"throttle_ratio": req.ThrottleRatio})
	if err != nil {
		return fmt.Errorf("marshal throttle payload: %w", err)
	}
	httpReq, err := http.NewRequestWithContext(ctx, "PUT", url, bytes.NewReader(body))
	if err != nil {
		return fmt.Errorf("create throttle request: %w", err)
	}
	httpReq.Header.Set("Content-Type", "application/json")
	httpReq.Header.Set("Authorization", w.tokenName+":"+w.tokenValue)
	resp, err := http.DefaultClient.Do(httpReq)
	if err != nil {
		return fmt.Errorf("throttle deploy request: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("throttle request failed: %s", resp.Status)
	}
	return nil
}
