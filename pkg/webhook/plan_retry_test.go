package webhook

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	grpccodes "google.golang.org/grpc/codes"
	grpcstatus "google.golang.org/grpc/status"

	"github.com/block/schemabot/pkg/api"
	ternv1 "github.com/block/schemabot/pkg/proto/ternv1"
	"github.com/block/schemabot/pkg/storage"
	"github.com/block/schemabot/pkg/tern"
)

// flakyPlanTernClient fails the first N plan calls with a configurable error
// and then succeeds, simulating a transient transport failure in front of a
// healthy remote deployment.
type flakyPlanTernClient struct {
	tern.Client
	mu        sync.Mutex
	planCalls int
	failPlans int
	planErr   error
}

func (c *flakyPlanTernClient) Plan(context.Context, *ternv1.PlanRequest) (*ternv1.PlanResponse, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.planCalls++
	if c.planCalls <= c.failPlans {
		return nil, c.planErr
	}
	return &ternv1.PlanResponse{PlanId: "plan-after-retry"}, nil
}

func (c *flakyPlanTernClient) IsRemote() bool   { return true }
func (c *flakyPlanTernClient) Endpoint() string { return "remote:9090" }

func (c *flakyPlanTernClient) calls() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.planCalls
}

type sequencePlanTernClient struct {
	tern.Client
	mu        sync.Mutex
	planCalls int
	errors    []error
}

func (c *sequencePlanTernClient) Plan(context.Context, *ternv1.PlanRequest) (*ternv1.PlanResponse, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.planCalls++
	if c.planCalls <= len(c.errors) {
		return nil, c.errors[c.planCalls-1]
	}
	return &ternv1.PlanResponse{PlanId: "plan-after-retry"}, nil
}

func (c *sequencePlanTernClient) IsRemote() bool   { return true }
func (c *sequencePlanTernClient) Endpoint() string { return "remote:9090" }

func (c *sequencePlanTernClient) calls() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.planCalls
}

type planRetryStorage struct {
	emptyStorage
}

func (s *planRetryStorage) Plans() storage.PlanStore { return &noopPlanCreateStore{} }

type noopPlanCreateStore struct {
	storage.PlanStore
}

func (s *noopPlanCreateStore) Create(context.Context, *storage.Plan) (int64, error) { return 1, nil }
func (s *noopPlanCreateStore) Get(context.Context, string) (*storage.Plan, error)   { return nil, nil }

func newPlanRetryTestHandler(client tern.Client, retryDelay time.Duration) *Handler {
	cfg := &api.ServerConfig{
		Databases: map[string]api.DatabaseConfig{
			"orders": {
				Type: storage.DatabaseTypeMySQL,
				Environments: map[string]api.EnvironmentConfig{
					"staging": {Deployment: "remote", Target: "orders-target"},
				},
			},
		},
		TernDeployments: api.TernConfig{
			"remote": {"staging": "remote:9090"},
		},
	}
	svc := api.New(&planRetryStorage{}, cfg, map[string]tern.Client{"remote/staging": client}, testLogger())
	return &Handler{
		service:                 svc,
		logger:                  testLogger(),
		transientPlanRetryDelay: retryDelay,
	}
}

func planRetryTestRequest() api.PlanRequest {
	return api.PlanRequest{
		Database:    "orders",
		Environment: "staging",
		Type:        storage.DatabaseTypeMySQL,
		SchemaFiles: map[string]*ternv1.SchemaFiles{
			"orders": {Files: map[string]string{
				"users.sql": "CREATE TABLE users (id bigint unsigned NOT NULL AUTO_INCREMENT PRIMARY KEY)",
			}},
		},
	}
}

// A brief remote outage during a webhook command's plan request is absorbed:
// the handler retries after the configured delay and the command flow continues
// with the successful plan instead of posting a failure comment.
func TestExecutePlanTransientRetryRecovers(t *testing.T) {
	client := &flakyPlanTernClient{
		failPlans: 1,
		planErr:   grpcstatus.Error(grpccodes.Unavailable, "upstream connect error"),
	}
	h := newPlanRetryTestHandler(client, time.Millisecond)

	resp, err := h.executePlanWithTransientRetry(t.Context(), planRetryTestRequest(), "octocat/hello-world", 1)

	require.NoError(t, err)
	require.NotNil(t, resp)
	assert.Equal(t, "plan-after-retry", resp.PlanID)
	assert.Equal(t, 2, client.calls(), "handler should stop retrying after the first successful plan")
}

// A longer transient outage can recover on the final bounded retry without the
// caller posting an operator-visible failure comment.
func TestExecutePlanTransientRetryRecoversOnFinalRetry(t *testing.T) {
	client := &flakyPlanTernClient{
		failPlans: maxTransientPlanRetries,
		planErr:   grpcstatus.Error(grpccodes.Unavailable, "upstream connect error"),
	}
	h := newPlanRetryTestHandler(client, time.Millisecond)

	resp, err := h.executePlanWithTransientRetry(t.Context(), planRetryTestRequest(), "octocat/hello-world", 1)

	require.NoError(t, err)
	require.NotNil(t, resp)
	assert.Equal(t, "plan-after-retry", resp.PlanID)
	assert.Equal(t, maxTransientPlanRetries+1, client.calls(), "handler should allow the final configured retry to recover")
}

// A sustained remote outage still surfaces after the bounded retries: the
// handler does not loop forever, and the caller posts the same operator-visible
// error it would have without retries.
func TestExecutePlanTransientRetryExhausted(t *testing.T) {
	client := &flakyPlanTernClient{
		failPlans: 10,
		planErr:   grpcstatus.Error(grpccodes.Unavailable, "upstream connect error"),
	}
	h := newPlanRetryTestHandler(client, time.Millisecond)

	resp, err := h.executePlanWithTransientRetry(t.Context(), planRetryTestRequest(), "octocat/hello-world", 1)

	require.Error(t, err)
	assert.Nil(t, resp)
	var remoteErr *api.RemoteDeploymentUnavailableError
	require.True(t, errors.As(err, &remoteErr), "exhausted retry should surface the remote unavailability error")
	assert.Equal(t, "remote", remoteErr.Deployment)
	assert.Equal(t, "orders-target", remoteErr.Target)
	assert.Equal(t, maxTransientPlanRetries+1, client.calls(), "handler should stop after the configured retry budget")
}

// Only transient remote unavailability is retried. Deterministic failures
// (validation, policy, planning errors) surface immediately so the user gets
// an actionable comment without delay.
func TestExecutePlanNoRetryForNonTransientErrors(t *testing.T) {
	client := &flakyPlanTernClient{
		failPlans: 10,
		planErr:   grpcstatus.Error(grpccodes.InvalidArgument, "schema name is required"),
	}
	h := newPlanRetryTestHandler(client, time.Millisecond)

	resp, err := h.executePlanWithTransientRetry(t.Context(), planRetryTestRequest(), "octocat/hello-world", 1)

	require.Error(t, err)
	assert.Nil(t, resp)
	assert.Contains(t, err.Error(), "schema name is required")
	assert.Equal(t, 1, client.calls(), "non-transient failures must not be retried")
}

// If a transient retry reaches a deterministic failure, retries stop and the
// deterministic error is surfaced immediately instead of spending the remaining
// retry budget.
func TestExecutePlanTransientRetryStopsOnNonTransientError(t *testing.T) {
	client := &sequencePlanTernClient{errors: []error{
		grpcstatus.Error(grpccodes.Unavailable, "upstream connect error"),
		grpcstatus.Error(grpccodes.InvalidArgument, "schema name is required"),
	}}
	h := newPlanRetryTestHandler(client, time.Millisecond)

	resp, err := h.executePlanWithTransientRetry(t.Context(), planRetryTestRequest(), "octocat/hello-world", 1)

	require.Error(t, err)
	assert.Nil(t, resp)
	assert.Contains(t, err.Error(), "schema name is required")
	assert.Equal(t, 2, client.calls(), "handler should stop when a retry reaches a deterministic failure")
}

// A cancelled webhook context aborts the retry wait immediately instead of
// holding the handler for the full retry delay.
func TestExecutePlanTransientRetryHonorsContextCancellation(t *testing.T) {
	client := &flakyPlanTernClient{
		failPlans: 10,
		planErr:   grpcstatus.Error(grpccodes.Unavailable, "upstream connect error"),
	}
	h := newPlanRetryTestHandler(client, time.Hour)

	ctx, cancel := context.WithTimeout(t.Context(), 50*time.Millisecond)
	defer cancel()

	start := time.Now()
	resp, err := h.executePlanWithTransientRetry(ctx, planRetryTestRequest(), "octocat/hello-world", 1)

	require.Error(t, err)
	assert.Nil(t, resp)
	require.ErrorIs(t, err, context.DeadlineExceeded)
	assert.Less(t, time.Since(start), 10*time.Second, "cancellation must abort the retry wait")
	assert.Equal(t, 1, client.calls(), "cancelled context must not send the retry attempt")
}
