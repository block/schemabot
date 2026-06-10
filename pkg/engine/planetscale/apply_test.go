package planetscale

import (
	"context"
	"errors"
	"log/slog"
	"os"
	"testing"
	"time"

	ps "github.com/planetscale/planetscale-go/planetscale"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/block/schemabot/pkg/engine"
	"github.com/block/schemabot/pkg/psclient"
	"github.com/block/schemabot/pkg/schema"
)

type permanentVSchemaErrorClient struct {
	psclient.PSClient
	updateCalls int
}

var _ psclient.PSClient = (*permanentVSchemaErrorClient)(nil)

func (c *permanentVSchemaErrorClient) UpdateKeyspaceVSchema(context.Context, *ps.UpdateKeyspaceVSchemaRequest) (*ps.VSchema, error) {
	c.updateCalls++
	return nil, &ps.Error{Code: ps.ErrInvalid}
}

func TestApply_MainBranchReuseIsPermanent(t *testing.T) {
	e := NewWithClient(slog.New(slog.NewTextHandler(os.Stdout, nil)), func(_, _ string) (psclient.PSClient, error) {
		return nil, nil
	})

	_, err := e.Apply(t.Context(), &engine.ApplyRequest{
		Database: "testdb",
		Options: map[string]string{
			"branch": "main",
		},
		Credentials: &engine.Credentials{
			Metadata: map[string]string{
				"organization": "org",
				"token_name":   "token",
				"token_value":  "secret",
				"main_branch":  "main",
			},
		},
	})

	require.Error(t, err)
	assert.False(t, engine.IsRetryable(err))
	assert.Contains(t, err.Error(), "cannot reuse the main branch")
}

func TestApplyKeyspaceChanges_PermanentVSchemaErrorIsPermanent(t *testing.T) {
	e := New(slog.New(slog.NewTextHandler(os.Stdout, nil)))
	client := &permanentVSchemaErrorClient{}

	err := e.applyKeyspaceChanges(t.Context(),
		engine.SchemaChange{
			Namespace: "testapp",
			Metadata:  map[string]string{"vschema_changed": "true"},
		},
		schema.SchemaFiles{
			"testapp": &schema.Namespace{Files: map[string]string{"vschema.json": "{}"}},
		},
		&ps.DatabaseBranchPassword{},
		client,
		"org",
		"database",
		"branch",
	)

	require.Error(t, err)
	assert.False(t, engine.IsRetryable(err))
	assert.Equal(t, 1, client.updateCalls)
}

// pendingPollClient returns the deploy request as pending on every poll until
// a configured number of polls have occurred, then returns an error. This
// drives the pending-poll loop through a transient API failure.
type pendingPollClient struct {
	psclient.PSClient
	number       uint64
	pollErr      error
	pollsBefore  int
	getCallCount int
}

func (c *pendingPollClient) GetDeployRequest(_ context.Context, req *ps.GetDeployRequestRequest) (*ps.DeployRequest, error) {
	c.getCallCount++
	if c.getCallCount > c.pollsBefore {
		return nil, c.pollErr
	}
	return &ps.DeployRequest{Number: req.Number, DeploymentState: deployState.Pending}, nil
}

// When PlanetScale returns a transient error while a deploy request is still
// computing its schema diff, the apply worker surfaces a wrapped error
// identifying the deploy request rather than panicking on a nil response.
func TestWaitForDeployRequestPending_PollErrorIsWrapped(t *testing.T) {
	e := New(slog.New(slog.NewTextHandler(os.Stdout, nil)))
	apiErr := &ps.Error{Code: ps.ErrInternal}
	client := &pendingPollClient{number: 7, pollErr: apiErr, pollsBefore: 1}

	_, err := e.waitForDeployRequestPending(t.Context(), client, "org", "testdb",
		&ps.DeployRequest{Number: 7, DeploymentState: deployState.Pending})

	require.Error(t, err)
	assert.Contains(t, err.Error(), "poll deploy request 7")
	assert.ErrorIs(t, err, apiErr)
	assert.Equal(t, 2, client.getCallCount)
}

// A deploy request that never leaves the pending state must not block the apply
// worker forever — cancelling the context stops the poll and returns a wrapped
// error naming the deploy request.
func TestWaitForDeployRequestPending_HonorsContextCancellation(t *testing.T) {
	e := New(slog.New(slog.NewTextHandler(os.Stdout, nil)))
	client := &pendingPollClient{number: 9, pollErr: errors.New("unreachable"), pollsBefore: 1_000_000}

	ctx, cancel := context.WithCancel(t.Context())
	cancel()

	done := make(chan error, 1)
	go func() {
		_, err := e.waitForDeployRequestPending(ctx, client, "org", "testdb",
			&ps.DeployRequest{Number: 9, DeploymentState: deployState.Pending})
		done <- err
	}()

	select {
	case err := <-done:
		require.Error(t, err)
		assert.ErrorIs(t, err, context.Canceled)
		assert.Contains(t, err.Error(), "deploy request 9")
		assert.Equal(t, 0, client.getCallCount)
	case <-time.After(500 * time.Millisecond):
		t.Fatal("waitForDeployRequestPending did not return after context cancellation")
	}
}

// A nil deploy request indicates an upstream caller never created or fetched it;
// the poll loop surfaces a wrapped error naming the database rather than
// dereferencing nil.
func TestWaitForDeployRequestPending_NilDeployRequestIsRejected(t *testing.T) {
	e := New(slog.New(slog.NewTextHandler(os.Stdout, nil)))
	client := &pendingPollClient{number: 11, pollErr: errors.New("unreachable"), pollsBefore: 1_000_000}

	dr, err := e.waitForDeployRequestPending(t.Context(), client, "org", "testdb", nil)

	require.Error(t, err)
	assert.Nil(t, dr)
	assert.Contains(t, err.Error(), "deploy request is nil")
	assert.Contains(t, err.Error(), "testdb")
	assert.Equal(t, 0, client.getCallCount)
}
