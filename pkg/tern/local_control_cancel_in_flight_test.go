package tern

import (
	"context"
	"errors"
	"log/slog"
	"testing"
	"time"

	ps "github.com/planetscale/planetscale-go/planetscale"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/block/schemabot/pkg/engine"
	"github.com/block/schemabot/pkg/engine/planetscale"
	ternv1 "github.com/block/schemabot/pkg/proto/ternv1"
	"github.com/block/schemabot/pkg/psclient"
	"github.com/block/schemabot/pkg/state"
	"github.com/block/schemabot/pkg/storage"
)

// cancelInFlightPSClient serves one deploy request whose cancel PlanetScale
// has accepted but not confirmed: it reads in_progress_cancel, rejects a
// further cancel because the deploy request is closed to new cancels, and
// rejects a cutover.
type cancelInFlightPSClient struct {
	psclient.PSClient
	cancelCalls int
	applyCalls  int
}

func (c *cancelInFlightPSClient) GetDeployRequest(_ context.Context, req *ps.GetDeployRequestRequest) (*ps.DeployRequest, error) {
	deployedAt := time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)
	return &ps.DeployRequest{
		Number:          req.Number,
		State:           "open",
		DeploymentState: state.DeployRequest.InProgressCancel,
		DeployedAt:      &deployedAt,
		UpdatedAt:       deployedAt,
	}, nil
}

func (c *cancelInFlightPSClient) CancelDeployRequest(context.Context, *ps.CancelDeployRequestRequest) (*ps.DeployRequest, error) {
	c.cancelCalls++
	return nil, errors.New("The deploy request is closed.")
}

func (c *cancelInFlightPSClient) ApplyDeployRequest(context.Context, *ps.ApplyDeployRequestRequest) (*ps.DeployRequest, error) {
	c.applyCalls++
	return nil, errors.New("deploy request cannot be applied while it is cancelling")
}

// cancelInFlightFixture wires the real PlanetScale engine, over a client that
// reports the deploy request cancelling, to an apply waiting for cutover.
type cancelInFlightFixture struct {
	client *LocalClient
	ps     *cancelInFlightPSClient
	eng    *planetscale.Engine
	apply  *storage.Apply
	task   *storage.Task
	resume *storage.EngineResumeState
}

func newCancelInFlightFixture(t *testing.T) *cancelInFlightFixture {
	t.Helper()
	operationID := int64(99)
	apply := &storage.Apply{
		ID:              42,
		ApplyIdentifier: "apply-vitess-cancel-in-flight",
		State:           state.Apply.WaitingForCutover,
	}
	task := &storage.Task{
		ID:               7,
		ApplyID:          apply.ID,
		ApplyOperationID: &operationID,
		TaskIdentifier:   "task-vitess-cancel-in-flight",
		State:            state.Task.WaitingForCutover,
	}
	resume := engineResumeStateFromPlanetScaleData(t, operationID, planetscale.ResumeData{
		BranchName:       "schemabot-testdb-cancel",
		DeployRequestID:  72,
		MigrationContext: "ctx-72",
		DeployRequestURL: "https://example.test/deploys/72",
	})
	psClient := &cancelInFlightPSClient{}
	eng := planetscale.NewWithClient(slog.Default(), func(_, _ string) (psclient.PSClient, error) { return psClient, nil })
	client := newVitessControlTestClient(apply, []*storage.Task{task}, resume, eng)
	client.config.Metadata = map[string]string{
		"organization": "org",
		"token_name":   "token",
		"token_value":  "secret",
	}
	return &cancelInFlightFixture{client: client, ps: psClient, eng: eng, apply: apply, task: task, resume: resume}
}

// An operator cancels an apply whose deploy request already has a cancel in
// flight, from the PlanetScale UI or from an earlier attempt whose answer was
// lost. PlanetScale rejects the second cancel, and the live-work probe reads
// the cancelling deploy request as running. A cancel PlanetScale has not
// confirmed can still end in a failed deploy, so the rejection surfaces for
// the drive to retry and the apply keeps its stored state; it settles only
// once the deploy request reports cancelled or failed.
func TestLocalClient_CancelRejectedWhileACancelIsInFlightSurfacesForRetry(t *testing.T) {
	fx := newCancelInFlightFixture(t)

	resp, err := fx.client.cancelOwnedApply(t.Context(), &ternv1.CancelRequest{ApplyId: fx.apply.ApplyIdentifier}, "github:alice")

	require.Error(t, err)
	assert.Nil(t, resp)
	assert.True(t, engine.IsRetryable(err), "the drive retries the cancel until the deploy request settles")
	assert.ErrorContains(t, err, `cancel deploy request #72 rejected in deployment state "in_progress_cancel"`)
	assert.Equal(t, 1, fx.ps.cancelCalls)
	assert.Equal(t, state.Task.WaitingForCutover, fx.task.State, "an unconfirmed cancel must not record the task cancelled")
	assert.Nil(t, fx.task.CompletedAt)
	assert.Equal(t, state.Apply.WaitingForCutover, fx.apply.State, "an unconfirmed cancel must not record the apply cancelled")
}

// While a cancel is in flight the deploy request reads as running, which
// ranks below a cutover wait, so the stored task stays waiting for cutover
// rather than moving backward (ST-4). The PR comment and CLI keep showing the
// cutover wait until the deploy request settles on cancelled or failed.
func TestInFlightCancelDoesNotRewindACutoverWait(t *testing.T) {
	fx := newCancelInFlightFixture(t)

	progress, err := fx.eng.Progress(t.Context(), &engine.ProgressRequest{
		Database:    "testdb",
		Credentials: fx.client.credentials(),
		ResumeState: &engine.ResumeState{MigrationContext: fx.resume.MigrationContext, Metadata: fx.resume.Metadata},
	})
	require.NoError(t, err)
	require.NotNil(t, progress)
	engineTask := engineStateToStorage(progress.State)

	assert.Equal(t, state.Task.Running, engineTask)
	assert.Equal(t, state.Task.WaitingForCutover, taskStateWithNoBackwardProgress(state.Task.WaitingForCutover, engineTask))
	assert.Equal(t, state.Task.CuttingOver, taskStateWithNoBackwardProgress(state.Task.CuttingOver, engineTask))
}

// A cutover request can still land while a cancel is in flight, because the
// stored task keeps showing the cutover wait. The drive forwards it to the
// engine, which sends it to PlanetScale without checking the deploy request's
// state. When PlanetScale rejects it, the rejection surfaces and the stored
// state stays as it was, so the apply settles on whatever the deploy request
// settles on.
func TestLocalClient_CutoverDuringAnInFlightCancelSurfacesTheRejection(t *testing.T) {
	fx := newCancelInFlightFixture(t)

	resp, err := fx.client.cutover(t.Context(), &ternv1.CutoverRequest{ApplyId: fx.apply.ApplyIdentifier}, "github:alice")

	require.Error(t, err)
	assert.Nil(t, resp)
	assert.ErrorContains(t, err, "cutover failed: cutover deploy request #72")
	assert.ErrorContains(t, err, "deploy request cannot be applied while it is cancelling")
	assert.Equal(t, 1, fx.ps.applyCalls)
	assert.Equal(t, state.Task.WaitingForCutover, fx.task.State)
	assert.Equal(t, state.Apply.WaitingForCutover, fx.apply.State)
}
