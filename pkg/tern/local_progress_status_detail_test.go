package tern

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	ternv1 "github.com/block/schemabot/pkg/proto/ternv1"
	"github.com/block/schemabot/pkg/state"
	"github.com/block/schemabot/pkg/storage"
)

// statusDetailLogStore serves an apply's log lines and records how a progress
// read asked for them.
type statusDetailLogStore struct {
	storage.ApplyLogStore
	logs         []*storage.ApplyLog
	recentErr    error
	recentLimits []int
	fullReads    int
}

func (s *statusDetailLogStore) GetByApply(context.Context, int64) ([]*storage.ApplyLog, error) {
	s.fullReads++
	return s.logs, nil
}

func (s *statusDetailLogStore) GetRecentByApply(_ context.Context, _ int64, limit int) ([]*storage.ApplyLog, error) {
	s.recentLimits = append(s.recentLimits, limit)
	if s.recentErr != nil {
		return nil, s.recentErr
	}
	return s.logs[max(len(s.logs)-limit, 0):], nil
}

// statusDetailFixture is an apply whose tasks are still pending, so the state
// the progress answer reports comes from the apply record.
func statusDetailFixture(t *testing.T, applyState string, logs *statusDetailLogStore) (*sharedApplyFixture, *bytes.Buffer) {
	t.Helper()
	fx := newSharedApplyFixture(state.ApplyOperation.Running, state.ApplyOperation.Running, state.Task.Pending, state.Task.Pending)
	fx.apply.State = applyState
	fx.client.storage.(*exactProgressStorage).logs = logs
	var logged bytes.Buffer
	fx.client.logger = slog.New(slog.NewTextHandler(&logged, nil))
	return fx, &logged
}

func progressStatusDetail(t *testing.T, fx *sharedApplyFixture) (*ternv1.ProgressResponse, string) {
	t.Helper()
	resp, err := fx.client.Progress(t.Context(), &ternv1.ProgressRequest{ApplyId: fx.apply.ApplyIdentifier, Environment: "staging"})
	require.NoError(t, err)
	return resp, resp.Metadata["status_detail"]
}

// While a branch is being prepared the CLI shows the newest event message, so
// the progress read asks storage for that one line instead of loading every
// log the apply has written.
func TestLocalClient_ProgressStatusDetailReadsOnlyTheNewestLog(t *testing.T) {
	logs := &statusDetailLogStore{logs: []*storage.ApplyLog{
		{ID: 1, Message: "Created branch my-branch"},
		{ID: 2, Message: "Refreshing schema for branch my-branch from main"},
		{ID: 3, Message: "Applying keyspace orders_003 (8/12)"},
	}}
	fx, _ := statusDetailFixture(t, state.Apply.ApplyingBranchChanges, logs)

	resp, detail := progressStatusDetail(t, fx)

	assert.Equal(t, ternv1.State_STATE_APPLYING_BRANCH_CHANGES, resp.State)
	assert.Equal(t, "Applying keyspace orders_003 (8/12)", detail)
	assert.Equal(t, []int{1}, logs.recentLimits, "the read asks for the single newest line")
	assert.Zero(t, logs.fullReads, "the read never loads the apply's whole log")
}

// A failed log read must not fail the progress answer: the apply is still
// reported in its branch setup state, without a detail line, and the log names
// the apply and the cause.
func TestLocalClient_ProgressStatusDetailLogReadFailureStillAnswers(t *testing.T) {
	logs := &statusDetailLogStore{recentErr: errors.New("connection lost")}
	fx, logged := statusDetailFixture(t, state.Apply.PreparingBranch, logs)

	resp, detail := progressStatusDetail(t, fx)

	assert.Equal(t, ternv1.State_STATE_PREPARING_BRANCH, resp.State)
	assert.Empty(t, detail)
	assert.Contains(t, logged.String(), "without its latest event message")
	assert.Contains(t, logged.String(), "apply_id="+fx.apply.ApplyIdentifier)
	assert.Contains(t, logged.String(), "connection lost")
}

// An apply that has written no log line yet has no detail to show, which is
// not an error.
func TestLocalClient_ProgressStatusDetailAbsentWithoutLogs(t *testing.T) {
	logs := &statusDetailLogStore{}
	fx, logged := statusDetailFixture(t, state.Apply.CreatingDeployRequest, logs)

	resp, detail := progressStatusDetail(t, fx)

	assert.Equal(t, ternv1.State_STATE_CREATING_DEPLOY_REQUEST, resp.State)
	assert.Empty(t, detail)
	assert.Equal(t, []int{1}, logs.recentLimits)
	assert.NotContains(t, logged.String(), "without its latest event message")
}

// Outside the branch setup states the progress read does not touch the log
// store at all.
func TestLocalClient_ProgressStatusDetailSkippedOutsideBranchSetup(t *testing.T) {
	logs := &statusDetailLogStore{logs: []*storage.ApplyLog{{ID: 1, Message: "Created branch my-branch"}}}
	fx, _ := statusDetailFixture(t, state.Apply.Pending, logs)

	_, detail := progressStatusDetail(t, fx)

	assert.Empty(t, detail)
	assert.Empty(t, logs.recentLimits)
	assert.Zero(t, logs.fullReads)
}
