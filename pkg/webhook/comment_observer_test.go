package webhook

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/block/schemabot/pkg/state"
	"github.com/block/schemabot/pkg/storage"
)

// stubApplyOperationStore returns a fixed operation set (or error) from
// ListByApply so the observer's comment-dispatch routing can be exercised
// without a database.
type stubApplyOperationStore struct {
	storage.ApplyOperationStore
	ops []*storage.ApplyOperation
	err error
}

func (s *stubApplyOperationStore) ListByApply(context.Context, int64) ([]*storage.ApplyOperation, error) {
	return s.ops, s.err
}

// stubStorage exposes only the ApplyOperations accessor the comment dispatch
// needs; every other store would panic, keeping the test honest about the path
// it covers.
type stubStorage struct {
	storage.Storage
	ops storage.ApplyOperationStore
}

func (s *stubStorage) ApplyOperations() storage.ApplyOperationStore { return s.ops }

func newDispatchTestObserver(opStore storage.ApplyOperationStore) *CommentObserver {
	return &CommentObserver{
		stor:   &stubStorage{ops: opStore},
		logger: slog.New(slog.NewTextHandler(io.Discard, nil)),
	}
}

// A multi-deployment apply renders the aggregated comment: the observer loads
// the operation rows and routes through the multi-deployment layout.
func TestFormatStatusCommentRoutesMultiDeployment(t *testing.T) {
	o := newDispatchTestObserver(&stubApplyOperationStore{ops: []*storage.ApplyOperation{
		{ID: 1, Deployment: "eu", State: state.ApplyOperation.Completed},
		{ID: 2, Deployment: "us", State: state.ApplyOperation.Running},
	}})

	body := o.formatStatusComment(runningApply(), nil)

	assert.Contains(t, body, "**Deployments**: 1 completed, 1 running")
	assert.Contains(t, body, "- ✅ eu — completed")
	assert.Contains(t, body, "- 🔄 us — running table copy")
}

// A single-operation apply (every apply today, until fan-out lands) renders the
// single-deployment comment unchanged — no aggregate header.
func TestFormatStatusCommentRoutesSingleDeployment(t *testing.T) {
	o := newDispatchTestObserver(&stubApplyOperationStore{ops: []*storage.ApplyOperation{
		{ID: 1, Deployment: "eu", State: state.ApplyOperation.Running},
	}})

	body := o.formatStatusComment(runningApply(), nil)

	assert.Contains(t, body, "## Schema Change In Progress")
	assert.NotContains(t, body, "**Deployments**:")
}

// A transient operation-load failure falls back to the single-deployment layout
// so a storage error never blocks the comment update.
func TestFormatStatusCommentFallsBackOnLoadError(t *testing.T) {
	o := newDispatchTestObserver(&stubApplyOperationStore{err: errors.New("db unavailable")})

	body := o.formatStatusComment(runningApply(), nil)

	assert.Contains(t, body, "## Schema Change In Progress")
	assert.NotContains(t, body, "**Deployments**:")
}
