package webhook

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/block/schemabot/pkg/state"
	"github.com/block/schemabot/pkg/storage"
)

// capturedLog records one logger call so tests can assert on the structured
// fields attached to observer logs.
type capturedLog struct {
	msg  string
	args []any
}

type capturingLogger struct {
	errors []capturedLog
}

func (l *capturingLogger) Info(msg string, args ...any) {}

func (l *capturingLogger) Error(msg string, args ...any) {
	l.errors = append(l.errors, capturedLog{msg: msg, args: args})
}

// fieldsOf converts a slog-style key/value arg list into a map for assertions.
func fieldsOf(t *testing.T, args []any) map[string]any {
	t.Helper()
	require.Equal(t, 0, len(args)%2, "log args must be key/value pairs")
	fields := make(map[string]any, len(args)/2)
	for i := 0; i < len(args); i += 2 {
		key, ok := args[i].(string)
		require.True(t, ok, "log arg key must be a string")
		fields[key] = args[i+1]
	}
	return fields
}

// Operators and agents search logs by apply identifier, repo, and PR to trace
// a schema change. Every observer error log must carry those identifiers —
// otherwise GitHub-side failures (comment posts, edits) are invisible in a
// search scoped to one apply.
func TestCommentObserverErrorLogsCarryApplyIdentifiers(t *testing.T) {
	logger := &capturingLogger{}
	observer := NewCommentObserver(CommentObserverConfig{
		Repo:   "org/repo",
		PR:     42,
		Logger: logger,
	})
	observer.SetApplyID(7)

	apply := &storage.Apply{
		ApplyIdentifier: "apply-abc123",
		Database:        "mydb",
		Environment:     "staging",
	}
	observer.logError(apply, "observer: failed to edit comment", "comment_state", "progress")

	require.Len(t, logger.errors, 1)
	assert.Equal(t, "observer: failed to edit comment", logger.errors[0].msg)
	fields := fieldsOf(t, logger.errors[0].args)
	assert.Equal(t, "org/repo", fields["repo"])
	assert.Equal(t, 42, fields["pr"])
	assert.Equal(t, "apply-abc123", fields["apply_id"])
	assert.Equal(t, "mydb", fields["database"])
	assert.Equal(t, "staging", fields["environment"])
	assert.Equal(t, "progress", fields["comment_state"])
	// When the apply record is present, the searchable string identifier is
	// the only apply ID logged — the row ID would be redundant noise.
	assert.NotContains(t, fields, "apply_db_id")
}

// Some observer error paths run before the apply record is available (e.g.,
// lease checks during construction-time races). Repo and PR still identify the
// failure; no apply identifier is logged, and the internal row ID is never
// surfaced so it cannot be confused with the user-facing apply_id.
func TestCommentObserverErrorLogsWithoutApplyRecord(t *testing.T) {
	logger := &capturingLogger{}
	observer := NewCommentObserver(CommentObserverConfig{
		Repo:    "org/repo",
		PR:      42,
		ApplyID: 7,
		Logger:  logger,
	})

	observer.logError(nil, "observer: apply lease unavailable; skipping GitHub side effect", "operation", "progress")

	require.Len(t, logger.errors, 1)
	fields := fieldsOf(t, logger.errors[0].args)
	assert.Equal(t, "org/repo", fields["repo"])
	assert.Equal(t, 42, fields["pr"])
	assert.Equal(t, "progress", fields["operation"])
	assert.NotContains(t, fields, "apply_id")
	assert.NotContains(t, fields, "apply_db_id")
	assert.NotContains(t, fields, "database")
}

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
