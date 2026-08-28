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

// stubApplyOperationStore returns a fixed operation set (or error) from
// ListByApply so the observer's comment-dispatch routing can be exercised
// without a database.
type stubApplyOperationStore struct {
	storage.ApplyOperationStore
	ops        []*storage.ApplyOperation
	err        error
	resumeByOp map[int64]*storage.EngineResumeState
	listCalls  int
}

func (s *stubApplyOperationStore) ListByApply(context.Context, int64) ([]*storage.ApplyOperation, error) {
	s.listCalls++
	return s.ops, s.err
}

func (s *stubApplyOperationStore) GetEngineResumeState(_ context.Context, opID int64) (*storage.EngineResumeState, error) {
	if rs, ok := s.resumeByOp[opID]; ok {
		return rs, nil
	}
	return nil, storage.ErrEngineResumeStateNotFound
}

// stubStorage exposes only the accessors the comment dispatch needs; every
// other store would panic, keeping the test honest about the path it covers.
type stubStorage struct {
	storage.Storage
	ops     storage.ApplyOperationStore
	applies storage.ApplyStore
	settled []*storage.ApplyControlRequest
}

func (s *stubStorage) ApplyOperations() storage.ApplyOperationStore { return s.ops }

func (s *stubStorage) Applies() storage.ApplyStore { return s.applies }

// stubApplyStore serves the authority gate's fresh re-read of the apply row
// from a fixed result.
type stubApplyStore struct {
	storage.ApplyStore
	apply *storage.Apply
	err   error
}

func (s *stubApplyStore) Get(context.Context, int64) (*storage.Apply, error) {
	return s.apply, s.err
}

func (s *stubStorage) Tasks() storage.TaskStore { return stubTaskStore{} }

func (s *stubStorage) ControlRequests() storage.ControlRequestStore {
	return stubControlRequestStore{settled: s.settled}
}

// stubControlRequestStore supplies the settled-control-request read the comment
// dispatch path makes, from whatever the test seeded.
type stubControlRequestStore struct {
	storage.ControlRequestStore
	settled []*storage.ApplyControlRequest
}

func (s stubControlRequestStore) ListSettled(context.Context, int64) ([]*storage.ApplyControlRequest, error) {
	return s.settled, nil
}

// stubTaskStore supplies the per-shard read the comment dispatch path makes; the
// routing tests have no shard rows, so it returns none.
type stubTaskStore struct {
	storage.TaskStore
}

func (stubTaskStore) GetShardProgressByApplyOperationID(context.Context, int64) ([]*storage.Task, error) {
	return nil, nil
}

func newDispatchTestObserver(opStore storage.ApplyOperationStore) *CommentObserver {
	return &CommentObserver{
		stor:   &stubStorage{ops: opStore},
		logger: slog.New(slog.NewTextHandler(io.Discard, nil)),
	}
}

// VSchema status must reach real PR comments through the production observer
// path: comments are built from storage (buildApplyCommentData), and VSchema is
// projected from each operation's engine resume metadata — the comment path
// never reads a progress response. These cases drive that path end to end from a
// stubbed resume state, covering the progress comment, the terminal summary, a
// multi-keyspace deploy, and the fail-soft cases that must render no section.
func TestCommentObserverRendersVSchemaFromEngineResumeState(t *testing.T) {
	const opID = 7
	psApply := func(s string) *storage.Apply {
		return &storage.Apply{
			ApplyIdentifier: "apply-1", Database: "commerce", Environment: "production",
			State: s, Engine: storage.EnginePlanetScale,
		}
	}
	observer := func(blob string) *CommentObserver {
		stub := &stubApplyOperationStore{ops: []*storage.ApplyOperation{{ID: opID, Deployment: "eu", State: state.ApplyOperation.Running}}}
		if blob != "" {
			stub.resumeByOp = map[int64]*storage.EngineResumeState{opID: {Metadata: blob}}
		}
		return newDispatchTestObserver(stub)
	}

	t.Run("progress comment shows the applying keyspace and diff", func(t *testing.T) {
		body := observer(`{"vschema_status":"applying","vschema_diffs":[{"namespace":"commerce_sharded","diff":"+ \"xxhash\": {\"type\": \"xxhash\"}"}]}`).
			formatStatusComment(psApply(state.Apply.Running), nil)
		assert.Contains(t, body, "### VSchema")
		assert.Contains(t, body, "**`commerce_sharded`**: Applying...")
		assert.Contains(t, body, `+ "xxhash": {"type": "xxhash"}`)
	})

	t.Run("terminal summary of a VSchema-only apply reports the VSchema outcome", func(t *testing.T) {
		body := observer(`{"vschema_status":"applied","vschema_diffs":[{"namespace":"commerce_sharded","diff":"+ \"xxhash\": {}"}]}`).
			formatTerminalSummaryComment(psApply(state.Apply.Completed))
		assert.Contains(t, body, "Applied successfully — your schema change is live!")
		assert.Contains(t, body, "**`commerce_sharded`**: Applied")
		assert.NotContains(t, body, "0 tables")
	})

	t.Run("multi-keyspace renders each keyspace independently", func(t *testing.T) {
		body := observer(`{"vschema_status":"applying","vschema_diffs":[{"namespace":"commerce","diff":"+ \"lookup\": {}"},{"namespace":"commerce_sharded","diff":"+ \"xxhash\": {}"}]}`).
			formatStatusComment(psApply(state.Apply.Running), nil)
		assert.Contains(t, body, "**`commerce`**: Applying...")
		assert.Contains(t, body, "**`commerce_sharded`**: Applying...")
	})

	t.Run("no engine resume state renders no VSchema section", func(t *testing.T) {
		body := observer("").formatStatusComment(psApply(state.Apply.Running), nil)
		assert.NotContains(t, body, "### VSchema")
	})

	t.Run("non-PlanetScale apply skips VSchema projection", func(t *testing.T) {
		body := observer(`{"vschema_status":"applying","vschema_diffs":[{"namespace":"x","diff":"+ y"}]}`).
			formatStatusComment(runningApply(), nil)
		assert.NotContains(t, body, "### VSchema")
	})
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

	assert.Contains(t, body, "## Schema Change Status")
	assert.NotContains(t, body, "**Deployments**:")
}

// A transient operation-load failure falls back to the single-deployment layout
// so a storage error never blocks the comment update.
func TestFormatStatusCommentFallsBackOnLoadError(t *testing.T) {
	o := newDispatchTestObserver(&stubApplyOperationStore{err: errors.New("db unavailable")})

	body := o.formatStatusComment(runningApply(), nil)

	assert.Contains(t, body, "## Schema Change Status")
	assert.NotContains(t, body, "**Deployments**:")
}

// A multi-deployment terminal apply renders the aggregated summary: the observer
// loads the operation rows and routes through the multi-deployment summary
// layout.
func TestFormatTerminalSummaryCommentRoutesMultiDeployment(t *testing.T) {
	o := newDispatchTestObserver(&stubApplyOperationStore{ops: []*storage.ApplyOperation{
		{ID: 1, Deployment: "eu", State: state.ApplyOperation.Completed},
		{ID: 2, Deployment: "us", State: state.ApplyOperation.Completed},
	}})

	body := o.formatTerminalSummaryComment(completedApply())

	assert.Contains(t, body, "## ✅ Schema Change Applied")
	assert.Contains(t, body, "**Deployments**: 2 completed")
	assert.Contains(t, body, "- ✅ eu — completed")
	assert.Contains(t, body, "- ✅ us — completed")
}

// A single-operation terminal apply renders the single-deployment summary
// unchanged — no aggregate header.
func TestFormatTerminalSummaryCommentRoutesSingleDeployment(t *testing.T) {
	o := newDispatchTestObserver(&stubApplyOperationStore{ops: []*storage.ApplyOperation{
		{ID: 1, Deployment: "eu", State: state.ApplyOperation.Completed},
	}})

	body := o.formatTerminalSummaryComment(completedApply())

	assert.Contains(t, body, "## ✅ Schema Change Applied")
	assert.NotContains(t, body, "**Deployments**:")
}

// A transient operation-load failure falls back to the single-deployment summary
// so a storage error never blocks the terminal comment.
func TestFormatTerminalSummaryCommentFallsBackOnLoadError(t *testing.T) {
	o := newDispatchTestObserver(&stubApplyOperationStore{err: errors.New("db unavailable")})

	body := o.formatTerminalSummaryComment(completedApply())

	assert.Contains(t, body, "## ✅ Schema Change Applied")
	assert.NotContains(t, body, "**Deployments**:")
}

// capturedLog records one logger call so tests can assert on the structured
// fields attached to observer logs.
type capturedLog struct {
	msg  string
	args []any
}

type capturingLogger struct {
	debugs []capturedLog
	infos  []capturedLog
	errors []capturedLog
}

func (l *capturingLogger) Debug(msg string, args ...any) {
	l.debugs = append(l.debugs, capturedLog{msg: msg, args: args})
}

func (l *capturingLogger) Info(msg string, args ...any) {
	l.infos = append(l.infos, capturedLog{msg: msg, args: args})
}

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

// The aggregate terminal observer is invoked by the operator that already won
// the non-terminal→terminal projection CAS for a multi-operation apply. That
// driver holds the operation lease, not the parent apply lease, so the one-shot
// observer must publish without an apply lease — unlike the normal per-driver
// observer, which fails closed when it cannot prove apply-lease ownership.
func TestAggregateTerminalObserverBypassesApplyLeaseCheck(t *testing.T) {
	logger := &capturingLogger{}
	cfg := CommentObserverConfig{Repo: "org/repo", PR: 42, ApplyID: 7, Logger: logger}
	unleasedApply := &storage.Apply{ApplyIdentifier: "apply-abc123", State: state.Apply.Completed}

	normal := NewCommentObserver(cfg)
	assert.False(t, normal.leaseStillOwnsObserver(unleasedApply, "terminal"),
		"a terminal apply short-circuits the in-flight gate, so a normal observer with no apply lease fails closed")

	aggregate := NewAggregateTerminalCommentObserver(cfg)
	assert.True(t, aggregate.leaseStillOwnsObserver(unleasedApply, "terminal"),
		"the aggregate terminal observer is authorized by the won CAS, not an apply lease")

	// The lease-scoped storage write helper must likewise not attach a lease the
	// aggregate driver does not hold, so comment-recording writes take storage's
	// no-apply-lease path instead of failing closed.
	ctx := aggregate.contextWithApplyLease(t.Context(), unleasedApply)
	_, hasLease := storage.ApplyLeaseFromContext(ctx)
	assert.False(t, hasLease, "aggregate terminal observer must not attach an apply lease to storage writes")
}

// The progress-comment authority exists only for applies whose work runs under
// operation leases while the parent apply lease is legitimately unheld. This
// pins the shape gate to the operator's drive-mode split: a multi-operation
// rollout with keyed work still in flight, or a generation manifest still
// promising operations, qualifies; terminal applies, settled keyed work,
// single-operation applies, and whole-deployment operations — whose drives
// hold the parent lease, so an unheld lease there means no driver — do not.
func TestOperationScopedWorkInFlight(t *testing.T) {
	runningApply := &storage.Apply{ApplyIdentifier: "apply-1", State: state.Apply.Running}

	t.Run("terminal apply has no in-flight work", func(t *testing.T) {
		// The operation rows alone would qualify — keyed, multiple, one still
		// running — so only the apply's terminal state produces the denial.
		o := newDispatchTestObserver(&stubApplyOperationStore{ops: []*storage.ApplyOperation{
			{ID: 1, Deployment: "primary", OperationKey: "orders/-80", State: state.ApplyOperation.Completed},
			{ID: 2, Deployment: "primary", OperationKey: "orders/80-", State: state.ApplyOperation.Running},
		}})
		inFlight, err := o.operationScopedWorkInFlight(t.Context(), &storage.Apply{ApplyIdentifier: "apply-1", State: state.Apply.Completed})
		require.NoError(t, err)
		assert.False(t, inFlight)
	})

	t.Run("a single keyed operation drives under the parent lease", func(t *testing.T) {
		o := newDispatchTestObserver(&stubApplyOperationStore{ops: []*storage.ApplyOperation{
			{ID: 1, Deployment: "primary", OperationKey: "orders/-80", State: state.ApplyOperation.Running},
		}})
		inFlight, err := o.operationScopedWorkInFlight(t.Context(), runningApply)
		require.NoError(t, err)
		assert.False(t, inFlight)
	})

	t.Run("non-terminal operation-keyed row is in flight", func(t *testing.T) {
		o := newDispatchTestObserver(&stubApplyOperationStore{ops: []*storage.ApplyOperation{
			{ID: 1, Deployment: "primary", OperationKey: "orders/-80", State: state.ApplyOperation.Completed},
			{ID: 2, Deployment: "primary", OperationKey: "orders/80-", State: state.ApplyOperation.Running},
		}})
		inFlight, err := o.operationScopedWorkInFlight(t.Context(), runningApply)
		require.NoError(t, err)
		assert.True(t, inFlight)
	})

	t.Run("settled keyed work is not in flight", func(t *testing.T) {
		o := newDispatchTestObserver(&stubApplyOperationStore{ops: []*storage.ApplyOperation{
			{ID: 1, Deployment: "primary", OperationKey: "orders/-80", State: state.ApplyOperation.Completed},
			{ID: 2, Deployment: "primary", OperationKey: "orders/80-", State: state.ApplyOperation.Completed},
		}})
		inFlight, err := o.operationScopedWorkInFlight(t.Context(), runningApply)
		require.NoError(t, err)
		assert.False(t, inFlight)
	})

	t.Run("whole-deployment operations are governed by the parent lease", func(t *testing.T) {
		o := newDispatchTestObserver(&stubApplyOperationStore{ops: []*storage.ApplyOperation{
			{ID: 1, Deployment: "primary", State: state.ApplyOperation.Running},
			{ID: 2, Deployment: "replica", State: state.ApplyOperation.Running},
		}})
		inFlight, err := o.operationScopedWorkInFlight(t.Context(), runningApply)
		require.NoError(t, err)
		assert.False(t, inFlight)
	})

	t.Run("manifest keys with no attached operation are in flight", func(t *testing.T) {
		manifestApply := &storage.Apply{
			ApplyIdentifier:       "apply-1",
			State:                 state.Apply.Running,
			ExpectedOperationKeys: []string{"orders/-80", "orders/80-"},
		}
		o := newDispatchTestObserver(&stubApplyOperationStore{ops: []*storage.ApplyOperation{
			{ID: 1, Deployment: "primary", OperationKey: "orders/-80", State: state.ApplyOperation.Completed},
		}})
		inFlight, err := o.operationScopedWorkInFlight(t.Context(), manifestApply)
		require.NoError(t, err)
		assert.True(t, inFlight)
	})

	t.Run("an operation-load failure is surfaced, never treated as in flight", func(t *testing.T) {
		o := newDispatchTestObserver(&stubApplyOperationStore{err: errors.New("db unavailable")})
		inFlight, err := o.operationScopedWorkInFlight(t.Context(), runningApply)
		require.Error(t, err)
		assert.False(t, inFlight)
	})
}

// inFlightKeyedOps is an operation set that qualifies for the durable
// progress-comment authority: a multi-operation rollout with keyed work still
// running.
func inFlightKeyedOps() []*storage.ApplyOperation {
	return []*storage.ApplyOperation{
		{ID: 1, Deployment: "primary", OperationKey: "orders/-80", State: state.ApplyOperation.Completed},
		{ID: 2, Deployment: "primary", OperationKey: "orders/80-", State: state.ApplyOperation.Running},
	}
}

// The observer's poller snapshot can be a tick old, so the authority gate
// re-reads the apply row before claiming. A dispatch wave that re-claimed the
// parent lease since the snapshot — or a projection that settled the apply
// terminal — owns the comment through its own path, and the authority must
// deny so two writers never edit the same comment concurrently.
func TestProgressCommentAuthorityDeniesOnFreshApplyRowChanges(t *testing.T) {
	snapshot := &storage.Apply{ID: 7, ApplyIdentifier: "apply-1", State: state.Apply.Running}

	t.Run("parent lease re-claimed since the snapshot", func(t *testing.T) {
		logger := &capturingLogger{}
		o := newDispatchTestObserver(&stubApplyOperationStore{ops: inFlightKeyedOps()})
		o.logger = logger
		o.stor.(*stubStorage).applies = &stubApplyStore{apply: &storage.Apply{
			ID: 7, ApplyIdentifier: "apply-1", State: state.Apply.Running,
			LeaseOwner: "driver-host/9/dispatch", LeaseToken: "wave-token",
		}}

		assert.False(t, o.progressCommentAuthorityOwnsObserver(snapshot, "progress"),
			"a freshly claimed parent lease must deny the durable authority")
		require.Len(t, logger.debugs, 1)
		assert.Contains(t, logger.debugs[0].msg, "parent apply lease re-claimed")
	})

	t.Run("apply settled terminal since the snapshot", func(t *testing.T) {
		logger := &capturingLogger{}
		o := newDispatchTestObserver(&stubApplyOperationStore{ops: inFlightKeyedOps()})
		o.logger = logger
		o.stor.(*stubStorage).applies = &stubApplyStore{apply: &storage.Apply{
			ID: 7, ApplyIdentifier: "apply-1", State: state.Apply.Completed,
		}}

		assert.False(t, o.progressCommentAuthorityOwnsObserver(snapshot, "progress"),
			"a freshly settled terminal apply must deny the durable authority")
		require.Len(t, logger.debugs, 1)
		assert.Contains(t, logger.debugs[0].msg, "apply settled terminal")
	})

	t.Run("a re-read failure fails closed", func(t *testing.T) {
		logger := &capturingLogger{}
		o := newDispatchTestObserver(&stubApplyOperationStore{ops: inFlightKeyedOps()})
		o.logger = logger
		o.stor.(*stubStorage).applies = &stubApplyStore{err: errors.New("db unavailable")}

		assert.False(t, o.progressCommentAuthorityOwnsObserver(snapshot, "progress"))
		require.Len(t, logger.errors, 1)
		assert.Contains(t, logger.errors[0].msg, "failed to re-read the apply")
	})

	t.Run("a vanished apply row fails closed", func(t *testing.T) {
		logger := &capturingLogger{}
		o := newDispatchTestObserver(&stubApplyOperationStore{ops: inFlightKeyedOps()})
		o.logger = logger
		o.stor.(*stubStorage).applies = &stubApplyStore{}

		assert.False(t, o.progressCommentAuthorityOwnsObserver(snapshot, "progress"))
		require.Len(t, logger.errors, 1)
		assert.Contains(t, logger.errors[0].msg, "apply row no longer exists")
	})
}

// One observer callback checks the side-effect gate several times — before the
// comment lookup, the client creation, the edit, and the tracking write. The
// authority decision is made once per callback against fresh storage rows and
// then reused, so a tick issues one operation scan and one claim write rather
// than one per gate check. Resetting the decision (the next callback) decides
// afresh.
func TestProgressCommentAuthorityDecidesOncePerCallback(t *testing.T) {
	snapshot := &storage.Apply{ID: 7, ApplyIdentifier: "apply-1", State: state.Apply.Running}
	opStore := &stubApplyOperationStore{ops: inFlightKeyedOps()}
	o := newDispatchTestObserver(opStore)
	o.logger = &capturingLogger{}
	o.stor.(*stubStorage).applies = &stubApplyStore{apply: &storage.Apply{
		ID: 7, ApplyIdentifier: "apply-1", State: state.Apply.Running,
		LeaseOwner: "driver-host/9/dispatch", LeaseToken: "wave-token",
	}}

	assert.False(t, o.progressCommentAuthorityOwnsObserver(snapshot, "progress"))
	assert.False(t, o.progressCommentAuthorityOwnsObserver(snapshot, "edit GitHub comment"))
	assert.Equal(t, 1, opStore.listCalls, "the second gate check in one callback must reuse the decision")

	o.resetProgressCommentAuthorityDecision()
	assert.False(t, o.progressCommentAuthorityOwnsObserver(snapshot, "progress"))
	assert.Equal(t, 2, opStore.listCalls, "the next callback must decide afresh")
}

// A per-driver observer for a multi-operation apply must not publish a separate
// apply-level summary comment: it holds only one operation's task slice, so
// publishing here would post a duplicate, partial summary. The aggregate
// CAS-winner observer owns that summary instead.
func TestShouldPublishSeparateSummaryDefersForMultiOperationApply(t *testing.T) {
	o := newDispatchTestObserver(nil)
	ops := []*storage.ApplyOperation{
		{ID: 1, Deployment: "eu", State: state.ApplyOperation.Completed},
		{ID: 2, Deployment: "us", State: state.ApplyOperation.Completed},
	}

	assert.False(t, o.shouldPublishSeparateSummary(completedApply(), ops, nil))
}

// A per-driver observer for a single-operation apply (every apply today, until
// fan-out lands) owns and publishes the terminal summary unchanged.
func TestShouldPublishSeparateSummaryPublishesForSingleOperationApply(t *testing.T) {
	o := newDispatchTestObserver(nil)
	ops := []*storage.ApplyOperation{
		{ID: 1, Deployment: "eu", State: state.ApplyOperation.Completed},
	}

	assert.True(t, o.shouldPublishSeparateSummary(completedApply(), ops, nil))
}

// On an operation-load failure the per-driver observer must not publish: a
// partial or duplicate summary is worse than none, and startup reconciliation
// repairs a genuinely missing summary.
func TestShouldPublishSeparateSummaryDefersOnLoadError(t *testing.T) {
	o := newDispatchTestObserver(nil)

	assert.False(t, o.shouldPublishSeparateSummary(completedApply(), nil, errors.New("db unavailable")))
}

// The aggregate CAS-winner observer always owns the terminal summary — its
// authority is the won non-terminal→terminal projection CAS, so it publishes
// regardless of the operation set or a load failure.
func TestShouldPublishSeparateSummaryAlwaysTrueForAggregateWinner(t *testing.T) {
	cfg := CommentObserverConfig{
		Repo:    "org/repo",
		PR:      42,
		ApplyID: 7,
		Logger:  slog.New(slog.NewTextHandler(io.Discard, nil)),
	}
	aggregate := NewAggregateTerminalCommentObserver(cfg)

	assert.True(t, aggregate.shouldPublishSeparateSummary(completedApply(), nil, errors.New("db unavailable")))
}
