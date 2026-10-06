package planetscale

import (
	"context"
	"errors"
	"log/slog"
	"os"
	"sync"
	"testing"
	"time"

	ps "github.com/planetscale/planetscale-go/planetscale"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/block/schemabot/pkg/ddl"
	"github.com/block/schemabot/pkg/engine"
	"github.com/block/schemabot/pkg/lint"
	"github.com/block/schemabot/pkg/psclient"
	"github.com/block/schemabot/pkg/schema"
	"github.com/block/spirit/pkg/table"
)

const (
	resumeUsersTable  = "CREATE TABLE `users` (\n  `id` bigint NOT NULL AUTO_INCREMENT,\n  PRIMARY KEY (`id`)\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci"
	resumeOrdersTable = "CREATE TABLE `orders` (\n  `id` bigint NOT NULL AUTO_INCREMENT,\n  PRIMARY KEY (`id`)\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci"
	resumeItemsTable  = "CREATE TABLE `items` (\n  `id` bigint NOT NULL AUTO_INCREMENT,\n  PRIMARY KEY (`id`)\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci"
)

// shortenEngineWaits compresses the branch poll and wait heartbeat so a test
// can watch several of each without waiting on production pacing.
func shortenEngineWaits(t *testing.T) {
	t.Helper()
	origPoll, origHeartbeat := branchReadyPollInterval, waitHeartbeatInterval
	branchReadyPollInterval, waitHeartbeatInterval = 5*time.Millisecond, 20*time.Millisecond
	t.Cleanup(func() {
		branchReadyPollInterval, waitHeartbeatInterval = origPoll, origHeartbeat
	})
}

// A resumed branch that already carries some of the plan's tables gets only
// the tables it lacks: the diff starts from the branch's real schema, so a
// table that exists is never re-created.
func TestDiffBranchForResume_DiffsFromTheBranchesRealSchema(t *testing.T) {
	e := &Engine{linter: lint.New(), logger: slog.New(slog.NewTextHandler(os.Stdout, nil))}
	current := map[string][]table.TableSchema{
		"commerce": {{Name: "users", Schema: resumeUsersTable}},
	}
	desired := schema.SchemaFiles{
		"commerce": {Files: map[string]string{"users.sql": resumeUsersTable + ";", "orders.sql": resumeOrdersTable + ";"}},
	}

	diff, err := e.diffBranchForResume(current, desired)

	require.NoError(t, err)
	require.Len(t, diff["commerce"], 1)
	assert.Equal(t, "orders", diff["commerce"][0].Table)
	assert.Equal(t, ddl.StatementCreateTable, diff["commerce"][0].Operation)
	assert.Contains(t, diff["commerce"][0].DDL, "CREATE TABLE `orders`")
}

// A resume runs only what the plan approved. Planned tables the branch already
// has are skipped, planned tables it still lacks run with the planned DDL, a
// keyspace whose only remaining work is its planned VSchema keeps that work,
// and a branch that differs on a table the plan never changes is refused
// outright rather than "repaired" with DDL nobody reviewed.
func TestRemainingPlannedChanges(t *testing.T) {
	createOrders := engine.TableChange{Table: "orders", Operation: ddl.StatementCreateTable, DDL: resumeOrdersTable}
	createItems := engine.TableChange{Table: "items", Operation: ddl.StatementCreateTable, DDL: resumeItemsTable}
	vschemaFiles := schema.SchemaFiles{
		"commerce": {Files: map[string]string{"vschema.json": `{"sharded":true}`}},
	}

	tests := []struct {
		name        string
		planned     []engine.SchemaChange
		branchDiff  map[string][]engine.TableChange
		schemaFiles schema.SchemaFiles
		want        []engine.SchemaChange
		wantRefusal string
	}{
		{
			name: "planned tables the branch already has are skipped",
			planned: []engine.SchemaChange{{
				Namespace:    "commerce",
				TableChanges: []engine.TableChange{createOrders, createItems},
			}},
			branchDiff: map[string][]engine.TableChange{
				"commerce": {{Table: "items", Operation: ddl.StatementCreateTable, DDL: "CREATE TABLE `items` (`id` bigint)"}},
			},
			want: []engine.SchemaChange{{
				Namespace:    "commerce",
				TableChanges: []engine.TableChange{createItems},
			}},
		},
		{
			name: "a branch with every planned table has nothing left",
			planned: []engine.SchemaChange{{
				Namespace:    "commerce",
				TableChanges: []engine.TableChange{createOrders},
			}},
			branchDiff: map[string][]engine.TableChange{},
			want:       nil,
		},
		{
			name: "a planned VSchema change is kept when no DDL remains",
			planned: []engine.SchemaChange{{
				Namespace:    "commerce",
				Metadata:     map[string]string{"vschema_changed": "true"},
				TableChanges: []engine.TableChange{createOrders},
			}},
			branchDiff:  map[string][]engine.TableChange{},
			schemaFiles: vschemaFiles,
			want: []engine.SchemaChange{{
				Namespace: "commerce",
				Metadata:  map[string]string{"vschema_changed": "true"},
			}},
		},
		{
			name: "the table is read from the planned DDL when the change does not name it",
			planned: []engine.SchemaChange{{
				Namespace:    "commerce",
				TableChanges: []engine.TableChange{{DDL: "ALTER TABLE `users` ADD COLUMN `email` varchar(255)"}},
			}},
			branchDiff: map[string][]engine.TableChange{
				"commerce": {{Table: "users", Operation: ddl.StatementAlterTable, DDL: "ALTER TABLE `users` ADD COLUMN `email` varchar(255)"}},
			},
			want: []engine.SchemaChange{{
				Namespace:    "commerce",
				TableChanges: []engine.TableChange{{DDL: "ALTER TABLE `users` ADD COLUMN `email` varchar(255)"}},
			}},
		},
		{
			name: "a difference on a table the plan does not change is refused",
			planned: []engine.SchemaChange{{
				Namespace:    "commerce",
				TableChanges: []engine.TableChange{createOrders},
			}},
			branchDiff: map[string][]engine.TableChange{
				"commerce": {
					{Table: "orders", Operation: ddl.StatementCreateTable, DDL: resumeOrdersTable},
					{Table: "users", Operation: ddl.StatementCreateTable, DDL: resumeUsersTable},
				},
				"inventory": {{Table: "stock", Operation: ddl.StatementCreateTable, DDL: "CREATE TABLE `stock` (`id` bigint)"}},
			},
			wantRefusal: "commerce.users, inventory.stock",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := remainingPlannedChanges(tt.planned, tt.branchDiff, tt.schemaFiles)
			if tt.wantRefusal != "" {
				require.Error(t, err)
				var permanent *engine.PermanentError
				assert.ErrorAs(t, err, &permanent, "a branch that differs outside the plan cannot be fixed by retrying")
				assert.Contains(t, err.Error(), tt.wantRefusal)
				assert.Contains(t, err.Error(), "refusing to run DDL outside the plan")
				assert.Nil(t, got)
				return
			}
			require.NoError(t, err)
			assert.Equal(t, tt.want, got)
		})
	}
}

// readinessClient reports the branch as not ready until the test releases it,
// so a test controls exactly how long a branch takes to prepare.
type readinessClient struct {
	psclient.PSClient

	mu       sync.Mutex
	ready    bool
	getErr   error
	created  []string
	getCalls int
}

func (c *readinessClient) GetBranch(_ context.Context, req *ps.GetDatabaseBranchRequest) (*ps.DatabaseBranch, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.getCalls++
	if c.getErr != nil {
		return nil, c.getErr
	}
	return &ps.DatabaseBranch{Name: req.Branch, Ready: c.ready}, nil
}

func (c *readinessClient) CreateBranch(_ context.Context, req *ps.CreateDatabaseBranchRequest) (*ps.DatabaseBranch, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.created = append(c.created, req.Name)
	return &ps.DatabaseBranch{Name: req.Name}, nil
}

func (c *readinessClient) markReady() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.ready = true
}

func (c *readinessClient) createdBranches() []string {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]string(nil), c.created...)
}

// A branch of a large database can take longer to prepare than the driver's
// stall window. The wait reports itself on the apply's timeline while it
// continues, which is what keeps the driver from cancelling a healthy drive as
// wedged, and it still returns as soon as the branch is ready.
func TestWaitForBranchReady_ReportsTheWaitWhileTheBranchPrepares(t *testing.T) {
	shortenEngineWaits(t)
	e := New(slog.New(slog.NewTextHandler(os.Stdout, nil)))
	client := &readinessClient{}

	var heartbeats []engine.ApplyEvent
	emit := func(event engine.ApplyEvent) {
		heartbeats = append(heartbeats, event)
		if len(heartbeats) == 2 {
			client.markReady()
		}
	}

	err := e.waitForBranchReady(t.Context(), client, "org", "testdb", "schemabot-testdb-slow", emit)

	require.NoError(t, err)
	require.Len(t, heartbeats, 2, "the wait returns at the first ready poll after the branch is released")
	for _, event := range heartbeats {
		assert.Contains(t, event.Message, "Waiting for branch schemabot-testdb-slow to be ready")
		assert.Contains(t, event.Message, "elapsed")
		assert.Equal(t, "schemabot-testdb-slow", event.Metadata["branch"])
	}
}

// A wait ended by the drive's own cancellation says so, so the caller can tell
// it apart from a branch that is gone.
func TestWaitForBranchReady_CancelledDriveIsNotAMissingBranch(t *testing.T) {
	shortenEngineWaits(t)
	e := New(slog.New(slog.NewTextHandler(os.Stdout, nil)))
	ctx, cancel := context.WithCancel(t.Context())
	cancel()

	err := e.waitForBranchReady(ctx, &readinessClient{}, "org", "testdb", "schemabot-testdb-slow", nil)

	require.Error(t, err)
	assert.ErrorIs(t, err, context.Canceled)
	assert.False(t, isNotFound(err))
}

// A drive cancelled while its resume waits on the branch leaves the branch and
// the stored resume state alone. Only the next driver, resuming against the
// same branch, picks the work back up — a cancelled wait never forks a second
// branch beside the first.
func TestResumeApply_CancelledBranchWaitDoesNotStartFresh(t *testing.T) {
	shortenEngineWaits(t)
	e := New(slog.New(slog.NewTextHandler(os.Stdout, nil)))
	client := &readinessClient{}
	req := resumeRequest(t, &psMetadata{BranchName: "schemabot-testdb-slow"}, "apply-1a2b3c4d5e6f7890")
	resumeState := req.ResumeState

	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	_, err := e.resumeApply(ctx, client, "org", req)

	require.Error(t, err)
	assert.ErrorIs(t, err, context.Canceled)
	assert.Empty(t, client.createdBranches(), "a cancelled wait must not create a new branch")
	assert.Same(t, resumeState, req.ResumeState, "the stored resume state must survive for the next driver")
}

// A resume whose branch is gone — deleted between the crash and the recovery —
// starts the apply fresh. The fresh Apply here fails fast building its
// PlanetScale client from the request's incomplete credentials, which proves the not-found took the start-fresh branch
// (clearing ResumeState) rather than failing the resume.
func TestResumeApply_DeletedBranchStartsFresh(t *testing.T) {
	shortenEngineWaits(t)
	e := New(slog.New(slog.NewTextHandler(os.Stdout, nil)))
	client := &readinessClient{getErr: &ps.Error{Code: ps.ErrNotFound}}
	req := resumeRequest(t, &psMetadata{BranchName: "schemabot-testdb-gone"}, "apply-1a2b3c4d5e6f7890")

	_, err := e.resumeApply(t.Context(), client, "org", req)

	require.Error(t, err)
	assert.Contains(t, err.Error(), "get planetscale client")
	assert.Nil(t, req.ResumeState)
}

// pendingThenReadyClient keeps a deploy request pending until the test
// releases it.
type pendingThenReadyClient struct {
	psclient.PSClient

	mu    sync.Mutex
	ready bool
}

func (c *pendingThenReadyClient) GetDeployRequest(_ context.Context, req *ps.GetDeployRequestRequest) (*ps.DeployRequest, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.ready {
		return &ps.DeployRequest{Number: req.Number, DeploymentState: deployState.Ready}, nil
	}
	return &ps.DeployRequest{Number: req.Number, DeploymentState: deployState.Pending}, nil
}

func (c *pendingThenReadyClient) markReady() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.ready = true
}

// PlanetScale can take minutes to compute a large deploy request's schema
// diff. The wait reports itself on the apply's timeline while it continues.
func TestWaitForDeployRequestPending_ReportsTheWait(t *testing.T) {
	shortenEngineWaits(t)
	e := New(slog.New(slog.NewTextHandler(os.Stdout, nil)))
	client := &pendingThenReadyClient{}

	var heartbeats []engine.ApplyEvent
	emit := func(event engine.ApplyEvent) {
		heartbeats = append(heartbeats, event)
		client.markReady()
	}

	dr, err := e.waitForDeployRequestPending(t.Context(), client, "org", "testdb",
		&ps.DeployRequest{Number: 21, DeploymentState: deployState.Pending}, emit)

	require.NoError(t, err)
	assert.Equal(t, deployState.Ready, dr.DeploymentState)
	require.Len(t, heartbeats, 1)
	assert.Contains(t, heartbeats[0].Message, "Waiting for PlanetScale to compute the schema diff of deploy request #21")
}

// A deploy request PlanetScale never finishes diffing fails the wait once its
// bound runs out, rather than holding the drive forever.
func TestWaitForDeployRequestPending_GivesUpAfterItsBound(t *testing.T) {
	orig := deployRequestPendingWait
	deployRequestPendingWait = 10 * time.Millisecond
	t.Cleanup(func() { deployRequestPendingWait = orig })
	e := New(slog.New(slog.NewTextHandler(os.Stdout, nil)))

	_, err := e.waitForDeployRequestPending(t.Context(), &pendingThenReadyClient{}, "org", "testdb",
		&ps.DeployRequest{Number: 22, DeploymentState: deployState.Pending}, nil)

	require.Error(t, err)
	assert.Contains(t, err.Error(), "deploy request #22 was still pending after 10ms")
}

// snapshotThenAcceptClient rejects VSchema writes with PlanetScale's
// snapshot-in-progress error a set number of times, then accepts them.
type snapshotThenAcceptClient struct {
	psclient.PSClient

	rejections int
	calls      int
}

func (c *snapshotThenAcceptClient) UpdateKeyspaceVSchema(context.Context, *ps.UpdateKeyspaceVSchemaRequest) (*ps.VSchema, error) {
	c.calls++
	if c.calls <= c.rejections {
		return nil, errors.New("Cannot update VSchema while a schema snapshot is in progress")
	}
	return &ps.VSchema{}, nil
}

// shortenKeyspaceRetries makes keyspace retries immediate and bounds the
// snapshot retry window to the given duration.
func shortenKeyspaceRetries(t *testing.T, delay, snapshotWait time.Duration) {
	t.Helper()
	origDelay, origWait := retryDelay, snapshotRetryWait
	retryDelay = func(int, error) time.Duration { return delay }
	snapshotRetryWait = snapshotWait
	t.Cleanup(func() { retryDelay, snapshotRetryWait = origDelay, origWait })
}

func applySnapshotDeferredKeyspace(t *testing.T, client *snapshotThenAcceptClient) ([]engine.ApplyEvent, error) {
	t.Helper()
	e := New(slog.New(slog.NewTextHandler(os.Stdout, nil)))
	var events []engine.ApplyEvent
	err := e.applyKeyspaceChanges(t.Context(),
		engine.SchemaChange{Namespace: "commerce", Metadata: map[string]string{"vschema_changed": "true"}},
		schema.SchemaFiles{"commerce": {Files: map[string]string{"vschema.json": "{}"}}},
		&ps.DatabaseBranchPassword{}, client, "org", "testdb", "schemabot-testdb-snap",
		func(event engine.ApplyEvent) { events = append(events, event) },
	)
	return events, err
}

// A VSchema write deferred by a schema snapshot keeps being retried for as long
// as the snapshot window allows, well past the attempt count a transient error
// gets, and each retry is on the apply's timeline with a fixed reason rather
// than the raw error.
func TestApplyKeyspaceChanges_SnapshotDeferralIsRetriedAndReported(t *testing.T) {
	shortenKeyspaceRetries(t, time.Millisecond, time.Minute)
	client := &snapshotThenAcceptClient{rejections: 3 * maxRetries}

	events, err := applySnapshotDeferredKeyspace(t, client)

	require.NoError(t, err)
	assert.Equal(t, 3*maxRetries+1, client.calls)
	require.Len(t, events, 3*maxRetries)
	assert.Contains(t, events[0].Message, "Retrying keyspace commerce on branch schemabot-testdb-snap")
	assert.Contains(t, events[0].Message, "(attempt 2)")
	assert.Contains(t, events[0].Message, "PlanetScale is taking a schema snapshot of the branch, retrying for up to 1m0s")
	assert.NotContains(t, events[0].Message, "Cannot update VSchema")
}

// A snapshot that outlasts the retry window fails the keyspace with an error
// that says PlanetScale was still snapshotting the branch, so the failure reads
// as PlanetScale's wait rather than as a broken change.
func TestApplyKeyspaceChanges_SnapshotDeferralGivesUpAfterItsWindow(t *testing.T) {
	shortenKeyspaceRetries(t, 5*time.Millisecond, 50*time.Millisecond)
	client := &snapshotThenAcceptClient{rejections: 1 << 20}

	_, err := applySnapshotDeferredKeyspace(t, client)

	require.Error(t, err)
	assert.Contains(t, err.Error(), "PlanetScale was still taking a schema snapshot of branch schemabot-testdb-snap after 50ms")
	assert.Contains(t, err.Error(), "Cannot update VSchema while a schema snapshot is in progress")
	assert.Greater(t, client.calls, maxRetries, "the snapshot window must allow more attempts than a transient error gets")
	assert.Less(t, client.calls, 1<<20)
}
