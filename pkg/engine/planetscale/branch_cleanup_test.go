package planetscale

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"os"
	"sync"
	"testing"
	"time"

	ps "github.com/planetscale/planetscale-go/planetscale"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/block/schemabot/pkg/engine"
	"github.com/block/schemabot/pkg/psclient"
)

// branchLifecycleClient serves a branch that exists and is ready, and fails the
// credential request that follows it — the shape of an apply that dies while
// preparing its branch, before any deploy request exists. Deletions are
// recorded so the test can assert on the cleanup. Every delete the engine
// issues is recorded as attempted; one whose context has already ended then
// fails without being recorded as deleted, as a real API request would.
type branchLifecycleClient struct {
	psclient.PSClient

	mu        sync.Mutex
	created   []string
	attempted []string
	deleted   []string
}

func (c *branchLifecycleClient) GetBranch(context.Context, *ps.GetDatabaseBranchRequest) (*ps.DatabaseBranch, error) {
	return &ps.DatabaseBranch{Ready: true, SafeMigrations: true}, nil
}

func (c *branchLifecycleClient) CreateBranch(_ context.Context, req *ps.CreateDatabaseBranchRequest) (*ps.DatabaseBranch, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.created = append(c.created, req.Name)
	return &ps.DatabaseBranch{Name: req.Name, Ready: true}, nil
}

func (c *branchLifecycleClient) DeleteBranch(ctx context.Context, req *ps.DeleteDatabaseBranchRequest) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.attempted = append(c.attempted, req.Branch)
	if err := ctx.Err(); err != nil {
		return err
	}
	c.deleted = append(c.deleted, req.Branch)
	return nil
}

func (c *branchLifecycleClient) deleteAttempts() []string {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]string(nil), c.attempted...)
}

// captureApplyLogs points req's apply logger at a JSON buffer carrying the
// apply id, as the driver scopes the logger to the apply it drives, and
// returns a lookup for the record with a given message. The lookup is called
// only after the drive has returned.
func captureApplyLogs(t *testing.T, req *engine.ApplyRequest) func(msg string) map[string]any {
	t.Helper()
	var buf bytes.Buffer
	req.Logger = slog.New(slog.NewJSONHandler(&buf, nil)).With("apply_id", "apply-1a2b3c4d5e6f7890")
	return func(msg string) map[string]any {
		t.Helper()
		for line := range bytes.SplitSeq(buf.Bytes(), []byte("\n")) {
			if len(line) == 0 {
				continue
			}
			var record map[string]any
			require.NoError(t, json.Unmarshal(line, &record), "log line: %s", line)
			if record["msg"] == msg {
				return record
			}
		}
		return nil
	}
}

func (c *branchLifecycleClient) RefreshSchema(context.Context, string, string, string) error {
	return nil
}

func (c *branchLifecycleClient) CreateBranchPassword(context.Context, *ps.DatabaseBranchPasswordRequest) (*ps.DatabaseBranchPassword, error) {
	return nil, errors.New("branch credentials unavailable")
}

func (c *branchLifecycleClient) snapshot() (created, deleted []string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]string(nil), c.created...), append([]string(nil), c.deleted...)
}

// A branch SchemaBot creates is stranded if the apply fails before a deploy
// request exists to own its teardown, and PlanetScale branches are quota'd —
// the strand eventually blocks unrelated schema changes. The apply must clean up
// after itself, while leaving an operator-supplied branch alone: SchemaBot did
// not create it and must not remove it.
func TestApplyDeletesItsOwnBranchWhenItFailsBeforeTheDeployRequest(t *testing.T) {
	applyRequest := func(options map[string]string) *engine.ApplyRequest {
		return &engine.ApplyRequest{
			PlanID:      "plan-0123456789abcdef",
			Database:    "commerce",
			Credentials: conformanceCredentials(),
			Options:     options,
		}
	}

	t.Run("a branch this apply created is deleted", func(t *testing.T) {
		client := &branchLifecycleClient{}
		_, err := conformanceEngine(client).Apply(t.Context(), applyRequest(nil))
		require.Error(t, err)

		created, deleted := client.snapshot()
		require.Len(t, created, 1)
		assert.Equal(t, created, deleted, "the apply deletes exactly the branch it created")
	})

	t.Run("an operator-supplied branch is left alone", func(t *testing.T) {
		client := &branchLifecycleClient{}
		_, err := conformanceEngine(client).Apply(t.Context(), applyRequest(map[string]string{"branch": "operator-branch"}))
		require.Error(t, err)

		created, deleted := client.snapshot()
		assert.Empty(t, created)
		assert.Empty(t, deleted)
	})
}

// handbackClient ends the drive while the branch is being prepared, the way
// the operator cancels a drive whose lease was lost, that looked stalled, or
// whose instance is shutting down.
type handbackClient struct {
	branchLifecycleClient
	cancel context.CancelCauseFunc
	cause  error
}

func (c *handbackClient) CreateBranchPassword(ctx context.Context, _ *ps.DatabaseBranchPasswordRequest) (*ps.DatabaseBranchPassword, error) {
	c.cancel(c.cause)
	return nil, ctx.Err()
}

// A drive that ends while its branch is being prepared hands the apply to
// another driver, which resumes from the branch the stored resume state names.
// Deleting that branch would discard the resume's starting point, and after a
// lost lease would pull it from under a peer already preparing it. A branch
// the stored state never named, because no state was recorded or because
// storage refused the save, cannot be resumed, so it is still deleted, and so
// is the branch of a drive an operator cancelled, since no driver resumes a
// cancelled apply. Either way the decision is logged on the apply's logger, so
// an operator filtering on the apply id sees what became of its branch.
func TestApplyKeepsItsBranchForTheDriverThatResumesIt(t *testing.T) {
	shortenEngineWaits(t)
	tests := []struct {
		name        string
		recorded    bool
		saveErr     error
		cause       error
		wantDeleted bool
	}{
		{name: "a branch the stored resume state names is kept", recorded: true, wantDeleted: false},
		{name: "a branch the stored resume state never named is deleted", recorded: false, wantDeleted: true},
		{name: "a branch whose resume state storage refused is deleted", recorded: true, saveErr: errors.New("apply apply-0123456789abcdef: storage unavailable"), wantDeleted: true},
		{name: "the stored branch of an apply an operator cancelled is deleted", recorded: true, cause: engine.ErrCancelledByOperator, wantDeleted: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx, cancel := context.WithCancelCause(t.Context())
			defer cancel(nil)
			client := &handbackClient{cancel: cancel, cause: tt.cause}
			var stored []*engine.ResumeState
			req := &engine.ApplyRequest{
				PlanID:      "plan-0123456789abcdef",
				Database:    "commerce",
				Credentials: conformanceCredentials(),
			}
			if tt.recorded {
				req.ResumeState = &engine.ResumeState{MigrationContext: "apply-0123456789abcdef"}
				req.OnStateChange = func(rs *engine.ResumeState) error {
					if tt.saveErr != nil {
						return tt.saveErr
					}
					stored = append(stored, rs)
					return nil
				}
			}

			logRecord := captureApplyLogs(t, req)

			_, err := conformanceEngine(client).Apply(ctx, req)
			require.ErrorIs(t, err, context.Canceled)

			created, deleted := client.snapshot()
			require.Len(t, created, 1)
			if tt.wantDeleted {
				assert.Equal(t, created, deleted)
				record := logRecord("deleted the branch created for an apply that failed before its deploy request")
				require.NotNil(t, record, "the delete is logged on the apply's logger")
				assert.Equal(t, "apply-1a2b3c4d5e6f7890", record["apply_id"])
				assert.Equal(t, created[0], record["branch"])
				return
			}
			assert.Empty(t, deleted, "the branch the resume will look for must survive the handback")
			record := logRecord("drive ended before the deploy request was created; keeping the branch for the driver that resumes the apply")
			require.NotNil(t, record, "the kept branch is logged on the apply's logger")
			assert.Equal(t, "apply-1a2b3c4d5e6f7890", record["apply_id"])
			assert.Equal(t, "commerce", record["planetscale_database"])
			assert.Equal(t, created[0], record["branch"])
			require.NotEmpty(t, stored)
			meta, decodeErr := decodePSMetadata(stored[len(stored)-1].Metadata)
			require.NoError(t, decodeErr)
			assert.Equal(t, created[0], meta.BranchName, "the kept branch is the one the stored state names")
		})
	}
}

// branchHandoverClient carries a fresh apply through branch preparation to the
// deploy request: credentials are issued, the request has no changes so no
// MySQL connection is opened, and the deploy request it creates reports no
// changes, so the apply returns right after creating it. The create request is
// recorded so the test can read the teardown decision the apply handed over.
type branchHandoverClient struct {
	branchLifecycleClient

	lastCreate  *ps.CreateDeployRequestRequest
	passwordTTL int
}

func (c *branchHandoverClient) CreateBranchPassword(_ context.Context, req *ps.DatabaseBranchPasswordRequest) (*ps.DatabaseBranchPassword, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.passwordTTL = req.TTL
	return &ps.DatabaseBranchPassword{}, nil
}

func (c *branchHandoverClient) CreateDeployRequest(_ context.Context, req *ps.CreateDeployRequestRequest) (*ps.DeployRequest, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.lastCreate = req
	return &ps.DeployRequest{Number: 91, DeploymentState: deployState.NoChanges}, nil
}

// Once the deploy request exists it owns the branch's teardown, and the apply
// tells it which way to go: a branch SchemaBot created is deleted when the
// deploy request deploys, while an operator-supplied branch is the operator's
// and outlives the deploy. A resumed drive that creates the deploy request
// later makes the same decision from the same stored options, so the fresh
// drive's answer is the one the resume has to match. The deploy request here
// reports no changes, and either way the returned resume state names it, so
// the drive's next progress poll settles the apply as completed.
func TestApplyHandsOnlyItsOwnBranchToTheDeployRequest(t *testing.T) {
	applyRequest := func(options map[string]string) *engine.ApplyRequest {
		return &engine.ApplyRequest{
			PlanID:      "plan-0123456789abcdef",
			Database:    "commerce",
			Credentials: conformanceCredentials(),
			Options:     options,
		}
	}

	t.Run("a branch this apply created is deleted by the deploy request", func(t *testing.T) {
		client := &branchHandoverClient{}
		result, err := conformanceEngine(client).Apply(t.Context(), applyRequest(nil))
		require.NoError(t, err)
		assert.True(t, result.Accepted)

		created, deleted := client.snapshot()
		require.Len(t, created, 1)
		assert.Empty(t, deleted, "the deploy request owns the teardown; the apply does not delete the branch itself")
		require.NotNil(t, client.lastCreate)
		assert.Equal(t, created[0], client.lastCreate.Branch)
		assert.True(t, client.lastCreate.AutoDeleteBranch)
		assert.Equal(t, branchPasswordTTL(0), client.passwordTTL, "the branch password is sized by the keyspaces the apply can touch")
		assertResumeStateNamesNoChangesDeployRequest(t, result, created[0])
	})

	t.Run("an operator-supplied branch outlives the deploy request", func(t *testing.T) {
		client := &branchHandoverClient{}
		result, err := conformanceEngine(client).Apply(t.Context(), applyRequest(map[string]string{"branch": "operator-branch"}))
		require.NoError(t, err)
		assert.True(t, result.Accepted)

		created, deleted := client.snapshot()
		assert.Empty(t, created)
		assert.Empty(t, deleted)
		require.NotNil(t, client.lastCreate)
		assert.Equal(t, "operator-branch", client.lastCreate.Branch)
		assert.False(t, client.lastCreate.AutoDeleteBranch)
		assertResumeStateNamesNoChangesDeployRequest(t, result, "operator-branch")
	})
}

// assertResumeStateNamesNoChangesDeployRequest checks that the fresh apply's
// no-changes result names deploy request #91 and its branch, so the drive's
// next progress poll reads that deploy request and settles the apply.
func assertResumeStateNamesNoChangesDeployRequest(t *testing.T, result *engine.ApplyResult, branch string) {
	t.Helper()
	require.NotNil(t, result.ResumeState, "the drive needs a resume state to follow the deploy request")
	stored, err := decodePSMetadata(result.ResumeState.Metadata)
	require.NoError(t, err)
	assert.Equal(t, uint64(91), stored.DeployRequestID)
	assert.Equal(t, branch, stored.BranchName)
	assert.Equal(t, engine.StateCompleted, noChangesProgressState(t, result.ResumeState),
		"the next progress poll settles the converged apply as completed")
}

// undeployedDeployRequestClient carries a fresh apply to its deploy request and
// fails the apply there. createdDR is what the create returns, deployErr (when
// set) rejects the deploy, reread is what every later read of the deploy
// request returns, cleanupReread (when set) is what the cleanup's read returns
// instead, and closed (when set) is what the close returns; without it the
// close returns reread marked closed. A rejected deploy re-reads the deploy
// request to check whether the deploy started, so the cleanup's read is the
// second. readErr, closeErr and deleteErr fail the read, the close and the
// branch delete; closeNil makes the close return no deploy request. endDrive
// ends the drive's context at the step endDriveAt names: the deploy, the
// cleanup's read (which then fails with the ended context's error, as a real
// request would), the cleanup's read whose response still lands, or the
// cleanup's close. Reads and closes are recorded.
type undeployedDeployRequestClient struct {
	branchHandoverClient

	createdDR     *ps.DeployRequest
	reread        *ps.DeployRequest
	cleanupReread *ps.DeployRequest
	closed        *ps.DeployRequest
	deployErr     error
	readErr       error
	closeErr      error
	closeNil      bool
	deleteErr     error
	endDrive      context.CancelFunc
	endDriveAt    string
	reads         int
	closes        []uint64
}

// cleanupRead is the read of the deploy request the failed-apply cleanup
// makes, after the one a rejected deploy makes to check whether it started.
const cleanupRead = 2

const (
	endDriveAtDeploy      = "deploy"
	endDriveAtReread      = "reread"
	endDriveAtRereadLands = "reread lands"
	endDriveAtClose       = "close"
)

func (c *undeployedDeployRequestClient) CreateDeployRequest(_ context.Context, req *ps.CreateDeployRequestRequest) (*ps.DeployRequest, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.lastCreate = req
	created := *c.createdDR
	return &created, nil
}

func (c *undeployedDeployRequestClient) GetDeployRequest(ctx context.Context, _ *ps.GetDeployRequestRequest) (*ps.DeployRequest, error) {
	c.mu.Lock()
	c.reads++
	read := c.reads
	c.mu.Unlock()
	if read == cleanupRead {
		switch c.endDriveAt {
		case endDriveAtReread:
			c.endDrive()
			if err := ctx.Err(); err != nil {
				return nil, err
			}
		case endDriveAtRereadLands:
			c.endDrive()
		}
	}
	if c.readErr != nil {
		return nil, c.readErr
	}
	reread := *c.reread
	if read == cleanupRead && c.cleanupReread != nil {
		reread = *c.cleanupReread
	}
	return &reread, nil
}

func (c *undeployedDeployRequestClient) DeleteBranch(ctx context.Context, req *ps.DeleteDatabaseBranchRequest) error {
	if c.deleteErr != nil {
		c.mu.Lock()
		defer c.mu.Unlock()
		c.attempted = append(c.attempted, req.Branch)
		return c.deleteErr
	}
	return c.branchHandoverClient.DeleteBranch(ctx, req)
}

func (c *undeployedDeployRequestClient) readCount() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.reads
}

func (c *undeployedDeployRequestClient) DeployDeployRequest(ctx context.Context, _ *ps.PerformDeployRequest) (*ps.DeployRequest, error) {
	if c.endDriveAt == endDriveAtDeploy {
		c.endDrive()
		return nil, ctx.Err()
	}
	return nil, c.deployErr
}

// CloseDeployRequest lands the close before ending the drive at endDriveAtClose,
// so the response the cleanup reads is a successful one.
func (c *undeployedDeployRequestClient) CloseDeployRequest(_ context.Context, req *ps.CloseDeployRequestRequest) (*ps.DeployRequest, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.closes = append(c.closes, req.Number)
	if c.closeErr != nil {
		return nil, c.closeErr
	}
	if c.closeNil {
		return nil, nil
	}
	closed := *c.reread
	if c.closed != nil {
		closed = *c.closed
	}
	closed.State = deployRequestClosed
	if c.endDriveAt == endDriveAtClose {
		c.endDrive()
	}
	return &closed, nil
}

// An apply that fails after creating its deploy request but before the deploy
// begins settles failed, yet the deploy request it left would stay open and
// deployable from the PlanetScale UI, and its auto_delete_branch only fires on
// a deploy, so the branch would hold quota forever. The failed apply closes the
// deploy request and deletes the branch it created. A deploy request that
// reports a deploy may have begun is never closed, one whose close response
// reports a deploy began between the read and the close keeps its branch, an
// operator-supplied branch is never deleted, and a drive whose context ended,
// before or during the cleanup, leaves what remains to whoever takes the apply
// over, and sends no request once it has ended. A deploy request already
// closed is not closed again, and a read, close or delete that fails stops the
// cleanup at that step. Each outcome is logged on the apply's logger, and a
// deploy that may be running names the reconcile step for the operator.
func TestApplyClosesTheUndeployedDeployRequestOfAFailedApply(t *testing.T) {
	shortenEngineWaits(t)
	deployedAt := time.Date(2026, 10, 15, 9, 30, 0, 0, time.UTC)
	refused := &ps.DeployRequest{Number: 91, State: "open", DeploymentState: deployState.Error}
	refusedDeployed := &ps.DeployRequest{Number: 91, State: "open", DeploymentState: deployState.Error, DeployedAt: &deployedAt}
	ready := &ps.DeployRequest{Number: 91, State: "open", DeploymentState: deployState.Ready}
	readyClosed := &ps.DeployRequest{Number: 91, State: deployRequestClosed, DeploymentState: deployState.Ready}
	queued := &ps.DeployRequest{Number: 91, State: "open", DeploymentState: deployState.Queued, DeployedAt: &deployedAt}
	queuedNotDeployed := &ps.DeployRequest{Number: 91, State: "open", DeploymentState: deployState.Queued}
	readyDeployed := &ps.DeployRequest{Number: 91, State: "open", DeploymentState: deployState.Ready, DeployedAt: &deployedAt}
	rejected := &ps.Error{Code: ps.ErrInvalid}
	unanswered := errors.New("connection reset by peer")
	const (
		deployMayRunLog = "the deploy request of a failed apply reports a deploy may have begun; the apply is being failed over a deploy that may now be running, so the deploy request and its branch are left in place; " + reconcileDeployOfFailedApply
		closeReportsLog = "closing the deploy request of a failed apply reported a deploy began after the cleanup read it; the apply is being failed over a deploy that may now be running, so its branch is left in place; " + reconcileDeployOfFailedApply
		readEndedLog    = "drive ended while reading the deploy request; stopping the cleanup of a failed apply's deploy request and leaving the deploy request and branch to the driver that takes over the apply"
		deletedLog      = "deleted the branch of a failed apply after closing its undeployed deploy request"
		nilCloseLog     = "closing the undeployed deploy request of a failed apply returned no deploy request; check it is closed, and delete its branch to reclaim branch quota"
	)

	tests := []struct {
		name      string
		options   map[string]string
		createdDR *ps.DeployRequest
		reread    *ps.DeployRequest
		// cleanupReread is what the cleanup's read returns when it differs
		// from the read the rejected deploy made.
		cleanupReread *ps.DeployRequest
		closed        *ps.DeployRequest
		deployErr     error
		readErr       error
		closeErr      error
		closeNil      bool
		deleteErr     error
		endDriveAt    string
		wantCloses    []uint64
		// wantDelete is "" when no delete is sent, "deleted" when the branch
		// is deleted, and "failed" when the delete is sent and fails.
		wantDelete string
		// wantNoReads asserts no request read the deploy request: the drive
		// ended before the cleanup could start.
		wantNoReads bool
		wantLog     string
		wantLevel   string
	}{
		{name: "a deploy request PlanetScale refused is closed and its branch deleted",
			createdDR: refused, reread: refused, wantCloses: []uint64{91}, wantDelete: "deleted",
			wantLog: deletedLog, wantLevel: "INFO"},
		{name: "a rejected deploy that never began is closed and its branch deleted",
			createdDR: ready, reread: ready, deployErr: rejected,
			wantCloses: []uint64{91}, wantDelete: "deleted", wantLog: deletedLog, wantLevel: "INFO"},
		{name: "a deploy request that reports a deploy began by the cleanup's read is left with its branch",
			createdDR: ready, reread: ready, cleanupReread: queued, deployErr: rejected, wantLog: deployMayRunLog, wantLevel: "ERROR"},
		{name: "a deploy whose outcome is unknown leaves the deploy request and branch for recovery",
			createdDR: ready, reread: ready, deployErr: unanswered,
			wantLog: deployOutcomeUnknownKeepsDeployRequestLog, wantLevel: "WARN"},
		{name: "a refused deploy request that carries a deploy timestamp is left with its branch",
			createdDR: refused, reread: refusedDeployed, wantLog: deployMayRunLog, wantLevel: "ERROR"},
		{name: "a close response showing the deploy queued keeps the branch",
			createdDR: ready, reread: ready, closed: queuedNotDeployed, deployErr: rejected, wantCloses: []uint64{91},
			wantLog: closeReportsLog, wantLevel: "ERROR"},
		{name: "a close response showing a deploy timestamp keeps the branch",
			createdDR: ready, reread: ready, closed: readyDeployed, deployErr: rejected, wantCloses: []uint64{91},
			wantLog: closeReportsLog, wantLevel: "ERROR"},
		{name: "an operator-supplied branch is kept after its deploy request is closed",
			options: map[string]string{"branch": "operator-branch"}, createdDR: refused, reread: refused, wantCloses: []uint64{91},
			wantLog: "the failed apply ran on an operator-supplied branch; keeping it for the operator", wantLevel: "INFO"},
		{name: "an already closed deploy request is not closed again and its branch is deleted",
			createdDR: ready, reread: readyClosed, deployErr: rejected, wantDelete: "deleted",
			wantLog: "the deploy request of a failed apply was already closed before it deployed", wantLevel: "INFO"},
		{name: "a failed read closes nothing and keeps the branch",
			createdDR: ready, reread: ready, deployErr: rejected, readErr: errors.New("planetscale unavailable"),
			wantLog: "failed to read the deploy request of a failed apply; close it manually if it never deployed, and delete its branch to reclaim branch quota", wantLevel: "ERROR"},
		{name: "a failed close keeps the branch",
			createdDR: ready, reread: ready, deployErr: rejected, closeErr: errors.New("planetscale unavailable"), wantCloses: []uint64{91},
			wantLog: "failed to close the undeployed deploy request of a failed apply; close it manually, and delete its branch to reclaim branch quota", wantLevel: "ERROR"},
		{name: "a close that returns no deploy request keeps the branch",
			createdDR: ready, reread: ready, deployErr: rejected, closeNil: true, wantCloses: []uint64{91},
			wantLog: nilCloseLog, wantLevel: "ERROR"},
		{name: "a failed branch delete is logged for manual cleanup",
			createdDR: ready, reread: ready, deployErr: rejected, deleteErr: errors.New("branch delete refused"), wantCloses: []uint64{91}, wantDelete: "failed",
			wantLog: "failed to delete the branch of a failed apply after closing its deploy request; delete it manually to reclaim branch quota", wantLevel: "ERROR"},
		{name: "a drive that ends after the deploy request leaves both for the driver that takes over",
			createdDR: ready, reread: ready, endDriveAt: endDriveAtDeploy, wantNoReads: true,
			wantLog: "drive ended after the deploy request was created; leaving the deploy request to the driver or control operation that takes over the apply", wantLevel: "INFO"},
		{name: "a drive that ends while the cleanup reads the deploy request closes nothing",
			createdDR: ready, reread: ready, deployErr: rejected, endDriveAt: endDriveAtReread,
			wantLog: readEndedLog, wantLevel: "WARN"},
		{name: "a read that lands as the drive ends closes nothing",
			createdDR: ready, reread: ready, deployErr: rejected, endDriveAt: endDriveAtRereadLands,
			wantLog: readEndedLog, wantLevel: "WARN"},
		{name: "a drive that ends while the cleanup closes the deploy request keeps the branch",
			createdDR: ready, reread: ready, deployErr: rejected, endDriveAt: endDriveAtClose, wantCloses: []uint64{91},
			wantLog: "drive ended while closing the deploy request of a failed apply; the close may already have landed; leaving the branch to the driver that takes over the apply", wantLevel: "WARN"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			client := &undeployedDeployRequestClient{
				createdDR:     tt.createdDR,
				reread:        tt.reread,
				cleanupReread: tt.cleanupReread,
				closed:        tt.closed,
				deployErr:     tt.deployErr,
				readErr:       tt.readErr,
				closeErr:      tt.closeErr,
				closeNil:      tt.closeNil,
				deleteErr:     tt.deleteErr,
				endDrive:      cancel,
				endDriveAt:    tt.endDriveAt,
			}
			req := &engine.ApplyRequest{
				PlanID:      "plan-0123456789abcdef",
				Database:    "commerce",
				Credentials: conformanceCredentials(),
				Options:     tt.options,
			}
			logRecord := captureApplyLogs(t, req)

			_, err := conformanceEngine(client).Apply(ctx, req)
			require.Error(t, err)

			created, deleted := client.snapshot()
			assert.Equal(t, tt.wantCloses, client.closes)
			if tt.wantNoReads {
				assert.Zero(t, client.readCount(), "no request reaches PlanetScale once the drive has ended")
			}
			switch tt.wantDelete {
			case "deleted":
				require.Len(t, created, 1)
				assert.Equal(t, created, client.deleteAttempts(), "exactly one delete, of the branch the apply created")
				assert.Equal(t, created, deleted)
			case "failed":
				require.Len(t, created, 1)
				assert.Equal(t, created, client.deleteAttempts(), "exactly one delete, of the branch the apply created")
				assert.Empty(t, deleted)
			default:
				assert.Empty(t, client.deleteAttempts())
			}
			record := logRecord(tt.wantLog)
			require.NotNil(t, record, "the outcome is logged on the apply's logger")
			assert.Equal(t, tt.wantLevel, record["level"])
			assert.Equal(t, "apply-1a2b3c4d5e6f7890", record["apply_id"])
			assert.Equal(t, float64(91), record["deploy_request"])
		})
	}
}

// resumeReclaimClient serves a resumed branch that is ready and issues its
// credentials, so a resume runs on to its diff. Deletions are recorded, and
// deleteErr, when set, fails each one after recording it.
type resumeReclaimClient struct {
	branchLifecycleClient
	deleteErr error
}

func (c *resumeReclaimClient) CreateBranchPassword(context.Context, *ps.DatabaseBranchPasswordRequest) (*ps.DatabaseBranchPassword, error) {
	return &ps.DatabaseBranchPassword{}, nil
}

func (c *resumeReclaimClient) DeleteBranch(ctx context.Context, req *ps.DeleteDatabaseBranchRequest) error {
	if err := c.branchLifecycleClient.DeleteBranch(ctx, req); err != nil {
		return err
	}
	return c.deleteErr
}

// A resumed drive that fails before it creates the deploy request leaves
// nothing else to tear the branch down, and the failure ends the apply
// whether or not it is classified as permanent, so the drive deletes a branch
// SchemaBot created rather than strand it against quota. It keeps the branch
// only when the branch is not SchemaBot's to delete or another drive may still
// need it: an operator-supplied branch belongs to the operator, and a drive
// whose context ended is handing the apply to the driver that resumes it from
// that branch, unless an operator cancelled the apply, which no driver
// resumes. A kept branch is never sent a delete, and each outcome is
// logged on the apply's logger with the reason.
func TestReclaimBranchAfterFailedResume(t *testing.T) {
	const ownedBranch = "schemabot-commerce-1a2b"
	permanent := engine.NewPermanentError("branch differs from the declared schema on tables the plan does not change (commerce.legacy_audit)")
	unclassified := errors.New("fetch branch schemabot-commerce-1a2b schema via MySQL on resume: connection refused")
	tests := []struct {
		name       string
		branch     string
		options    map[string]string
		cause      error
		driveEnded bool
		// operatorCancelled ends the drive with the operator's cancel cause.
		operatorCancelled bool
		wantDeleted       []string
		wantLog           string
	}{
		{
			name: "a permanent failure deletes the branch SchemaBot created", branch: ownedBranch, cause: permanent,
			wantDeleted: []string{ownedBranch},
			wantLog:     "deleted the branch created for an apply that failed before its deploy request",
		},
		{
			name: "an unclassified failure deletes the branch SchemaBot created", branch: ownedBranch, cause: unclassified,
			wantDeleted: []string{ownedBranch},
			wantLog:     "deleted the branch created for an apply that failed before its deploy request",
		},
		{
			name: "an operator-supplied branch is never deleted", branch: "my-dev-branch", options: map[string]string{"branch": "my-dev-branch"}, cause: permanent,
			wantLog: "resumed apply failed before its deploy request; keeping the operator-supplied branch",
		},
		{
			name: "a drive that ended keeps the branch for the driver that resumes it", branch: ownedBranch, cause: permanent, driveEnded: true,
			wantLog: "resumed drive ended before its deploy request; keeping the branch for the driver that resumes the apply",
		},
		{
			name: "a drive an operator cancelled deletes the branch no driver will resume", branch: ownedBranch, cause: permanent, operatorCancelled: true,
			wantDeleted: []string{ownedBranch},
			wantLog:     "deleted the branch created for an apply that failed before its deploy request",
		},
		{
			name: "an operator cancel still keeps an operator-supplied branch", branch: "my-dev-branch", options: map[string]string{"branch": "my-dev-branch"}, cause: permanent, operatorCancelled: true,
			wantLog: "resumed apply failed before its deploy request; keeping the operator-supplied branch",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx, cancel := context.WithCancelCause(t.Context())
			defer cancel(nil)
			if tt.driveEnded {
				cancel(nil)
			}
			if tt.operatorCancelled {
				cancel(engine.ErrCancelledByOperator)
			}
			client := &resumeReclaimClient{}
			req := resumeRequest(t, &psMetadata{BranchName: tt.branch}, "apply-1a2b3c4d5e6f7890")
			req.Options = tt.options
			logRecord := captureApplyLogs(t, req)

			conformanceEngine(client).reclaimBranchAfterFailedResume(ctx, client, "org", req, tt.branch, tt.cause)

			_, deleted := client.snapshot()
			assert.Equal(t, tt.wantDeleted, deleted)
			assert.Equal(t, tt.wantDeleted, client.deleteAttempts(), "a kept branch is never sent a delete")
			record := logRecord(tt.wantLog)
			require.NotNil(t, record, "the outcome is logged on the apply's logger")
			assert.Equal(t, "apply-1a2b3c4d5e6f7890", record["apply_id"])
			assert.Equal(t, tt.branch, record["branch"])
			assert.Equal(t, tt.cause.Error(), record["apply_error"])
		})
	}
}

// inFlightDeleteClient holds each branch delete open until its context ends,
// signalling started once the request is in flight, and records whether the
// delete was abandoned because its context ended. A delete detached from the
// drive's context is released only by the test's fallback, so it is recorded
// as not abandoned. Branch credentials are refused, so a fresh drive fails
// while preparing its branch and goes on to delete it.
type inFlightDeleteClient struct {
	branchLifecycleClient
	started   chan struct{}
	abandoned chan bool
}

func newInFlightDeleteClient() *inFlightDeleteClient {
	return &inFlightDeleteClient{started: make(chan struct{}), abandoned: make(chan bool, 1)}
}

func (c *inFlightDeleteClient) DeleteBranch(ctx context.Context, _ *ps.DeleteDatabaseBranchRequest) error {
	close(c.started)
	select {
	case <-ctx.Done():
		c.abandoned <- true
		return ctx.Err()
	case <-time.After(5 * time.Second):
		c.abandoned <- false
		return nil
	}
}

// A resumed drive starts deleting its branch after a permanent failure, and
// its lease is lost while the delete request is still in flight. A peer may
// already be resuming the apply from that branch, so the old drive abandons
// the delete the moment its context ends rather than finishing it. The
// request may already have reached PlanetScale, so the log says only that the
// delete was abandoned and that a resuming driver starts fresh if the branch
// is gone, instead of asking an operator to delete it by hand.
func TestReclaimBranchAfterFailedResume_LeaseLostDuringTheDeleteAbandonsIt(t *testing.T) {
	var logs bytes.Buffer
	client := newInFlightDeleteClient()
	req := resumeRequest(t, &psMetadata{BranchName: "schemabot-commerce-1a2b"}, "apply-1a2b3c4d5e6f7890")
	req.Logger = slog.New(slog.NewJSONHandler(&logs, nil))
	ctx, loseLease := context.WithCancel(t.Context())
	defer loseLease()

	done := make(chan struct{})
	go func() {
		defer close(done)
		conformanceEngine(client).reclaimBranchAfterFailedResume(ctx, client, "org", req, "schemabot-commerce-1a2b",
			engine.NewPermanentError("apply keyspace commerce: vschema rejected"))
	}()
	<-client.started
	loseLease()
	<-done

	assert.True(t, <-client.abandoned, "the delete must end with the drive's context, not run on past the lost lease")
	var record map[string]any
	require.NoError(t, json.Unmarshal(logs.Bytes(), &record), "exactly one log line: %s", logs.String())
	assert.Equal(t, "WARN", record["level"])
	assert.Equal(t, abandonedDeleteLog, record["msg"])
	assert.Equal(t, "schemabot-commerce-1a2b", record["branch"])
}

const abandonedDeleteLog = "drive ended while deleting the branch; abandoned the delete request, which may already have landed; a resuming driver starts fresh if the branch is gone"

// A fresh drive fails while preparing a branch the stored resume state names,
// and starts deleting it. Its lease is lost while the delete request is still
// in flight, and a peer may already be resuming the apply from that branch,
// so the old drive abandons the delete when its context ends, exactly as a
// resumed drive does, and logs that on the apply's logger.
func TestApply_LeaseLostDuringTheDeleteOfARecordedBranchAbandonsIt(t *testing.T) {
	shortenEngineWaits(t)
	client := newInFlightDeleteClient()
	req := &engine.ApplyRequest{
		PlanID:        "plan-0123456789abcdef",
		Database:      "commerce",
		Credentials:   conformanceCredentials(),
		ResumeState:   &engine.ResumeState{MigrationContext: "apply-1a2b3c4d5e6f7890"},
		OnStateChange: func(*engine.ResumeState) error { return nil },
	}
	logRecord := captureApplyLogs(t, req)
	ctx, loseLease := context.WithCancel(t.Context())
	defer loseLease()

	done := make(chan error, 1)
	go func() {
		_, err := conformanceEngine(client).Apply(ctx, req)
		done <- err
	}()
	<-client.started
	loseLease()
	err := <-done

	require.ErrorContains(t, err, "branch credentials unavailable", "the drive returns its own failure, not the abandoned delete")
	assert.True(t, <-client.abandoned, "the delete must end with the drive's context, not run on past the lost lease")
	created, _ := client.snapshot()
	require.Len(t, created, 1)
	record := logRecord(abandonedDeleteLog)
	require.NotNil(t, record, "the abandoned delete is logged on the apply's logger")
	assert.Equal(t, "WARN", record["level"])
	assert.Equal(t, "apply-1a2b3c4d5e6f7890", record["apply_id"])
	assert.Equal(t, created[0], record["branch"])
}

// A branch the reclaim could not delete is still against quota, so the failure
// is logged at error level with everything an operator needs to find the apply
// and delete the branch by hand: the apply, the organization, the PlanetScale
// database, the branch, and the failure that ended the drive.
func TestReclaimBranchAfterFailedResume_LogsAFailedDeleteForTriage(t *testing.T) {
	var logs bytes.Buffer
	client := &resumeReclaimClient{deleteErr: errors.New("branch delete refused")}
	req := resumeRequest(t, &psMetadata{BranchName: "schemabot-commerce-1a2b"}, "apply-1a2b3c4d5e6f7890")
	req.Logger = slog.New(slog.NewJSONHandler(&logs, nil)).With("apply_id", "apply-1a2b3c4d5e6f7890")
	cause := engine.NewPermanentError("apply keyspace commerce: vschema rejected")

	conformanceEngine(client).reclaimBranchAfterFailedResume(t.Context(), client, "org", req, "schemabot-commerce-1a2b", cause)

	var record map[string]any
	require.NoError(t, json.Unmarshal(logs.Bytes(), &record), "exactly one log line: %s", logs.String())
	assert.Equal(t, "ERROR", record["level"])
	assert.Equal(t, "apply-1a2b3c4d5e6f7890", record["apply_id"])
	assert.Equal(t, "org", record["organization"])
	assert.Equal(t, "testdb", record["planetscale_database"])
	assert.Equal(t, "schemabot-commerce-1a2b", record["branch"])
	assert.Equal(t, "apply keyspace commerce: vschema rejected", record["apply_error"])
	assert.Equal(t, "branch delete refused", record["error"])
}

// A Vitess drive fails the apply for good on any engine error, classified or
// not, so a resume that fails before its deploy request with an unclassified
// error has no retry coming. The branch it was preparing is reclaimed, as the
// fresh drive reclaims its own, and the drive returns the diff's own error.
func TestResumeApply_UnclassifiedFailureBeforeTheDeployRequestReclaimsTheBranch(t *testing.T) {
	shortenEngineWaits(t)
	client := &resumeReclaimClient{}
	req := resumeRequest(t, &psMetadata{BranchName: "schemabot-testdb-1a2b"}, "apply-1a2b3c4d5e6f7890")
	req.Changes = []engine.SchemaChange{{
		Namespace:    "commerce",
		TableChanges: []engine.TableChange{{DDL: "this is not a statement"}},
	}}

	_, err := conformanceEngine(client).resumeApply(t.Context(), client, "org", req)

	require.ErrorContains(t, err, "resume branch schemabot-testdb-1a2b")
	_, deleted := client.snapshot()
	assert.Equal(t, []string{"schemabot-testdb-1a2b"}, deleted)
}

// deployRequestRejectingClient serves a resumable branch and refuses the
// deploy request the resume asks for.
type deployRequestRejectingClient struct {
	resumeReclaimClient
}

func (c *deployRequestRejectingClient) CreateDeployRequest(context.Context, *ps.CreateDeployRequestRequest) (*ps.DeployRequest, error) {
	return nil, engine.NewPermanentError("deploy request rejected")
}

// A resume whose deploy request is refused has still not handed the branch to
// a deploy request, so the branch is reclaimed like any other failure before
// that point.
func TestResumeApply_RejectedDeployRequestReclaimsTheBranch(t *testing.T) {
	shortenEngineWaits(t)
	client := &deployRequestRejectingClient{}
	req := resumeRequest(t, &psMetadata{BranchName: "schemabot-testdb-1a2b"}, "apply-1a2b3c4d5e6f7890")

	_, err := conformanceEngine(client).resumeApply(t.Context(), client, "org", req)

	require.ErrorContains(t, err, "create deploy request on resume")
	_, deleted := client.snapshot()
	assert.Equal(t, []string{"schemabot-testdb-1a2b"}, deleted)
}

// resumedDeployRequestClient serves a resumable branch. The deploy request a
// resume creates is created; reread is what every read of a deploy request
// returns, the recovered one a resume from stored state reattaches to
// included; deployErr rejects every deploy. When endDrive is set, the drive's
// context ends as the create returns, and a deploy the ended drive sends fails
// with its error. Reads and closes are recorded.
type resumedDeployRequestClient struct {
	resumeReclaimClient

	created       *ps.DeployRequest
	reread        *ps.DeployRequest
	cleanupReread *ps.DeployRequest
	deployErr     error
	endDrive      context.CancelFunc
	reads         int
	closes        []uint64
}

func (c *resumedDeployRequestClient) CreateDeployRequest(context.Context, *ps.CreateDeployRequestRequest) (*ps.DeployRequest, error) {
	if c.endDrive != nil {
		c.endDrive()
	}
	created := *c.created
	return &created, nil
}

func (c *resumedDeployRequestClient) GetDeployRequest(context.Context, *ps.GetDeployRequestRequest) (*ps.DeployRequest, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.reads++
	reread := *c.reread
	if c.reads == cleanupRead && c.cleanupReread != nil {
		reread = *c.cleanupReread
	}
	return &reread, nil
}

func (c *resumedDeployRequestClient) DeployDeployRequest(ctx context.Context, _ *ps.PerformDeployRequest) (*ps.DeployRequest, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return nil, c.deployErr
}

func (c *resumedDeployRequestClient) CloseDeployRequest(_ context.Context, req *ps.CloseDeployRequestRequest) (*ps.DeployRequest, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.closes = append(c.closes, req.Number)
	closed := *c.reread
	closed.State = deployRequestClosed
	return &closed, nil
}

// A resume that creates its deploy request and then fails before the deploy
// begins leaves the deploy request open and deployable, and the branch against
// quota, exactly as a fresh drive would. It retires them under the fresh
// drive's rules: the deploy request is closed only while it reads as never
// deployed, an operator-supplied branch is kept, and a drive whose context
// ended leaves both to whoever takes the apply over without sending another
// request. A deploy request the resume recovered from stored state, rather
// than created, is never closed: the resume only reattaches to it.
func TestResumeApply_ClosesTheUndeployedDeployRequestItCreated(t *testing.T) {
	shortenEngineWaits(t)
	const branch = "schemabot-testdb-1a2b"
	deployedAt := time.Date(2026, 10, 15, 9, 30, 0, 0, time.UTC)
	refused := &ps.DeployRequest{Number: 42, State: "open", DeploymentState: deployState.Error}
	ready := &ps.DeployRequest{Number: 42, State: "open", DeploymentState: deployState.Ready}
	queued := &ps.DeployRequest{Number: 42, State: "open", DeploymentState: deployState.Queued, DeployedAt: &deployedAt}
	rejected := &ps.Error{Code: ps.ErrInvalid}
	unanswered := errors.New("connection reset by peer")

	tests := []struct {
		name string
		// recovered names a deploy request in the stored state, so the resume
		// reattaches to it instead of creating one.
		recovered bool
		options   map[string]string
		created   *ps.DeployRequest
		reread    *ps.DeployRequest
		// cleanupReread is what the cleanup's read returns when it differs
		// from the read the rejected deploy made.
		cleanupReread *ps.DeployRequest
		deployErr     error
		endDrive      bool
		wantErr       string
		wantCloses    []uint64
		wantDeleted   []string
		wantReads     int
	}{
		{name: "a deploy request the resume created and PlanetScale refused is closed and its branch deleted",
			created: refused, reread: refused, wantErr: "deploy request #42 failed on resume",
			wantCloses: []uint64{42}, wantDeleted: []string{branch}, wantReads: 1},
		{name: "a rejected deploy of a deploy request the resume created is closed and its branch deleted",
			created: ready, reread: ready, deployErr: rejected, wantErr: "deploy on resume",
			wantCloses: []uint64{42}, wantDeleted: []string{branch}, wantReads: 2},
		{name: "an operator-supplied branch is kept after the deploy request the resume created is closed",
			options: map[string]string{"branch": branch}, created: refused, reread: refused, wantErr: "deploy request #42 failed on resume",
			wantCloses: []uint64{42}, wantReads: 1},
		{name: "a deploy request the resume created that reports a deploy began by the cleanup's read is left with its branch",
			created: ready, reread: ready, cleanupReread: queued, deployErr: rejected, wantErr: "deploy on resume", wantReads: 2},
		{name: "a deploy of a deploy request the resume created whose outcome is unknown leaves both for recovery",
			created: ready, reread: ready, deployErr: unanswered, wantErr: "deploy on resume", wantReads: 1},
		{name: "a resumed drive that ends after creating the deploy request leaves both for the driver that takes over",
			created: ready, reread: ready, endDrive: true, wantErr: "deploy on resume"},
		{name: "a deploy request recovered from stored state is never closed",
			recovered: true, reread: ready, deployErr: rejected, wantErr: "recovered deploy request on resume", wantReads: 2},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			client := &resumedDeployRequestClient{created: tt.created, reread: tt.reread, cleanupReread: tt.cleanupReread, deployErr: tt.deployErr}
			if tt.endDrive {
				client.endDrive = cancel
			}
			meta := &psMetadata{BranchName: branch}
			if tt.recovered {
				meta.DeployRequestID = 42
			}
			req := resumeRequest(t, meta, "apply-1a2b3c4d5e6f7890")
			req.Options = tt.options

			_, err := conformanceEngine(client).resumeApply(ctx, client, "org", req)

			require.ErrorContains(t, err, tt.wantErr)
			assert.Equal(t, tt.wantCloses, client.closes)
			assert.Equal(t, tt.wantDeleted, client.deleteAttempts(), "only the branch the resume handed to the deploy request it created is deleted")
			assert.Equal(t, tt.wantReads, client.reads)
		})
	}
}

// deployRequestReclaimClient serves the cancelled apply's stored deploy request
// as dr, or fails the read with getErr, and records each read and delete.
type deployRequestReclaimClient struct {
	resumeReclaimClient
	dr     *ps.DeployRequest
	getErr error
	reads  []uint64
	// databases records the database each read and delete addressed.
	databases []string
}

func (c *deployRequestReclaimClient) GetDeployRequest(_ context.Context, req *ps.GetDeployRequestRequest) (*ps.DeployRequest, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.reads = append(c.reads, req.Number)
	c.databases = append(c.databases, "read "+req.Database)
	if c.getErr != nil {
		return nil, c.getErr
	}
	return c.dr, nil
}

func (c *deployRequestReclaimClient) DeleteBranch(ctx context.Context, req *ps.DeleteDatabaseBranchRequest) error {
	c.mu.Lock()
	c.databases = append(c.databases, "delete "+req.Database)
	c.mu.Unlock()
	return c.resumeReclaimClient.DeleteBranch(ctx, req)
}

// The client that would read the deploy request cannot be built. Only a read
// proves the deploy request will never deploy, so the branch is kept, never
// sent a delete, and reported so the apply log names it.
func TestReclaimCancelledResumeState_UnreachableClientWithDeployRequestKeepsBranch(t *testing.T) {
	const ownedBranch = "schemabot-commerce-1a2b"
	e := NewWithClient(slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelError})),
		func(_, _ string) (psclient.PSClient, error) {
			return nil, errors.New("planetscale unreachable")
		})
	encoded, err := encodePSMetadata(&psMetadata{BranchName: ownedBranch, DeployRequestID: 42})
	require.NoError(t, err)

	result, err := e.ReclaimCancelledResumeState(t.Context(), &engine.ReclaimResumeStateRequest{
		Database:    "commerce",
		Credentials: conformanceCredentials(),
		ResumeState: &engine.ResumeState{Metadata: encoded},
	})

	require.NoError(t, err)
	require.NotNil(t, result)
	assert.Empty(t, result.Reclaimed)
	assert.Equal(t, []string{"branch " + ownedBranch}, result.Kept)
	assert.Contains(t, result.KeptReason, "delete the branch by hand")
}

// A database registered under an identifier other than its PlanetScale name
// carries the PlanetScale name in its credential metadata. The reclaim reads
// the deploy request and deletes the branch on the database as PlanetScale
// knows it, so the branch is found and deleted rather than missed under the
// registered identifier.
func TestReclaimCancelledResumeState_ResolvesDatabaseName(t *testing.T) {
	const ownedBranch = "schemabot-commerce-1a2b"
	client := &deployRequestReclaimClient{
		dr: &ps.DeployRequest{Number: 42, State: deployRequestClosed, DeploymentState: "ready"},
	}
	encoded, err := encodePSMetadata(&psMetadata{BranchName: ownedBranch, DeployRequestID: 42})
	require.NoError(t, err)
	creds := conformanceCredentials()
	creds.Metadata["database"] = "commerce_main"

	result, err := conformanceEngine(client).ReclaimCancelledResumeState(t.Context(), &engine.ReclaimResumeStateRequest{
		Database:    "commerce",
		Credentials: creds,
		ResumeState: &engine.ResumeState{Metadata: encoded},
	})

	require.NoError(t, err)
	require.NotNil(t, result)
	assert.Equal(t, []string{"read commerce_main", "delete commerce_main"}, client.databases)
	assert.Equal(t, []string{"branch " + ownedBranch}, result.Reclaimed)
}

// A cancel of a deferred deploy closes its deploy request before the deploy
// starts, and a deploy request tears its branch down only when its deploy
// finishes, so a branch SchemaBot created for a deploy request closed that way
// has nothing else to remove it. The reclaim reads the stored deploy request
// and deletes the branch only when the read proves the deploy request closed
// without deploying. A deploy request that is still open, or that closed by
// deploying, owns the branch; a deploy request that cannot be read keeps the
// branch and reports it, so the apply log can say it is still in PlanetScale;
// and an operator-supplied branch is kept for the operator without reading
// the deploy request at all.
func TestReclaimCancelledResumeState_DeployRequest(t *testing.T) {
	const ownedBranch = "schemabot-commerce-1a2b"
	deployedAt := time.Date(2026, 10, 1, 12, 0, 0, 0, time.UTC)
	tests := []struct {
		name          string
		meta          *psMetadata
		options       map[string]string
		dr            *ps.DeployRequest
		getErr        error
		wantReads     []uint64
		wantAttempts  []string
		wantReclaimed []string
		wantKept      []string
	}{
		{
			name:      "a branch whose deploy request closed before it deployed is deleted",
			meta:      &psMetadata{BranchName: ownedBranch, DeployRequestID: 42},
			dr:        &ps.DeployRequest{Number: 42, State: deployRequestClosed, DeploymentState: "ready"},
			wantReads: []uint64{42}, wantAttempts: []string{ownedBranch}, wantReclaimed: []string{"branch " + ownedBranch},
		},
		{
			name:      "a branch whose deploy request closed by deploying is left to the deploy request",
			meta:      &psMetadata{BranchName: ownedBranch, DeployRequestID: 42},
			dr:        &ps.DeployRequest{Number: 42, State: deployRequestClosed, DeploymentState: "complete", DeployedAt: &deployedAt},
			wantReads: []uint64{42},
		},
		{
			name:      "a branch whose deploy request is still open is left to the deploy request",
			meta:      &psMetadata{BranchName: ownedBranch, DeployRequestID: 42},
			dr:        &ps.DeployRequest{Number: 42, State: "open", DeploymentState: "ready"},
			wantReads: []uint64{42},
		},
		{
			name:      "a branch whose deploy request cannot be read is kept and reported",
			meta:      &psMetadata{BranchName: ownedBranch, DeployRequestID: 42},
			getErr:    errors.New("planetscale unavailable"),
			wantReads: []uint64{42}, wantKept: []string{"branch " + ownedBranch},
		},
		{
			name: "an operator-supplied branch with a deploy request is kept for the operator",
			meta: &psMetadata{BranchName: "my-dev-branch", DeployRequestID: 42}, options: map[string]string{"branch": "my-dev-branch"},
			dr:       &ps.DeployRequest{Number: 42, State: deployRequestClosed, DeploymentState: "ready"},
			wantKept: []string{"branch my-dev-branch"},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			client := &deployRequestReclaimClient{dr: tt.dr, getErr: tt.getErr}
			encoded, err := encodePSMetadata(tt.meta)
			require.NoError(t, err)

			result, err := conformanceEngine(client).ReclaimCancelledResumeState(t.Context(), &engine.ReclaimResumeStateRequest{
				Database:    "commerce",
				Credentials: conformanceCredentials(),
				Options:     tt.options,
				ResumeState: &engine.ResumeState{Metadata: encoded},
			})

			require.NoError(t, err, "a reclaim that keeps the branch because a deploy request may own it is not a failure")
			require.NotNil(t, result)
			assert.Equal(t, tt.wantReads, client.reads)
			assert.Equal(t, tt.wantAttempts, client.deleteAttempts(), "a kept branch is never sent a delete")
			assert.Equal(t, tt.wantReclaimed, result.Reclaimed)
			assert.Equal(t, tt.wantKept, result.Kept)
			assert.Equal(t, len(tt.wantKept) > 0, result.KeptReason != "", "a kept branch carries its reason")
		})
	}
}

// A cancel that lands between drives finds the branch only in the apply's
// stored resume state: the drive that created it has exited, and the drive
// consuming the cancel stands down before it resumes. These cases cover a
// state without a deploy request, plus the deploy-request cases that need no
// read; TestReclaimCancelledResumeState_DeployRequest covers the rest. The
// reclaim deletes a branch SchemaBot created, and leaves alone a branch the
// operator supplied and a branch a drive already removed, reporting each so
// the apply log can say what is still in PlanetScale and why.
func TestReclaimCancelledResumeState(t *testing.T) {
	const ownedBranch = "schemabot-commerce-1a2b"
	tests := []struct {
		name          string
		meta          *psMetadata
		options       map[string]string
		deleteErr     error
		wantAttempts  []string
		wantReclaimed []string
		wantKept      []string
		wantErr       string
	}{
		{
			name: "a branch with no deploy request is deleted", meta: &psMetadata{BranchName: ownedBranch},
			wantAttempts: []string{ownedBranch}, wantReclaimed: []string{"branch " + ownedBranch},
		},
		{
			name: "an operator-supplied branch is kept for the operator", meta: &psMetadata{BranchName: "my-dev-branch"}, options: map[string]string{"branch": "my-dev-branch"},
			wantKept: []string{"branch my-dev-branch"},
		},
		{
			name: "a branch a drive already removed is not a failure", meta: &psMetadata{BranchName: ownedBranch}, deleteErr: &ps.Error{Code: ps.ErrNotFound},
			wantAttempts: []string{ownedBranch},
		},
		{
			name: "a delete that fails reports the branch as left in place", meta: &psMetadata{BranchName: ownedBranch}, deleteErr: errors.New("planetscale unavailable"),
			wantAttempts: []string{ownedBranch}, wantKept: []string{"branch " + ownedBranch},
			wantErr: "delete branch " + ownedBranch + " of the cancelled apply on commerce: planetscale unavailable",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			client := &resumeReclaimClient{deleteErr: tt.deleteErr}
			encoded, err := encodePSMetadata(tt.meta)
			require.NoError(t, err)

			result, err := conformanceEngine(client).ReclaimCancelledResumeState(t.Context(), &engine.ReclaimResumeStateRequest{
				Database:    "commerce",
				Credentials: conformanceCredentials(),
				Options:     tt.options,
				ResumeState: &engine.ResumeState{Metadata: encoded},
			})

			if tt.wantErr != "" {
				require.EqualError(t, err, tt.wantErr)
			} else {
				require.NoError(t, err)
			}
			require.NotNil(t, result)
			assert.Equal(t, tt.wantAttempts, client.deleteAttempts(), "a kept branch is never sent a delete")
			assert.Equal(t, tt.wantReclaimed, result.Reclaimed)
			assert.Equal(t, tt.wantKept, result.Kept)
			assert.Equal(t, len(tt.wantKept) > 0, result.KeptReason != "", "a kept branch carries its reason")
		})
	}

	t.Run("an apply that persisted no state holds no branch", func(t *testing.T) {
		client := &resumeReclaimClient{}
		result, err := conformanceEngine(client).ReclaimCancelledResumeState(t.Context(), &engine.ReclaimResumeStateRequest{
			Database:    "commerce",
			Credentials: conformanceCredentials(),
		})
		require.NoError(t, err)
		assert.Empty(t, result.Reclaimed)
		assert.Empty(t, result.Kept)
		assert.Empty(t, client.deleteAttempts())
	})
}
