package webhook

import (
	"context"
	"errors"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	grpccodes "google.golang.org/grpc/codes"
	grpcstatus "google.golang.org/grpc/status"

	"github.com/block/schemabot/pkg/api"
	ghclient "github.com/block/schemabot/pkg/github"
	ternv1 "github.com/block/schemabot/pkg/proto/ternv1"
	"github.com/block/schemabot/pkg/state"
	"github.com/block/schemabot/pkg/storage"
	"github.com/block/schemabot/pkg/tern"
	"github.com/block/schemabot/pkg/webhook/action"
)

// rollbackCommandCore returns a durability disposition (retry, err) that a
// future durable issue_comment driver consumes. The synchronous goSafe wrapper
// discards it, so these tests pin the contract directly on the core.

// rollbackTestStorage serves the stores the rollback command core touches:
// applies for the source lookup, plans and tasks for the source-apply
// guardrails, and locks for acquisition.
type rollbackTestStorage struct {
	emptyStorage
	applies storage.ApplyStore
	plans   storage.PlanStore
	tasks   storage.TaskStore
	locks   storage.LockStore
}

func (s *rollbackTestStorage) Applies() storage.ApplyStore { return s.applies }
func (s *rollbackTestStorage) Plans() storage.PlanStore    { return s.plans }
func (s *rollbackTestStorage) Tasks() storage.TaskStore    { return s.tasks }
func (s *rollbackTestStorage) Locks() storage.LockStore    { return s.locks }

type rollbackTestApplyStore struct {
	storage.ApplyStore
	apply *storage.Apply
	err   error
}

func (s *rollbackTestApplyStore) GetByApplyIdentifier(_ context.Context, _ string) (*storage.Apply, error) {
	return s.apply, s.err
}

type rollbackTestPlanStore struct {
	storage.PlanStore
	plan *storage.Plan
	err  error
}

func (s *rollbackTestPlanStore) GetByID(_ context.Context, _ int64) (*storage.Plan, error) {
	return s.plan, s.err
}

type rollbackTestTaskStore struct {
	storage.TaskStore
	tasks []*storage.Task
	err   error
}

func (s *rollbackTestTaskStore) GetByDatabase(_ context.Context, _ string) ([]*storage.Task, error) {
	return s.tasks, s.err
}

// rollbackVanishingApplyStore serves the apply on the first lookup and nil on
// every later one, modeling an apply deleted between the command's own lookup
// and the source-apply validation's re-read.
type rollbackVanishingApplyStore struct {
	storage.ApplyStore
	apply *storage.Apply
	calls int
}

func (s *rollbackVanishingApplyStore) GetByApplyIdentifier(_ context.Context, _ string) (*storage.Apply, error) {
	s.calls++
	if s.calls == 1 {
		return s.apply, nil
	}
	return nil, nil
}

// rollbackFlakyPlanStore serves the plan for the first failAfter reads and
// fails afterwards, reaching the validation sites that re-read the plan later
// in the command.
type rollbackFlakyPlanStore struct {
	storage.PlanStore
	plan      *storage.Plan
	failAfter int
	calls     int
}

func (s *rollbackFlakyPlanStore) GetByID(_ context.Context, _ int64) (*storage.Plan, error) {
	s.calls++
	if s.calls > s.failAfter {
		return nil, errors.New("storage read failed")
	}
	return s.plan, nil
}

// rollbackCancelingPlanStore serves the plan on the first read and cancels the
// command's parent context before failing later reads, modeling a command
// whose own context has already expired by the time a post-lock failure needs
// the acquired lock released.
type rollbackCancelingPlanStore struct {
	storage.PlanStore
	plan   *storage.Plan
	cancel context.CancelFunc
	calls  int
}

func (s *rollbackCancelingPlanStore) GetByID(_ context.Context, _ int64) (*storage.Plan, error) {
	s.calls++
	if s.calls == 1 {
		return s.plan, nil
	}
	s.cancel()
	return nil, errors.New("storage read failed")
}

// rollbackShrinkingTaskStore serves the tasks on the first read and an empty
// set afterwards, modeling a concurrent apply changing which schema change the
// rollback planner would select between validation and revalidation.
type rollbackShrinkingTaskStore struct {
	storage.TaskStore
	tasks []*storage.Task
	calls int
}

func (s *rollbackShrinkingTaskStore) GetByDatabase(_ context.Context, _ string) ([]*storage.Task, error) {
	s.calls++
	if s.calls == 1 {
		return s.tasks, nil
	}
	return nil, nil
}

type rollbackTestLockStore struct {
	storage.LockStore
	lock       *storage.Lock
	getErr     error
	acquireErr error
	releaseErr error

	acquireCalls int
	releaseCalls int
	// releaseCtxErr snapshots the release context's cancellation state at
	// call time, proving the release ran detached from an already-cancelled
	// command context.
	releaseCtxErr error
}

func (s *rollbackTestLockStore) Get(_ context.Context, _, _ string) (*storage.Lock, error) {
	return s.lock, s.getErr
}

func (s *rollbackTestLockStore) Acquire(_ context.Context, _ *storage.Lock) error {
	s.acquireCalls++
	return s.acquireErr
}

func (s *rollbackTestLockStore) Release(ctx context.Context, _, _, _ string) error {
	s.releaseCalls++
	s.releaseCtxErr = ctx.Err()
	return s.releaseErr
}

// completedSourceApply is a rollback source apply that passes every guardrail:
// completed, in the requested environment, and scoped to the requesting PR.
func completedSourceApply() *storage.Apply {
	return &storage.Apply{
		ID:              7,
		ApplyIdentifier: "apply-123",
		PlanID:          10,
		Database:        "orders",
		DatabaseType:    storage.DatabaseTypeMySQL,
		Environment:     "staging",
		Repository:      "octocat/hello-world",
		PullRequest:     1,
		State:           state.Apply.Completed,
	}
}

// capturedSourcePlan is the source apply's plan with an original-files capture,
// the artifact a rollback plan is generated from.
func capturedSourcePlan() *storage.Plan {
	return &storage.Plan{
		ID: 10,
		Namespaces: map[string]*storage.NamespacePlanData{
			"default": {OriginalFilesCaptured: true},
		},
	}
}

// latestCompletedSourceTask makes the source apply the schema change the
// rollback planner would select for its database and environment.
func latestCompletedSourceTask() *storage.Task {
	return &storage.Task{
		ApplyID:      7,
		PlanID:       10,
		DatabaseType: storage.DatabaseTypeMySQL,
		Environment:  "staging",
		State:        state.Task.Completed,
	}
}

// validRollbackStorage returns storage where every source-apply guardrail
// passes and no lock is held, positioned just before plan generation.
func validRollbackStorage(locks storage.LockStore) *rollbackTestStorage {
	return &rollbackTestStorage{
		applies: &rollbackTestApplyStore{apply: completedSourceApply()},
		plans:   &rollbackTestPlanStore{plan: capturedSourcePlan()},
		tasks:   &rollbackTestTaskStore{tasks: []*storage.Task{latestCompletedSourceTask()}},
		locks:   locks,
	}
}

func rollbackCommand() CommandResult {
	return CommandResult{Action: action.Rollback, ApplyID: "apply-123", Environment: "staging"}
}

// rollbackPlanTernClient is a Tern client whose Plan call fails, standing in
// for the remote engine's answer during rollback plan generation.
type rollbackPlanTernClient struct {
	tern.Client
	planErr error
}

func (c *rollbackPlanTernClient) Plan(context.Context, *ternv1.PlanRequest) (*ternv1.PlanResponse, error) {
	return nil, c.planErr
}

func (c *rollbackPlanTernClient) IsRemote() bool { return true }

func (c *rollbackPlanTernClient) Endpoint() string { return "test-endpoint" }

// remotePlanRollbackStorage returns storage where every guardrail passes and
// the stored plan carries the routing metadata needed to reach the remote
// Plan RPC, positioned exactly at the engine's answer.
func remotePlanRollbackStorage(locks storage.LockStore) *rollbackTestStorage {
	apply := completedSourceApply()
	apply.Deployment = "tenant-a"
	plan := capturedSourcePlan()
	plan.Database = apply.Database
	plan.DatabaseType = apply.DatabaseType
	plan.Environment = apply.Environment
	plan.Target = "orders-staging"
	return &rollbackTestStorage{
		applies: &rollbackTestApplyStore{apply: apply},
		plans:   &rollbackTestPlanStore{plan: plan},
		tasks:   &rollbackTestTaskStore{tasks: []*storage.Task{latestCompletedSourceTask()}},
		locks:   locks,
	}
}

// A GitHub App resolution failure is deterministic per deployment config —
// the same repo resolves to the same missing App on every attempt — so the
// core must report it as terminal rather than re-driving a delivery that can
// only fail until an operator fixes the config. No PR comment could be posted
// without a client, so the core also returns the error: the delivery is
// recorded as failed (its only triage trail) rather than completed.
func TestRollbackCommandCoreAppResolutionFailureIsTerminal(t *testing.T) {
	st := &rollbackTestStorage{applies: &rollbackTestApplyStore{apply: completedSourceApply()}}
	h := actorAuthClientSetTestHandler(t, st, ghclient.NewSingleClientSet("unrelated-app", &fakeClientFactory{}))

	retry, err := h.rollbackCommandCore(t.Context(), "octocat/hello-world", 1, 12345, "testuser", rollbackCommand())

	require.Error(t, err)
	assert.ErrorIs(t, err, errGitHubAppResolution)
	assert.False(t, retry, "a deterministic GitHub App resolution failure must not be re-driven; recovery is fixing the deployment config")
}

// The authorization client is created per delivery, so a transient
// client-creation failure (an installation token fetch, for example) may
// clear on a later attempt: the core must report it as retryable with a
// non-nil error.
func TestRollbackCommandCoreAuthorizationClientTransientFailureIsRetryable(t *testing.T) {
	st := &rollbackTestStorage{applies: &rollbackTestApplyStore{apply: completedSourceApply()}}
	h := actorAuthClientSetTestHandler(t, st, ghclient.NewSingleClientSet(defaultAppName, &fakeClientFactory{
		forInstallationErr: errors.New("installation token unavailable"),
	}))

	retry, err := h.rollbackCommandCore(t.Context(), "octocat/hello-world", 1, 12345, "testuser", rollbackCommand())

	require.Error(t, err)
	assert.NotErrorIs(t, err, errGitHubAppResolution)
	assert.True(t, retry, "a transient authorization client-creation failure must stay retryable for a durable driver")
}

// Transient infrastructure failures — the source-apply lookup, a storage read
// inside the source guardrails, the lock status read, lock acquisition, or an
// unevaluable authorization gate — are failures the same delivery could clear
// on a later attempt, so the core must report them as retryable with a non-nil
// error.
func TestRollbackCommandCoreTransientFailuresAreRetryable(t *testing.T) {
	t.Run("apply lookup failure is retryable", func(t *testing.T) {
		client, mux := setupGitHubServer(t)
		comments := recordComments(t, mux)
		st := &rollbackTestStorage{
			applies: &rollbackTestApplyStore{err: errors.New("storage read failed")},
		}
		h := unlockTestHandler(t, st, ghclient.NewInstallationClient(client, testLogger()))

		retry, err := h.rollbackCommandCore(t.Context(), "octocat/hello-world", 1, 12345, "testuser", rollbackCommand())

		require.Error(t, err)
		assert.True(t, retry, "a transient apply lookup failure must stay retryable for a durable driver")
		body := requireComment(t, comments, "apply lookup failure comment")
		assert.Contains(t, body, "Failed to look up the apply.")
		assert.Regexp(t, "report error reference `[0-9a-f]{8}`", body)
	})

	// The source-apply guardrails read the plan from storage; a failed read is
	// not a verdict on the rollback, so the delivery stays retryable.
	t.Run("source plan read failure is retryable", func(t *testing.T) {
		client, mux := setupGitHubServer(t)
		comments := recordComments(t, mux)
		st := &rollbackTestStorage{
			applies: &rollbackTestApplyStore{apply: completedSourceApply()},
			plans:   &rollbackTestPlanStore{err: errors.New("storage read failed")},
			tasks:   &rollbackTestTaskStore{},
		}
		h := unlockTestHandler(t, st, ghclient.NewInstallationClient(client, testLogger()))

		retry, err := h.rollbackCommandCore(t.Context(), "octocat/hello-world", 1, 12345, "testuser", rollbackCommand())

		require.Error(t, err)
		assert.True(t, retry, "an internal source validation failure must stay retryable, not become the command's answer")
		body := requireComment(t, comments, "source validation failure comment")
		assert.Contains(t, body, "Rollback source validation failed.")
		assert.Regexp(t, "report error reference `[0-9a-f]{8}`", body)
		assert.NotContains(t, body, "load source plan",
			"raw error text must never render in PR markdown")
	})

	t.Run("rollback plan source read failure is retryable", func(t *testing.T) {
		client, mux := setupGitHubServer(t)
		comments := recordComments(t, mux)
		lockStore := &rollbackTestLockStore{}
		st := &rollbackTestStorage{
			applies: &rollbackTestApplyStore{apply: completedSourceApply()},
			plans:   &rollbackFlakyPlanStore{plan: capturedSourcePlan(), failAfter: 2},
			tasks:   &rollbackTestTaskStore{tasks: []*storage.Task{latestCompletedSourceTask()}},
			locks:   lockStore,
		}
		h := unlockTestHandler(t, st, ghclient.NewInstallationClient(client, testLogger()))

		retry, err := h.rollbackCommandCore(t.Context(), "octocat/hello-world", 1, 12345, "testuser", rollbackCommand())

		require.Error(t, err)
		assert.True(t, retry, "a failed plan storage read must stay retryable")
		assert.Equal(t, 1, lockStore.releaseCalls)
		body := requireComment(t, comments, "rollback plan read failure comment")
		assert.Contains(t, body, "Rollback plan failed.")
		assert.Regexp(t, "report error reference `[0-9a-f]{8}`", body)
		assert.NotContains(t, body, "get rollback source plan",
			"raw error text must never render in PR markdown")
	})

	// The source apply is revalidated after lock acquisition; an internal
	// failure there is not a verdict on the rollback either, so the lock is
	// released and the delivery stays retryable.
	t.Run("internal revalidation failure after lock acquisition is retryable", func(t *testing.T) {
		client, mux := setupGitHubServer(t)
		comments := recordComments(t, mux)
		lockStore := &rollbackTestLockStore{}
		st := &rollbackTestStorage{
			applies: &rollbackTestApplyStore{apply: completedSourceApply()},
			plans:   &rollbackFlakyPlanStore{plan: capturedSourcePlan(), failAfter: 1},
			tasks:   &rollbackTestTaskStore{tasks: []*storage.Task{latestCompletedSourceTask()}},
			locks:   lockStore,
		}
		h := unlockTestHandler(t, st, ghclient.NewInstallationClient(client, testLogger()))

		retry, err := h.rollbackCommandCore(t.Context(), "octocat/hello-world", 1, 12345, "testuser", rollbackCommand())

		require.Error(t, err)
		assert.True(t, retry, "an internal revalidation failure must stay retryable, not become the command's answer")
		assert.Equal(t, 1, lockStore.releaseCalls, "the command-acquired lock must be released so a re-drive can start over")
		body := requireComment(t, comments, "revalidation failure comment")
		assert.Contains(t, body, "Rollback source validation failed.")
		assert.Regexp(t, "report error reference `[0-9a-f]{8}`", body)
		assert.NotContains(t, body, "load source plan",
			"raw error text must never render in PR markdown")
	})

	// The lock release after a post-lock failure must run even when the
	// command's own context has already been cancelled — the common retryable
	// case is an expired deadline — so a re-drive is not blocked by a lock the
	// failed attempt could not release.
	t.Run("lock release after a post-lock failure survives command cancellation", func(t *testing.T) {
		client, mux := setupGitHubServer(t)
		recordComments(t, mux)
		parent, cancel := context.WithCancel(t.Context())
		defer cancel()
		lockStore := &rollbackTestLockStore{}
		st := &rollbackTestStorage{
			applies: &rollbackTestApplyStore{apply: completedSourceApply()},
			plans:   &rollbackCancelingPlanStore{plan: capturedSourcePlan(), cancel: cancel},
			tasks:   &rollbackTestTaskStore{tasks: []*storage.Task{latestCompletedSourceTask()}},
			locks:   lockStore,
		}
		h := unlockTestHandler(t, st, ghclient.NewInstallationClient(client, testLogger()))

		retry, err := h.rollbackCommandCore(parent, "octocat/hello-world", 1, 12345, "testuser", rollbackCommand())

		require.Error(t, err)
		assert.True(t, retry, "an internal revalidation failure must stay retryable")
		require.Equal(t, 1, lockStore.releaseCalls, "the command-acquired lock must be released so a re-drive can start over")
		assert.NoError(t, lockStore.releaseCtxErr, "the release must run detached from the cancelled command context")
	})

	// A release that did not happen must not be asserted by the retryable
	// disposition: the failure joins the returned error so the durable
	// driver's record shows the database is still locked.
	t.Run("failed lock release joins the retryable revalidation error", func(t *testing.T) {
		client, mux := setupGitHubServer(t)
		recordComments(t, mux)
		lockStore := &rollbackTestLockStore{releaseErr: errors.New("storage write failed")}
		st := &rollbackTestStorage{
			applies: &rollbackTestApplyStore{apply: completedSourceApply()},
			plans:   &rollbackFlakyPlanStore{plan: capturedSourcePlan(), failAfter: 1},
			tasks:   &rollbackTestTaskStore{tasks: []*storage.Task{latestCompletedSourceTask()}},
			locks:   lockStore,
		}
		h := unlockTestHandler(t, st, ghclient.NewInstallationClient(client, testLogger()))

		retry, err := h.rollbackCommandCore(t.Context(), "octocat/hello-world", 1, 12345, "testuser", rollbackCommand())

		require.Error(t, err)
		assert.True(t, retry, "a failed release must not change the retryable disposition")
		assert.Contains(t, err.Error(), "release rollback lock", "the failed release must be visible in the returned error")
	})

	t.Run("lock status read failure is retryable", func(t *testing.T) {
		client, mux := setupGitHubServer(t)
		comments := recordComments(t, mux)
		lockStore := &rollbackTestLockStore{getErr: errors.New("storage read failed")}
		st := validRollbackStorage(lockStore)
		h := unlockTestHandler(t, st, ghclient.NewInstallationClient(client, testLogger()))

		retry, err := h.rollbackCommandCore(t.Context(), "octocat/hello-world", 1, 12345, "testuser", rollbackCommand())

		require.Error(t, err)
		assert.True(t, retry, "an unknown lock state must be retried, not treated as the command's answer")
		assert.Zero(t, lockStore.acquireCalls, "no lock may be acquired while lock state is unknown")
		body := requireComment(t, comments, "lock read failure comment")
		assert.Contains(t, body, "Failed to check the rollback lock status.")
		assert.Regexp(t, "report error reference `[0-9a-f]{8}`", body)
	})

	t.Run("lock acquire failure is retryable", func(t *testing.T) {
		client, mux := setupGitHubServer(t)
		comments := recordComments(t, mux)
		lockStore := &rollbackTestLockStore{acquireErr: errors.New("storage write failed")}
		st := validRollbackStorage(lockStore)
		h := unlockTestHandler(t, st, ghclient.NewInstallationClient(client, testLogger()))

		retry, err := h.rollbackCommandCore(t.Context(), "octocat/hello-world", 1, 12345, "testuser", rollbackCommand())

		require.Error(t, err)
		assert.True(t, retry, "a failed lock acquisition must stay retryable so a re-drive can acquire it")
		body := requireComment(t, comments, "lock acquire failure comment")
		assert.Contains(t, body, "Failed to acquire the rollback lock.")
		assert.Regexp(t, "report error reference `[0-9a-f]{8}`", body)
	})

	// A gate that could not evaluate its inputs fails closed for this delivery
	// but is not the command's answer: the same delivery may authorize cleanly
	// once the membership read succeeds.
	t.Run("authorization evaluation failure is retryable", func(t *testing.T) {
		client, mux := setupGitHubServer(t)
		comments := recordComments(t, mux)
		mux.HandleFunc("GET /orgs/octocat/teams/schema-admins/members", teamMembersHandler(t, http.StatusForbidden))
		lockStore := &rollbackTestLockStore{}
		st := validRollbackStorage(lockStore)
		cfg := actorAuthTestConfig(true, func(cfg *api.ServerConfig) {
			cfg.PRCommandAuthorization.AdminTeams = []string{"octocat/schema-admins"}
		})
		h := actorAuthStorageTestHandler(cfg, st, ghclient.NewInstallationClient(client, testLogger()))

		retry, err := h.rollbackCommandCore(t.Context(), "octocat/hello-world", 1, 12345, "mona", rollbackCommand())

		require.Error(t, err)
		assert.True(t, retry, "an unevaluable authorization gate fails closed for this delivery but stays retryable")
		assert.Zero(t, lockStore.acquireCalls, "no lock may be acquired while authorization is unknown")
		body := requireComment(t, comments, "authorization failure comment")
		assert.Contains(t, body, "SchemaBot Authorization Check Failed")
	})

	// An unreachable remote deployment is a transport condition, not the
	// engine's answer: the same rollback plan request is safe to re-send once
	// the deployment is reachable, so the delivery stays retryable.
	t.Run("remote plan unavailability is retryable", func(t *testing.T) {
		client, mux := setupGitHubServer(t)
		comments := recordComments(t, mux)
		lockStore := &rollbackTestLockStore{}
		st := remotePlanRollbackStorage(lockStore)
		h := unlockTestHandler(t, st, ghclient.NewInstallationClient(client, testLogger()))
		h.service.RegisterTernClient("tenant-a", "staging",
			&rollbackPlanTernClient{planErr: grpcstatus.Error(grpccodes.Unavailable, "connection refused")})

		retry, err := h.rollbackCommandCore(t.Context(), "octocat/hello-world", 1, 12345, "testuser", rollbackCommand())

		require.Error(t, err)
		assert.True(t, retry, "remote unavailability during plan generation must stay retryable")
		assert.Equal(t, 1, lockStore.releaseCalls, "the command-acquired lock must be released so a re-drive can start over")
		body := requireComment(t, comments, "plan unavailability comment")
		assert.Contains(t, body, "Rollback plan failed.")
		assert.Regexp(t, "report error reference `[0-9a-f]{8}`", body)
		assert.NotContains(t, body, "connection refused",
			"raw transport error text must never render in PR markdown")
	})
}

// Terminal outcomes are the command's answer — a missing apply ID, an apply
// that does not exist, an environment another instance owns, a deterministic
// guardrail rejection, a lock held by another PR, or an authorization block on
// the merits — so the core reports them as (retry=false, err=nil): a durable
// driver must not re-drive them.
func TestRollbackCommandCoreTerminalDispositions(t *testing.T) {
	// No apply ID can never resolve on a re-drive: the posted usage hint is
	// the command's answer.
	t.Run("missing apply ID is terminal", func(t *testing.T) {
		client, mux := setupGitHubServer(t)
		comments := recordComments(t, mux)
		st := &rollbackTestStorage{}
		h := unlockTestHandler(t, st, ghclient.NewInstallationClient(client, testLogger()))

		retry, err := h.rollbackCommandCore(t.Context(), "octocat/hello-world", 1, 12345, "testuser",
			CommandResult{Action: action.Rollback, Environment: "staging"})

		require.NoError(t, err)
		assert.False(t, retry, "a missing apply ID is a deterministic usage error the same delivery always reproduces")
		body := requireComment(t, comments, "missing apply ID comment")
		assert.Contains(t, body, "Missing Apply ID")
	})

	t.Run("apply not found is terminal", func(t *testing.T) {
		client, mux := setupGitHubServer(t)
		comments := recordComments(t, mux)
		st := &rollbackTestStorage{applies: &rollbackTestApplyStore{}}
		h := unlockTestHandler(t, st, ghclient.NewInstallationClient(client, testLogger()))

		retry, err := h.rollbackCommandCore(t.Context(), "octocat/hello-world", 1, 12345, "testuser", rollbackCommand())

		require.NoError(t, err)
		assert.False(t, retry, "an apply that does not exist is the command's answer; a re-drive cannot conjure it")
		body := requireComment(t, comments, "apply not found comment")
		assert.Contains(t, body, "Apply Not Found")
		assert.Contains(t, body, "apply-123")
	})

	// The source-apply validation re-reads the apply; one that vanished between
	// the command's own lookup and the validation gets the same not-found
	// answer on every re-drive.
	t.Run("apply vanished before validation is terminal", func(t *testing.T) {
		client, mux := setupGitHubServer(t)
		comments := recordComments(t, mux)
		st := &rollbackTestStorage{
			applies: &rollbackVanishingApplyStore{apply: completedSourceApply()},
			plans:   &rollbackTestPlanStore{plan: capturedSourcePlan()},
			tasks:   &rollbackTestTaskStore{tasks: []*storage.Task{latestCompletedSourceTask()}},
		}
		h := unlockTestHandler(t, st, ghclient.NewInstallationClient(client, testLogger()))

		retry, err := h.rollbackCommandCore(t.Context(), "octocat/hello-world", 1, 12345, "testuser", rollbackCommand())

		require.NoError(t, err)
		assert.False(t, retry, "a not-found validation answer is the command's answer; a re-drive cannot conjure the apply")
		body := requireComment(t, comments, "apply not found comment")
		assert.Contains(t, body, "Apply Not Found")
		assert.Contains(t, body, "apply-123")
	})

	// On an aggregate repo an unscoped rollback fans out to every deployment,
	// but the apply lives in exactly one tenant's storage; a deployment that
	// doesn't have it stays silent so only the owning deployment answers — a
	// deliberate silent no-op, not a failure.
	t.Run("unowned unscoped fan-out is terminal and silent", func(t *testing.T) {
		client, mux := setupGitHubServer(t)
		comments := recordComments(t, mux)
		st := &rollbackTestStorage{applies: &rollbackTestApplyStore{}}
		h := actorAuthStorageTestHandler(aggregateLeaderConfig(), st, ghclient.NewInstallationClient(client, testLogger()))

		retry, err := h.rollbackCommandCore(t.Context(), "octocat/hello-world", 1, 12345, "testuser", rollbackCommand())

		require.NoError(t, err)
		assert.False(t, retry, "a non-owning fan-out skip is the command's terminal answer, not a retryable failure")
		assert.Empty(t, comments, "the terminal skip must stay silent")
	})

	// Another instance owns the apply's environment; that instance answers, so
	// this one must stay silent and never re-drive.
	t.Run("non-allowed environment is terminal and silent", func(t *testing.T) {
		client, mux := setupGitHubServer(t)
		comments := recordComments(t, mux)
		st := &rollbackTestStorage{applies: &rollbackTestApplyStore{apply: completedSourceApply()}}
		cfg := &api.ServerConfig{AllowedEnvironments: []string{"production"}}
		h := actorAuthStorageTestHandler(cfg, st, ghclient.NewInstallationClient(client, testLogger()))

		retry, err := h.rollbackCommandCore(t.Context(), "octocat/hello-world", 1, 12345, "testuser", rollbackCommand())

		require.NoError(t, err)
		assert.False(t, retry, "an environment another instance owns is terminal here; the owning instance answers")
		assert.Empty(t, comments, "a non-owning instance must stay silent")
	})

	// A source apply that is not completed deterministically fails the
	// guardrails on every re-drive: the posted rejection is the command's
	// answer.
	t.Run("guardrail rejection of an incomplete source apply is terminal", func(t *testing.T) {
		client, mux := setupGitHubServer(t)
		comments := recordComments(t, mux)
		runningApply := completedSourceApply()
		runningApply.State = state.Apply.Running
		st := &rollbackTestStorage{
			applies: &rollbackTestApplyStore{apply: runningApply},
			plans:   &rollbackTestPlanStore{},
			tasks:   &rollbackTestTaskStore{},
		}
		h := unlockTestHandler(t, st, ghclient.NewInstallationClient(client, testLogger()))

		retry, err := h.rollbackCommandCore(t.Context(), "octocat/hello-world", 1, 12345, "testuser", rollbackCommand())

		require.NoError(t, err)
		assert.False(t, retry, "a deterministic guardrail rejection is the command's answer; re-driving would re-reject")
		body := requireComment(t, comments, "guardrail rejection comment")
		assert.Contains(t, body, "Rollback Not Allowed")
		assert.Contains(t, body, "rollback requires a completed apply")
	})

	t.Run("missing source plan invariant is terminal", func(t *testing.T) {
		client, mux := setupGitHubServer(t)
		comments := recordComments(t, mux)
		st := &rollbackTestStorage{
			applies: &rollbackTestApplyStore{apply: completedSourceApply()},
			plans:   &rollbackTestPlanStore{},
			tasks:   &rollbackTestTaskStore{},
		}
		h := unlockTestHandler(t, st, ghclient.NewInstallationClient(client, testLogger()))

		retry, err := h.rollbackCommandCore(t.Context(), "octocat/hello-world", 1, 12345, "testuser", rollbackCommand())

		require.NoError(t, err)
		assert.False(t, retry, "missing required stored data cannot be repaired by re-driving")
		body := requireComment(t, comments, "missing source plan comment")
		assert.Contains(t, body, "Rollback source validation failed.")
		assert.Regexp(t, "report error reference `[0-9a-f]{8}`", body)
		assert.NotContains(t, body, "source plan 10 not found",
			"raw error text must never render in PR markdown")
		assert.NotContains(t, body, "Rollback Not Allowed")
	})

	// A concurrent apply can change which schema change the rollback planner
	// would select between validation and the post-lock revalidation. The
	// revalidation's guardrail rejection is deterministic for the re-driven
	// command, so the lock is released and the posted rejection is the answer.
	t.Run("guardrail rejection at post-lock revalidation is terminal", func(t *testing.T) {
		client, mux := setupGitHubServer(t)
		comments := recordComments(t, mux)
		lockStore := &rollbackTestLockStore{}
		st := &rollbackTestStorage{
			applies: &rollbackTestApplyStore{apply: completedSourceApply()},
			plans:   &rollbackTestPlanStore{plan: capturedSourcePlan()},
			tasks:   &rollbackShrinkingTaskStore{tasks: []*storage.Task{latestCompletedSourceTask()}},
			locks:   lockStore,
		}
		h := unlockTestHandler(t, st, ghclient.NewInstallationClient(client, testLogger()))

		retry, err := h.rollbackCommandCore(t.Context(), "octocat/hello-world", 1, 12345, "testuser", rollbackCommand())

		require.NoError(t, err)
		assert.False(t, retry, "a deterministic revalidation rejection is the command's answer; re-driving would re-reject")
		assert.Equal(t, 1, lockStore.releaseCalls, "the command-acquired lock must be released after the rejection")
		body := requireComment(t, comments, "revalidation rejection comment")
		assert.Contains(t, body, "Rollback Not Allowed")
		assert.Contains(t, body, "no completed schema change task found")
	})

	// A failed release does not turn a deterministic rejection into a
	// retryable delivery: a re-drive would re-reject under the same lock owner
	// and skip the release anyway, so the rejection stays the answer and the
	// stuck lock is an operator unlock, surfaced through the error log.
	t.Run("failed lock release keeps a terminal rejection terminal", func(t *testing.T) {
		client, mux := setupGitHubServer(t)
		comments := recordComments(t, mux)
		lockStore := &rollbackTestLockStore{releaseErr: errors.New("storage write failed")}
		st := &rollbackTestStorage{
			applies: &rollbackTestApplyStore{apply: completedSourceApply()},
			plans:   &rollbackTestPlanStore{plan: capturedSourcePlan()},
			tasks:   &rollbackShrinkingTaskStore{tasks: []*storage.Task{latestCompletedSourceTask()}},
			locks:   lockStore,
		}
		h := unlockTestHandler(t, st, ghclient.NewInstallationClient(client, testLogger()))

		retry, err := h.rollbackCommandCore(t.Context(), "octocat/hello-world", 1, 12345, "testuser", rollbackCommand())

		require.NoError(t, err)
		assert.False(t, retry, "a deterministic rejection stays the command's answer even when the release failed")
		assert.Equal(t, 1, lockStore.releaseCalls, "the release must still be attempted")
		body := requireComment(t, comments, "revalidation rejection comment")
		assert.Contains(t, body, "Rollback Not Allowed")
	})

	// A rollback plan cannot be generated for a source plan that predates
	// stored routing metadata; the posted error is the command's answer, and a
	// re-drive against the same stored plan would only re-post it.
	t.Run("deterministic plan-generation failure is terminal", func(t *testing.T) {
		client, mux := setupGitHubServer(t)
		comments := recordComments(t, mux)
		lockStore := &rollbackTestLockStore{}
		apply := completedSourceApply()
		apply.Deployment = "tenant-a"
		plan := capturedSourcePlan()
		plan.Database = apply.Database
		plan.DatabaseType = apply.DatabaseType
		plan.Environment = apply.Environment
		st := &rollbackTestStorage{
			applies: &rollbackTestApplyStore{apply: apply},
			plans:   &rollbackTestPlanStore{plan: plan},
			tasks:   &rollbackTestTaskStore{tasks: []*storage.Task{latestCompletedSourceTask()}},
			locks:   lockStore,
		}
		h := unlockTestHandler(t, st, ghclient.NewInstallationClient(client, testLogger()))

		retry, err := h.rollbackCommandCore(t.Context(), "octocat/hello-world", 1, 12345, "testuser", rollbackCommand())

		require.NoError(t, err)
		assert.False(t, retry, "a deterministic plan-generation failure is the command's answer; re-driving would re-post the same error")
		assert.Equal(t, 1, lockStore.releaseCalls, "the command-acquired lock must be released after the failed plan")
		body := requireComment(t, comments, "plan-generation failure comment")
		assert.Contains(t, body, "missing server-side routing metadata")
	})

	// A stored plan whose identity fields do not match the apply it is being
	// rolled back for is corrupt cross-reference data a re-drive re-reads
	// unchanged; the posted error is the command's answer.
	t.Run("mismatched source plan is terminal", func(t *testing.T) {
		client, mux := setupGitHubServer(t)
		comments := recordComments(t, mux)
		lockStore := &rollbackTestLockStore{}
		st := validRollbackStorage(lockStore)
		h := unlockTestHandler(t, st, ghclient.NewInstallationClient(client, testLogger()))

		retry, err := h.rollbackCommandCore(t.Context(), "octocat/hello-world", 1, 12345, "testuser", rollbackCommand())

		require.NoError(t, err)
		assert.False(t, retry, "a source plan that belongs to another database is a data invariant violation a re-drive re-reads unchanged")
		assert.Equal(t, 1, lockStore.releaseCalls, "the command-acquired lock must be released after the failed plan")
		body := requireComment(t, comments, "plan mismatch comment")
		assert.Contains(t, body, "belongs to")
	})

	// An apply that predates stored deployment metadata cannot be routed to
	// its engine; the stored record does not change between re-drives.
	t.Run("missing stored deployment is terminal", func(t *testing.T) {
		client, mux := setupGitHubServer(t)
		comments := recordComments(t, mux)
		lockStore := &rollbackTestLockStore{}
		apply := completedSourceApply()
		plan := capturedSourcePlan()
		plan.Database = apply.Database
		plan.DatabaseType = apply.DatabaseType
		plan.Environment = apply.Environment
		st := &rollbackTestStorage{
			applies: &rollbackTestApplyStore{apply: apply},
			plans:   &rollbackTestPlanStore{plan: plan},
			tasks:   &rollbackTestTaskStore{tasks: []*storage.Task{latestCompletedSourceTask()}},
			locks:   lockStore,
		}
		h := unlockTestHandler(t, st, ghclient.NewInstallationClient(client, testLogger()))

		retry, err := h.rollbackCommandCore(t.Context(), "octocat/hello-world", 1, 12345, "testuser", rollbackCommand())

		require.NoError(t, err)
		assert.False(t, retry, "an apply without stored deployment metadata cannot be routed on any attempt")
		assert.Equal(t, 1, lockStore.releaseCalls, "the command-acquired lock must be released after the failed plan")
		body := requireComment(t, comments, "missing deployment comment")
		assert.Contains(t, body, "rollback source apply is invalid")
		assert.Contains(t, body, "missing stored deployment metadata")
	})

	// The engine's rejection of the rollback plan request is deterministic for
	// the same stored plan: re-driving would re-send the same doomed RPC, so
	// the posted error is the command's answer.
	t.Run("deterministic remote plan rejection is terminal", func(t *testing.T) {
		client, mux := setupGitHubServer(t)
		comments := recordComments(t, mux)
		lockStore := &rollbackTestLockStore{}
		st := remotePlanRollbackStorage(lockStore)
		h := unlockTestHandler(t, st, ghclient.NewInstallationClient(client, testLogger()))
		h.service.RegisterTernClient("tenant-a", "staging",
			&rollbackPlanTernClient{planErr: grpcstatus.Error(grpccodes.InvalidArgument, "rollback DDL rejected by engine")})

		retry, err := h.rollbackCommandCore(t.Context(), "octocat/hello-world", 1, 12345, "testuser", rollbackCommand())

		require.NoError(t, err)
		assert.False(t, retry, "a deterministic engine rejection is the command's answer; re-driving would re-send the same doomed RPC")
		assert.Equal(t, 1, lockStore.releaseCalls, "the command-acquired lock must be released after the failed plan")
		body := requireComment(t, comments, "engine rejection comment")
		assert.Contains(t, body, "rollback DDL rejected by engine")
	})

	t.Run("lock held by another PR is terminal", func(t *testing.T) {
		client, mux := setupGitHubServer(t)
		comments := recordComments(t, mux)
		lockStore := &rollbackTestLockStore{lock: &storage.Lock{
			DatabaseName: "orders",
			DatabaseType: storage.DatabaseTypeMySQL,
			Repository:   "octocat/other-repo",
			PullRequest:  7,
			Owner:        "octocat/other-repo#7",
		}}
		st := validRollbackStorage(lockStore)
		h := unlockTestHandler(t, st, ghclient.NewInstallationClient(client, testLogger()))

		retry, err := h.rollbackCommandCore(t.Context(), "octocat/hello-world", 1, 12345, "testuser", rollbackCommand())

		require.NoError(t, err)
		assert.False(t, retry, "another PR's lock ownership is a deterministic rejection the same input always reproduces")
		assert.Zero(t, lockStore.acquireCalls, "another PR's lock must not be overwritten")
		body := requireComment(t, comments, "blocked-by-lock comment")
		assert.Contains(t, body, "Rollback Blocked")
		assert.Contains(t, body, "octocat/other-repo#7")
	})

	// An actor the gate evaluated and denied gets the same denial on every
	// re-drive: the posted rejection is the command's answer.
	t.Run("authorization block on the merits is terminal", func(t *testing.T) {
		client, mux := setupGitHubServer(t)
		comments := recordComments(t, mux)
		lockStore := &rollbackTestLockStore{}
		st := validRollbackStorage(lockStore)
		cfg := actorAuthTestConfig(true, func(cfg *api.ServerConfig) {
			cfg.PRCommandAuthorization.AdminUsers = []string{"hubot"}
		})
		h := actorAuthStorageTestHandler(cfg, st, ghclient.NewInstallationClient(client, testLogger()))

		retry, err := h.rollbackCommandCore(t.Context(), "octocat/hello-world", 1, 12345, "mona", rollbackCommand())

		require.NoError(t, err)
		assert.False(t, retry, "an evaluated denial is the command's answer; re-driving would re-deny")
		assert.Zero(t, lockStore.acquireCalls, "a denied rollback must not acquire any lock")
		body := requireComment(t, comments, "authorization denial comment")
		assert.Contains(t, body, "SchemaBot Command Not Authorized")
	})
}
