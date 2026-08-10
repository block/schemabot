package webhook

import (
	"context"
	"errors"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/block/schemabot/pkg/api"
	ghclient "github.com/block/schemabot/pkg/github"
	"github.com/block/schemabot/pkg/state"
	"github.com/block/schemabot/pkg/storage"
	"github.com/block/schemabot/pkg/webhook/action"
)

// rollbackCommandCore returns a durability disposition (retry, err) that a
// future durable issue_comment driver consumes. The synchronous goSafe wrapper
// discards it, so these tests pin the contract directly on the core.
//
// The retryable plan-generation disposition (remote-deployment unavailability
// inside ExecuteRollbackPlanForApply) is not exercised here: producing it
// requires a live remote gRPC target, which this layer fakes at the storage
// boundary. The deterministic plan-generation rejection, and every other exit
// site, are pinned below — deterministic answers are terminal, infrastructure
// failures are retryable.

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

	acquireCalls int
	releaseCalls int
}

func (s *rollbackTestLockStore) Get(_ context.Context, _, _ string) (*storage.Lock, error) {
	return s.lock, s.getErr
}

func (s *rollbackTestLockStore) Acquire(_ context.Context, _ *storage.Lock) error {
	s.acquireCalls++
	return s.acquireErr
}

func (s *rollbackTestLockStore) Release(_ context.Context, _, _, _ string) error {
	s.releaseCalls++
	return nil
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
		assert.Contains(t, body, "Failed to look up apply")
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
		assert.Contains(t, body, "load source plan")
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
		assert.Contains(t, body, "load source plan")
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
		assert.Contains(t, body, "Failed to check lock status")
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
		assert.Contains(t, body, "Failed to acquire lock")
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
