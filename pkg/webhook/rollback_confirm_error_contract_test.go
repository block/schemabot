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
	"github.com/block/schemabot/pkg/storage"
	"github.com/block/schemabot/pkg/webhook/action"
)

// rollbackConfirmCommandCore returns a durability disposition (retry, err)
// that a future durable issue_comment driver consumes. The synchronous goSafe
// wrapper discards it, so these tests pin the contract directly on the core.
//
// Exits at or after the ExecuteApply dispatch are terminal regardless of
// outcome because a re-drive could double-execute rollback DDL; the dispatch
// failure test pins that boundary. Post-acceptance exits (a lost apply row, a
// failed check update) require an accepted apply from a live engine, which
// this layer fakes at the storage boundary, so they are not exercised here.
// Also unexercised, terminal by inspection: the mid-command GitHub App
// factory re-resolution (reachable only when the deployment config changes
// between the bootstrap and execution) and the post-dispatch not-accepted
// guards, which are defensive against the concrete api.Service.

// rollbackConfirmTestStorage provides the stores the rollback-confirm command
// core touches: locks for the pinned-rollback resolution and release, and
// plans for loading the pinned rollback plan.
type rollbackConfirmTestStorage struct {
	emptyStorage
	locks storage.LockStore
	plans storage.PlanStore
}

func (s *rollbackConfirmTestStorage) Locks() storage.LockStore { return s.locks }
func (s *rollbackConfirmTestStorage) Plans() storage.PlanStore { return s.plans }

type rollbackConfirmTestLockStore struct {
	storage.LockStore
	locks       []*storage.Lock
	listErr     error
	releaseErr  error
	releaseNoOp bool

	releaseCalls int
}

func (s *rollbackConfirmTestLockStore) GetByPR(_ context.Context, _ string, _ int) ([]*storage.Lock, error) {
	if s.listErr != nil {
		return nil, s.listErr
	}
	return s.locks, nil
}

func (s *rollbackConfirmTestLockStore) ReleaseIfPendingPlanID(_ context.Context, _, _, _, _ string) (bool, error) {
	s.releaseCalls++
	if s.releaseErr != nil {
		return false, s.releaseErr
	}
	return !s.releaseNoOp, nil
}

type rollbackConfirmTestPlanStore struct {
	storage.PlanStore
	plans map[string]*storage.Plan
	err   error
}

func (s *rollbackConfirmTestPlanStore) Get(_ context.Context, planIdentifier string) (*storage.Plan, error) {
	if s.err != nil {
		return nil, s.err
	}
	return s.plans[planIdentifier], nil
}

// pinnedRollbackLock is a lock pinned by a preceding rollback command for the
// requesting PR, positioned for rollback-confirm to resolve.
func pinnedRollbackLock() *storage.Lock {
	return &storage.Lock{
		DatabaseName:  "orders",
		DatabaseType:  storage.DatabaseTypeMySQL,
		Owner:         "octocat/hello-world#1",
		Repository:    "octocat/hello-world",
		PullRequest:   1,
		PendingPlanID: "rollback:rbplan-1",
	}
}

// pinnedRollbackPlan is the rollback plan the lock pins, with one DDL change
// remaining so the command proceeds to execution.
func pinnedRollbackPlan() *storage.Plan {
	return &storage.Plan{
		ID:             20,
		PlanIdentifier: "rbplan-1",
		Database:       "orders",
		DatabaseType:   storage.DatabaseTypeMySQL,
		Environment:    "staging",
		Repository:     "octocat/hello-world",
		PullRequest:    1,
		Namespaces: map[string]*storage.NamespacePlanData{
			"default": {Tables: []storage.TableChange{{
				Table:     "users",
				DDL:       "ALTER TABLE `users` DROP COLUMN `email`",
				Operation: "alter",
			}}},
		},
	}
}

// emptyPinnedRollbackPlan is a pinned rollback plan with nothing left to roll
// back, positioned for the release-and-notify exit.
func emptyPinnedRollbackPlan() *storage.Plan {
	plan := pinnedRollbackPlan()
	plan.Namespaces = map[string]*storage.NamespacePlanData{"default": {}}
	return plan
}

// pinnedRollbackStorage returns storage where the pinned rollback lock and
// plan resolve cleanly for the requesting PR.
func pinnedRollbackStorage(locks *rollbackConfirmTestLockStore, plan *storage.Plan) *rollbackConfirmTestStorage {
	return &rollbackConfirmTestStorage{
		locks: locks,
		plans: &rollbackConfirmTestPlanStore{plans: map[string]*storage.Plan{plan.PlanIdentifier: plan}},
	}
}

func rollbackConfirmCommand() CommandResult {
	return CommandResult{Action: action.RollbackConfirm, Environment: "staging"}
}

// A command bootstrap failure (here, the per-installation GitHub client cannot
// be created) is a transient infrastructure failure the same delivery could
// clear on a later attempt, so the core must report it as retryable with a
// non-nil error.
func TestRollbackConfirmCommandCoreBootstrapFailureIsRetryable(t *testing.T) {
	h := &Handler{
		ghClients: ghclient.NewSingleClientSet(defaultAppName, &fakeClientFactory{
			forInstallationErr: errors.New("installation token unavailable"),
		}),
		logger: testLogger(),
	}

	retry, err := h.rollbackConfirmCommandCore(t.Context(), "octocat/hello-world", 1, "staging", 12345, "hubot", rollbackConfirmCommand())

	require.Error(t, err)
	assert.True(t, retry, "a command bootstrap failure is a transient infra failure a durable driver should re-drive")
}

// A GitHub App resolution failure inside the bootstrap is deterministic per
// deployment config — the same repo resolves to the same missing App on every
// attempt — so the core must report it as terminal rather than re-driving a
// delivery that can only fail until an operator fixes the config. The command
// never ran and no PR comment could be posted, so the core also returns the
// error: the delivery is recorded as failed (its only triage trail) rather
// than completed.
func TestRollbackConfirmCommandCoreAppResolutionFailureIsTerminal(t *testing.T) {
	h := &Handler{
		ghClients: ghclient.NewClientSet(nil),
		logger:    testLogger(),
	}

	retry, err := h.rollbackConfirmCommandCore(t.Context(), "octocat/hello-world", 1, "staging", 12345, "hubot", rollbackConfirmCommand())

	require.Error(t, err)
	assert.ErrorIs(t, err, errGitHubAppResolution)
	assert.False(t, retry, "a deterministic GitHub App resolution failure must not be re-driven; recovery is fixing the deployment config")
}

// Transient infrastructure failures — a storage read inside the pinned-plan
// resolution, an unevaluable authorization gate, or a failed lock release when
// nothing is left to roll back — are failures the same delivery could clear on
// a later attempt, so the core must report them as retryable with a non-nil
// error.
func TestRollbackConfirmCommandCoreTransientFailuresAreRetryable(t *testing.T) {
	t.Run("lock list read failure is retryable", func(t *testing.T) {
		client, mux := setupGitHubServer(t)
		comments := recordComments(t, mux)
		st := &rollbackConfirmTestStorage{
			locks: &rollbackConfirmTestLockStore{listErr: errors.New("storage read failed")},
		}
		h := unlockTestHandler(t, st, ghclient.NewInstallationClient(client, testLogger()))

		retry, err := h.rollbackConfirmCommandCore(t.Context(), "octocat/hello-world", 1, "staging", 12345, "testuser", rollbackConfirmCommand())

		require.Error(t, err)
		assert.True(t, retry, "a transient lock list read failure must stay retryable for a durable driver")
		body := requireComment(t, comments, "lock list failure comment")
		assert.Contains(t, body, "list locks")
	})

	t.Run("pinned plan read failure is retryable", func(t *testing.T) {
		client, mux := setupGitHubServer(t)
		comments := recordComments(t, mux)
		st := &rollbackConfirmTestStorage{
			locks: &rollbackConfirmTestLockStore{locks: []*storage.Lock{pinnedRollbackLock()}},
			plans: &rollbackConfirmTestPlanStore{err: errors.New("storage read failed")},
		}
		h := unlockTestHandler(t, st, ghclient.NewInstallationClient(client, testLogger()))

		retry, err := h.rollbackConfirmCommandCore(t.Context(), "octocat/hello-world", 1, "staging", 12345, "testuser", rollbackConfirmCommand())

		require.Error(t, err)
		assert.True(t, retry, "a transient pinned-plan read failure must stay retryable, not become the command's answer")
		body := requireComment(t, comments, "pinned plan read failure comment")
		assert.Contains(t, body, "load rollback plan")
	})

	// A gate that could not evaluate its inputs fails closed for this delivery
	// but is not the command's answer: the same delivery may authorize cleanly
	// once the membership read succeeds.
	t.Run("authorization evaluation failure is retryable", func(t *testing.T) {
		client, mux := setupGitHubServer(t)
		comments := recordComments(t, mux)
		mux.HandleFunc("GET /orgs/octocat/teams/schema-admins/members", teamMembersHandler(t, http.StatusForbidden))
		lockStore := &rollbackConfirmTestLockStore{locks: []*storage.Lock{pinnedRollbackLock()}}
		st := pinnedRollbackStorage(lockStore, pinnedRollbackPlan())
		cfg := actorAuthTestConfig(true, func(cfg *api.ServerConfig) {
			cfg.PRCommandAuthorization.AdminTeams = []string{"octocat/schema-admins"}
		})
		h := actorAuthStorageTestHandler(cfg, st, ghclient.NewInstallationClient(client, testLogger()))

		retry, err := h.rollbackConfirmCommandCore(t.Context(), "octocat/hello-world", 1, "staging", 12345, "mona", rollbackConfirmCommand())

		require.Error(t, err)
		assert.True(t, retry, "an unevaluable authorization gate fails closed for this delivery but stays retryable")
		assert.Zero(t, lockStore.releaseCalls, "no lock may be released while authorization is unknown")
		body := requireComment(t, comments, "authorization failure comment")
		assert.Contains(t, body, "SchemaBot Authorization Check Failed")
	})

	// Nothing has executed yet when there is nothing left to roll back, so a
	// failed lock release stays retryable: a re-drive re-resolves the pinned
	// plan and retries the release instead of stranding the lock.
	t.Run("failed lock release with nothing to roll back is retryable", func(t *testing.T) {
		client, mux := setupGitHubServer(t)
		comments := recordComments(t, mux)
		lockStore := &rollbackConfirmTestLockStore{
			locks:      []*storage.Lock{pinnedRollbackLock()},
			releaseErr: errors.New("storage write failed"),
		}
		st := pinnedRollbackStorage(lockStore, emptyPinnedRollbackPlan())
		h := unlockTestHandler(t, st, ghclient.NewInstallationClient(client, testLogger()))

		retry, err := h.rollbackConfirmCommandCore(t.Context(), "octocat/hello-world", 1, "staging", 12345, "testuser", rollbackConfirmCommand())

		require.Error(t, err)
		assert.True(t, retry, "a failed release before any execution must stay retryable so a re-drive can release the lock")
		assert.Equal(t, 1, lockStore.releaseCalls)
		body := requireComment(t, comments, "release failure comment")
		assert.Contains(t, body, "failed to release the lock")
	})
}

// Terminal outcomes are the command's answer — no pending rollback, a
// deterministic pinned-plan rejection, an authorization block on the merits,
// nothing left to roll back, or a dispatch failure once execution has been
// attempted — so the core reports them as (retry=false, err=nil): a durable
// driver must not re-drive them.
func TestRollbackConfirmCommandCoreTerminalDispositions(t *testing.T) {
	t.Run("no pending rollback is terminal", func(t *testing.T) {
		client, mux := setupGitHubServer(t)
		comments := recordComments(t, mux)
		st := &rollbackConfirmTestStorage{locks: &rollbackConfirmTestLockStore{}}
		h := unlockTestHandler(t, st, ghclient.NewInstallationClient(client, testLogger()))

		retry, err := h.rollbackConfirmCommandCore(t.Context(), "octocat/hello-world", 1, "staging", 12345, "testuser", rollbackConfirmCommand())

		require.NoError(t, err)
		assert.False(t, retry, "a re-drive cannot conjure a pending rollback; the no-lock answer is terminal")
		body := requireComment(t, comments, "no pending rollback comment")
		assert.Contains(t, body, "No Lock Found")
	})

	// On an aggregate repo an unscoped rollback-confirm fans out to every
	// deployment, but only the deployment holding the pinned rollback lock has
	// anything to confirm; one with no pending rollback stays silent so only
	// the owning deployment answers — a deliberate silent no-op, not a failure.
	t.Run("unowned unscoped fan-out is terminal and silent", func(t *testing.T) {
		client, mux := setupGitHubServer(t)
		comments := recordComments(t, mux)
		st := &rollbackConfirmTestStorage{locks: &rollbackConfirmTestLockStore{}}
		h := actorAuthStorageTestHandler(aggregateLeaderConfig(), st, ghclient.NewInstallationClient(client, testLogger()))

		retry, err := h.rollbackConfirmCommandCore(t.Context(), "octocat/hello-world", 1, "staging", 12345, "testuser", rollbackConfirmCommand())

		require.NoError(t, err)
		assert.False(t, retry, "a non-owning fan-out skip is the command's terminal answer, not a retryable failure")
		assert.Empty(t, comments, "the terminal skip must stay silent")
	})

	// A rollback lock held by another owner is a deterministic rejection: a
	// re-drive re-reads the same foreign lock and reproduces the answer.
	t.Run("foreign rollback lock is terminal", func(t *testing.T) {
		client, mux := setupGitHubServer(t)
		comments := recordComments(t, mux)
		lock := pinnedRollbackLock()
		lock.Owner = "octocat/hello-world#2"
		st := pinnedRollbackStorage(&rollbackConfirmTestLockStore{locks: []*storage.Lock{lock}}, pinnedRollbackPlan())
		h := unlockTestHandler(t, st, ghclient.NewInstallationClient(client, testLogger()))

		retry, err := h.rollbackConfirmCommandCore(t.Context(), "octocat/hello-world", 1, "staging", 12345, "testuser", rollbackConfirmCommand())

		require.NoError(t, err)
		assert.False(t, retry, "a lock held by another PR is a deterministic rejection a re-drive reproduces")
		body := requireComment(t, comments, "foreign lock rejection comment")
		assert.Contains(t, body, "belongs to octocat/hello-world#2")
	})

	// A pinned plan that no longer matches the lock's database is a
	// deterministic rejection: the pin itself is wrong, and a re-drive
	// re-reads the same mismatch.
	t.Run("mismatched pinned plan is terminal", func(t *testing.T) {
		client, mux := setupGitHubServer(t)
		comments := recordComments(t, mux)
		plan := pinnedRollbackPlan()
		plan.Database = "billing"
		st := pinnedRollbackStorage(&rollbackConfirmTestLockStore{locks: []*storage.Lock{pinnedRollbackLock()}}, plan)
		h := unlockTestHandler(t, st, ghclient.NewInstallationClient(client, testLogger()))

		retry, err := h.rollbackConfirmCommandCore(t.Context(), "octocat/hello-world", 1, "staging", 12345, "testuser", rollbackConfirmCommand())

		require.NoError(t, err)
		assert.False(t, retry, "a mismatched pinned plan is a deterministic rejection a re-drive reproduces")
		body := requireComment(t, comments, "mismatched plan rejection comment")
		assert.Contains(t, body, "mismatched pinned plan")
	})

	// A pinned plan whose row is missing is a dangling pin a re-drive re-reads
	// unchanged; recovery is an operator running unlock, not a retry.
	t.Run("dangling pinned plan is terminal", func(t *testing.T) {
		client, mux := setupGitHubServer(t)
		comments := recordComments(t, mux)
		st := &rollbackConfirmTestStorage{
			locks: &rollbackConfirmTestLockStore{locks: []*storage.Lock{pinnedRollbackLock()}},
			plans: &rollbackConfirmTestPlanStore{},
		}
		h := unlockTestHandler(t, st, ghclient.NewInstallationClient(client, testLogger()))

		retry, err := h.rollbackConfirmCommandCore(t.Context(), "octocat/hello-world", 1, "staging", 12345, "testuser", rollbackConfirmCommand())

		require.NoError(t, err)
		assert.False(t, retry, "a dangling plan pin is a deterministic rejection a re-drive reproduces")
		body := requireComment(t, comments, "dangling plan rejection comment")
		assert.Contains(t, body, "rollback plan not found")
	})

	// Two pending rollback plans for the same environment need the user to
	// cancel one; a re-drive re-reads both pins and reproduces the answer.
	t.Run("multiple pending rollback plans are terminal", func(t *testing.T) {
		client, mux := setupGitHubServer(t)
		comments := recordComments(t, mux)
		lockA := pinnedRollbackLock()
		lockB := pinnedRollbackLock()
		lockB.DatabaseName = "billing"
		lockB.PendingPlanID = "rollback:rbplan-2"
		planA := pinnedRollbackPlan()
		planB := pinnedRollbackPlan()
		planB.PlanIdentifier = "rbplan-2"
		planB.Database = "billing"
		st := &rollbackConfirmTestStorage{
			locks: &rollbackConfirmTestLockStore{locks: []*storage.Lock{lockA, lockB}},
			plans: &rollbackConfirmTestPlanStore{plans: map[string]*storage.Plan{
				"rbplan-1": planA,
				"rbplan-2": planB,
			}},
		}
		h := unlockTestHandler(t, st, ghclient.NewInstallationClient(client, testLogger()))

		retry, err := h.rollbackConfirmCommandCore(t.Context(), "octocat/hello-world", 1, "staging", 12345, "testuser", rollbackConfirmCommand())

		require.NoError(t, err)
		assert.False(t, retry, "ambiguous pending rollbacks need a user decision; the rejection is terminal")
		body := requireComment(t, comments, "multiple plans rejection comment")
		assert.Contains(t, body, "multiple rollback plans are pending")
	})

	// A gate that evaluated its inputs and blocked on the merits is the
	// command's answer: the same actor gets the same block on every re-drive.
	t.Run("authorization block on the merits is terminal", func(t *testing.T) {
		client, mux := setupGitHubServer(t)
		comments := recordComments(t, mux)
		lockStore := &rollbackConfirmTestLockStore{locks: []*storage.Lock{pinnedRollbackLock()}}
		st := pinnedRollbackStorage(lockStore, pinnedRollbackPlan())
		cfg := actorAuthTestConfig(true, func(cfg *api.ServerConfig) {
			cfg.PRCommandAuthorization.AdminUsers = []string{"hubot"}
		})
		h := actorAuthStorageTestHandler(cfg, st, ghclient.NewInstallationClient(client, testLogger()))

		retry, err := h.rollbackConfirmCommandCore(t.Context(), "octocat/hello-world", 1, "staging", 12345, "mona", rollbackConfirmCommand())

		require.NoError(t, err)
		assert.False(t, retry, "an authorization block on the merits is the command's answer, not a transient failure")
		assert.Zero(t, lockStore.releaseCalls, "a blocked command must not release the pinned lock")
		body := requireComment(t, comments, "authorization block comment")
		assert.Contains(t, body, "SchemaBot Command Not Authorized")
	})

	// Nothing left to roll back releases the lock and answers; a re-drive
	// would find no pinned rollback and give the no-lock answer anyway.
	t.Run("nothing to roll back with released lock is terminal", func(t *testing.T) {
		client, mux := setupGitHubServer(t)
		comments := recordComments(t, mux)
		lockStore := &rollbackConfirmTestLockStore{locks: []*storage.Lock{pinnedRollbackLock()}}
		st := pinnedRollbackStorage(lockStore, emptyPinnedRollbackPlan())
		h := unlockTestHandler(t, st, ghclient.NewInstallationClient(client, testLogger()))

		retry, err := h.rollbackConfirmCommandCore(t.Context(), "octocat/hello-world", 1, "staging", 12345, "testuser", rollbackConfirmCommand())

		require.NoError(t, err)
		assert.False(t, retry, "an already-rolled-back answer with a released lock is terminal")
		assert.Equal(t, 1, lockStore.releaseCalls)
		body := requireComment(t, comments, "already rolled back comment")
		assert.Contains(t, body, "Already Rolled Back")
	})

	// A pinned rollback lock already released — or replaced by a newer
	// same-owner intent — between resolution and release makes the conditional
	// release a no-op: the answer is still that nothing is left to roll back,
	// and the current lock state stays untouched.
	t.Run("nothing to roll back with superseded pinned lock is terminal", func(t *testing.T) {
		client, mux := setupGitHubServer(t)
		comments := recordComments(t, mux)
		lockStore := &rollbackConfirmTestLockStore{
			locks:       []*storage.Lock{pinnedRollbackLock()},
			releaseNoOp: true,
		}
		st := pinnedRollbackStorage(lockStore, emptyPinnedRollbackPlan())
		h := unlockTestHandler(t, st, ghclient.NewInstallationClient(client, testLogger()))

		retry, err := h.rollbackConfirmCommandCore(t.Context(), "octocat/hello-world", 1, "staging", 12345, "testuser", rollbackConfirmCommand())

		require.NoError(t, err)
		assert.False(t, retry, "a no-op conditional release is terminal; the release's job is already done")
		assert.Equal(t, 1, lockStore.releaseCalls)
		body := requireComment(t, comments, "already rolled back comment")
		assert.Contains(t, body, "Already Rolled Back")
	})

	// Once the ExecuteApply dispatch is attempted, the rollback DDL may
	// already be executing on the target, so even a dispatch failure must not
	// be re-driven: the pinned lock survives and the user re-issues the
	// command after triage. The observer the command registered for the apply
	// it did not create is withdrawn, so it cannot attach to the next apply on
	// the target. The plan here names a deployment but no target, which the
	// apply request rejects as missing routing metadata.
	t.Run("dispatch failure is terminal", func(t *testing.T) {
		client, mux := setupGitHubServer(t)
		comments := recordComments(t, mux)
		plan := pinnedRollbackPlan()
		plan.Deployment = "default"
		lockStore := &rollbackConfirmTestLockStore{locks: []*storage.Lock{pinnedRollbackLock()}}
		st := pinnedRollbackStorage(lockStore, plan)
		h := unlockTestHandler(t, st, ghclient.NewInstallationClient(client, testLogger()))

		retry, err := h.rollbackConfirmCommandCore(t.Context(), "octocat/hello-world", 1, "staging", 12345, "testuser", rollbackConfirmCommand())

		require.NoError(t, err)
		assert.False(t, retry, "an attempted dispatch must never be re-driven; a re-drive could double-execute rollback DDL")
		assert.Zero(t, lockStore.releaseCalls, "the pinned lock must survive a dispatch failure so the user can re-issue the command")
		body := requireComment(t, comments, "dispatch failure comment")
		assert.Contains(t, body, "Plan `rbplan-1` was stored without its `target`")
		assert.False(t, h.service.HasPendingObserver("orders", "default", "staging"),
			"a failed dispatch must withdraw the observer it registered for the apply it did not create")
	})
}

// A rollback plan stored before plans recorded their routing has no
// deployment, so the apply it pins can never be dispatched, and confirming it
// again would be refused the same way. The comment names the plan and the
// missing field from what SchemaBot stored, then the commands that replace the
// plan; the refusal's own text, which tells an API caller to "retry apply",
// never reaches the PR.
func TestRollbackConfirmLegacyPlanWithoutRoutingNamesTheReplan(t *testing.T) {
	client, mux := setupGitHubServer(t)
	comments := recordComments(t, mux)
	lockStore := &rollbackConfirmTestLockStore{locks: []*storage.Lock{pinnedRollbackLock()}}
	st := pinnedRollbackStorage(lockStore, pinnedRollbackPlan())
	h := unlockTestHandler(t, st, ghclient.NewInstallationClient(client, testLogger()))

	retry, err := h.rollbackConfirmCommandCore(t.Context(), "octocat/hello-world", 1, "staging", 12345, "testuser", rollbackConfirmCommand())

	require.NoError(t, err)
	assert.False(t, retry, "an attempted dispatch must never be re-driven")
	body := requireComment(t, comments, "legacy rollback plan refusal comment")
	assert.Contains(t, body, "Plan `rbplan-1` was stored without its `deployment`, so SchemaBot cannot route it and nothing was applied. "+
		"Run `schemabot rollback -e staging` followed by the apply ID from the rollback plan comment to create a new rollback plan, then confirm it with `schemabot rollback-confirm -e staging`.")
	assert.NotContains(t, body, "server-side routing metadata")
	assert.NotContains(t, body, "retry apply")
	assert.NotContains(t, body, "Failed to execute rollback")
}

// A rollback-confirm that asked for deferred cutover on a database type that
// cannot defer it is refused before anything is stored, and the refusal is
// deterministic: the same command would be refused the same way. The comment
// therefore carries the operator's next action — the rollback-confirm to
// re-issue in the same environment without `--defer-cutover` — and says the
// pinned rollback it found is still there for that re-issue, instead of only
// naming the refused feature.
func TestRollbackConfirmDeferredCutoverRefusalNamesTheReissue(t *testing.T) {
	client, mux := setupGitHubServer(t)
	comments := recordComments(t, mux)
	lock := pinnedRollbackLock()
	lock.DatabaseType = storage.DatabaseTypePostgres
	plan := pinnedRollbackPlan()
	plan.DatabaseType = storage.DatabaseTypePostgres
	plan.Deployment = "default"
	plan.Target = "orders-primary"
	lockStore := &rollbackConfirmTestLockStore{locks: []*storage.Lock{lock}}
	st := pinnedRollbackStorage(lockStore, plan)
	h := unlockTestHandler(t, st, ghclient.NewInstallationClient(client, testLogger()))
	cmd := rollbackConfirmCommand()
	cmd.DeferCutover = true

	retry, err := h.rollbackConfirmCommandCore(t.Context(), "octocat/hello-world", 1, "staging", 12345, "testuser", cmd)

	require.NoError(t, err)
	assert.False(t, retry, "a feature the database type refuses is refused the same way on every re-drive")
	assert.Zero(t, lockStore.releaseCalls, "the pinned rollback must survive the refusal for the re-issued command")
	body := requireComment(t, comments, "deferred cutover refusal comment")
	assert.Contains(t, body, "deferred cutover is not supported for database_type: postgres")
	assert.Contains(t, body, "Run `schemabot rollback-confirm -e staging` again without `--defer-cutover`.")
	assert.Contains(t, body, "The pending rollback stays pinned for it.")
	assert.NotContains(t, body, "Failed to execute rollback")
	assert.False(t, h.service.HasPendingObserver("orders", "default", "staging"),
		"the refused command must withdraw the observer it registered, or the next apply on this target would report into this PR")
}
