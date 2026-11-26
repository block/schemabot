package webhook

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	grpccodes "google.golang.org/grpc/codes"
	grpcstatus "google.golang.org/grpc/status"

	"github.com/block/schemabot/pkg/api"
	ghclient "github.com/block/schemabot/pkg/github"
	ternv1 "github.com/block/schemabot/pkg/proto/ternv1"
	"github.com/block/schemabot/pkg/storage"
)

// rollbackInternalFailureText is the raw text of a storage or transport failure.
// It names an internal address, so it belongs in the server log and must never
// appear in a PR comment.
const rollbackInternalFailureText = "dial tcp 10.1.2.3:3306: connect: connection refused"

func assertNoInternalFailureText(t *testing.T, body string) {
	t.Helper()
	assert.NotContains(t, body, rollbackInternalFailureText)
	assert.NotContains(t, body, "10.1.2.3")
	assert.NotContains(t, body, "connection refused")
}

// A rollback that stops on an internal failure tells the PR which step failed and
// what to do next, and keeps the failure's own text, which can name internal
// addresses, in the server log. Each case fails a different step with the same
// raw text and checks that the comment carries only the fixed line.
func TestRollbackCommentsKeepInternalFailuresOutOfThePR(t *testing.T) {
	storageFailure := errors.New(rollbackInternalFailureText)

	t.Run("apply lookup", func(t *testing.T) {
		client, mux := setupGitHubServer(t)
		comments := recordComments(t, mux)
		st := &rollbackTestStorage{applies: &rollbackTestApplyStore{err: storageFailure}}
		h := unlockTestHandler(t, st, ghclient.NewInstallationClient(client, testLogger()))

		retry, err := h.rollbackCommandCore(t.Context(), "octocat/hello-world", 1, 12345, "testuser", rollbackCommand())

		require.Error(t, err)
		assert.True(t, retry)
		body := requireComment(t, comments, "apply lookup failure comment")
		assert.Contains(t, body, "SchemaBot could not look up the apply to roll back, so the rollback did not start. "+rollbackRetryAdvice)
		assertNoInternalFailureText(t, body)
	})

	t.Run("lock status read", func(t *testing.T) {
		client, mux := setupGitHubServer(t)
		comments := recordComments(t, mux)
		st := validRollbackStorage(&rollbackTestLockStore{getErr: storageFailure})
		h := unlockTestHandler(t, st, ghclient.NewInstallationClient(client, testLogger()))

		retry, err := h.rollbackCommandCore(t.Context(), "octocat/hello-world", 1, 12345, "testuser", rollbackCommand())

		require.Error(t, err)
		assert.True(t, retry)
		body := requireComment(t, comments, "lock status failure comment")
		assert.Contains(t, body, "SchemaBot could not check the lock for `orders`, so the rollback did not start. "+rollbackRetryAdvice)
		assertNoInternalFailureText(t, body)
	})

	t.Run("lock acquire", func(t *testing.T) {
		client, mux := setupGitHubServer(t)
		comments := recordComments(t, mux)
		st := validRollbackStorage(&rollbackTestLockStore{acquireErr: storageFailure})
		h := unlockTestHandler(t, st, ghclient.NewInstallationClient(client, testLogger()))

		retry, err := h.rollbackCommandCore(t.Context(), "octocat/hello-world", 1, 12345, "testuser", rollbackCommand())

		require.Error(t, err)
		assert.True(t, retry)
		body := requireComment(t, comments, "lock acquire failure comment")
		assert.Contains(t, body, "SchemaBot could not take the lock for `orders`, so the rollback did not start. "+rollbackRetryAdvice)
		assertNoInternalFailureText(t, body)
	})

	t.Run("source apply check", func(t *testing.T) {
		client, mux := setupGitHubServer(t)
		comments := recordComments(t, mux)
		st := &rollbackTestStorage{
			applies: &rollbackTestApplyStore{apply: completedSourceApply()},
			plans:   &rollbackTestPlanStore{err: storageFailure},
			tasks:   &rollbackTestTaskStore{},
		}
		h := unlockTestHandler(t, st, ghclient.NewInstallationClient(client, testLogger()))

		retry, err := h.rollbackCommandCore(t.Context(), "octocat/hello-world", 1, 12345, "testuser", rollbackCommand())

		require.Error(t, err)
		assert.True(t, retry)
		body := requireComment(t, comments, "source apply check failure comment")
		assert.Contains(t, body, "SchemaBot could not check that the apply can be rolled back, so the rollback did not start. "+rollbackRetryAdvice)
		assertNoInternalFailureText(t, body)
	})

	t.Run("rollback plan on an unreachable remote deployment", func(t *testing.T) {
		client, mux := setupGitHubServer(t)
		comments := recordComments(t, mux)
		st := remotePlanRollbackStorage(&rollbackTestLockStore{})
		h := unlockTestHandler(t, st, ghclient.NewInstallationClient(client, testLogger()))
		h.service.RegisterTernClient("tenant-a", "staging",
			&rollbackPlanTernClient{planErr: grpcstatus.Error(grpccodes.Unavailable, rollbackInternalFailureText)})

		retry, err := h.rollbackCommandCore(t.Context(), "octocat/hello-world", 1, 12345, "testuser", rollbackCommand())

		require.Error(t, err)
		assert.True(t, retry)
		body := requireComment(t, comments, "remote deployment failure comment")
		assert.Contains(t, body, "SchemaBot could not reach the remote deployment `tenant-a` for target `orders-staging`, so the rollback did not start. "+
			"Run the command again once that deployment is healthy; if it fails again, the SchemaBot server logs have the cause.")
		assertNoInternalFailureText(t, body)
	})

	t.Run("rollback plan the engine failed without a reason for the requester", func(t *testing.T) {
		client, mux := setupGitHubServer(t)
		comments := recordComments(t, mux)
		st := remotePlanRollbackStorage(&rollbackTestLockStore{})
		h := unlockTestHandler(t, st, ghclient.NewInstallationClient(client, testLogger()))
		h.service.RegisterTernClient("tenant-a", "staging",
			&rollbackPlanTernClient{planErr: grpcstatus.Error(grpccodes.Internal, rollbackInternalFailureText)})

		retry, err := h.rollbackCommandCore(t.Context(), "octocat/hello-world", 1, 12345, "testuser", rollbackCommand())

		require.NoError(t, err)
		assert.False(t, retry)
		body := requireComment(t, comments, "engine failure comment")
		assert.Contains(t, body, "SchemaBot could not create the rollback plan for `orders` in `staging`, so the rollback did not start. "+
			"Running the command again fails the same way until the cause is fixed; the SchemaBot server logs have the cause.")
		assert.NotContains(t, body, "Run the command again")
		assertNoInternalFailureText(t, body)
	})
}

// A site that can receive either a refusal SchemaBot wrote for the requester or
// an internal failure renders each as its own kind: the refusal keeps its text
// and the internal failure gets the fixed line.
func TestRollbackCommentsKeepRefusalsReadable(t *testing.T) {
	// A lock someone else took between the command's check and its acquire is a
	// refusal, not a storage failure, and it is the command's answer: a re-drive
	// would post the same comment again.
	t.Run("lock taken by someone else while the command ran", func(t *testing.T) {
		client, mux := setupGitHubServer(t)
		comments := recordComments(t, mux)
		st := validRollbackStorage(&rollbackTestLockStore{acquireErr: storage.ErrLockHeld})
		h := unlockTestHandler(t, st, ghclient.NewInstallationClient(client, testLogger()))

		retry, err := h.rollbackCommandCore(t.Context(), "octocat/hello-world", 1, 12345, "testuser", rollbackCommand())

		require.NoError(t, err)
		assert.False(t, retry)
		body := requireComment(t, comments, "lock conflict comment")
		assert.Contains(t, body, "Someone else took the lock for `orders` while this command ran, so the rollback did not start. "+
			"Run the command again to see who holds it.")
		assert.NotContains(t, body, "SchemaBot could not take the lock")
	})

	// The engine's refusal of a rollback plan is the answer for whoever edits the
	// schema files, so the requester sees its reason.
	t.Run("engine refusal of the rollback plan", func(t *testing.T) {
		client, mux := setupGitHubServer(t)
		comments := recordComments(t, mux)
		st := remotePlanRollbackStorage(&rollbackTestLockStore{})
		h := unlockTestHandler(t, st, ghclient.NewInstallationClient(client, testLogger()))
		h.service.RegisterTernClient("tenant-a", "staging",
			&rollbackPlanTernClient{planErr: grpcstatus.Error(grpccodes.FailedPrecondition, "table users is shared with another database")})

		retry, err := h.rollbackCommandCore(t.Context(), "octocat/hello-world", 1, 12345, "testuser", rollbackCommand())

		require.NoError(t, err)
		assert.False(t, retry)
		body := requireComment(t, comments, "engine refusal comment")
		assert.Contains(t, body, "table users is shared with another database")
		assert.NotContains(t, body, "SchemaBot could not create the rollback plan")
	})

	t.Run("rollback-confirm rejection of a lock another pull request owns", func(t *testing.T) {
		client, mux := setupGitHubServer(t)
		comments := recordComments(t, mux)
		lock := pinnedRollbackLock()
		lock.Owner = "octocat/hello-world#2"
		st := pinnedRollbackStorage(&rollbackConfirmTestLockStore{locks: []*storage.Lock{lock}}, pinnedRollbackPlan())
		h := unlockTestHandler(t, st, ghclient.NewInstallationClient(client, testLogger()))

		retry, err := h.rollbackConfirmCommandCore(t.Context(), "octocat/hello-world", 1, "staging", 12345, "testuser", rollbackConfirmCommand())

		require.NoError(t, err)
		assert.False(t, retry)
		body := requireComment(t, comments, "foreign lock rejection comment")
		assert.Contains(t, body, "belongs to octocat/hello-world#2")
		assert.NotContains(t, body, "SchemaBot could not read the pending rollback")
	})

	t.Run("rollback-confirm storage failure reading the pending rollback", func(t *testing.T) {
		client, mux := setupGitHubServer(t)
		comments := recordComments(t, mux)
		st := &rollbackConfirmTestStorage{
			locks: &rollbackConfirmTestLockStore{listErr: errors.New(rollbackInternalFailureText)},
		}
		h := unlockTestHandler(t, st, ghclient.NewInstallationClient(client, testLogger()))

		retry, err := h.rollbackConfirmCommandCore(t.Context(), "octocat/hello-world", 1, "staging", 12345, "testuser", rollbackConfirmCommand())

		require.Error(t, err)
		assert.True(t, retry)
		body := requireComment(t, comments, "pending rollback read failure comment")
		assert.Contains(t, body, "SchemaBot could not read the pending rollback for this pull request in `staging`, so nothing was confirmed and nothing ran. "+rollbackRetryAdvice)
		assertNoInternalFailureText(t, body)
	})
}

// The pin of a rollback plan on the lock runs only after the engine generated
// the plan, so its message is checked directly: a lock another pull request
// took is a refusal, and any other failure is named by its step alone.
func TestRollbackLockFailureMessage(t *testing.T) {
	step := "save the rollback plan on the lock for `orders`"

	assert.Equal(t,
		"SchemaBot could not save the rollback plan on the lock for `orders`, so the rollback did not start. "+rollbackRetryAdvice,
		rollbackLockFailureMessage(errors.New(rollbackInternalFailureText), "orders", step))
	assert.Equal(t,
		"Someone else took the lock for `orders` while this command ran, so the rollback did not start. "+
			"Run the command again to see who holds it.",
		rollbackLockFailureMessage(storage.ErrLockHeld, "orders", step))
}

// pinLockTakenStore takes the command's lock on the first Acquire and reports
// the lock held by someone else on the second, which pins the rollback plan.
type pinLockTakenStore struct {
	*rollbackTestLockStore
}

func (s *pinLockTakenStore) Acquire(ctx context.Context, lock *storage.Lock) error {
	if err := s.rollbackTestLockStore.Acquire(ctx, lock); err != nil {
		return err
	}
	if s.acquireCalls > 1 {
		return storage.ErrLockHeld
	}
	return nil
}

// planStoringPlanStore accepts the rollback plan the engine generated.
type planStoringPlanStore struct {
	*rollbackTestPlanStore
}

func (s *planStoringPlanStore) Create(context.Context, *storage.Plan) (int64, error) {
	return 11, nil
}

// Someone else taking the lock between the rollback plan and its pin is the
// command's answer: the command releases the lock it took, the comment names
// the refusal, and the command is not re-driven.
func TestRollbackPlanPinLockTakenIsTerminal(t *testing.T) {
	client, mux := setupGitHubServer(t)
	comments := recordComments(t, mux)
	lockStore := &pinLockTakenStore{rollbackTestLockStore: &rollbackTestLockStore{}}
	st := remotePlanRollbackStorage(lockStore)
	st.plans = &planStoringPlanStore{rollbackTestPlanStore: st.plans.(*rollbackTestPlanStore)}
	h := unlockTestHandler(t, st, ghclient.NewInstallationClient(client, testLogger()))
	h.service.RegisterTernClient("tenant-a", "staging", &rollbackPlanTernClient{planResp: &ternv1.PlanResponse{
		PlanId: "plan-rollback",
		Changes: []*ternv1.SchemaChange{{
			Namespace: "default",
			TableChanges: []*ternv1.TableChange{{
				Namespace:  "default",
				TableName:  "users",
				Ddl:        "ALTER TABLE `users` DROP INDEX `idx_email`",
				ChangeType: ternv1.ChangeType_CHANGE_TYPE_ALTER,
			}},
		}},
	}})

	retry, err := h.rollbackCommandCore(t.Context(), "octocat/hello-world", 1, 12345, "testuser", rollbackCommand())

	require.NoError(t, err)
	assert.False(t, retry, "a lock someone else took is the command's answer")
	assert.Equal(t, 2, lockStore.acquireCalls, "the command takes the lock, then pins the plan on it")
	assert.Equal(t, 1, lockStore.releaseCalls, "the command releases the lock it took")
	body := requireComment(t, comments, "pin lock taken comment")
	assert.Contains(t, body, "Someone else took the lock for `orders` while this command ran, so the rollback did not start.")
}

// A storage failure taking the lock is an internal error: the comment names the
// step and the rerun advice, and the command stays retryable.
func TestRollbackLockAcquireStorageFailureMessage(t *testing.T) {
	assert.Equal(t,
		"SchemaBot could not take the lock for `orders`, so the rollback did not start. "+rollbackRetryAdvice,
		rollbackLockFailureMessage(errors.New(rollbackInternalFailureText), "orders", "take the lock for `orders`"))
}

// A remote deployment the rollback plan request could not reach is named by the
// parts of it the error carries, and never with empty backticks.
func TestRemoteDeploymentUnavailableForRollbackMessage(t *testing.T) {
	const retry = " healthy; if it fails again, the SchemaBot server logs have the cause."

	for name, tc := range map[string]struct {
		err  *api.RemoteDeploymentUnavailableError
		want string
	}{
		"deployment and target": {
			err: &api.RemoteDeploymentUnavailableError{Deployment: "tenant-a", Target: "orders-staging"},
			want: "SchemaBot could not reach the remote deployment `tenant-a` for target `orders-staging`, so the rollback did not start. " +
				"Run the command again once that deployment is" + retry,
		},
		"deployment only": {
			err: &api.RemoteDeploymentUnavailableError{Deployment: "tenant-a"},
			want: "SchemaBot could not reach the remote deployment `tenant-a`, so the rollback did not start. " +
				"Run the command again once that deployment is" + retry,
		},
		"neither": {
			err: &api.RemoteDeploymentUnavailableError{},
			want: "SchemaBot could not reach the remote schema change service, so the rollback did not start. " +
				"Run the command again once the service is" + retry,
		},
	} {
		t.Run(name, func(t *testing.T) {
			got := remoteDeploymentUnavailableForRollbackMessage(tc.err)
			assert.Equal(t, tc.want, got)
			assert.NotContains(t, got, "``")
		})
	}
}

// The check-update line keeps the text SchemaBot wrote for a refusal and shows a
// fixed line for any other failure, so the storage or GitHub error stays in the
// server log.
func TestRollbackCheckUpdateFailedMessage(t *testing.T) {
	other := rollbackCheckUpdateFailedMessage(errors.New(rollbackInternalFailureText), "orders", "staging")
	assert.Equal(t,
		"The rollback for `orders` in `staging` was accepted, but SchemaBot could not update the required status check, so the check does not show it yet. "+
			"The rollback itself is not affected. Once the rollback finishes, run plan again to refresh the check; "+
			"if it still does not match, the SchemaBot server logs have the cause.",
		other)
	assert.NotContains(t, other, rollbackInternalFailureText)

	drift := rollbackCheckUpdateFailedMessage(fmt.Errorf("apply start refused for repo r pr 1: %w", errApplyStartDriftBlocked), "orders", "staging")
	assert.Equal(t,
		"The rollback for `orders` in `staging` was accepted, but the required status check still shows the deployment drift found at review time, "+
			"so SchemaBot left it as it was. The rollback itself is not affected. "+
			"Reconcile the deployments the check summary names, then run plan again.",
		drift)
	assert.NotContains(t, drift, "repo r pr 1")

	placement := rollbackCheckUpdateFailedMessage(fmt.Errorf("apply start refused for repo r pr 1: %w", errApplyStartPlacementBlocked), "orders", "staging")
	assert.Equal(t,
		"The rollback for `orders` in `staging` was accepted, but the required status check still shows that namespace placement refused the plan for this environment, "+
			"so SchemaBot left it as it was. The rollback itself is not affected. Fix the namespace placement, then run plan again.",
		placement)
	assert.NotContains(t, placement, "repo r pr 1")
}
