package webhook

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/block/schemabot/pkg/api"
	ghclient "github.com/block/schemabot/pkg/github"
	"github.com/block/schemabot/pkg/state"
	"github.com/block/schemabot/pkg/storage"
	"github.com/block/schemabot/pkg/webhook/action"
)

// unlockCommandCore returns a durability disposition (retry, err) that a future
// durable issue_comment driver consumes. The synchronous goSafe wrapper discards
// it, so these tests pin the contract directly on the core.

// activeApplyStore reports a non-terminal apply for every database, the shape
// of a lock still protecting an in-flight apply.
type activeApplyStore struct {
	storage.ApplyStore
}

func (s *activeApplyStore) GetByDatabase(_ context.Context, database, _, _ string) ([]*storage.Apply, error) {
	return []*storage.Apply{{
		Database:        database,
		Environment:     "staging",
		ApplyIdentifier: "apply-123",
		State:           state.Apply.Running,
	}}, nil
}

func prOwnedOrdersLock() *storage.Lock {
	return &storage.Lock{
		DatabaseName: "orders",
		DatabaseType: storage.DatabaseTypeMySQL,
		Repository:   "octocat/hello-world",
		PullRequest:  1,
		Owner:        "octocat/hello-world#1",
	}
}

// serveConfiglessRepo registers the GitHub routes config discovery walks on a
// repo with no schemabot.yaml anywhere: the PR itself touches no config file
// and the repo tree holds none, so discovery deterministically resolves to
// "no config found".
func serveConfiglessRepo(t *testing.T, mux *http.ServeMux) {
	t.Helper()
	mux.HandleFunc("GET /repos/octocat/hello-world/pulls/1", func(w http.ResponseWriter, _ *http.Request) {
		_, err := fmt.Fprint(w, `{"head":{"ref":"feature-branch","sha":"newsha222"},"base":{"ref":"main","sha":"def456"},"user":{"login":"testuser"}}`)
		require.NoError(t, err)
	})
	mux.HandleFunc("GET /repos/octocat/hello-world/pulls/1/files", func(w http.ResponseWriter, _ *http.Request) {
		_, err := fmt.Fprint(w, `[]`)
		require.NoError(t, err)
	})
	mux.HandleFunc("GET /repos/octocat/hello-world/git/trees/", func(w http.ResponseWriter, _ *http.Request) {
		_, err := fmt.Fprint(w, `{"sha":"newsha222","tree":[],"truncated":false}`)
		require.NoError(t, err)
	})
}

// A GitHub App resolution failure is deterministic per deployment config —
// the same repo resolves to the same missing App on every attempt — so the
// core must report it as terminal rather than re-driving a delivery that can
// only fail until an operator fixes the config. No PR comment could be posted
// without a client, so the core also returns the error: the delivery is
// recorded as failed (its only triage trail) rather than completed.
func TestUnlockCommandCoreAppResolutionFailureIsTerminal(t *testing.T) {
	t.Run("database inference cannot resolve an App", func(t *testing.T) {
		h := &Handler{
			ghClients: ghclient.NewClientSet(nil),
			logger:    testLogger(),
		}

		retry, err := h.unlockCommandCore(t.Context(), time.Now(), "octocat/hello-world", 1, 12345, "testuser",
			CommandResult{Action: action.Unlock, Force: true})

		require.Error(t, err)
		assert.ErrorIs(t, err, errGitHubAppResolution)
		assert.False(t, retry, "a deterministic GitHub App resolution failure must not be re-driven; recovery is fixing the deployment config")
	})

	t.Run("authorization client cannot resolve an App", func(t *testing.T) {
		lockStore := &unlockTestLockStore{locks: []*storage.Lock{prOwnedOrdersLock()}}
		st := &unlockTestStorage{locks: lockStore, applies: &noActiveAppliesStore{}}
		h := actorAuthClientSetTestHandler(t, st, ghclient.NewSingleClientSet("unrelated-app", &fakeClientFactory{}))

		retry, err := h.unlockCommandCore(t.Context(), time.Now(), "octocat/hello-world", 1, 12345, "testuser",
			CommandResult{Action: action.Unlock})

		require.Error(t, err)
		assert.ErrorIs(t, err, errGitHubAppResolution)
		assert.False(t, retry, "a deterministic GitHub App resolution failure must not be re-driven; recovery is fixing the deployment config")
		assert.Zero(t, lockStore.releaseCalls, "no lock may be released when the authorization gate cannot run")
	})
}

// The authorization client is created per delivery, so a transient
// client-creation failure (an installation token fetch, for example) may
// clear on a later attempt: the core must report it as retryable with a
// non-nil error while still releasing nothing.
func TestUnlockCommandCoreAuthorizationClientTransientFailureIsRetryable(t *testing.T) {
	lockStore := &unlockTestLockStore{locks: []*storage.Lock{prOwnedOrdersLock()}}
	st := &unlockTestStorage{locks: lockStore, applies: &noActiveAppliesStore{}}
	h := actorAuthClientSetTestHandler(t, st, ghclient.NewSingleClientSet(defaultAppName, &fakeClientFactory{
		forInstallationErr: errors.New("installation token unavailable"),
	}))

	retry, err := h.unlockCommandCore(t.Context(), time.Now(), "octocat/hello-world", 1, 12345, "testuser",
		CommandResult{Action: action.Unlock})

	require.Error(t, err)
	assert.NotErrorIs(t, err, errGitHubAppResolution)
	assert.True(t, retry, "a transient authorization client-creation failure must stay retryable for a durable driver")
	assert.Zero(t, lockStore.releaseCalls, "no lock may be released when the authorization gate cannot run")
}

// Transient infrastructure failures — a GitHub read during database inference,
// a storage lock lookup, the active-apply verification, or a lock release —
// are failures the same delivery could clear on a later attempt, so the core
// must report them as retryable with a non-nil error. Released locks drop out
// of the lookup, so a re-drive retries only the locks that remain.
func TestUnlockCommandCoreTransientFailuresAreRetryable(t *testing.T) {
	t.Run("inference read failure is retryable", func(t *testing.T) {
		client, mux := setupGitHubServer(t)
		comments := recordComments(t, mux)
		lockStore := &unlockTestLockStore{}
		st := &unlockTestStorage{locks: lockStore, applies: &noActiveAppliesStore{}}
		h := unlockTestHandler(t, st, ghclient.NewInstallationClient(client, testLogger()))

		retry, err := h.unlockCommandCore(t.Context(), time.Now(), "octocat/hello-world", 1, 12345, "testuser",
			CommandResult{Action: action.Unlock, Force: true})

		require.Error(t, err)
		assert.True(t, retry, "a failed GitHub read during inference must stay retryable for a durable driver")
		body := requireComment(t, comments, "inference failure comment")
		assert.Contains(t, body, "SchemaBot could not work out which database to force unlock for this PR, so no lock was released. Run the command again, or add `-d` and the database name to `schemabot unlock --force`.")
		assert.NotContains(t, body, "fetch PR info", "the GitHub client's error must stay in the server log")
	})

	t.Run("lock lookup failure is retryable", func(t *testing.T) {
		client, mux := setupGitHubServer(t)
		comments := recordComments(t, mux)
		lockStore := &unlockTestLockStore{getByPRErr: errors.New("dial tcp 10.1.2.3:3306: connect: connection refused")}
		st := &unlockTestStorage{locks: lockStore, applies: &noActiveAppliesStore{}}
		h := unlockTestHandler(t, st, ghclient.NewInstallationClient(client, testLogger()))

		retry, err := h.unlockCommandCore(t.Context(), time.Now(), "octocat/hello-world", 1, 12345, "testuser", CommandResult{Action: action.Unlock})

		require.Error(t, err)
		assert.True(t, retry, "a transient lock lookup failure must stay retryable for a durable driver")
		body := requireComment(t, comments, "lock lookup failure comment")
		assert.Contains(t, body, "SchemaBot could not look up the locks for this PR, so no lock was released. Run the command again; if it fails again, the SchemaBot server logs have the cause.")
		assert.NotContains(t, body, "dial tcp")
		assert.NotContains(t, body, "connection refused")
	})

	t.Run("lock lookup failure for a named database names it", func(t *testing.T) {
		client, mux := setupGitHubServer(t)
		comments := recordComments(t, mux)
		lockStore := &unlockTestLockStore{getByPRErr: errors.New("dial tcp 10.1.2.3:3306: connect: connection refused")}
		st := &unlockTestStorage{locks: lockStore, applies: &noActiveAppliesStore{}}
		h := unlockTestHandler(t, st, ghclient.NewInstallationClient(client, testLogger()))

		retry, err := h.unlockCommandCore(t.Context(), time.Now(), "octocat/hello-world", 1, 12345, "testuser",
			CommandResult{Action: action.Unlock, Database: "orders"})

		require.Error(t, err)
		assert.True(t, retry, "a transient lock lookup failure must stay retryable for a durable driver")
		body := requireComment(t, comments, "named-database lock lookup failure comment")
		assert.Contains(t, body, "SchemaBot could not look up the lock for `orders`, so no lock was released.")
		assert.NotContains(t, body, "dial tcp")
	})

	t.Run("active-apply verification failure is retryable", func(t *testing.T) {
		client, mux := setupGitHubServer(t)
		comments := recordComments(t, mux)
		lockStore := &unlockTestLockStore{locks: []*storage.Lock{prOwnedOrdersLock()}}
		st := &unlockTestStorage{locks: lockStore, applies: &failingApplyLookupStore{}}
		h := unlockTestHandler(t, st, ghclient.NewInstallationClient(client, testLogger()))

		retry, err := h.unlockCommandCore(t.Context(), time.Now(), "octocat/hello-world", 1, 12345, "testuser", CommandResult{Action: action.Unlock})

		require.Error(t, err)
		assert.True(t, retry, "an unverifiable active-apply state must be retried, not treated as the command's answer")
		assert.Zero(t, lockStore.releaseCalls, "no lock may be released while apply state is unknown")
		body := requireComment(t, comments, "verification failure comment")
		assert.Contains(t, body, "Failed to verify active applies for database `orders`")
	})

	t.Run("lock release failure is retryable", func(t *testing.T) {
		client, _ := setupGitHubServer(t)
		lockStore := &unlockTestLockStore{
			locks:       []*storage.Lock{prOwnedOrdersLock()},
			releaseErrs: map[string]error{"orders": errors.New("storage write failed")},
		}
		st := &unlockTestStorage{locks: lockStore, applies: &noActiveAppliesStore{}}
		h := unlockTestHandler(t, st, ghclient.NewInstallationClient(client, testLogger()))

		retry, err := h.unlockCommandCore(t.Context(), time.Now(), "octocat/hello-world", 1, 12345, "testuser", CommandResult{Action: action.Unlock})

		require.Error(t, err)
		assert.True(t, retry, "a failed lock release must stay retryable so a re-drive can release the remaining locks")
		assert.Equal(t, 1, lockStore.releaseCalls, "the release must have been attempted")
	})

	// One lock's release failure must not strand the remaining locks: the loop
	// continues, releases the rest, and reports the delivery retryable so a
	// re-drive picks up only the lock still held.
	t.Run("partial release failure releases remaining locks and stays retryable", func(t *testing.T) {
		client, mux := setupGitHubServer(t)
		comments := recordComments(t, mux)
		paymentsLock := &storage.Lock{
			DatabaseName: "payments",
			DatabaseType: storage.DatabaseTypeMySQL,
			Repository:   "octocat/hello-world",
			PullRequest:  1,
			Owner:        "octocat/hello-world#1",
		}
		lockStore := &unlockTestLockStore{
			locks:       []*storage.Lock{prOwnedOrdersLock(), paymentsLock},
			releaseErrs: map[string]error{"orders": errors.New("storage write failed")},
		}
		st := &unlockTestStorage{locks: lockStore, applies: &noActiveAppliesStore{}}
		h := unlockTestHandler(t, st, ghclient.NewInstallationClient(client, testLogger()))

		retry, err := h.unlockCommandCore(t.Context(), time.Now(), "octocat/hello-world", 1, 12345, "testuser", CommandResult{Action: action.Unlock})

		require.Error(t, err)
		assert.True(t, retry, "the failed release keeps the delivery retryable for the lock still held")
		assert.ErrorContains(t, err, "orders")
		assert.Equal(t, 2, lockStore.releaseCalls, "the failed release must not stop the loop")
		assert.Equal(t, []string{"payments"}, lockStore.released, "the remaining lock must still be released")
		body := requireComment(t, comments, "released-lock success comment")
		assert.Contains(t, body, "Lock Released")
		assert.Contains(t, body, "payments")
	})

	// A gate that could not evaluate its inputs fails closed for this delivery
	// but is not the command's answer: the same delivery may authorize cleanly
	// once the membership read succeeds.
	t.Run("authorization evaluation failure is retryable", func(t *testing.T) {
		client, mux := setupGitHubServer(t)
		comments := recordComments(t, mux)
		handleTeamMembers(t, mux, "octocat", "schema-admins", http.StatusForbidden)
		lockStore := &unlockTestLockStore{locks: []*storage.Lock{prOwnedOrdersLock()}}
		st := &unlockTestStorage{locks: lockStore, applies: &noActiveAppliesStore{}}
		cfg := actorAuthTestConfig(true, func(cfg *api.ServerConfig) {
			cfg.PRCommandAuthorization.AdminTeams = []string{"octocat/schema-admins"}
		})
		h := actorAuthStorageTestHandler(cfg, st, ghclient.NewInstallationClient(client, testLogger()))

		retry, err := h.unlockCommandCore(t.Context(), time.Now(), "octocat/hello-world", 1, 12345, "mona", CommandResult{Action: action.Unlock})

		require.Error(t, err)
		assert.True(t, retry, "an unevaluable authorization gate fails closed for this delivery but stays retryable")
		assert.Zero(t, lockStore.releaseCalls, "no lock may be released while authorization is unknown")
		body := requireComment(t, comments, "authorization failure comment")
		assert.Contains(t, body, "SchemaBot Authorization Check Failed")
	})
}

// Terminal outcomes are the command's answer — no locks to release, a lock
// still protecting an active apply, a deterministic inference or force-unlock
// ownership rejection, an authorization block on the merits, or a completed
// release — so the core reports them as (retry=false, err=nil): a durable
// driver must not re-drive them.
func TestUnlockCommandCoreTerminalDispositions(t *testing.T) {
	// A repo without any schemabot.yaml deterministically fails force-unlock
	// database inference: re-driving cannot conjure a config, so the posted
	// rejection is the command's answer.
	t.Run("no-config inference rejection is terminal", func(t *testing.T) {
		client, mux := setupGitHubServer(t)
		comments := recordComments(t, mux)
		serveConfiglessRepo(t, mux)
		lockStore := &unlockTestLockStore{}
		st := &unlockTestStorage{locks: lockStore, applies: &noActiveAppliesStore{}}
		h := unlockTestHandler(t, st, ghclient.NewInstallationClient(client, testLogger()))

		retry, err := h.unlockCommandCore(t.Context(), time.Now(), "octocat/hello-world", 1, 12345, "testuser",
			CommandResult{Action: action.Unlock, Force: true})

		require.NoError(t, err)
		assert.False(t, retry, "a missing config is a deterministic rejection the same delivery always reproduces")
		body := requireComment(t, comments, "no-config inference rejection comment")
		assert.Contains(t, body, "Failed to infer database for force unlock")
		assert.Contains(t, body, "No `schemabot.yaml` configuration file was found in this repository")
	})

	t.Run("no locks found is terminal", func(t *testing.T) {
		client, mux := setupGitHubServer(t)
		comments := recordComments(t, mux)
		lockStore := &unlockTestLockStore{}
		st := &unlockTestStorage{locks: lockStore, applies: &noActiveAppliesStore{}}
		h := unlockTestHandler(t, st, ghclient.NewInstallationClient(client, testLogger()))

		retry, err := h.unlockCommandCore(t.Context(), time.Now(), "octocat/hello-world", 1, 12345, "testuser", CommandResult{Action: action.Unlock})

		require.NoError(t, err)
		assert.False(t, retry, "an empty lock set is the command's answer, not a failure")
		body := requireComment(t, comments, "no-locks comment")
		assert.Contains(t, body, "No Locks Found")
	})

	// On an aggregate repo an unscoped unlock fans out to every deployment;
	// one that holds no locks stays silent so only the owning deployment
	// answers — a deliberate silent no-op, not a failure.
	t.Run("unowned unscoped fan-out is terminal and silent", func(t *testing.T) {
		client, mux := setupGitHubServer(t)
		comments := recordComments(t, mux)
		lockStore := &unlockTestLockStore{}
		st := &unlockTestStorage{locks: lockStore, applies: &noActiveAppliesStore{}}
		h := actorAuthStorageTestHandler(aggregateLeaderConfig(), st, ghclient.NewInstallationClient(client, testLogger()))

		retry, err := h.unlockCommandCore(t.Context(), time.Now(), "octocat/hello-world", 1, 12345, "testuser", CommandResult{Action: action.Unlock})

		require.NoError(t, err)
		assert.False(t, retry, "a non-owning fan-out skip is the command's terminal answer, not a retryable failure")
		assert.Empty(t, comments, "the terminal skip must stay silent")
	})

	t.Run("active apply protecting the lock is terminal", func(t *testing.T) {
		client, mux := setupGitHubServer(t)
		comments := recordComments(t, mux)
		lockStore := &unlockTestLockStore{locks: []*storage.Lock{prOwnedOrdersLock()}}
		st := &unlockTestStorage{locks: lockStore, applies: &activeApplyStore{}}
		h := unlockTestHandler(t, st, ghclient.NewInstallationClient(client, testLogger()))

		retry, err := h.unlockCommandCore(t.Context(), time.Now(), "octocat/hello-world", 1, 12345, "testuser", CommandResult{Action: action.Unlock})

		require.NoError(t, err)
		assert.False(t, retry, "an active apply verifiably blocking the unlock is the command's answer; re-driving would re-block")
		assert.Zero(t, lockStore.releaseCalls, "a lock protecting an active apply must not be released")
		body := requireComment(t, comments, "cannot-unlock comment")
		assert.Contains(t, body, "Cannot Unlock")
		assert.Contains(t, body, "apply-123")
	})

	t.Run("force unlock of a lock held by another PR is terminal", func(t *testing.T) {
		client, mux := setupGitHubServer(t)
		comments := recordComments(t, mux)
		lockStore := &unlockTestLockStore{locks: []*storage.Lock{{
			DatabaseName: "orders",
			DatabaseType: storage.DatabaseTypeMySQL,
			Repository:   "octocat/other-repo",
			PullRequest:  7,
			Owner:        "octocat/other-repo#7",
		}}}
		st := &unlockTestStorage{locks: lockStore, applies: &noActiveAppliesStore{}}
		h := unlockTestHandler(t, st, ghclient.NewInstallationClient(client, testLogger()))

		retry, err := h.unlockCommandCore(t.Context(), time.Now(), "octocat/hello-world", 1, 12345, "testuser",
			CommandResult{Action: action.Unlock, Force: true, Database: "orders"})

		require.NoError(t, err)
		assert.False(t, retry, "another PR's lock ownership is a deterministic rejection the same input always reproduces")
		assert.Zero(t, lockStore.releaseCalls, "another PR's lock must not be force-released")
		body := requireComment(t, comments, "ownership rejection comment")
		assert.Contains(t, body, "held by octocat/other-repo#7")
	})

	// An actor the gate evaluated and denied gets the same denial on every
	// re-drive: the posted rejection is the command's answer.
	t.Run("authorization block on the merits is terminal", func(t *testing.T) {
		client, mux := setupGitHubServer(t)
		comments := recordComments(t, mux)
		lockStore := &unlockTestLockStore{locks: []*storage.Lock{prOwnedOrdersLock()}}
		st := &unlockTestStorage{locks: lockStore, applies: &noActiveAppliesStore{}}
		cfg := actorAuthTestConfig(true, func(cfg *api.ServerConfig) {
			cfg.PRCommandAuthorization.AdminUsers = []string{"hubot"}
		})
		h := actorAuthStorageTestHandler(cfg, st, ghclient.NewInstallationClient(client, testLogger()))

		retry, err := h.unlockCommandCore(t.Context(), time.Now(), "octocat/hello-world", 1, 12345, "mona", CommandResult{Action: action.Unlock})

		require.NoError(t, err)
		assert.False(t, retry, "an evaluated denial is the command's answer; re-driving would re-deny")
		assert.Zero(t, lockStore.releaseCalls, "a denied unlock must not release any lock")
		body := requireComment(t, comments, "authorization denial comment")
		assert.Contains(t, body, "SchemaBot Command Not Authorized")
	})

	t.Run("released locks are terminal success", func(t *testing.T) {
		client, mux := setupGitHubServer(t)
		comments := recordComments(t, mux)
		lockStore := &unlockTestLockStore{locks: []*storage.Lock{prOwnedOrdersLock()}}
		st := &unlockTestStorage{locks: lockStore, applies: &noActiveAppliesStore{}}
		h := unlockTestHandler(t, st, ghclient.NewInstallationClient(client, testLogger()))

		retry, err := h.unlockCommandCore(t.Context(), time.Now(), "octocat/hello-world", 1, 12345, "testuser", CommandResult{Action: action.Unlock})

		require.NoError(t, err)
		assert.False(t, retry, "a completed release is the command's answer; a re-drive would find nothing to release")
		assert.Equal(t, 1, lockStore.releaseCalls, "exactly one owned-lock release expected")
		body := requireComment(t, comments, "release success comment")
		assert.Contains(t, body, "Lock Released")
	})

	// A lock that vanished (or changed owner) between lookup and release lost
	// a race to a concurrent releaser — the targeted lock is gone, which is
	// the command's goal. Retrying would only find nothing and post a
	// confusing second answer, so the outcome stays terminal.
	t.Run("a lock released concurrently is terminal, not retried", func(t *testing.T) {
		client, mux := setupGitHubServer(t)
		comments := recordComments(t, mux)
		ordersLock := prOwnedOrdersLock()
		billingLock := prOwnedOrdersLock()
		billingLock.DatabaseName = "billing"
		lockStore := &unlockTestLockStore{
			locks:       []*storage.Lock{ordersLock, billingLock},
			releaseErrs: map[string]error{"orders": storage.ErrLockNotFound},
		}
		st := &unlockTestStorage{locks: lockStore, applies: &noActiveAppliesStore{}}
		h := unlockTestHandler(t, st, ghclient.NewInstallationClient(client, testLogger()))

		retry, err := h.unlockCommandCore(t.Context(), time.Now(), "octocat/hello-world", 1, 12345, "testuser", CommandResult{Action: action.Unlock})

		require.NoError(t, err)
		assert.False(t, retry, "a lock already gone converges with the command's goal; re-driving would find nothing")
		assert.Equal(t, []string{"billing"}, lockStore.released, "the remaining lock is still released")
		body := requireComment(t, comments, "release success comment")
		assert.Contains(t, body, "billing", "the success comment names only the lock this command released")
	})

	// When every matched lock lost the race to a concurrent releaser, the
	// per-lock skips post nothing — but the command was acknowledged, so it
	// must still answer with an aggregate comment instead of ending silently.
	t.Run("every matched lock released concurrently still answers", func(t *testing.T) {
		client, mux := setupGitHubServer(t)
		comments := recordComments(t, mux)
		ordersLock := prOwnedOrdersLock()
		billingLock := prOwnedOrdersLock()
		billingLock.DatabaseName = "billing"
		lockStore := &unlockTestLockStore{
			locks: []*storage.Lock{ordersLock, billingLock},
			releaseErrs: map[string]error{
				"orders":  storage.ErrLockNotFound,
				"billing": storage.ErrLockNotOwned,
			},
		}
		st := &unlockTestStorage{locks: lockStore, applies: &noActiveAppliesStore{}}
		h := unlockTestHandler(t, st, ghclient.NewInstallationClient(client, testLogger()))

		retry, err := h.unlockCommandCore(t.Context(), time.Now(), "octocat/hello-world", 1, 12345, "testuser", CommandResult{Action: action.Unlock})

		require.NoError(t, err)
		assert.False(t, retry, "locks already gone converge with the command's goal; re-driving would find nothing")
		assert.Empty(t, lockStore.released, "this command released nothing")
		body := requireComment(t, comments, "aggregate already-released answer")
		assert.Contains(t, body, "Locks Already Released")
		assert.Contains(t, body, "already been released by a concurrent operation")
	})

	// A lock acquired after the command was received is outside the command's
	// intent: a delayed re-drive must not release it, only the locks that
	// existed when the command was issued.
	t.Run("locks acquired after the command are left in place", func(t *testing.T) {
		client, mux := setupGitHubServer(t)
		comments := recordComments(t, mux)
		issuedAt := time.Now()
		oldLock := prOwnedOrdersLock()
		oldLock.CreatedAt = issuedAt.Add(-time.Hour)
		newLock := prOwnedOrdersLock()
		newLock.DatabaseName = "billing"
		newLock.CreatedAt = issuedAt.Add(time.Minute)
		lockStore := &unlockTestLockStore{locks: []*storage.Lock{oldLock, newLock}}
		st := &unlockTestStorage{locks: lockStore, applies: &noActiveAppliesStore{}}
		h := unlockTestHandler(t, st, ghclient.NewInstallationClient(client, testLogger()))

		retry, err := h.unlockCommandCore(t.Context(), issuedAt, "octocat/hello-world", 1, 12345, "testuser", CommandResult{Action: action.Unlock})

		require.NoError(t, err)
		assert.False(t, retry, "releasing the covered locks is the command's answer")
		assert.Equal(t, []string{"orders"}, lockStore.released, "only the lock that predates the command may be released")
		body := requireComment(t, comments, "release success comment")
		assert.Contains(t, body, "Lock Released")
	})

	// When every matched lock postdates the command — including a force unlock
	// matching another session's newer CLI lock — the command is stale: it
	// terminal-fails with a prompt for a fresh command instead of releasing
	// state it never covered.
	t.Run("a command older than every matched lock is terminal without releasing", func(t *testing.T) {
		client, mux := setupGitHubServer(t)
		comments := recordComments(t, mux)
		issuedAt := time.Now()
		cliLock := &storage.Lock{
			DatabaseName: "orders",
			DatabaseType: storage.DatabaseTypeMySQL,
			Owner:        "cli:someone@host",
			CreatedAt:    issuedAt.Add(time.Minute),
		}
		lockStore := &unlockTestLockStore{locks: []*storage.Lock{cliLock}}
		st := &unlockTestStorage{locks: lockStore, applies: &noActiveAppliesStore{}}
		h := unlockTestHandler(t, st, ghclient.NewInstallationClient(client, testLogger()))

		retry, err := h.unlockCommandCore(t.Context(), issuedAt, "octocat/hello-world", 1, 12345, "testuser",
			CommandResult{Action: action.Unlock, Force: true, Database: "orders"})

		require.NoError(t, err)
		assert.False(t, retry, "a stale command is terminal; the recovery path is a fresh comment")
		assert.Zero(t, lockStore.releaseCalls, "a stale command must not release any lock")
		body := requireComment(t, comments, "stale-command answer")
		assert.Contains(t, body, "acquired after the command was received")
		assert.Contains(t, body, "Comment `schemabot unlock` again")
	})

	t.Run("a lock timestamp in the receipt second is left in place", func(t *testing.T) {
		client, mux := setupGitHubServer(t)
		comments := recordComments(t, mux)
		issuedAt := time.Now().Truncate(time.Second).Add(500 * time.Millisecond)
		lock := prOwnedOrdersLock()
		lock.CreatedAt = issuedAt.Add(-time.Hour)
		lock.UpdatedAt = issuedAt.Truncate(time.Second)
		lockStore := &unlockTestLockStore{locks: []*storage.Lock{lock}}
		st := &unlockTestStorage{locks: lockStore, applies: &noActiveAppliesStore{}}
		h := unlockTestHandler(t, st, ghclient.NewInstallationClient(client, testLogger()))

		retry, err := h.unlockCommandCore(t.Context(), issuedAt, "octocat/hello-world", 1, 12345, "testuser", CommandResult{Action: action.Unlock})

		require.NoError(t, err)
		assert.False(t, retry)
		assert.Zero(t, lockStore.releaseCalls, "a lock refreshed in the receipt second must stay held")
		body := requireComment(t, comments, "stale-command answer")
		assert.Contains(t, body, "At least one current lock")
		assert.Contains(t, body, "this command released nothing")
	})

	t.Run("mixed reacquired and already gone locks report the held lock", func(t *testing.T) {
		client, mux := setupGitHubServer(t)
		comments := recordComments(t, mux)
		ordersLock := prOwnedOrdersLock()
		billingLock := prOwnedOrdersLock()
		billingLock.DatabaseName = "billing"
		lockStore := &unlockTestLockStore{
			locks: []*storage.Lock{ordersLock, billingLock},
			releaseErrs: map[string]error{
				"orders":  storage.ErrLockIntentChanged,
				"billing": storage.ErrLockNotFound,
			},
		}
		st := &unlockTestStorage{locks: lockStore, applies: &noActiveAppliesStore{}}
		h := unlockTestHandler(t, st, ghclient.NewInstallationClient(client, testLogger()))

		retry, err := h.unlockCommandCore(t.Context(), time.Now(), "octocat/hello-world", 1, 12345, "testuser", CommandResult{Action: action.Unlock})

		require.NoError(t, err)
		assert.False(t, retry)
		assert.Empty(t, lockStore.released, "this command released nothing")
		body := requireComment(t, comments, "mixed stale-command answer")
		assert.Contains(t, body, "At least one current lock")
		assert.Contains(t, body, "this command released nothing")
		assert.NotContains(t, body, "Every lock matched")
	})
}

// serveUnlockInferencePR registers the PR read and changed-files routes
// force-unlock database inference walks, listing the given paths as modified
// files.
func serveUnlockInferencePR(t *testing.T, mux *http.ServeMux, files ...string) {
	t.Helper()
	mux.HandleFunc("GET /repos/octocat/hello-world/pulls/1", func(w http.ResponseWriter, _ *http.Request) {
		_, err := fmt.Fprint(w, `{"head":{"ref":"feature-branch","sha":"newsha222"},"base":{"ref":"main","sha":"def456"},"user":{"login":"testuser"}}`)
		require.NoError(t, err)
	})
	mux.HandleFunc("GET /repos/octocat/hello-world/pulls/1/files", func(w http.ResponseWriter, _ *http.Request) {
		entries := make([]map[string]string, 0, len(files))
		for _, f := range files {
			entries = append(entries, map[string]string{"filename": f, "status": "modified"})
		}
		require.NoError(t, json.NewEncoder(w).Encode(entries))
	})
}

// serveConfigFile registers the contents route for one schemabot.yaml so
// config discovery resolves the given path to the given file content.
func serveConfigFile(t *testing.T, mux *http.ServeMux, path, content string) {
	t.Helper()
	mux.HandleFunc("GET /repos/octocat/hello-world/contents/"+path, func(w http.ResponseWriter, _ *http.Request) {
		require.NoError(t, json.NewEncoder(w).Encode(map[string]string{
			"type":     "file",
			"encoding": "base64",
			"content":  base64.StdEncoding.EncodeToString([]byte(content)),
		}))
	})
}

// Every deterministic force-unlock inference rejection — a repo whose only
// config is malformed, a PR ambiguously spanning several managed configs, or
// schema another deployment owns — reproduces on any re-drive of the same
// delivery, so each classification must ride the rejection marker and report
// (retry=false, err=nil) with the rejection posted as the command's answer.
func TestUnlockCommandCoreInferenceRejectionsAreTerminal(t *testing.T) {
	cases := []struct {
		name        string
		config      *api.ServerConfig
		serve       func(t *testing.T, mux *http.ServeMux)
		wantComment string
	}{
		{
			// A repo whose only schemabot.yaml cannot be parsed is a
			// deterministic discovery outcome: re-driving cannot repair the
			// file.
			name:   "invalid config rejection is terminal",
			config: &api.ServerConfig{},
			serve: func(t *testing.T, mux *http.ServeMux) {
				serveUnlockInferencePR(t, mux)
				mux.HandleFunc("GET /repos/octocat/hello-world/git/trees/", func(w http.ResponseWriter, _ *http.Request) {
					_, err := fmt.Fprint(w, `{"sha":"newsha222","tree":[{"path":"schemabot.yaml","type":"blob"}],"truncated":false}`)
					require.NoError(t, err)
				})
				serveConfigFile(t, mux, "schemabot.yaml", "type: mysql\n")
			},
			wantComment: "A `schemabot.yaml` in this repository is not valid",
		},
		{
			// A PR spanning several configs this deployment manages is
			// ambiguous for a single-database command; the same delivery
			// always resolves the same config set, so the "scope with -d"
			// answer is terminal.
			name: "multiple managed configs rejection is terminal",
			config: &api.ServerConfig{
				Databases: map[string]api.DatabaseConfig{
					"orders":   {Type: storage.DatabaseTypeMySQL},
					"payments": {Type: storage.DatabaseTypeMySQL},
				},
			},
			serve: func(t *testing.T, mux *http.ServeMux) {
				serveUnlockInferencePR(t, mux, "db/orders/schemabot.yaml", "db/payments/schemabot.yaml")
				serveConfigFile(t, mux, "db/orders/schemabot.yaml", "database: orders\ntype: mysql\n")
				serveConfigFile(t, mux, "db/payments/schemabot.yaml", "database: payments\ntype: mysql\n")
			},
			wantComment: "multiple SchemaBot configs match this PR",
		},
		{
			// A config outside this deployment's allowed_dirs is another
			// deployment's schema: this deployment deterministically has
			// nothing to unlock, the same outcome as no config at all.
			name: "unowned schema rejection is terminal",
			config: &api.ServerConfig{
				Databases: map[string]api.DatabaseConfig{
					"orders": {
						Type:         storage.DatabaseTypeMySQL,
						AllowedRepos: []string{"octocat/hello-world"},
						AllowedDirs:  []string{"db/orders/schema"},
					},
				},
			},
			serve: func(t *testing.T, mux *http.ServeMux) {
				serveUnlockInferencePR(t, mux, "db/other/schemabot.yaml")
				serveConfigFile(t, mux, "db/other/schemabot.yaml", "database: orders\ntype: mysql\n")
			},
			wantComment: "No `schemabot.yaml` configuration file was found in this repository",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			client, mux := setupGitHubServer(t)
			comments := recordComments(t, mux)
			tc.serve(t, mux)
			lockStore := &unlockTestLockStore{}
			st := &unlockTestStorage{locks: lockStore, applies: &noActiveAppliesStore{}}
			h := actorAuthStorageTestHandler(tc.config, st, ghclient.NewInstallationClient(client, testLogger()))

			retry, err := h.unlockCommandCore(t.Context(), time.Now(), "octocat/hello-world", 1, 12345, "testuser",
				CommandResult{Action: action.Unlock, Force: true})

			require.NoError(t, err)
			assert.False(t, retry, "a deterministic inference rejection is the command's answer; re-driving reproduces it")
			body := requireComment(t, comments, "inference rejection comment")
			assert.Contains(t, body, "Failed to infer database for force unlock")
			assert.Contains(t, body, tc.wantComment)
			assert.NotContains(t, body, "invalid schemabot.yaml config found", "the discovery error's own text, which names repository paths, stays in the server log")
			assert.NotContains(t, body, "no schemabot.yaml config found")
		})
	}
}

// A force unlock that SchemaBot refuses shows the words SchemaBot wrote for
// the refusal, and a rejection wrapping an error SchemaBot did not write shows
// the fixed line instead of that error's text.
func TestUnlockRejectionShowsOnlyAuthoredText(t *testing.T) {
	t.Run("a lock another PR holds names the holder", func(t *testing.T) {
		client, mux := setupGitHubServer(t)
		comments := recordComments(t, mux)
		lockStore := &unlockTestLockStore{locks: []*storage.Lock{{
			DatabaseName: "orders",
			DatabaseType: storage.DatabaseTypeMySQL,
			Repository:   "octocat/other-repo",
			PullRequest:  2,
		}}}
		st := &unlockTestStorage{locks: lockStore, applies: &noActiveAppliesStore{}}
		h := unlockTestHandler(t, st, ghclient.NewInstallationClient(client, testLogger()))

		retry, err := h.unlockCommandCore(t.Context(), time.Now(), "octocat/hello-world", 1, 12345, "testuser",
			CommandResult{Action: action.Unlock, Force: true, Database: "orders"})

		require.NoError(t, err)
		assert.False(t, retry, "a lock held by another PR is the command's answer; re-driving reproduces it")
		body := requireComment(t, comments, "lock held by another PR comment")
		assert.Contains(t, body, "Failed to look up locks: lock for orders is held by octocat/other-repo#2")
		assert.Zero(t, lockStore.releaseCalls)
	})

	t.Run("a force unlock with no database names the missing target", func(t *testing.T) {
		st := &unlockTestStorage{locks: &unlockTestLockStore{}, applies: &noActiveAppliesStore{}}
		h := unlockTestHandler(t, st, nil)

		_, err := h.locksForUnlock(t.Context(), "octocat/hello-world", 1, CommandResult{Action: action.Unlock, Force: true})

		require.Error(t, err)
		assert.True(t, isUnlockRejection(err))
		assert.Equal(t, "--force requires a database target", userFacingError(err))
	})

	t.Run("the cause of a refusal stays out of the comment line", func(t *testing.T) {
		err := unlockRefusal("multiple SchemaBot configs match this PR", errors.New(rawInfrastructureError))

		assert.Equal(t, "multiple SchemaBot configs match this PR", userFacingError(err))
		assert.Contains(t, err.Error(), rawInfrastructureError, "the server log keeps the cause")
	})

	t.Run("a rejection wrapping an error SchemaBot did not write shows the fixed line", func(t *testing.T) {
		err := unlockRejection(fmt.Errorf("read config: %w", errors.New(rawInfrastructureError)))

		got := userFacingError(err)

		assert.Equal(t, unexpectedFailureMessage, got)
		requireNoInfrastructureDetail(t, got)
	})

	t.Run("a rejection wrapping a discovery sentinel shows the sentinel's line without the wrap", func(t *testing.T) {
		err := unlockRejection(fmt.Errorf("%w at services/orders/schemabot.yaml", ghclient.ErrInvalidConfig))

		got := userFacingError(err)

		assert.Contains(t, got, "A `schemabot.yaml` in this repository is not valid")
		assert.NotContains(t, got, "services/orders")
	})
}
