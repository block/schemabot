package webhook

import (
	"context"
	"errors"
	"fmt"
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

		retry, err := h.unlockCommandCore("octocat/hello-world", 1, 12345, "testuser",
			CommandResult{Action: action.Unlock, Force: true})

		require.Error(t, err)
		assert.True(t, retry, "a failed GitHub read during inference must stay retryable for a durable driver")
		body := requireComment(t, comments, "inference failure comment")
		assert.Contains(t, body, "Failed to infer database for force unlock")
	})

	t.Run("lock lookup failure is retryable", func(t *testing.T) {
		client, mux := setupGitHubServer(t)
		comments := recordComments(t, mux)
		lockStore := &unlockTestLockStore{getByPRErr: errors.New("storage read failed")}
		st := &unlockTestStorage{locks: lockStore, applies: &noActiveAppliesStore{}}
		h := unlockTestHandler(t, st, ghclient.NewInstallationClient(client, testLogger()))

		retry, err := h.unlockCommandCore("octocat/hello-world", 1, 12345, "testuser", CommandResult{Action: action.Unlock})

		require.Error(t, err)
		assert.True(t, retry, "a transient lock lookup failure must stay retryable for a durable driver")
		body := requireComment(t, comments, "lock lookup failure comment")
		assert.Contains(t, body, "Failed to look up locks")
	})

	t.Run("active-apply verification failure is retryable", func(t *testing.T) {
		client, mux := setupGitHubServer(t)
		comments := recordComments(t, mux)
		lockStore := &unlockTestLockStore{locks: []*storage.Lock{prOwnedOrdersLock()}}
		st := &unlockTestStorage{locks: lockStore, applies: &failingApplyLookupStore{}}
		h := unlockTestHandler(t, st, ghclient.NewInstallationClient(client, testLogger()))

		retry, err := h.unlockCommandCore("octocat/hello-world", 1, 12345, "testuser", CommandResult{Action: action.Unlock})

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

		retry, err := h.unlockCommandCore("octocat/hello-world", 1, 12345, "testuser", CommandResult{Action: action.Unlock})

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

		retry, err := h.unlockCommandCore("octocat/hello-world", 1, 12345, "testuser", CommandResult{Action: action.Unlock})

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
		mux.HandleFunc("GET /orgs/octocat/teams/schema-admins/members", teamMembersHandler(t, http.StatusForbidden))
		lockStore := &unlockTestLockStore{locks: []*storage.Lock{prOwnedOrdersLock()}}
		st := &unlockTestStorage{locks: lockStore, applies: &noActiveAppliesStore{}}
		cfg := actorAuthTestConfig(true, func(cfg *api.ServerConfig) {
			cfg.PRCommandAuthorization.AdminTeams = []string{"octocat/schema-admins"}
		})
		h := actorAuthStorageTestHandler(cfg, st, ghclient.NewInstallationClient(client, testLogger()))

		retry, err := h.unlockCommandCore("octocat/hello-world", 1, 12345, "mona", CommandResult{Action: action.Unlock})

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

		retry, err := h.unlockCommandCore("octocat/hello-world", 1, 12345, "testuser",
			CommandResult{Action: action.Unlock, Force: true})

		require.NoError(t, err)
		assert.False(t, retry, "a missing config is a deterministic rejection the same delivery always reproduces")
		body := requireComment(t, comments, "no-config inference rejection comment")
		assert.Contains(t, body, "Failed to infer database for force unlock")
		assert.Contains(t, body, "no schemabot.yaml config found")
	})

	t.Run("no locks found is terminal", func(t *testing.T) {
		client, mux := setupGitHubServer(t)
		comments := recordComments(t, mux)
		lockStore := &unlockTestLockStore{}
		st := &unlockTestStorage{locks: lockStore, applies: &noActiveAppliesStore{}}
		h := unlockTestHandler(t, st, ghclient.NewInstallationClient(client, testLogger()))

		retry, err := h.unlockCommandCore("octocat/hello-world", 1, 12345, "testuser", CommandResult{Action: action.Unlock})

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

		retry, err := h.unlockCommandCore("octocat/hello-world", 1, 12345, "testuser", CommandResult{Action: action.Unlock})

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

		retry, err := h.unlockCommandCore("octocat/hello-world", 1, 12345, "testuser", CommandResult{Action: action.Unlock})

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

		retry, err := h.unlockCommandCore("octocat/hello-world", 1, 12345, "testuser",
			CommandResult{Action: action.Unlock, Force: true, Database: "orders"})

		require.NoError(t, err)
		assert.False(t, retry, "another PR's lock ownership is a deterministic rejection the same input always reproduces")
		assert.Zero(t, lockStore.forceReleaseCalls, "another PR's lock must not be force-released")
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

		retry, err := h.unlockCommandCore("octocat/hello-world", 1, 12345, "mona", CommandResult{Action: action.Unlock})

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

		retry, err := h.unlockCommandCore("octocat/hello-world", 1, 12345, "testuser", CommandResult{Action: action.Unlock})

		require.NoError(t, err)
		assert.False(t, retry, "a completed release is the command's answer; a re-drive would find nothing to release")
		assert.Equal(t, 1, lockStore.releaseCalls, "exactly one owned-lock release expected")
		body := requireComment(t, comments, "release success comment")
		assert.Contains(t, body, "Lock Released")
	})
}
