package webhook

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/block/schemabot/pkg/api"
	ghclient "github.com/block/schemabot/pkg/github"
	"github.com/block/schemabot/pkg/secrets"
	"github.com/block/schemabot/pkg/storage"
)

// rollbackFakeDSN stands in for connection details an engine client failure
// can carry. The failing secrets lookup names it, so it is in the raw error and
// must never reach the PR.
const rollbackFakeDSN = "user:pw@tcp(10.0.0.5:3306)"

// rollbackFlakyResolver is a secrets prefix whose lookups always fail, the way
// a secrets backend does during an outage.
const rollbackFlakyResolver = "rollbackwebhookflaky"

// rollbackEngineClientConfig configures deployment "brokendb" with a DSN whose
// secrets lookup fails, so its engine client cannot be built until the backend
// recovers, and leaves deployment "ghost" out of the configuration.
func rollbackEngineClientConfig(t *testing.T) *api.ServerConfig {
	t.Helper()
	secrets.Register(rollbackFlakyResolver, func(string) (string, error) {
		return "", errors.New("secrets backend timed out reading " + rollbackFakeDSN)
	})
	t.Cleanup(func() { secrets.Unregister(rollbackFlakyResolver) })
	return &api.ServerConfig{
		Databases: map[string]api.DatabaseConfig{
			"brokendb": {
				Type: storage.DatabaseTypeMySQL,
				Environments: map[string]api.EnvironmentConfig{
					"staging": {DSN: rollbackFlakyResolver + ":orders"},
				},
			},
		},
		TernDeployments: api.TernConfig{"other": api.TernEndpoints{"staging": "localhost:9090"}},
	}
}

// rollbackStorageForDeployment is remotePlanRollbackStorage with the source
// apply and its plan on the given deployment.
func rollbackStorageForDeployment(deployment string, locks storage.LockStore) *rollbackTestStorage {
	st := remotePlanRollbackStorage(locks)
	st.applies.(*rollbackTestApplyStore).apply.Deployment = deployment
	st.plans.(*rollbackTestPlanStore).plan.Deployment = deployment
	return st
}

// A rollback whose engine client cannot be built because the deployment's DSN
// did not resolve is a failure the same command can clear on a later attempt.
// The core reports it as retryable and releases the lock it took, so a retry
// can start over. The PR comment says to retry and that the cause is in the
// server log; it never repeats the raw error, which names the DSN source. The
// returned error is what a driver posts once its retries run out, so it carries
// none of the DSN either.
func TestRollbackCommandCoreEngineClientFailureIsRetryableAndSanitized(t *testing.T) {
	client, mux := setupGitHubServer(t)
	comments := recordComments(t, mux)
	lockStore := &rollbackTestLockStore{}
	st := rollbackStorageForDeployment("brokendb", lockStore)
	h := actorAuthStorageTestHandler(rollbackEngineClientConfig(t), st, ghclient.NewInstallationClient(client, testLogger()))

	retry, err := h.rollbackCommandCore(t.Context(), "octocat/hello-world", 1, 12345, "testuser", rollbackCommand())

	require.Error(t, err)
	assert.True(t, retry, "an engine client that could not be built must stay retryable")
	assert.False(t, api.IsTerminalControlError(err))
	assert.Equal(t, 1, lockStore.releaseCalls, "the command-acquired lock must be released so a retry can start over")
	assert.Contains(t, err.Error(), "SchemaBot could not connect to the schema change engine for this deployment.")
	assertNoRollbackDSNFragment(t, err.Error())

	body := requireComment(t, comments, "engine client failure comment")
	assert.Contains(t, body, "SchemaBot could not connect to the schema change engine for this deployment. "+
		"Retry in a moment. If it keeps failing, a SchemaBot operator can find the cause in the server log.")
	assert.NotContains(t, body, "resolve DSN")
	assertNoRollbackDSNFragment(t, body)
}

// A rollback whose apply ran on a deployment that is no longer in the server
// configuration fails the same way on every attempt, so the posted error is the
// command's answer. It names the apply's database, deployment, and environment
// and what to change in the server configuration.
func TestRollbackCommandCoreDeploymentNotConfiguredIsTerminal(t *testing.T) {
	client, mux := setupGitHubServer(t)
	comments := recordComments(t, mux)
	lockStore := &rollbackTestLockStore{}
	st := rollbackStorageForDeployment("ghost", lockStore)
	h := actorAuthStorageTestHandler(rollbackEngineClientConfig(t), st, ghclient.NewInstallationClient(client, testLogger()))

	retry, err := h.rollbackCommandCore(t.Context(), "octocat/hello-world", 1, 12345, "testuser", rollbackCommand())

	require.NoError(t, err)
	assert.False(t, retry, "a deployment missing from the server configuration cannot be repaired by retrying")
	assert.Equal(t, 1, lockStore.releaseCalls, "the command-acquired lock must be released after the terminal answer")

	body := requireComment(t, comments, "deployment not configured comment")
	assert.Contains(t, body, "no tern client configured for database &#34;orders&#34; "+
		"(deployment=&#34;ghost&#34;, environment=&#34;staging&#34;) — add this database to the server config")
}

// assertNoRollbackDSNFragment checks that no part of the fake DSN reached text
// a PR reader sees.
func assertNoRollbackDSNFragment(t *testing.T, text string) {
	t.Helper()
	for _, fragment := range []string{"10.0.0.5", "user:pw", "3306", "tcp("} {
		assert.NotContains(t, text, fragment)
	}
}
