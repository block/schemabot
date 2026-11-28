package commands

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/block/schemabot/pkg/cmd/client"
	"github.com/block/schemabot/pkg/glyph"
)

// answerAtTerminal makes the command believe a person is at a terminal and
// answers its prompt with answer.
func answerAtTerminal(t *testing.T, answer string) {
	t.Helper()
	path := filepath.Join(t.TempDir(), "answer")
	require.NoError(t, os.WriteFile(path, []byte(answer), 0o600))
	input, err := os.Open(path)
	require.NoError(t, err)
	original := os.Stdin
	os.Stdin = input
	previous := stdinIsTerminal
	stdinIsTerminal = func() bool { return true }
	t.Cleanup(func() {
		os.Stdin = original
		stdinIsTerminal = previous
		require.NoError(t, input.Close())
	})
}

// withoutTerminal makes the command believe nobody can answer a prompt, and
// fails the test if the command reads an answer anyway.
func withoutTerminal(t *testing.T) {
	t.Helper()
	original := os.Stdin
	previous := stdinIsTerminal
	stdinIsTerminal = func() bool { return false }
	input, err := os.Open(os.DevNull)
	require.NoError(t, err)
	os.Stdin = input
	t.Cleanup(func() {
		os.Stdin = original
		stdinIsTerminal = previous
		require.NoError(t, input.Close())
	})
}

// The lock takeover question in unlock --force and apply --force is asked only
// when stdin is somewhere a person can answer: stdin on the null device, as in
// a container started without a terminal, is refused, and another character
// device is treated as a terminal.
func TestStdinIsTerminalFollowsTheNullDevice(t *testing.T) {
	original := os.Stdin
	t.Cleanup(func() { os.Stdin = original })

	null, err := os.Open(os.DevNull)
	require.NoError(t, err)
	t.Cleanup(func() { assert.NoError(t, null.Close()) })
	os.Stdin = null
	assert.False(t, stdinIsTerminal(), "the null device is not somewhere a person can answer")

	zero, err := os.Open("/dev/zero")
	require.NoError(t, err)
	t.Cleanup(func() { assert.NoError(t, zero.Close()) })
	os.Stdin = zero
	assert.True(t, stdinIsTerminal(), "another character device is treated as a terminal")
}

// anotherOwnersLock is a lock on orders (mysql) that a pull request took two
// hours ago.
func anotherOwnersLock() *client.LockInfo {
	return &client.LockInfo{
		ID: 3, Database: "orders", DatabaseType: "mysql", Owner: "octo-org/orders#7",
		Repository: "octo-org/orders", PullRequest: 7, CreatedAt: time.Now().Add(-2 * time.Hour),
	}
}

// What an operator sees about the lock they are about to take, then the question.
var (
	unlockTakeoverWarning = "\n" + glyph.Attention + "  The lock on orders (mysql) belongs to another owner\n" +
		"   Held by: octo-org/orders#7\n" +
		"   Since:   2 hours ago\n" +
		"   Releasing it removes their claim on this database.\n"
	unlockTakeoverPrompt = "Release this lock? Only 'yes' will be accepted: "
)

// A forced unlock of a lock another owner holds shows who holds it and asks
// first. Declining sends nothing to the server and says the lock is unchanged.
func TestUnlockCmdForceDeclinedLeavesAnotherOwnersLockAlone(t *testing.T) {
	srv := &lockTestServer{held: anotherOwnersLock(), releaseStatus: http.StatusOK, releaseBody: map[string]any{}}
	endpoint := srv.start(t)
	answerAtTerminal(t, "no\n")

	out, err := runUnlockCmd(endpoint, true)
	require.NoError(t, err)
	assert.Equal(t, unlockTakeoverWarning+unlockTakeoverPrompt+"\nLock not changed.\n", out)
	assert.Empty(t, srv.releases, "a declined release must send nothing")
}

// Answering yes releases the lock the prompt named, by force.
func TestUnlockCmdForceConfirmedReleasesAnotherOwnersLock(t *testing.T) {
	srv := &lockTestServer{held: anotherOwnersLock(), releaseStatus: http.StatusOK, releaseBody: map[string]any{}}
	endpoint := srv.start(t)
	answerAtTerminal(t, "yes\n")

	out, err := runUnlockCmd(endpoint, true)
	require.NoError(t, err)
	assert.Contains(t, out, unlockTakeoverWarning+unlockTakeoverPrompt)
	assert.Contains(t, out, "Force released lock for orders (mysql)\n   Previous owner: octo-org/orders#7\n")
	require.Len(t, srv.releases, 1)
	assert.Equal(t, map[string]any{"database": "orders", "database_type": "mysql", "force": true}, srv.releases[0])
}

// With -y a forced unlock asks nothing, even with no terminal, so scripts that
// pass it keep working.
func TestUnlockCmdForceWithYesAsksNothing(t *testing.T) {
	srv := &lockTestServer{held: anotherOwnersLock(), releaseStatus: http.StatusOK, releaseBody: map[string]any{}}
	endpoint := srv.start(t)
	withoutTerminal(t)

	out, err := runApprovedForceUnlockCmd(endpoint)
	require.NoError(t, err)
	assert.NotContains(t, out, "belongs to another owner")
	assert.NotContains(t, out, unlockTakeoverPrompt)
	assert.Contains(t, out, "Force released lock for orders (mysql)\n")
	require.Len(t, srv.releases, 1)
}

// With no terminal and no -y a forced unlock of another owner's lock is
// refused, naming the holder and -y, and sends nothing to the server.
func TestUnlockCmdForceWithoutTerminalOrYesIsRefused(t *testing.T) {
	srv := &lockTestServer{held: anotherOwnersLock(), releaseStatus: http.StatusOK, releaseBody: map[string]any{}}
	endpoint := srv.start(t)
	withoutTerminal(t)

	out, err := runUnlockCmd(endpoint, true)
	require.Error(t, err)
	assert.Equal(t, "the lock on orders (mysql) is held by octo-org/orders#7 and there is no terminal to ask you on, "+
		"so it was not released; add -y to release it without asking", err.Error())
	assert.NotContains(t, out, "Force released")
	assert.Empty(t, srv.releases, "a refused release must send nothing")
}

// A forced unlock of a lock this command's owner holds asks nothing, as an
// owned release does.
func TestUnlockCmdForceOfTheCallersOwnLockAsksNothing(t *testing.T) {
	held := anotherOwnersLock()
	held.Owner = client.GenerateCLIOwner()
	srv := &lockTestServer{held: held, releaseStatus: http.StatusOK, releaseBody: map[string]any{}}
	endpoint := srv.start(t)
	withoutTerminal(t)

	out, err := runUnlockCmd(endpoint, true)
	require.NoError(t, err)
	assert.NotContains(t, out, "belongs to another owner")
	assert.Contains(t, out, "Force released lock for orders (mysql)\n")
	require.Len(t, srv.releases, 1)
}

// The answer covers the lock the prompt named. When another lock holds the
// database by the time the unlock goes to release it, nothing is released.
func TestUnlockCmdForceDoesNotReleaseALockItDidNotShow(t *testing.T) {
	replacement := anotherOwnersLock()
	replacement.ID = 4
	replacement.Owner = "octo-org/orders#9"
	srv := &lockTestServer{held: anotherOwnersLock(), heldLater: replacement, releaseStatus: http.StatusOK, releaseBody: map[string]any{}}
	endpoint := srv.start(t)
	answerAtTerminal(t, "yes\n")

	out, err := runUnlockCmd(endpoint, true)
	require.Error(t, err)
	assert.Equal(t, "the lock on orders (mysql) is now held by octo-org/orders#9, a lock you were not shown, "+
		"so nothing was changed; run the command again to review it", err.Error())
	assert.NotContains(t, out, "Force released")
	assert.Empty(t, srv.releases, "a lock the prompt did not name must not be released")
}

// A server that does not report lock IDs cannot tell the lock the prompt named
// from one the same owner took again, so a forced command that would ask is
// refused before it asks, and -y is named as the way to go ahead.
func TestForcedTakeoverIsRefusedWhenTheServerReportsNoLockID(t *testing.T) {
	t.Run("unlock", func(t *testing.T) {
		held := anotherOwnersLock()
		held.ID = 0
		srv := &lockTestServer{held: held, releaseStatus: http.StatusOK, releaseBody: map[string]any{}}
		endpoint := srv.start(t)
		answerAtTerminal(t, "yes\n")

		out, err := runUnlockCmd(endpoint, true)
		require.Error(t, err)
		assert.Equal(t, "the lock on orders (mysql) is held by octo-org/orders#7, and this SchemaBot server does not report lock IDs, "+
			"so the lock released could not be checked against the one you approve; it was not released. "+
			"Add -y to release it without asking, or upgrade the server", err.Error())
		assert.NotContains(t, out, unlockTakeoverPrompt)
		assert.Empty(t, srv.releases)
	})
	t.Run("apply", func(t *testing.T) {
		srv := &forceApplyServer{locks: []string{lockBody(t, 0, "cli:alice@build-host")}}
		endpoint := srv.start(t)
		answerAtTerminal(t, "yes\n")

		out, err := srv.run(t, endpoint, false)
		require.Error(t, err)
		assert.Equal(t, "the lock on testdb (mysql) is held by cli:alice@build-host, and this SchemaBot server does not report lock IDs, "+
			"so the lock released could not be checked against the one you approve; nothing was applied. "+
			"Add -y to take it over without asking, or upgrade the server", err.Error())
		assert.NotContains(t, out, "Do you want to apply")
		assert.Empty(t, srv.releases)
		assert.Zero(t, srv.acquires)
	})
}

// forceApplyServer serves a forced apply of the testdb mysql database up to
// the lock acquire, which it answers with an error so the apply ends there.
// Each lock lookup answers the next entry of locks, and the last entry answers
// every lookup after it; an empty entry is no lock. It records the lock
// releases it receives and counts the acquires.
type forceApplyServer struct {
	locks []string

	mu       sync.Mutex
	lookups  int
	releases []map[string]any
	acquires int
}

func (s *forceApplyServer) start(t *testing.T) string {
	t.Helper()
	t.Setenv("SCHEMABOT_TOKEN", "fictional-force-lock-test")
	plan := planWithTablesAndEngine("mysql", createUsers())
	plan.PlanID = "plan-force-lock"
	planBody, err := json.Marshal(plan)
	require.NoError(t, err)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		s.mu.Lock()
		defer s.mu.Unlock()
		switch r.Method + " " + r.URL.Path {
		case "POST /api/plan":
			_, writeErr := w.Write(planBody)
			assert.NoError(t, writeErr)
		case "GET /api/status":
			_, writeErr := w.Write([]byte(`{}`))
			assert.NoError(t, writeErr)
		case "GET /api/locks/testdb/mysql":
			lock := s.locks[min(s.lookups, len(s.locks)-1)]
			s.lookups++
			if lock == "" {
				w.WriteHeader(http.StatusNotFound)
				_, writeErr := w.Write([]byte(`{"error":"lock not found"}`))
				assert.NoError(t, writeErr)
				return
			}
			_, writeErr := w.Write([]byte(`{"lock":` + lock + `}`))
			assert.NoError(t, writeErr)
		case "DELETE /api/locks":
			var sent map[string]any
			assert.NoError(t, json.NewDecoder(r.Body).Decode(&sent))
			s.releases = append(s.releases, sent)
			_, writeErr := w.Write([]byte(`{}`))
			assert.NoError(t, writeErr)
		case "POST /api/locks/acquire":
			s.acquires++
			w.WriteHeader(http.StatusInternalServerError)
			_, writeErr := w.Write([]byte(`{"error":"storage unavailable"}`))
			assert.NoError(t, writeErr)
		default:
			http.Error(w, "unexpected request", http.StatusBadRequest)
		}
	}))
	t.Cleanup(server.Close)
	return server.URL
}

func (s *forceApplyServer) run(t *testing.T, endpoint string, autoApprove bool) (string, error) {
	t.Helper()
	cmd := ApplyCmd{SchemaDir: writeTestSchemaDir(t), Environment: "staging", AutoApprove: autoApprove, Force: true, Output: OutputFormatLog}
	var err error
	out := stripAnsi(captureStdout(func() { err = cmd.Run(t.Context(), &Globals{Endpoint: endpoint}) }))
	return out, err
}

const (
	applyTakeoverWarning = "\n" + glyph.Attention + "  The lock on testdb (mysql) belongs to another owner\n" +
		"   Held by: cli:alice@build-host\n" +
		"   Since:   unknown\n" +
		"   Applying with --force releases it and takes the lock for you.\n"
	applyTakeoverPrompt = "\nDo you want to apply these changes and take over that lock? Only 'yes' will be accepted: "
)

// A forced apply over another owner's lock shows who holds it in the apply
// confirmation. Declining stops before the lock is touched.
func TestForcedApplyDeclinedLeavesAnotherOwnersLockAlone(t *testing.T) {
	srv := &forceApplyServer{locks: []string{lockBody(t, 5, "cli:alice@build-host")}}
	endpoint := srv.start(t)
	answerAtTerminal(t, "no\n")

	out, err := srv.run(t, endpoint, false)
	require.NoError(t, err)
	assert.Contains(t, out, applyTakeoverWarning+applyTakeoverPrompt+"\nApply cancelled. The lock was not changed.\n")
	assert.Empty(t, srv.releases, "a declined apply must not release the lock")
	assert.Zero(t, srv.acquires, "a declined apply must not take the lock")
}

// Answering yes breaks the lock the prompt named and goes on to take it.
func TestForcedApplyConfirmedBreaksAnotherOwnersLock(t *testing.T) {
	srv := &forceApplyServer{locks: []string{lockBody(t, 5, "cli:alice@build-host")}}
	endpoint := srv.start(t)
	answerAtTerminal(t, "yes\n")

	out, err := srv.run(t, endpoint, false)
	require.ErrorContains(t, err, "acquire lock: ")
	assert.Contains(t, out, applyTakeoverWarning+applyTakeoverPrompt)
	assert.Contains(t, out, "Force released lock for testdb (mysql)\n   Previous owner: cli:alice@build-host\n")
	require.Len(t, srv.releases, 1)
	assert.Equal(t, map[string]any{"database": "testdb", "database_type": "mysql", "force": true}, srv.releases[0])
	assert.Equal(t, 1, srv.acquires)
}

// With -y a forced apply shows no lock prompt and needs no terminal, so
// scripts that pass it behave as they did.
func TestForcedApplyWithYesAsksNothing(t *testing.T) {
	srv := &forceApplyServer{locks: []string{lockBody(t, 5, "cli:alice@build-host")}}
	endpoint := srv.start(t)
	withoutTerminal(t)

	out, err := srv.run(t, endpoint, true)
	require.ErrorContains(t, err, "acquire lock: ")
	assert.NotContains(t, out, "belongs to another owner")
	assert.NotContains(t, out, "take over that lock")
	assert.Contains(t, out, "Force released lock for testdb (mysql)\n")
	assert.Len(t, srv.releases, 1)
}

// With no terminal and no -y a forced apply over another owner's lock is
// refused before it shows a plan, naming the holder and -y, and changes nothing.
func TestForcedApplyWithoutTerminalOrYesIsRefused(t *testing.T) {
	srv := &forceApplyServer{locks: []string{lockBody(t, 5, "cli:alice@build-host")}}
	endpoint := srv.start(t)
	withoutTerminal(t)

	out, err := srv.run(t, endpoint, false)
	require.Error(t, err)
	assert.Equal(t, "the lock on testdb (mysql) is held by cli:alice@build-host and there is no terminal to ask you on, "+
		"so nothing was applied; add -y to take it over without asking", err.Error())
	assert.NotContains(t, out, "Do you want to apply")
	assert.Empty(t, srv.releases)
	assert.Zero(t, srv.acquires)
}

// A forced apply where this owner holds the lock, or nobody does, keeps the
// plain apply confirmation: there is no one to take a lock from.
func TestForcedApplyWithNoOtherOwnersLockKeepsThePlainPrompt(t *testing.T) {
	cases := map[string][]string{
		"the callers own lock": {lockBody(t, 5, client.GenerateCLIOwner())},
		"no lock":              {""},
	}
	for name, locks := range cases {
		t.Run(name, func(t *testing.T) {
			srv := &forceApplyServer{locks: locks}
			endpoint := srv.start(t)
			answerAtTerminal(t, "no\n")

			out, err := srv.run(t, endpoint, false)
			require.NoError(t, err)
			assert.Contains(t, out, "\nDo you want to apply these changes? Only 'yes' will be accepted: \nApply cancelled.\n")
			assert.NotContains(t, out, "belongs to another owner")
			assert.Empty(t, srv.releases)
		})
	}
}

// The prompt covers the lock it named. When a different owner holds the lock by
// the time the apply goes to break it, nothing is released.
func TestForcedApplyDoesNotBreakALockItDidNotShow(t *testing.T) {
	srv := &forceApplyServer{locks: []string{
		lockBody(t, 5, "cli:alice@build-host"),
		lockBody(t, 6, "cli:bob@build-host"),
	}}
	endpoint := srv.start(t)
	answerAtTerminal(t, "yes\n")

	out, err := srv.run(t, endpoint, false)
	require.Error(t, err)
	assert.Equal(t, "the lock on testdb (mysql) is now held by cli:bob@build-host, a lock you were not shown, "+
		"so nothing was changed; run the command again to review it", err.Error())
	assert.NotContains(t, out, "Force released")
	assert.Empty(t, srv.releases)
	assert.Zero(t, srv.acquires)
}
