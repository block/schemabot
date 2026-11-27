package commands

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/block/schemabot/pkg/apitypes"
	"github.com/block/schemabot/pkg/cmd/client"
)

// lockTestServer answers the lock API for one database lock. held is the lock
// the server holds on orders (mysql), or nil when there is none, and
// releaseStatus and releaseBody answer every release request. A non-zero
// lookupStatus or listStatus makes the lock lookup or the lock listing fail
// with that status. It records the body of each release request and the paths
// of the reads it serves.
type lockTestServer struct {
	held          *client.LockInfo
	listed        []*client.LockInfo
	releaseStatus int
	releaseBody   any
	lookupStatus  int
	listStatus    int

	releases []map[string]any
	reads    []string
}

// lockReadFailure is the error body the server sends when a lock read fails.
var lockReadFailure = apitypes.ErrorResponse{Error: "storage unavailable"}

func (s *lockTestServer) start(t *testing.T) string {
	t.Helper()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch {
		case r.Method == http.MethodDelete && r.URL.Path == "/api/locks":
			var body map[string]any
			assert.NoError(t, json.NewDecoder(r.Body).Decode(&body))
			s.releases = append(s.releases, body)
			w.WriteHeader(s.releaseStatus)
			assert.NoError(t, json.NewEncoder(w).Encode(s.releaseBody))
		case r.Method == http.MethodGet && r.URL.Path == "/api/locks/orders/mysql":
			s.reads = append(s.reads, r.URL.Path)
			if s.lookupStatus != 0 {
				w.WriteHeader(s.lookupStatus)
				assert.NoError(t, json.NewEncoder(w).Encode(lockReadFailure))
				return
			}
			if s.held == nil {
				w.WriteHeader(http.StatusNotFound)
				assert.NoError(t, json.NewEncoder(w).Encode(apitypes.ErrorResponse{Error: "lock not found"}))
				return
			}
			assert.NoError(t, json.NewEncoder(w).Encode(map[string]any{"lock": s.held}))
		case r.Method == http.MethodGet && r.URL.Path == "/api/locks":
			s.reads = append(s.reads, r.URL.Path)
			if s.listStatus != 0 {
				w.WriteHeader(s.listStatus)
				assert.NoError(t, json.NewEncoder(w).Encode(lockReadFailure))
				return
			}
			assert.NoError(t, json.NewEncoder(w).Encode(map[string]any{"locks": s.listed}))
		default:
			http.Error(w, "unexpected request", http.StatusBadRequest)
		}
	}))
	t.Cleanup(server.Close)
	return server.URL
}

// runLockCmd runs a lock command against endpoint and returns its output with
// the terminal colors removed.
func runLockCmd(endpoint string, run func(*Globals) error) (string, error) {
	var runErr error
	output := captureStdout(func() {
		runErr = run(&Globals{Endpoint: endpoint})
	})
	return stripAnsi(output), runErr
}

func runUnlockCmd(endpoint string, force bool) (string, error) {
	cmd := UnlockCmd{Database: "orders", Type: "mysql", Force: force}
	return runLockCmd(endpoint, cmd.Run)
}

// runApprovedForceUnlockCmd runs a forced unlock that was given -y.
func runApprovedForceUnlockCmd(endpoint string) (string, error) {
	cmd := UnlockCmd{Database: "orders", Type: "mysql", Force: true, AutoApprove: true}
	return runLockCmd(endpoint, cmd.Run)
}

// An operator lists the database locks and sees each lock's database, type,
// owner, age, pull request and last activity, followed by the commands that
// release one.
func TestLocksCmdListsEveryLock(t *testing.T) {
	srv := &lockTestServer{listed: []*client.LockInfo{
		{Database: "orders", DatabaseType: "mysql", Owner: "octo-org/orders#12", Repository: "octo-org/orders", PullRequest: 12, CreatedAt: time.Now().Add(-2 * time.Hour), UpdatedAt: time.Now().Add(-5 * time.Minute)},
		{Database: "games", DatabaseType: "vitess", Owner: "cli:dana@laptop"},
	}}
	endpoint := srv.start(t)

	out, err := runLockCmd(endpoint, (&LocksCmd{}).Run)
	require.NoError(t, err)
	assert.Contains(t, out, "🔒 Active Locks (2)\n")
	assert.Contains(t, out, "  1. orders (mysql)\n     Owner: octo-org/orders#12\n     Since: 2 hours ago\n")
	assert.Contains(t, out, "     PR:    octo-org/orders#12\n     Last activity: 5 minutes ago\n")
	assert.Contains(t, out, "  2. games (vitess)\n     Owner: cli:dana@laptop\n     Since: unknown\n")
	assert.Contains(t, out, "schemabot unlock -d <database> -t <type> --force  # override ownership")
}

// With no locks held, the listing says so in one line.
func TestLocksCmdReportsNoLocks(t *testing.T) {
	srv := &lockTestServer{}
	endpoint := srv.start(t)

	out, err := runLockCmd(endpoint, (&LocksCmd{}).Run)
	require.NoError(t, err)
	assert.Equal(t, "No active locks.\n", out)
}

// A listing the server fails to answer is an error, not an empty listing that
// reads as "no locks".
func TestLocksCmdReturnsAListFailureAsAnError(t *testing.T) {
	srv := &lockTestServer{listStatus: http.StatusInternalServerError}
	endpoint := srv.start(t)

	out, err := runLockCmd(endpoint, (&LocksCmd{}).Run)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "list locks: ")
	assert.Contains(t, err.Error(), "storage unavailable")
	assert.NotContains(t, out, "No active locks.")
}

// Releasing a lock of another database type sends the release for the database
// and type the operator named, not the mysql default.
func TestUnlockCmdReleasesTheNamedType(t *testing.T) {
	srv := &lockTestServer{releaseStatus: http.StatusOK, releaseBody: map[string]any{}}
	endpoint := srv.start(t)

	out, err := runLockCmd(endpoint, (&UnlockCmd{Database: "games", Type: "vitess"}).Run)
	require.NoError(t, err)
	assert.Equal(t, "🔓 Lock released for games (vitess)\n", out)
	require.Len(t, srv.releases, 1)
	assert.Equal(t, "games", srv.releases[0]["database"])
	assert.Equal(t, "vitess", srv.releases[0]["database_type"])
}

// An operator releasing a lock they hold sends a release naming themselves as
// the owner, without forcing it, and is told the lock is released.
func TestUnlockCmdReleasesTheCallersOwnLock(t *testing.T) {
	srv := &lockTestServer{releaseStatus: http.StatusOK, releaseBody: map[string]any{}}
	endpoint := srv.start(t)

	out, err := runUnlockCmd(endpoint, false)
	require.NoError(t, err)
	assert.Equal(t, "🔓 Lock released for orders (mysql)\n", out)
	require.Len(t, srv.releases, 1)
	assert.Equal(t, map[string]any{
		"database":      "orders",
		"database_type": "mysql",
		"owner":         client.GenerateCLIOwner(),
	}, srv.releases[0], "an owned release names the caller and never forces")
}

// An operator releasing a lock someone else holds is refused. The CLI names
// the current owner and points at --force, exits non-zero without printing an
// error, and does not retry the release by force on its own.
func TestUnlockCmdNamesTheOwnerOfALockItDoesNotHold(t *testing.T) {
	srv := &lockTestServer{
		held:          &client.LockInfo{Database: "orders", DatabaseType: "mysql", Owner: "octo-org/orders#7"},
		releaseStatus: http.StatusForbidden,
		releaseBody: apitypes.ErrorResponse{
			Error:     "lock is not owned by you",
			ErrorCode: apitypes.ErrCodeLockNotOwned,
		},
	}
	endpoint := srv.start(t)

	out, err := runUnlockCmd(endpoint, false)
	require.ErrorIs(t, err, ErrSilent)
	assert.Equal(t, 1, ExitCodeFor(err))
	assert.Contains(t, out, "Cannot release lock")
	assert.Contains(t, out, "  Database:      orders (mysql)\n  Current owner: octo-org/orders#7\n")
	assert.Contains(t, out, "Use --force to release a lock owned by someone else.")
	require.Len(t, srv.releases, 1, "a refused release must not be retried")
	assert.NotContains(t, srv.releases[0], "force")
}

// A release refused for ownership still fails when the owner cannot be named,
// whether the owner lookup fails or the lock is gone by the time it runs.
func TestUnlockCmdRefusalWithoutAKnownOwnerIsAnError(t *testing.T) {
	refusal := apitypes.ErrorResponse{Error: "lock is not owned by you", ErrorCode: apitypes.ErrCodeLockNotOwned}
	for name, srv := range map[string]*lockTestServer{
		"owner lookup fails": {lookupStatus: http.StatusInternalServerError, releaseStatus: http.StatusForbidden, releaseBody: refusal},
		"lock is gone":       {releaseStatus: http.StatusForbidden, releaseBody: refusal},
	} {
		t.Run(name, func(t *testing.T) {
			endpoint := srv.start(t)

			out, err := runUnlockCmd(endpoint, false)
			require.EqualError(t, err, "lock is not owned by you")
			assert.NotContains(t, out, "Lock released")
			require.Len(t, srv.releases, 1, "a refused release must not be retried")
		})
	}
}

// A release the server denies for authorization rather than ownership is
// returned as an error with the server's reason, not reported as a lock owned
// by someone else.
func TestUnlockCmdReturnsAnAuthorizationDenialAsAnError(t *testing.T) {
	srv := &lockTestServer{
		held:          &client.LockInfo{Database: "orders", DatabaseType: "mysql", Owner: "octo-org/orders#7"},
		releaseStatus: http.StatusForbidden,
		releaseBody:   apitypes.ErrorResponse{Error: "releasing locks on orders requires membership in db-admins"},
	}
	endpoint := srv.start(t)

	out, err := runUnlockCmd(endpoint, false)
	require.Error(t, err)
	assert.NotErrorIs(t, err, ErrSilent)
	assert.Equal(t, "release lock: releasing locks on orders requires membership in db-admins", err.Error())
	assert.NotContains(t, out, "Cannot release lock")
	assert.Empty(t, srv.reads, "a denial is not an ownership refusal, so the owner is not looked up")
}

// An operator force-releasing another owner's lock sends a forced release that
// names no owner, and is told whose lock was released.
func TestUnlockCmdForceReleasesAnotherOwnersLock(t *testing.T) {
	srv := &lockTestServer{
		held:          &client.LockInfo{Database: "orders", DatabaseType: "mysql", Owner: "octo-org/orders#7"},
		releaseStatus: http.StatusOK,
		releaseBody:   map[string]any{},
	}
	endpoint := srv.start(t)

	out, err := runApprovedForceUnlockCmd(endpoint)
	require.NoError(t, err)
	assert.Contains(t, out, "Force released lock for orders (mysql)\n")
	assert.Contains(t, out, "   Previous owner: octo-org/orders#7\n")
	require.Len(t, srv.releases, 1)
	assert.Equal(t, map[string]any{
		"database":      "orders",
		"database_type": "mysql",
		"force":         true,
	}, srv.releases[0])
}

// A forced release the server denies is an error carrying the server's reason,
// so the operator and any script running the command see it failed.
func TestUnlockCmdReturnsAForceReleaseDenialAsAnError(t *testing.T) {
	srv := &lockTestServer{
		held:          &client.LockInfo{Database: "orders", DatabaseType: "mysql", Owner: "octo-org/orders#7"},
		releaseStatus: http.StatusForbidden,
		releaseBody:   apitypes.ErrorResponse{Error: "force releasing locks on orders requires membership in db-admins"},
	}
	endpoint := srv.start(t)

	out, err := runApprovedForceUnlockCmd(endpoint)
	require.Error(t, err)
	assert.Equal(t, "force release lock: force releasing locks on orders requires membership in db-admins", err.Error())
	assert.NotContains(t, out, "Force released")
}

// When the lock lookup before a forced release fails, the command fails and
// sends no release, since it cannot tell whether a lock is held.
func TestUnlockCmdForceStopsWhenTheLockLookupFails(t *testing.T) {
	srv := &lockTestServer{lookupStatus: http.StatusInternalServerError, releaseStatus: http.StatusOK, releaseBody: map[string]any{}}
	endpoint := srv.start(t)

	_, err := runUnlockCmd(endpoint, true)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "check lock: ")
	assert.Contains(t, err.Error(), "storage unavailable")
	assert.Empty(t, srv.releases, "a failed lookup must not be followed by a forced release")
}

// Forcing the release of a database that holds no lock reports that no lock
// was found and sends no release.
func TestUnlockCmdForceSendsNothingWhenNoLockIsHeld(t *testing.T) {
	srv := &lockTestServer{releaseStatus: http.StatusOK, releaseBody: map[string]any{}}
	endpoint := srv.start(t)

	out, err := runUnlockCmd(endpoint, true)
	require.NoError(t, err)
	assert.Equal(t, "No lock found for orders (mysql)\n", out)
	assert.Empty(t, srv.releases, "a database with no lock must not be sent a forced release")
}
