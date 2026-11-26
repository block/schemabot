package commands

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/block/schemabot/pkg/cmd/client"
)

// lockBody is the JSON lock the fake server reports for "testdb" (mysql),
// held by owner under lock row id; an id of zero is a server that does not
// report the row.
func lockBody(t *testing.T, id int64, owner string) string {
	t.Helper()
	body, err := json.Marshal(client.LockInfo{ID: id, Database: "testdb", DatabaseType: "mysql", Owner: owner})
	require.NoError(t, err)
	return string(body)
}

// acquireNoAnswerHeadlines is the first line of every message a command can
// print after an acquire that got no answer, so a test can check that only
// the one case that applies is shown.
var acquireNoAnswerHeadlines = []string{
	"is held under your owner, though the acquire request failed.",
	"is held under your owner, but the server did not say which lock it is, so it was left in place.",
	"is the one you already held, so the failed acquire took nothing.",
	"so the failed acquire took nothing.\n   Nothing was submitted. Run the command again once that lock is released.",
	"is not held, so the failed acquire took nothing.",
	"may have been taken: the acquire request failed, and the lock could not be checked.",
}

// releasedUnlessInUse is the message for a lock row under this command's owner
// that the command then releases unless an apply runs under it.
const releasedUnlessInUse = "🔒 Lock for testdb (mysql) is held under your owner, though the acquire request failed.\n" +
	"   Nothing was submitted, so it is released unless an apply runs under it.\n"

// The server reads the lock acquire of an apply or a rollback and the
// connection drops before it answers, so the command cannot tell whether it
// took the lock. It looks the lock up and tells the operator only the case
// that applies, on lines of its own, then fails with the acquire error. It
// submits nothing, and the only release it sends is the row-pinned one that
// keeps a lock an apply runs under.
func TestLockAcquireWithNoAnswerSaysWhatItLeftBehind(t *testing.T) {
	owner := client.GenerateCLIOwner()
	cases := []struct {
		name            string
		lockBefore      string
		lockAfter       string
		lockAfterStatus int
		releaseStatus   int
		releaseBody     string
		want            string
		wantReleases    int32
		wantLookupErr   bool
		wantNoUnlock    bool
	}{
		{
			// The lock was free before and a new row is held under this
			// command's owner now. Another command of the same user on the
			// same host shares the owner, so the row is not claimed as this
			// acquire's: it is released only if no apply runs under it, and
			// here none does.
			name:          "new row under this owner is released",
			lockAfter:     lockBody(t, acquiredLockRowID, owner),
			releaseStatus: http.StatusOK,
			want:          releasedUnlessInUse + "🔓 Lock released for testdb (mysql)\n",
			wantReleases:  1,
			wantNoUnlock:  true,
		},
		{
			// The owner held an earlier lock row that was released, and a
			// new row is held now: it is released the same way.
			name:          "row taken after an earlier one was released is released",
			lockBefore:    lockBody(t, 5, owner),
			lockAfter:     lockBody(t, acquiredLockRowID, owner),
			releaseStatus: http.StatusOK,
			want:          releasedUnlessInUse + "🔓 Lock released for testdb (mysql)\n",
			wantReleases:  1,
			wantNoUnlock:  true,
		},
		{
			// Another command of the same user on the same host reused the
			// new row and started an apply under it. The row-pinned release
			// keeps the lock, and the operator is told which apply holds it
			// rather than sent to unlock it.
			name:          "new row an apply runs under is kept",
			lockAfter:     lockBody(t, acquiredLockRowID, owner),
			releaseStatus: http.StatusConflict,
			releaseBody:   lockInUseAnswer,
			want: releasedUnlessInUse +
				"🔒 Lock kept for testdb (mysql): apply apply-b2c3d4e5 (running) runs under it.\n" +
				"   Watch that apply: schemabot progress apply-b2c3d4e5 -e staging\n",
			wantReleases: 1,
			wantNoUnlock: true,
		},
		{
			// The owner already held this very lock row before the command
			// ran, so the acquire took nothing new and the lock may guard an
			// earlier apply: it is left alone and no unlock is offered.
			name:       "lock already held before",
			lockBefore: lockBody(t, acquiredLockRowID, owner),
			lockAfter:  lockBody(t, acquiredLockRowID, owner),
			want: "🔒 Lock for testdb (mysql) is the one you already held, so the failed acquire took nothing.\n" +
				"   Nothing was submitted. Run the command again to retry.\n",
			wantNoUnlock: true,
		},
		{
			// The lock was free before, and the lookup finds a lock under
			// this owner without a row ID, from a server too old to report
			// it. Nothing says whether the acquire took it, and there is no
			// row to release, so it is left in place and no unlock is offered.
			name:      "lock under this owner without a row ID",
			lockAfter: lockBody(t, 0, owner),
			want: "🔒 Lock for testdb (mysql) is held under your owner, but the server did not say which lock it is, so it was left in place.\n" +
				"   Nothing was submitted. See who holds it: schemabot locks\n",
			wantNoUnlock: true,
		},
		{
			// Another owner took the lock, so the operator is told who holds
			// it and is not sent to unlock it.
			name:      "lock held by another owner",
			lockAfter: lockBody(t, acquiredLockRowID, "cli:alice@build-host"),
			want: "🔒 Lock for testdb (mysql) is held by cli:alice@build-host, so the failed acquire took nothing.\n" +
				"   Nothing was submitted. Run the command again once that lock is released.\n",
			wantNoUnlock: true,
		},
		{
			// No lock is held, so the acquire took nothing and the command
			// can be run again.
			name: "lock free",
			want: "🔓 Lock for testdb (mysql) is not held, so the failed acquire took nothing.\n" +
				"   Nothing was submitted. It is safe to run the command again.\n",
			wantNoUnlock: true,
		},
		{
			// The lookup fails too, so the operator gets wording that holds
			// either way: how to check, and how to release it if it is
			// theirs and no apply of theirs runs under it.
			name:            "lookup fails",
			lockAfterStatus: http.StatusInternalServerError,
			want: "🔒 Lock for testdb (mysql) may have been taken: the acquire request failed, and the lock could not be checked.\n" +
				"   Nothing was submitted. See who holds it: schemabot locks\n" +
				"   If you hold it and no apply of yours runs under it, release it: schemabot unlock -d testdb -t mysql\n",
			wantLookupErr: true,
		},
	}
	commands := []struct {
		name string
		run  func(t *testing.T, server *httptest.Server) (string, error)
	}{
		{"apply", func(t *testing.T, server *httptest.Server) (string, error) {
			return runUnsubmittedApply(t.Context(), writeTestSchemaDir(t), server)
		}},
		{"rollback", runUnsubmittedRollback},
	}
	for _, command := range commands {
		for _, tc := range cases {
			t.Run(command.name+"/"+tc.name, func(t *testing.T) {
				t.Setenv("SCHEMABOT_TOKEN", "fictional-unsubmitted-lock-test")
				server, releases := unsubmittedLockServer{dropAcquire: true, lockBefore: tc.lockBefore, lockAfter: tc.lockAfter, lockAfterStatus: tc.lockAfterStatus,
					applyStatus: http.StatusOK, applyBody: `{"accepted":true,"apply_id":"apply-never"}`, releaseStatus: tc.releaseStatus, releaseBody: tc.releaseBody}.start(t)

				out, err := command.run(t, server)

				require.Error(t, err)
				assert.True(t, client.ServerMayHaveActed(err), "the command fails with the acquire error: %v", err)
				assert.True(t, strings.HasPrefix(err.Error(), "acquire lock: "), err.Error())
				if tc.wantLookupErr {
					assert.Contains(t, err.Error(), "look up lock for testdb after the acquire failed")
				} else {
					assert.NotContains(t, err.Error(), "look up lock")
				}
				assert.NotContains(t, err.Error(), "release lock for testdb", "a release that answered is not a failure")
				assert.Contains(t, out, "\n"+tc.want)
				shown := 0
				for _, headline := range acquireNoAnswerHeadlines {
					shown += strings.Count(out, headline)
				}
				assert.Equal(t, 1, shown, "only the case that applies is shown:\n%s", out)
				if tc.wantNoUnlock {
					assert.NotContains(t, out, "schemabot unlock", "no owner-only unlock is offered")
				}
				assert.NotContains(t, out, "Lock acquired")
				assert.NotContains(t, out, "Applying")
				assert.NotContains(t, out, "may already have acted", "the lock message is not interrupted by the request warning")
				assert.Equal(t, tc.wantReleases, releases.Load(), out)
			})
		}
	}
}
