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
// held by owner under lock row id.
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
	"was taken, though the acquire request failed.",
	"is the one you already held, so the failed acquire took nothing.",
	"so the failed acquire took nothing.\n   Nothing was submitted. Run the command again once that lock is released.",
	"is not held, so the failed acquire took nothing.",
	"may have been taken: the acquire request failed, and the lock could not be checked.",
}

// The server reads the lock acquire of an apply or a rollback and the
// connection drops before it answers, so the command cannot tell whether it
// took the lock. It looks the lock up and tells the operator only the case
// that applies, on lines of its own, then fails with the acquire error. It
// submits nothing and releases nothing in every case.
func TestLockAcquireWithNoAnswerSaysWhatItLeftBehind(t *testing.T) {
	owner := client.GenerateCLIOwner()
	cases := []struct {
		name            string
		lockBefore      string
		lockAfter       string
		lockAfterStatus int
		want            string
		wantLookupErr   bool
	}{
		{
			// The lock was free before and is held under this command's
			// owner now, so the acquire took it: the operator is given the
			// command that releases it.
			name:      "lock taken",
			lockAfter: lockBody(t, acquiredLockRowID, owner),
			want: "🔒 Lock for testdb (mysql) was taken, though the acquire request failed.\n" +
				"   Nothing was submitted. Release it now: schemabot unlock -d testdb -t mysql\n",
		},
		{
			// The owner held an earlier lock row that was released, and the
			// acquire took a new one: that is still a lock this command
			// took.
			name:       "lock taken after an earlier one was released",
			lockBefore: lockBody(t, 5, owner),
			lockAfter:  lockBody(t, acquiredLockRowID, owner),
			want: "🔒 Lock for testdb (mysql) was taken, though the acquire request failed.\n" +
				"   Nothing was submitted. Release it now: schemabot unlock -d testdb -t mysql\n",
		},
		{
			// The owner already held this very lock row before the command
			// ran, so the acquire took nothing new and the lock may guard an
			// earlier apply: no unlock command is offered.
			name:       "lock already held before",
			lockBefore: lockBody(t, acquiredLockRowID, owner),
			lockAfter:  lockBody(t, acquiredLockRowID, owner),
			want: "🔒 Lock for testdb (mysql) is the one you already held, so the failed acquire took nothing.\n" +
				"   Nothing was submitted. Run the command again to retry.\n",
		},
		{
			// Another owner took the lock, so the operator is told who holds
			// it and is not sent to unlock it.
			name:      "lock held by another owner",
			lockAfter: lockBody(t, acquiredLockRowID, "cli:alice@build-host"),
			want: "🔒 Lock for testdb (mysql) is held by cli:alice@build-host, so the failed acquire took nothing.\n" +
				"   Nothing was submitted. Run the command again once that lock is released.\n",
		},
		{
			// No lock is held, so the acquire took nothing and the command
			// can be run again.
			name: "lock free",
			want: "🔓 Lock for testdb (mysql) is not held, so the failed acquire took nothing.\n" +
				"   Nothing was submitted. It is safe to run the command again.\n",
		},
		{
			// The lookup fails too, so the operator gets wording that holds
			// either way: how to check, and how to release it if it is
			// theirs.
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
					applyStatus: http.StatusOK, applyBody: `{"accepted":true,"apply_id":"apply-never"}`, releaseStatus: http.StatusOK}.start(t)

				out, err := command.run(t, server)

				require.Error(t, err)
				assert.True(t, client.ServerMayHaveActed(err), "the command fails with the acquire error: %v", err)
				assert.True(t, strings.HasPrefix(err.Error(), "acquire lock: "), err.Error())
				if tc.wantLookupErr {
					assert.Contains(t, err.Error(), "look up lock for testdb after the acquire failed")
				} else {
					assert.NotContains(t, err.Error(), "look up lock")
				}
				assert.Contains(t, out, "\n"+tc.want)
				shown := 0
				for _, headline := range acquireNoAnswerHeadlines {
					shown += strings.Count(out, headline)
				}
				assert.Equal(t, 1, shown, "only the case that applies is shown:\n%s", out)
				assert.NotContains(t, out, "Lock acquired")
				assert.NotContains(t, out, "Applying")
				assert.NotContains(t, out, "may already have acted", "the lock message is not interrupted by the request warning")
				assert.Equal(t, int32(0), releases.Load(), out)
			})
		}
	}
}
