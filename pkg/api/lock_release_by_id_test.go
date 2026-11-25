package api

import (
	"encoding/json"
	"log/slog"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/block/schemabot/pkg/apitypes"
	"github.com/block/schemabot/pkg/auth"
	"github.com/block/schemabot/pkg/storage"
)

// lockReleaseByIDCase is one answer of a release that names the lock row a
// lock acquire reported.
type lockReleaseByIDCase struct {
	name string
	// held is the lock held when the release arrives; nil when none is.
	held *storage.Lock
	// applyUsingLock is an apply that can still change and records a lock
	// row as its lock.
	applyUsingLock *storage.Apply
	body           string
	wantStatus     int
	wantCode       string
	wantMessage    string
	// wantHeld is the lock held after the release; nil when it is released.
	wantHeld *storage.Lock
	// wantHolder is the apply the answer names as keeping the lock.
	wantHolder *apitypes.LockHolder
}

const releaseLockRow7 = `{"database":"payments","database_type":"mysql","owner":"cli:bob@laptop","id":7}`

// lockReleaseByIDCases are the answers a release naming lock row 7 under the
// owner cli:bob@laptop gets, whoever may release by that owner.
func lockReleaseByIDCases(acquirer *storage.LockAcquirer) []lockReleaseByIDCase {
	const owner = "cli:bob@laptop"
	return []lockReleaseByIDCase{
		{
			name:       "the row it names is held and unused, so it is released",
			held:       paymentsLock(7, owner, acquirer),
			body:       releaseLockRow7,
			wantStatus: http.StatusOK,
		},
		{
			// Two commands of one account on one host share an owner: the
			// second found the row the first took and its apply runs under it.
			name: "an apply of a command that reused the row runs under it, so it is kept",
			held: paymentsLock(7, owner, acquirer),
			applyUsingLock: &storage.Apply{
				ApplyIdentifier: "apply-b2c3d4e5", LockID: 7, Database: "payments", DatabaseType: "mysql",
				Environment: "staging", State: "running",
			},
			body:        releaseLockRow7,
			wantStatus:  http.StatusConflict,
			wantCode:    apitypes.ErrCodeLockInUse,
			wantMessage: `lock on database "payments" was kept: apply apply-b2c3d4e5 (running) runs under it; nothing was released`,
			wantHeld:    paymentsLock(7, owner, acquirer),
			wantHolder:  &apitypes.LockHolder{ApplyID: "apply-b2c3d4e5", State: "running", Environment: "staging"},
		},
		{
			// The row was force-released and the same owner took the lock
			// again, so row 9 now holds it.
			name:        "a newer row of the same owner holds the lock, so it is kept",
			held:        paymentsLock(9, owner, acquirer),
			body:        releaseLockRow7,
			wantStatus:  http.StatusConflict,
			wantCode:    apitypes.ErrCodeLockReplaced,
			wantMessage: `lock on database "payments" is no longer the lock this release named: it was released and taken again since; nothing was released`,
			wantHeld:    paymentsLock(9, owner, acquirer),
		},
		{
			name:        "no lock is held",
			body:        releaseLockRow7,
			wantStatus:  http.StatusNotFound,
			wantMessage: "lock not found",
		},
	}
}

// runLockReleaseByIDCase sends tc's release as user under cfg and checks the
// answer and the lock left held.
func runLockReleaseByIDCase(t *testing.T, cfg *ServerConfig, user *auth.User, tc lockReleaseByIDCase) {
	t.Helper()
	locks := &memoryLockStore{lock: tc.held, applyUsingLock: tc.applyUsingLock}
	svc := New(&mockStorageWithApplyStores{locks: locks}, cfg, nil, slog.New(slog.DiscardHandler))
	rec := scopedDenialRequest(t, svc.handleLockRelease, user, http.MethodDelete, "/api/locks", tc.body)

	require.Equal(t, tc.wantStatus, rec.Code, rec.Body.String())
	assert.Equal(t, tc.wantHeld, locks.lock)
	if tc.wantStatus == http.StatusOK {
		assert.JSONEq(t, `{"status":"released"}`, rec.Body.String())
		return
	}
	var resp apitypes.ErrorResponse
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	assert.Equal(t, tc.wantCode, resp.ErrorCode)
	assert.Equal(t, tc.wantMessage, resp.Error)
	assert.Equal(t, tc.wantHolder, resp.LockHolder)
}

// A release that names the lock row its acquire reported removes that row
// only while no apply that can still change runs under it. The owner string is
// shared by every command one account runs on one host, so a lock another of
// those commands reused for its apply, or a lock taken again under the same
// owner since, stays held, and the answer says which.
func TestLockReleaseByIDKeepsALockItDidNotTakeOrThatAnApplyUses(t *testing.T) {
	admin := &auth.User{Subject: "alice", Groups: []string{"schema-admins"}}
	for _, tc := range lockReleaseByIDCases(nil) {
		t.Run(tc.name, func(t *testing.T) {
			runLockReleaseByIDCase(t, scopedWriteConfig(), admin, tc)
		})
	}
}

// A scoped operator's release that names a lock row gets the same answers,
// after the check that the lock belongs to their operator group.
func TestScopedLockReleaseByIDKeepsALockItDidNotTakeOrThatAnApplyUses(t *testing.T) {
	cfg := scopedWriteConfig()
	teammate := &auth.User{Subject: "carol", Groups: []string{"payments-team"}}
	teamAcquirer := &storage.LockAcquirer{Subject: "bob", OperatorGroups: []string{"payments-team"}}
	for _, tc := range lockReleaseByIDCases(teamAcquirer) {
		t.Run(tc.name, func(t *testing.T) {
			runLockReleaseByIDCase(t, cfg, teammate, tc)
		})
	}
}

// A release that names a lock row keeps the lock when that row is held by
// another owner, or when a plan has been pinned to it since, and says which.
func TestLockReleaseByIDKeepsAnotherOwnersRowAndAPinnedPlan(t *testing.T) {
	admin := &auth.User{Subject: "alice", Groups: []string{"schema-admins"}}
	t.Run("another owner holds the row", func(t *testing.T) {
		runLockReleaseByIDCase(t, scopedWriteConfig(), admin, lockReleaseByIDCase{
			held:        paymentsLock(7, "cli:carol@laptop", nil),
			body:        releaseLockRow7,
			wantStatus:  http.StatusForbidden,
			wantCode:    apitypes.ErrCodeLockNotOwned,
			wantMessage: "lock is not owned by you",
			wantHeld:    paymentsLock(7, "cli:carol@laptop", nil),
		})
	})
	t.Run("a plan was pinned to the row", func(t *testing.T) {
		held := paymentsLock(7, "cli:bob@laptop", nil)
		held.PendingPlanID = "plan-1"
		wantHeld := paymentsLock(7, "cli:bob@laptop", nil)
		wantHeld.PendingPlanID = "plan-1"
		runLockReleaseByIDCase(t, scopedWriteConfig(), admin, lockReleaseByIDCase{
			held:        held,
			body:        releaseLockRow7,
			wantStatus:  http.StatusConflict,
			wantCode:    apitypes.ErrCodeLockReplaced,
			wantMessage: `lock on database "payments" is no longer the lock this release named: a plan was pinned to it since; nothing was released`,
			wantHeld:    wantHeld,
		})
	})
}

// A scoped operator's release naming a row that another owner's newer row has
// replaced says the lock was taken again, not that it belongs to someone else:
// the row it named is gone, which is the fact the operator acts on.
func TestScopedLockReleaseByIDReportsAReplacedRowBeforeOwnership(t *testing.T) {
	teammate := &auth.User{Subject: "carol", Groups: []string{"payments-team"}}
	teamAcquirer := &storage.LockAcquirer{Subject: "dave", OperatorGroups: []string{"payments-team"}}
	runLockReleaseByIDCase(t, scopedWriteConfig(), teammate, lockReleaseByIDCase{
		held:        paymentsLock(9, "cli:dave@laptop", teamAcquirer),
		body:        releaseLockRow7,
		wantStatus:  http.StatusConflict,
		wantCode:    apitypes.ErrCodeLockReplaced,
		wantMessage: `lock on database "payments" is no longer the lock this release named: it was released and taken again since; nothing was released`,
		wantHeld:    paymentsLock(9, "cli:dave@laptop", teamAcquirer),
	})
}

// A release that sends no lock row releases by owner, as it always has: an
// older CLI and `schemabot unlock` send none.
func TestLockReleaseWithoutIDReleasesByOwner(t *testing.T) {
	admin := &auth.User{Subject: "alice", Groups: []string{"schema-admins"}}
	runLockReleaseByIDCase(t, scopedWriteConfig(), admin, lockReleaseByIDCase{
		held: paymentsLock(9, "cli:bob@laptop", nil),
		applyUsingLock: &storage.Apply{
			ApplyIdentifier: "apply-b2c3d4e5", LockID: 9, Database: "payments", DatabaseType: "mysql", State: "running",
		},
		body:       `{"database":"payments","database_type":"mysql","owner":"cli:bob@laptop"}`,
		wantStatus: http.StatusOK,
	})
}

// A lock row ID is refused where it cannot mean what it says: a forced release
// removes whichever lock is held, and no lock row has an ID below one.
func TestLockReleaseRefusesAnIDItCannotHonour(t *testing.T) {
	admin := &auth.User{Subject: "alice", Groups: []string{"schema-admins"}}
	for _, tc := range []lockReleaseByIDCase{
		{
			name:        "with force",
			body:        `{"database":"payments","database_type":"mysql","force":true,"id":7}`,
			wantMessage: "id cannot be combined with force: a forced release removes whichever lock is held",
		},
		{
			name:        "negative",
			body:        `{"database":"payments","database_type":"mysql","owner":"cli:bob@laptop","id":-7}`,
			wantMessage: "id must be the positive lock ID a lock acquire reported",
		},
		{
			name:        "zero",
			body:        `{"database":"payments","database_type":"mysql","owner":"cli:bob@laptop","id":0}`,
			wantMessage: "id must be the positive lock ID a lock acquire reported",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			tc.held = paymentsLock(7, "cli:bob@laptop", nil)
			tc.wantHeld = paymentsLock(7, "cli:bob@laptop", nil)
			tc.wantStatus = http.StatusBadRequest
			runLockReleaseByIDCase(t, scopedWriteConfig(), admin, tc)
		})
	}
}
