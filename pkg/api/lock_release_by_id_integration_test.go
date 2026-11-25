//go:build integration

package api

import (
	"database/sql"
	"encoding/json"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"

	_ "github.com/block/mysql"
	"github.com/block/spirit/pkg/utils"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/block/schemabot/pkg/apitypes"
	"github.com/block/schemabot/pkg/storage"
	"github.com/block/schemabot/pkg/storage/mysqlstore"
	"github.com/block/schemabot/pkg/tern"
)

// lockReleaseByIDOwner is the owner two commands one operator runs on one host
// share.
const lockReleaseByIDOwner = "cli:alice@laptop"

// lockReleaseByIDService is a service over real storage holding a stored plan
// for testapp's one production target, and that storage.
func lockReleaseByIDService(t *testing.T) (*Service, storage.Storage) {
	t.Helper()
	ctx := t.Context()
	db, err := sql.Open("block-mysql", newStorageDatabaseWithSchema(t).DSN)
	require.NoError(t, err)
	require.NoError(t, db.PingContext(ctx))
	t.Cleanup(func() { _ = db.Close() })
	st := mysqlstore.New(db)

	_, err = st.Plans().Create(ctx, &storage.Plan{
		PlanIdentifier: "plan-lock-reuse",
		Database:       "testapp",
		DatabaseType:   storage.DatabaseTypeMySQL,
		Deployment:     "eu",
		Target:         "testapp-001",
		Environment:    "production",
		Namespaces: map[string]*storage.NamespacePlanData{"testapp": {Tables: []storage.TableChange{
			{Namespace: "testapp", Table: "users", Operation: "alter", DDL: "ALTER TABLE `users` ADD COLUMN `email` varchar(255)"},
		}}},
		CreatedAt: time.Now(),
	})
	require.NoError(t, err)

	cfg := &ServerConfig{
		Databases: map[string]DatabaseConfig{
			"testapp": {
				Type: storage.DatabaseTypeMySQL,
				Environments: map[string]EnvironmentConfig{
					"production": {Deployment: "eu", Targets: targetNames("testapp-001")},
				},
			},
		},
	}
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelError}))
	svc := New(st, cfg, map[string]tern.Client{"eu/production": &mockTernClient{isRemote: true}}, logger)
	t.Cleanup(func() { utils.CloseAndLog(svc) })
	return svc, st
}

// serveLockRequest sends body to handler as method on target and returns the
// answer.
func serveLockRequest(t *testing.T, handler http.HandlerFunc, method, target, body string) *httptest.ResponseRecorder {
	t.Helper()
	req := httptest.NewRequestWithContext(t.Context(), method, target, strings.NewReader(body))
	rec := httptest.NewRecorder()
	handler(rec, req)
	return rec
}

// acquireTestappLock acquires the testapp lock under lockReleaseByIDOwner as
// a CLI command does and returns the answer.
func acquireTestappLock(t *testing.T, svc *Service) LockResponse {
	t.Helper()
	rec := serveLockRequest(t, svc.handleLockAcquire, http.MethodPost, "/api/locks/acquire",
		`{"database":"testapp","database_type":"mysql","owner":"`+lockReleaseByIDOwner+`"}`)
	require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())
	var resp LockResponse
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	require.NotNil(t, resp.Lock)
	require.NotZero(t, resp.Lock.ID, "the acquire names the lock row")
	return resp
}

// releaseTestappLockRow releases lock row id of testapp under
// lockReleaseByIDOwner as a CLI command does and returns the answer.
func releaseTestappLockRow(t *testing.T, svc *Service, id int64) *httptest.ResponseRecorder {
	t.Helper()
	return serveLockRequest(t, svc.handleLockRelease, http.MethodDelete, "/api/locks",
		`{"database":"testapp","database_type":"mysql","owner":"`+lockReleaseByIDOwner+`","id":`+strconv.FormatInt(id, 10)+`}`)
}

// An operator runs two commands at once from one laptop, so both share the
// owner cli:alice@laptop. Command A takes the testapp lock; command B finds
// that lock held under its owner and its apply is accepted under it. The
// server then refuses A's submit, and A releases the lock it took. The release
// names A's lock row, which B's apply records, so the lock stays held for B's
// apply and the answer names it.
func TestLockReleaseByID_KeepsALockAnotherCommandsApplyRunsUnder(t *testing.T) {
	ctx := t.Context()
	svc, st := lockReleaseByIDService(t)

	commandA := acquireTestappLock(t, svc)
	require.True(t, commandA.Created, "command A took the lock")
	commandB := acquireTestappLock(t, svc)
	require.False(t, commandB.Created, "command B found the lock command A took")
	require.Equal(t, commandA.Lock.ID, commandB.Lock.ID)

	applyRec := serveLockRequest(t, svc.handleApply, http.MethodPost, "/api/apply",
		`{"plan_id":"plan-lock-reuse","environment":"production","caller":"`+lockReleaseByIDOwner+`"}`)
	require.Equal(t, http.StatusOK, applyRec.Code, applyRec.Body.String())
	var applied apitypes.ApplyResponse
	require.NoError(t, json.Unmarshal(applyRec.Body.Bytes(), &applied))
	require.NotEmpty(t, applied.ApplyID)
	apply, err := st.Applies().GetByApplyIdentifier(ctx, applied.ApplyID)
	require.NoError(t, err)
	require.NotNil(t, apply)
	require.Equal(t, commandA.Lock.ID, apply.LockID, "command B's apply records the lock row command A took")

	rec := releaseTestappLockRow(t, svc, commandA.Lock.ID)

	require.Equal(t, http.StatusConflict, rec.Code, rec.Body.String())
	var resp apitypes.ErrorResponse
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	assert.Equal(t, apitypes.ErrCodeLockInUse, resp.ErrorCode)
	require.NotNil(t, resp.LockHolder)
	assert.Equal(t, applied.ApplyID, resp.LockHolder.ApplyID)
	assert.Equal(t, apply.State, resp.LockHolder.State)
	assert.Equal(t, "production", resp.LockHolder.Environment)

	held, err := st.Locks().Get(ctx, "testapp", storage.DatabaseTypeMySQL)
	require.NoError(t, err)
	require.NotNil(t, held, "the lock command B's apply runs under stays held")
	assert.Equal(t, commandA.Lock.ID, held.ID)
}

// Command A's lock is force-released, and command B, under the same owner,
// takes the lock again as a new row. A's release names its own row, so the
// lock B holds now stays held.
func TestLockReleaseByID_KeepsALockTakenAgainUnderTheSameOwner(t *testing.T) {
	ctx := t.Context()
	svc, st := lockReleaseByIDService(t)

	commandA := acquireTestappLock(t, svc)
	require.NoError(t, st.Locks().ForceRelease(ctx, "testapp", storage.DatabaseTypeMySQL))
	commandB := acquireTestappLock(t, svc)
	require.True(t, commandB.Created)
	require.NotEqual(t, commandA.Lock.ID, commandB.Lock.ID)

	rec := releaseTestappLockRow(t, svc, commandA.Lock.ID)

	require.Equal(t, http.StatusConflict, rec.Code, rec.Body.String())
	var resp apitypes.ErrorResponse
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	assert.Equal(t, apitypes.ErrCodeLockReplaced, resp.ErrorCode)
	held, err := st.Locks().Get(ctx, "testapp", storage.DatabaseTypeMySQL)
	require.NoError(t, err)
	require.NotNil(t, held, "the lock command B took stays held")
	assert.Equal(t, commandB.Lock.ID, held.ID)

	rec = releaseTestappLockRow(t, svc, commandB.Lock.ID)
	require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())
	held, err = st.Locks().Get(ctx, "testapp", storage.DatabaseTypeMySQL)
	require.NoError(t, err)
	assert.Nil(t, held, "command B's own release, naming its row, releases it")
}
