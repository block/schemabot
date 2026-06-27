//go:build integration

package webhook

import (
	"context"
	"database/sql"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/block/schemabot/pkg/api"
	ghclient "github.com/block/schemabot/pkg/github"
	ternv1 "github.com/block/schemabot/pkg/proto/ternv1"
	"github.com/block/schemabot/pkg/state"
	"github.com/block/schemabot/pkg/storage"
	"github.com/block/schemabot/pkg/storage/mysqlstore"
	"github.com/block/schemabot/pkg/tern"
	"github.com/block/spirit/pkg/utils"
	_ "github.com/go-sql-driver/mysql"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestE2EStopCommandRecordsDurableRequest verifies that a PR comment stop
// command records durable operator intent, acknowledges duplicate stop requests,
// and preserves each caller in apply logs for incident triage.
func TestE2EStopCommandRecordsDurableRequest(t *testing.T) {
	ctx := t.Context()
	schemabotDB, err := sql.Open("mysql", e2eSchemabotDSN)
	require.NoError(t, err)
	require.NoError(t, schemabotDB.PingContext(ctx))

	store := mysqlstore.New(schemabotDB)
	applyIdentifier := "apply_abcd1234"
	database := "stop_pr_comments_db"
	cleanupStopCommandTestRows(t, schemabotDB, applyIdentifier, database)
	t.Cleanup(func() {
		cleanupStopCommandTestRows(t, schemabotDB, applyIdentifier, database)
		utils.CloseAndLog(schemabotDB)
	})

	now := time.Now().UTC()
	apply := &storage.Apply{
		ApplyIdentifier: applyIdentifier,
		PlanID:          1,
		Database:        database,
		DatabaseType:    storage.DatabaseTypeMySQL,
		Repository:      "octocat/hello-world",
		PullRequest:     1,
		Environment:     "staging",
		Deployment:      database,
		Caller:          "github:creator@octocat/hello-world#1",
		Engine:          storage.EngineSpirit,
		State:           state.Apply.Running,
		Options:         []byte("{}"),
		CreatedAt:       now,
		UpdatedAt:       now,
	}
	tasks := []*storage.Task{
		{
			TaskIdentifier: "task_stop_pr_comments_users",
			PlanID:         1,
			Database:       database,
			DatabaseType:   storage.DatabaseTypeMySQL,
			Engine:         storage.EngineSpirit,
			Repository:     "octocat/hello-world",
			PullRequest:    1,
			Environment:    "staging",
			State:          state.Task.Running,
			Namespace:      database,
			TableName:      "users",
			DDL:            "ALTER TABLE `users` ADD COLUMN `email` varchar(255)",
			DDLAction:      "alter",
			Options:        []byte("{}"),
			CreatedAt:      now,
			UpdatedAt:      now,
		},
	}
	applyID, err := store.Applies().CreateWithTasks(ctx, apply, tasks)
	require.NoError(t, err)

	client, mux := setupGitHubServer(t)
	comments := make(chan string, 10)
	reactions := make(chan string, 10)
	mux.HandleFunc("POST /repos/octocat/hello-world/issues/1/comments", func(w http.ResponseWriter, r *http.Request) {
		var body struct {
			Body string `json:"body"`
		}
		require.NoError(t, json.NewDecoder(r.Body).Decode(&body))
		comments <- body.Body
		w.WriteHeader(http.StatusCreated)
		_ = json.NewEncoder(w).Encode(map[string]any{"id": 99})
	})
	mux.HandleFunc("POST /repos/octocat/hello-world/issues/comments/42/reactions", func(w http.ResponseWriter, r *http.Request) {
		var body struct {
			Content string `json:"content"`
		}
		require.NoError(t, json.NewDecoder(r.Body).Decode(&body))
		reactions <- body.Content
		w.WriteHeader(http.StatusCreated)
		_ = json.NewEncoder(w).Encode(map[string]any{"id": 1})
	})

	service := apiServiceForStopCommandTest(t, store, database)
	service.RegisterTernClient(database, "staging", &stopCommandTernClient{remote: true})
	h := &Handler{
		service:   service,
		ghClients: ghclient.NewSingleClientSet(defaultAppName, &fakeClientFactory{client: ghclient.NewInstallationClient(client, testLogger())}),
		logger:    testLogger(),
	}

	postStopCommand(t, h, applyIdentifier, "alice")
	firstComment := readComment(t, comments)
	assert.Contains(t, firstComment, "Stop Request Accepted")
	assert.Contains(t, firstComment, "`"+applyIdentifier+"`")
	assert.Contains(t, firstComment, "@alice")

	controlReq, err := store.ControlRequests().GetPending(ctx, applyID, storage.ControlOperationStop)
	require.NoError(t, err)
	require.NotNil(t, controlReq)
	assert.Equal(t, "github:alice@octocat/hello-world#1", controlReq.RequestedBy)
	assert.Equal(t, storage.ControlRequestPending, controlReq.Status)

	postStopCommand(t, h, applyIdentifier, "bob")
	secondComment := readComment(t, comments)
	assert.Contains(t, secondComment, "Stop was already requested")
	assert.Contains(t, secondComment, "@bob")

	logs, err := store.ApplyLogs().List(ctx, storage.ApplyLogFilter{ApplyID: applyID, Limit: 20})
	require.NoError(t, err)
	assert.True(t, applyLogContains(logs, "Stop requested by user (caller: github:alice@octocat/hello-world#1)"))
	assert.True(t, applyLogContains(logs, "Stop requested by user while stop request already pending (caller: github:bob@octocat/hello-world#1)"))

	assertReactionEventually(t, reactions)
	assertReactionEventually(t, reactions)
}

// TestE2ECancelCommandRecordsDurableRequest verifies that a PR comment cancel
// command records permanent cancel intent and preserves the caller in apply logs.
func TestE2ECancelCommandRecordsDurableRequest(t *testing.T) {
	ctx := t.Context()
	schemabotDB, err := sql.Open("mysql", e2eSchemabotDSN)
	require.NoError(t, err)
	require.NoError(t, schemabotDB.PingContext(ctx))

	store := mysqlstore.New(schemabotDB)
	applyIdentifier := "apply_cace1234"
	database := "cancel_pr_comments_db"
	cleanupStopCommandTestRows(t, schemabotDB, applyIdentifier, database)
	t.Cleanup(func() {
		cleanupStopCommandTestRows(t, schemabotDB, applyIdentifier, database)
		utils.CloseAndLog(schemabotDB)
	})
	applyID := createStopCommandApply(t, store, applyIdentifier, database)

	client, mux := setupGitHubServer(t)
	comments := make(chan string, 10)
	reactions := make(chan string, 10)
	mux.HandleFunc("POST /repos/octocat/hello-world/issues/1/comments", func(w http.ResponseWriter, r *http.Request) {
		var body struct {
			Body string `json:"body"`
		}
		require.NoError(t, json.NewDecoder(r.Body).Decode(&body))
		comments <- body.Body
		w.WriteHeader(http.StatusCreated)
		_ = json.NewEncoder(w).Encode(map[string]any{"id": 99})
	})
	mux.HandleFunc("POST /repos/octocat/hello-world/issues/comments/42/reactions", func(w http.ResponseWriter, r *http.Request) {
		var body struct {
			Content string `json:"content"`
		}
		require.NoError(t, json.NewDecoder(r.Body).Decode(&body))
		reactions <- body.Content
		w.WriteHeader(http.StatusCreated)
		_ = json.NewEncoder(w).Encode(map[string]any{"id": 1})
	})

	service := apiServiceForStopCommandTest(t, store, database)
	service.RegisterTernClient(database, "staging", &stopCommandTernClient{remote: true})
	h := &Handler{
		service:   service,
		ghClients: ghclient.NewSingleClientSet(defaultAppName, &fakeClientFactory{client: ghclient.NewInstallationClient(client, testLogger())}),
		logger:    testLogger(),
	}

	postCancelCommand(t, h, applyIdentifier, "alice")
	comment := readComment(t, comments)
	assert.Contains(t, comment, "Cancel Request Accepted")
	assert.Contains(t, comment, "`"+applyIdentifier+"`")
	assert.Contains(t, comment, "@alice")

	controlReq, err := store.ControlRequests().GetPending(ctx, applyID, storage.ControlOperationCancel)
	require.NoError(t, err)
	require.NotNil(t, controlReq)
	assert.Equal(t, "github:alice@octocat/hello-world#1", controlReq.RequestedBy)
	assert.Equal(t, storage.ControlRequestPending, controlReq.Status)
	logs, err := store.ApplyLogs().List(ctx, storage.ApplyLogFilter{ApplyID: applyID, Limit: 20})
	require.NoError(t, err)
	assert.True(t, applyLogContains(logs, "Cancel requested by user (caller: github:alice@octocat/hello-world#1)"))
	assertReactionEventually(t, reactions)
}

// TestE2EStopCommandQueuesDeferredCutoverLocalApplyWithoutRunner verifies that
// a PR comment stop command records durable stop intent when storage has an
// active local schema change but this process does not own the Spirit runner.
func TestE2EStopCommandQueuesDeferredCutoverLocalApplyWithoutRunner(t *testing.T) {
	ctx := t.Context()
	schemabotDB, err := sql.Open("mysql", e2eSchemabotDSN)
	require.NoError(t, err)
	require.NoError(t, schemabotDB.PingContext(ctx))

	store := mysqlstore.New(schemabotDB)
	applyIdentifier := "apply_bcde2345"
	database := "stop_pr_comments_active_db"
	cleanupStopCommandTestRows(t, schemabotDB, applyIdentifier, database)
	t.Cleanup(func() {
		cleanupStopCommandTestRows(t, schemabotDB, applyIdentifier, database)
		utils.CloseAndLog(schemabotDB)
	})

	applyID := createStopCommandApply(t, store, applyIdentifier, database)
	storedApply, err := store.Applies().GetByApplyIdentifier(ctx, applyIdentifier)
	require.NoError(t, err)
	require.NotNil(t, storedApply)
	storedApply.State = state.Apply.WaitingForCutover
	storedApply.SetOptions(storage.ApplyOptions{DeferCutover: true})
	require.NoError(t, store.Applies().Update(ctx, storedApply))
	storedTasks, err := store.Tasks().GetByApplyID(ctx, applyID)
	require.NoError(t, err)
	require.Len(t, storedTasks, 1)
	storedTasks[0].State = state.Task.WaitingForCutover
	storedTasks[0].UpdatedAt = time.Now().UTC()
	require.NoError(t, store.Tasks().Update(ctx, storedTasks[0]))

	client, mux := setupGitHubServer(t)
	comments := make(chan string, 10)
	reactions := make(chan string, 10)
	mux.HandleFunc("POST /repos/octocat/hello-world/issues/1/comments", func(w http.ResponseWriter, r *http.Request) {
		var body struct {
			Body string `json:"body"`
		}
		require.NoError(t, json.NewDecoder(r.Body).Decode(&body))
		comments <- body.Body
		w.WriteHeader(http.StatusCreated)
		_ = json.NewEncoder(w).Encode(map[string]any{"id": 99})
	})
	mux.HandleFunc("POST /repos/octocat/hello-world/issues/comments/42/reactions", func(w http.ResponseWriter, r *http.Request) {
		var body struct {
			Content string `json:"content"`
		}
		require.NoError(t, json.NewDecoder(r.Body).Decode(&body))
		reactions <- body.Content
		w.WriteHeader(http.StatusCreated)
		_ = json.NewEncoder(w).Encode(map[string]any{"id": 1})
	})

	service := apiServiceForStopCommandTest(t, store, database)
	localClient, err := tern.NewLocalClient(tern.LocalConfig{
		Database:  database,
		Type:      storage.DatabaseTypeMySQL,
		TargetDSN: e2eTargetDSN,
	}, store, testLogger())
	require.NoError(t, err)
	t.Cleanup(func() {
		utils.CloseAndLog(localClient)
	})
	service.RegisterTernClient(database, "staging", localClient)
	h := &Handler{
		service:   service,
		ghClients: ghclient.NewSingleClientSet(defaultAppName, &fakeClientFactory{client: ghclient.NewInstallationClient(client, testLogger())}),
		logger:    testLogger(),
	}

	postStopCommand(t, h, applyIdentifier, "alice")
	comment := readComment(t, comments)
	assert.Contains(t, comment, "Stop Request Accepted")
	assert.Contains(t, comment, "@alice")

	storedApply, err = store.Applies().GetByApplyIdentifier(ctx, applyIdentifier)
	require.NoError(t, err)
	require.NotNil(t, storedApply)
	assert.Equal(t, state.Apply.WaitingForCutover, storedApply.State)
	assert.True(t, storedApply.GetOptions().DeferCutover)
	storedTasks, err = store.Tasks().GetByApplyID(ctx, applyID)
	require.NoError(t, err)
	require.Len(t, storedTasks, 1)
	assert.Equal(t, state.Task.WaitingForCutover, storedTasks[0].State)
	pendingStop, err := store.ControlRequests().GetPending(ctx, applyID, storage.ControlOperationStop)
	require.NoError(t, err)
	require.NotNil(t, pendingStop)
	assert.Equal(t, storage.ControlRequestPending, pendingStop.Status)
	assertReactionEventually(t, reactions)
}

// TestE2EStartCommandRecordsDurableRequest verifies that a PR comment start
// command records durable operator intent for a stopped apply without directly
// claiming execution from the webhook process.
func TestE2EStartCommandRecordsDurableRequest(t *testing.T) {
	ctx := t.Context()
	schemabotDB, err := sql.Open("mysql", e2eSchemabotDSN)
	require.NoError(t, err)
	require.NoError(t, schemabotDB.PingContext(ctx))

	store := mysqlstore.New(schemabotDB)
	applyIdentifier := "apply_bcdf2345"
	database := "start_pr_comments_db"
	cleanupStopCommandTestRows(t, schemabotDB, applyIdentifier, database)
	t.Cleanup(func() {
		cleanupStopCommandTestRows(t, schemabotDB, applyIdentifier, database)
		utils.CloseAndLog(schemabotDB)
	})

	applyID := createStopCommandApply(t, store, applyIdentifier, database)
	storedApply, err := store.Applies().GetByApplyIdentifier(ctx, applyIdentifier)
	require.NoError(t, err)
	require.NotNil(t, storedApply)
	storedApply.State = state.Apply.Stopped
	storedApply.UpdatedAt = time.Now().UTC()
	require.NoError(t, store.Applies().Update(ctx, storedApply))
	storedTasks, err := store.Tasks().GetByApplyID(ctx, applyID)
	require.NoError(t, err)
	require.Len(t, storedTasks, 1)
	storedTasks[0].State = state.Task.Stopped
	storedTasks[0].UpdatedAt = time.Now().UTC()
	require.NoError(t, store.Tasks().Update(ctx, storedTasks[0]))

	client, mux := setupGitHubServer(t)
	comments := make(chan string, 10)
	reactions := make(chan string, 10)
	mux.HandleFunc("POST /repos/octocat/hello-world/issues/1/comments", func(w http.ResponseWriter, r *http.Request) {
		var body struct {
			Body string `json:"body"`
		}
		require.NoError(t, json.NewDecoder(r.Body).Decode(&body))
		comments <- body.Body
		w.WriteHeader(http.StatusCreated)
		_ = json.NewEncoder(w).Encode(map[string]any{"id": 99})
	})
	mux.HandleFunc("POST /repos/octocat/hello-world/issues/comments/42/reactions", func(w http.ResponseWriter, r *http.Request) {
		var body struct {
			Content string `json:"content"`
		}
		require.NoError(t, json.NewDecoder(r.Body).Decode(&body))
		reactions <- body.Content
		w.WriteHeader(http.StatusCreated)
		_ = json.NewEncoder(w).Encode(map[string]any{"id": 1})
	})

	service := apiServiceForStopCommandTest(t, store, database)
	service.RegisterTernClient(database, "staging", &stopCommandTernClient{})
	h := &Handler{
		service:   service,
		ghClients: ghclient.NewSingleClientSet(defaultAppName, &fakeClientFactory{client: ghclient.NewInstallationClient(client, testLogger())}),
		logger:    testLogger(),
	}

	postStartCommand(t, h, applyIdentifier, "alice")
	firstComment := readComment(t, comments)
	assert.Contains(t, firstComment, "Start Request Accepted")
	assert.Contains(t, firstComment, "`"+applyIdentifier+"`")
	assert.Contains(t, firstComment, "@alice")

	controlReq, err := store.ControlRequests().GetPending(ctx, applyID, storage.ControlOperationStart)
	require.NoError(t, err)
	require.NotNil(t, controlReq)
	assert.Equal(t, "github:alice@octocat/hello-world#1", controlReq.RequestedBy)
	assert.Equal(t, storage.ControlRequestPending, controlReq.Status)

	postStartCommand(t, h, applyIdentifier, "bob")
	secondComment := readComment(t, comments)
	assert.Contains(t, secondComment, "Start was already requested")
	assert.Contains(t, secondComment, "@bob")

	logs, err := store.ApplyLogs().List(ctx, storage.ApplyLogFilter{ApplyID: applyID, Limit: 20})
	require.NoError(t, err)
	assert.True(t, applyLogContains(logs, "Start requested by user (caller: github:alice@octocat/hello-world#1)"))
	assert.True(t, applyLogContains(logs, "Start requested by user (caller: github:bob@octocat/hello-world#1)"))

	assertReactionEventually(t, reactions)
	assertReactionEventually(t, reactions)
}

// TestE2EStartCommandRejectsCompletedApply verifies that a PR comment start
// command surfaces an actionable error when the apply is already terminal and
// does not record a durable start request.
func TestE2EStartCommandRejectsCompletedApply(t *testing.T) {
	ctx := t.Context()
	schemabotDB, err := sql.Open("mysql", e2eSchemabotDSN)
	require.NoError(t, err)
	require.NoError(t, schemabotDB.PingContext(ctx))

	store := mysqlstore.New(schemabotDB)
	applyIdentifier := "apply_cfab3456"
	database := "start_pr_comments_completed_db"
	cleanupStopCommandTestRows(t, schemabotDB, applyIdentifier, database)
	t.Cleanup(func() {
		cleanupStopCommandTestRows(t, schemabotDB, applyIdentifier, database)
		utils.CloseAndLog(schemabotDB)
	})

	applyID := createStopCommandApply(t, store, applyIdentifier, database)
	storedApply, err := store.Applies().GetByApplyIdentifier(ctx, applyIdentifier)
	require.NoError(t, err)
	require.NotNil(t, storedApply)
	storedApply.State = state.Apply.Completed
	storedApply.UpdatedAt = time.Now().UTC()
	require.NoError(t, store.Applies().Update(ctx, storedApply))

	client, mux := setupGitHubServer(t)
	comments := make(chan string, 10)
	reactions := make(chan string, 10)
	mux.HandleFunc("POST /repos/octocat/hello-world/issues/1/comments", func(w http.ResponseWriter, r *http.Request) {
		var body struct {
			Body string `json:"body"`
		}
		require.NoError(t, json.NewDecoder(r.Body).Decode(&body))
		comments <- body.Body
		w.WriteHeader(http.StatusCreated)
		_ = json.NewEncoder(w).Encode(map[string]any{"id": 99})
	})
	mux.HandleFunc("POST /repos/octocat/hello-world/issues/comments/42/reactions", func(w http.ResponseWriter, r *http.Request) {
		var body struct {
			Content string `json:"content"`
		}
		require.NoError(t, json.NewDecoder(r.Body).Decode(&body))
		reactions <- body.Content
		w.WriteHeader(http.StatusCreated)
		_ = json.NewEncoder(w).Encode(map[string]any{"id": 1})
	})

	service := apiServiceForStopCommandTest(t, store, database)
	service.RegisterTernClient(database, "staging", &stopCommandTernClient{})
	h := &Handler{
		service:   service,
		ghClients: ghclient.NewSingleClientSet(defaultAppName, &fakeClientFactory{client: ghclient.NewInstallationClient(client, testLogger())}),
		logger:    testLogger(),
	}

	postStartCommand(t, h, applyIdentifier, "alice")
	comment := readComment(t, comments)
	assert.Contains(t, comment, "Start Failed")
	assert.Contains(t, comment, "already completed and cannot be started")

	pendingStart, err := store.ControlRequests().GetPending(ctx, applyID, storage.ControlOperationStart)
	require.NoError(t, err)
	assert.Nil(t, pendingStart)
	assertReactionEventually(t, reactions)
}

// TestE2ECutoverCommandRecordsDurableRequest verifies that a PR comment
// cutover command records durable operator intent for the exact apply and
// environment, then leaves the operator owner to perform the data-plane action.
func TestE2ECutoverCommandRecordsDurableRequest(t *testing.T) {
	ctx := t.Context()
	schemabotDB, err := sql.Open("mysql", e2eSchemabotDSN)
	require.NoError(t, err)
	require.NoError(t, schemabotDB.PingContext(ctx))

	store := mysqlstore.New(schemabotDB)
	applyIdentifier := "apply_cdef3456"
	database := "cutover_pr_comments_db"
	cleanupStopCommandTestRows(t, schemabotDB, applyIdentifier, database)
	t.Cleanup(func() {
		cleanupStopCommandTestRows(t, schemabotDB, applyIdentifier, database)
		utils.CloseAndLog(schemabotDB)
	})

	applyID := createStopCommandApply(t, store, applyIdentifier, database)
	storedApply, err := store.Applies().GetByApplyIdentifier(ctx, applyIdentifier)
	require.NoError(t, err)
	require.NotNil(t, storedApply)
	storedApply.State = state.Apply.WaitingForCutover
	storedApply.SetOptions(storage.ApplyOptions{DeferCutover: true})
	require.NoError(t, store.Applies().Update(ctx, storedApply))
	storedTasks, err := store.Tasks().GetByApplyID(ctx, applyID)
	require.NoError(t, err)
	require.Len(t, storedTasks, 1)
	storedTasks[0].State = state.Task.WaitingForCutover
	storedTasks[0].UpdatedAt = time.Now().UTC()
	require.NoError(t, store.Tasks().Update(ctx, storedTasks[0]))

	client, mux := setupGitHubServer(t)
	comments := make(chan string, 10)
	reactions := make(chan string, 10)
	mux.HandleFunc("POST /repos/octocat/hello-world/issues/1/comments", func(w http.ResponseWriter, r *http.Request) {
		var body struct {
			Body string `json:"body"`
		}
		require.NoError(t, json.NewDecoder(r.Body).Decode(&body))
		comments <- body.Body
		w.WriteHeader(http.StatusCreated)
		_ = json.NewEncoder(w).Encode(map[string]any{"id": 99})
	})
	mux.HandleFunc("POST /repos/octocat/hello-world/issues/comments/42/reactions", func(w http.ResponseWriter, r *http.Request) {
		var body struct {
			Content string `json:"content"`
		}
		require.NoError(t, json.NewDecoder(r.Body).Decode(&body))
		reactions <- body.Content
		w.WriteHeader(http.StatusCreated)
		_ = json.NewEncoder(w).Encode(map[string]any{"id": 1})
	})

	service := apiServiceForStopCommandTest(t, store, database)
	service.RegisterTernClient(database, "staging", &stopCommandTernClient{remote: true})
	h := &Handler{
		service:   service,
		ghClients: ghclient.NewSingleClientSet(defaultAppName, &fakeClientFactory{client: ghclient.NewInstallationClient(client, testLogger())}),
		logger:    testLogger(),
	}

	postCutoverCommand(t, h, applyIdentifier, "alice")
	comment := readComment(t, comments)
	assert.Contains(t, comment, "Cutover Request Accepted")
	assert.Contains(t, comment, "`"+applyIdentifier+"`")
	assert.Contains(t, comment, "@alice")

	controlReq, err := store.ControlRequests().GetPending(ctx, applyID, storage.ControlOperationCutover)
	require.NoError(t, err)
	require.NotNil(t, controlReq)
	assert.Equal(t, "github:alice@octocat/hello-world#1", controlReq.RequestedBy)
	assert.Equal(t, storage.ControlRequestPending, controlReq.Status)

	logs, err := store.ApplyLogs().List(ctx, storage.ApplyLogFilter{ApplyID: applyID, Limit: 20})
	require.NoError(t, err)
	assert.True(t, applyLogContains(logs, "Cutover requested by user (caller: github:alice@octocat/hello-world#1)"))
	assertReactionEventually(t, reactions)
}

// TestE2ECutoverCommandRejectsPendingStop verifies that a PR comment cutover
// command honors an outstanding stop request for the same apply, preserving the
// operator's stop intent and surfacing the rejection back to the PR.
func TestE2ECutoverCommandRejectsPendingStop(t *testing.T) {
	ctx := t.Context()
	schemabotDB, err := sql.Open("mysql", e2eSchemabotDSN)
	require.NoError(t, err)
	require.NoError(t, schemabotDB.PingContext(ctx))

	store := mysqlstore.New(schemabotDB)
	applyIdentifier := "apply_defa4567"
	database := "cutover_pr_comments_stop_pending_db"
	cleanupStopCommandTestRows(t, schemabotDB, applyIdentifier, database)
	t.Cleanup(func() {
		cleanupStopCommandTestRows(t, schemabotDB, applyIdentifier, database)
		utils.CloseAndLog(schemabotDB)
	})

	applyID := createStopCommandApply(t, store, applyIdentifier, database)
	storedApply, err := store.Applies().GetByApplyIdentifier(ctx, applyIdentifier)
	require.NoError(t, err)
	require.NotNil(t, storedApply)
	storedApply.State = state.Apply.WaitingForCutover
	storedApply.SetOptions(storage.ApplyOptions{DeferCutover: true})
	require.NoError(t, store.Applies().Update(ctx, storedApply))
	_, alreadyPending, err := store.ControlRequests().RequestPending(ctx, &storage.ApplyControlRequest{
		ApplyID:     applyID,
		Operation:   storage.ControlOperationStop,
		RequestedBy: "github:stopper@octocat/hello-world#1",
	})
	require.NoError(t, err)
	require.False(t, alreadyPending)

	client, mux := setupGitHubServer(t)
	comments := make(chan string, 10)
	reactions := make(chan string, 10)
	mux.HandleFunc("POST /repos/octocat/hello-world/issues/1/comments", func(w http.ResponseWriter, r *http.Request) {
		var body struct {
			Body string `json:"body"`
		}
		require.NoError(t, json.NewDecoder(r.Body).Decode(&body))
		comments <- body.Body
		w.WriteHeader(http.StatusCreated)
		_ = json.NewEncoder(w).Encode(map[string]any{"id": 99})
	})
	mux.HandleFunc("POST /repos/octocat/hello-world/issues/comments/42/reactions", func(w http.ResponseWriter, r *http.Request) {
		var body struct {
			Content string `json:"content"`
		}
		require.NoError(t, json.NewDecoder(r.Body).Decode(&body))
		reactions <- body.Content
		w.WriteHeader(http.StatusCreated)
		_ = json.NewEncoder(w).Encode(map[string]any{"id": 1})
	})

	service := apiServiceForStopCommandTest(t, store, database)
	ternClient := &stopCommandTernClient{remote: true}
	service.RegisterTernClient(database, "staging", ternClient)
	h := &Handler{
		service:   service,
		ghClients: ghclient.NewSingleClientSet(defaultAppName, &fakeClientFactory{client: ghclient.NewInstallationClient(client, testLogger())}),
		logger:    testLogger(),
	}

	postCutoverCommand(t, h, applyIdentifier, "cutter")
	comment := readComment(t, comments)
	assert.Contains(t, comment, "pending stop request")
	assert.Contains(t, comment, "cutover")

	pendingStop, err := store.ControlRequests().GetPending(ctx, applyID, storage.ControlOperationStop)
	require.NoError(t, err)
	require.NotNil(t, pendingStop)
	assert.Equal(t, "github:stopper@octocat/hello-world#1", pendingStop.RequestedBy)
	pendingCutover, err := store.ControlRequests().GetPending(ctx, applyID, storage.ControlOperationCutover)
	require.NoError(t, err)
	assert.Nil(t, pendingCutover)
	logs, err := store.ApplyLogs().List(ctx, storage.ApplyLogFilter{ApplyID: applyID, Limit: 20})
	require.NoError(t, err)
	assert.True(t, applyLogContains(logs, "Pending stop request blocked cutover (caller: github:stopper@octocat/hello-world#1)"))
	assertReactionEventually(t, reactions)
}

func apiServiceForStopCommandTest(t *testing.T, store storage.Storage, database string) *api.Service {
	t.Helper()
	return api.New(store, &api.ServerConfig{
		Databases: map[string]api.DatabaseConfig{
			database: {
				Type: storage.DatabaseTypeMySQL,
				Environments: map[string]api.EnvironmentConfig{
					"staging": {},
				},
			},
		},
	}, nil, testLogger())
}

func createStopCommandApply(t *testing.T, store storage.Storage, applyIdentifier, database string) int64 {
	t.Helper()
	now := time.Now().UTC()
	apply := &storage.Apply{
		ApplyIdentifier: applyIdentifier,
		PlanID:          1,
		Database:        database,
		DatabaseType:    storage.DatabaseTypeMySQL,
		Repository:      "octocat/hello-world",
		PullRequest:     1,
		Environment:     "staging",
		Deployment:      database,
		Caller:          "github:creator@octocat/hello-world#1",
		Engine:          storage.EngineSpirit,
		State:           state.Apply.Running,
		Options:         []byte("{}"),
		CreatedAt:       now,
		UpdatedAt:       now,
	}
	tasks := []*storage.Task{
		{
			TaskIdentifier: "task_" + applyIdentifier,
			PlanID:         1,
			Database:       database,
			DatabaseType:   storage.DatabaseTypeMySQL,
			Engine:         storage.EngineSpirit,
			Repository:     "octocat/hello-world",
			PullRequest:    1,
			Environment:    "staging",
			State:          state.Task.Running,
			Namespace:      database,
			TableName:      "users",
			DDL:            "ALTER TABLE `users` ADD COLUMN `email` varchar(255)",
			DDLAction:      "alter",
			Options:        []byte("{}"),
			CreatedAt:      now,
			UpdatedAt:      now,
		},
	}
	applyID, err := store.Applies().CreateWithTasks(t.Context(), apply, tasks)
	require.NoError(t, err)
	return applyID
}

func cleanupStopCommandTestRows(t *testing.T, db *sql.DB, applyIdentifier, database string) {
	t.Helper()
	cleanupCtx := context.WithoutCancel(t.Context())
	statements := []string{
		"DELETE al FROM `apply_logs` al JOIN `applies` a ON al.`apply_id` = a.`id` WHERE a.`apply_identifier` = ?",
		"DELETE acr FROM `apply_control_requests` acr JOIN `applies` a ON acr.`apply_id` = a.`id` WHERE a.`apply_identifier` = ?",
		"DELETE FROM `tasks` WHERE `database_name` = ?",
		"DELETE FROM `applies` WHERE `apply_identifier` = ?",
	}
	args := [][]any{{applyIdentifier}, {applyIdentifier}, {database}, {applyIdentifier}}
	for i, stmt := range statements {
		_, err := db.ExecContext(cleanupCtx, stmt, args[i]...)
		require.NoError(t, err)
	}
}

func postStopCommand(t *testing.T, h *Handler, applyIdentifier, user string) {
	t.Helper()
	req := buildWebhookRequest(t, webhookPayloadOpts{
		comment:   "schemabot stop " + applyIdentifier + " -e staging",
		userLogin: user,
		isPR:      true,
	}, nil)
	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, req)
	require.Equal(t, http.StatusOK, rr.Code)
	assert.Contains(t, rr.Body.String(), "stop started")
}

func postCancelCommand(t *testing.T, h *Handler, applyIdentifier, user string) {
	t.Helper()
	req := buildWebhookRequest(t, webhookPayloadOpts{
		comment:   "schemabot cancel " + applyIdentifier + " -e staging",
		userLogin: user,
		isPR:      true,
	}, nil)
	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, req)
	require.Equal(t, http.StatusOK, rr.Code)
	assert.Contains(t, rr.Body.String(), "cancel started")
}

func postStartCommand(t *testing.T, h *Handler, applyIdentifier, user string) {
	t.Helper()
	req := buildWebhookRequest(t, webhookPayloadOpts{
		comment:   "schemabot start " + applyIdentifier + " -e staging",
		userLogin: user,
		isPR:      true,
	}, nil)
	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, req)
	require.Equal(t, http.StatusOK, rr.Code)
	assert.Contains(t, rr.Body.String(), "start started")
}

func postCutoverCommand(t *testing.T, h *Handler, applyIdentifier, user string) {
	t.Helper()
	req := buildWebhookRequest(t, webhookPayloadOpts{
		comment:   "schemabot cutover " + applyIdentifier + " -e staging",
		userLogin: user,
		isPR:      true,
	}, nil)
	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, req)
	require.Equal(t, http.StatusOK, rr.Code)
	assert.Contains(t, rr.Body.String(), "cutover started")
}

func readComment(t *testing.T, comments chan string) string {
	t.Helper()
	select {
	case body := <-comments:
		return body
	case <-time.After(webhookIntegrationCheckRunDeadline):
		t.Fatal("timed out waiting for control command comment")
		return ""
	}
}

func assertReactionEventually(t *testing.T, reactions chan string) {
	t.Helper()
	select {
	case reaction := <-reactions:
		assert.Equal(t, "eyes", reaction)
	case <-time.After(webhookIntegrationCheckRunDeadline):
		t.Fatal("timed out waiting for acknowledgment reaction")
	}
}

func applyLogContains(logs []*storage.ApplyLog, want string) bool {
	for _, log := range logs {
		if strings.Contains(log.Message, want) {
			return true
		}
	}
	return false
}

type stopCommandTernClient struct {
	tern.Client
	mu          sync.Mutex
	remote      bool
	stopCalls   int
	cancelCalls int
	onStop      func(context.Context, *ternv1.StopRequest) (*ternv1.StopResponse, error)
}

func (c *stopCommandTernClient) Plan(context.Context, *ternv1.PlanRequest) (*ternv1.PlanResponse, error) {
	return nil, nil
}

func (c *stopCommandTernClient) Apply(context.Context, *ternv1.ApplyRequest) (*ternv1.ApplyResponse, error) {
	return nil, nil
}

func (c *stopCommandTernClient) Progress(context.Context, *ternv1.ProgressRequest) (*ternv1.ProgressResponse, error) {
	return nil, nil
}

func (c *stopCommandTernClient) Cutover(context.Context, *ternv1.CutoverRequest) (*ternv1.CutoverResponse, error) {
	return nil, nil
}

func (c *stopCommandTernClient) Stop(ctx context.Context, req *ternv1.StopRequest) (*ternv1.StopResponse, error) {
	c.mu.Lock()
	c.stopCalls++
	c.mu.Unlock()
	if c.onStop != nil {
		return c.onStop(ctx, req)
	}
	return &ternv1.StopResponse{Accepted: true, StoppedCount: 1}, nil
}

func (c *stopCommandTernClient) Cancel(context.Context, *ternv1.CancelRequest) (*ternv1.CancelResponse, error) {
	c.mu.Lock()
	c.cancelCalls++
	c.mu.Unlock()
	return &ternv1.CancelResponse{Accepted: true, CancelledCount: 1}, nil
}

func (c *stopCommandTernClient) Start(context.Context, *ternv1.StartRequest) (*ternv1.StartResponse, error) {
	return nil, nil
}

func (c *stopCommandTernClient) Volume(context.Context, *ternv1.VolumeRequest) (*ternv1.VolumeResponse, error) {
	return nil, nil
}

func (c *stopCommandTernClient) Revert(context.Context, *ternv1.RevertRequest) (*ternv1.RevertResponse, error) {
	return nil, nil
}

func (c *stopCommandTernClient) SkipRevert(context.Context, *ternv1.SkipRevertRequest) (*ternv1.SkipRevertResponse, error) {
	return nil, nil
}

func (c *stopCommandTernClient) Health(context.Context) error { return nil }

func (c *stopCommandTernClient) ResumeApply(context.Context, *storage.Apply) error { return nil }

func (c *stopCommandTernClient) Endpoint() string { return "stop-command-test" }

func (c *stopCommandTernClient) IsRemote() bool { return c.remote }

func (c *stopCommandTernClient) SetPendingObserver(tern.ProgressObserver) {}

func (c *stopCommandTernClient) SetObserver(int64, tern.ProgressObserver) {}

func (c *stopCommandTernClient) Close() error { return nil }
