//go:build integration

package tern

import (
	"context"
	"database/sql"
	"fmt"
	"log/slog"
	"net"
	"os"
	"sync"
	"testing"
	"time"

	drivermysql "github.com/block/mysql"
	"github.com/block/spirit/pkg/utils"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	waitutil "github.com/block/schemabot/e2e/testutil"
	"github.com/block/schemabot/pkg/ddl"
	"github.com/block/schemabot/pkg/engine"
	ternv1 "github.com/block/schemabot/pkg/proto/ternv1"
	"github.com/block/schemabot/pkg/schema"
	"github.com/block/schemabot/pkg/state"
	"github.com/block/schemabot/pkg/storage"
	"github.com/block/schemabot/pkg/storage/mysqlstore"
)

// failOnceThenSucceedEngine fails its first apply attempt retryably and
// completes every later attempt, modeling a transient target-side failure
// (for example an out-of-memory during the row copy) that the data plane's
// own recovery resolves by retrying.
type failOnceThenSucceedEngine struct {
	engine.Engine

	mu            sync.Mutex
	applyAttempts int
}

// Name reports the canonical spirit engine name: the data plane persists it on
// the apply row and the gRPC Progress handler rejects non-canonical names.
func (e *failOnceThenSucceedEngine) Name() string { return storage.EngineSpirit }

// Plan echoes the reviewed change so the dispatch-time drift gate sees the
// live target agreeing with the reviewed plan.
func (e *failOnceThenSucceedEngine) Plan(context.Context, *engine.PlanRequest) (*engine.PlanResult, error) {
	return &engine.PlanResult{
		Changes: []engine.SchemaChange{{
			Namespace: "testdb",
			TableChanges: []engine.TableChange{{
				Table:     "users",
				Operation: ddl.StatementAlterTable,
				DDL:       "ALTER TABLE `users` ADD COLUMN retry_note VARCHAR(255)",
			}},
		}},
	}, nil
}

func (e *failOnceThenSucceedEngine) Apply(context.Context, *engine.ApplyRequest) (*engine.ApplyResult, error) {
	e.mu.Lock()
	e.applyAttempts++
	e.mu.Unlock()
	return &engine.ApplyResult{Accepted: true}, nil
}

func (e *failOnceThenSucceedEngine) Progress(context.Context, *engine.ProgressRequest) (*engine.ProgressResult, error) {
	e.mu.Lock()
	attempts := e.applyAttempts
	e.mu.Unlock()
	if attempts <= 1 {
		return &engine.ProgressResult{
			State:        engine.StateFailed,
			Retryable:    true,
			ErrorMessage: "failed to execute chunklet insert: Error 1041 (HY000): Out of memory",
			Tables: []engine.TableProgress{{
				Namespace: "testdb",
				Table:     "users",
				State:     state.Task.FailedRetryable,
				Progress:  100,
			}},
		}, nil
	}
	return &engine.ProgressResult{
		State: engine.StateCompleted,
		Tables: []engine.TableProgress{{
			Namespace: "testdb",
			Table:     "users",
			State:     state.Task.Completed,
			Progress:  100,
		}},
	}, nil
}

// createControlPlaneStorage creates a fresh logical database on the shared
// container and bootstraps the storage schema in it. The control plane and
// data plane of a remote apply own separate storage in production; sharing one
// database in a cross-plane test would let the two planes' apply rows collide
// on the one-active-apply-per-target gate.
func createControlPlaneStorage(t *testing.T, dpDSN string) storage.Storage {
	t.Helper()
	cfg, err := drivermysql.ParseDSN(dpDSN)
	require.NoError(t, err, "parse data-plane DSN")
	databaseName := fmt.Sprintf("schemabot_cp_%d", time.Now().UnixNano())

	adminCfg := *cfg
	adminCfg.DBName = ""
	adminDB, err := sql.Open("block-mysql", adminCfg.FormatDSN())
	require.NoError(t, err, "open admin connection for control-plane storage database")
	defer utils.CloseAndLog(adminDB)
	require.NoError(t, adminDB.PingContext(t.Context()), "ping admin connection")
	_, err = adminDB.ExecContext(t.Context(), "CREATE DATABASE `"+databaseName+"`")
	require.NoError(t, err, "create control-plane storage database %s", databaseName)

	// Each embedded schema file holds exactly one CREATE TABLE statement, so a
	// fresh database bootstraps by executing them verbatim.
	bootCfg := *cfg
	bootCfg.DBName = databaseName
	bootCfg.MultiStatements = true
	bootDB, err := sql.Open("block-mysql", bootCfg.FormatDSN())
	require.NoError(t, err, "open bootstrap connection for control-plane storage")
	defer utils.CloseAndLog(bootDB)
	require.NoError(t, bootDB.PingContext(t.Context()), "ping bootstrap connection")
	entries, err := schema.MySQLFS.ReadDir("mysql")
	require.NoError(t, err, "read embedded storage schema directory")
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		content, err := schema.MySQLFS.ReadFile("mysql/" + entry.Name())
		require.NoError(t, err, "read storage schema file %s", entry.Name())
		_, err = bootDB.ExecContext(t.Context(), string(content))
		require.NoError(t, err, "create control-plane storage table from %s", entry.Name())
	}

	storeCfg := *cfg
	storeCfg.DBName = databaseName
	db, err := sql.Open("block-mysql", storeCfg.FormatDSN())
	require.NoError(t, err, "open control-plane storage connection")
	require.NoError(t, db.PingContext(t.Context()), "ping control-plane storage connection")
	return mysqlstore.New(db)
}

// TestGRPCClient_RemoteRetryablePauseSurvivesDataPlaneRetry exercises the
// cross-plane retry contract over a real gRPC connection with real storage on
// both planes. A remote task fails in a way the data plane will retry, so the
// data plane parks its apply in failed_retryable between recovery attempts —
// and the wire reports that pause as STATE_FAILED because the tern proto has
// no failed-retryable state. The control-plane drive must read the per-table
// statuses, treat the snapshot as a pause, mirror it on the stored task row,
// and keep polling: when the data plane's next recovery attempt succeeds, the
// stored control-plane apply lands completed. Terminalizing on the paused
// snapshot instead would orphan a live remote apply that goes on to cut over,
// leaving a stored failure permanently diverging from a completed schema
// change.
func TestGRPCClient_RemoteRetryablePauseSurvivesDataPlaneRetry(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	_, dpDSN := setupMySQLContainer(t)
	setupStorageSchema(t, dpDSN)
	cleanupTasks(t, dpDSN)
	cleanupTestTables(t, dpDSN)

	ctx := t.Context()
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelError}))

	// Data plane: a LocalClient whose engine fails the first attempt retryably,
	// served over a real gRPC connection like a remote Tern deployment.
	dpStor := createStorage(t, dpDSN)
	t.Cleanup(func() { utils.CloseAndLog(dpStor) })
	dpClient, err := NewLocalClient(LocalConfig{
		Database:  "testdb",
		Type:      storage.DatabaseTypeMySQL,
		TargetDSN: dpDSN,
	}, dpStor, logger)
	require.NoError(t, err)
	t.Cleanup(func() { utils.CloseAndLog(dpClient) })
	dpClient.spiritEngine = &failOnceThenSucceedEngine{}

	lis, err := (&net.ListenConfig{}).Listen(ctx, "tcp", "localhost:0")
	require.NoError(t, err, "listen for data-plane gRPC server")
	grpcSrv := grpc.NewServer()
	NewServer(dpClient, logger).Register(grpcSrv)
	go func() { _ = grpcSrv.Serve(lis) }()
	t.Cleanup(grpcSrv.Stop)

	// Control plane: a GRPCClient with its own storage database, mirroring the
	// separate control-plane/data-plane storage of a remote deployment.
	conn, err := grpc.NewClient(lis.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err, "dial data-plane gRPC server")
	cpStor := createControlPlaneStorage(t, dpDSN)
	t.Cleanup(func() { utils.CloseAndLog(cpStor) })
	cpClient := &GRPCClient{conn: conn, client: ternv1.NewTernClient(conn), storage: cpStor}
	t.Cleanup(func() { utils.CloseAndLog(cpClient) })

	now := time.Now()
	nano := now.UnixNano()
	plan := &storage.Plan{
		PlanIdentifier: fmt.Sprintf("plan-cp-retryable-%d", nano),
		Database:       "testdb",
		DatabaseType:   storage.DatabaseTypeMySQL,
		Deployment:     "testdb",
		Environment:    localClientTestEnvironment,
		CreatedAt:      now,
		Namespaces: map[string]*storage.NamespacePlanData{
			"testdb": {Tables: []storage.TableChange{{
				Namespace: "testdb",
				Table:     "users",
				DDL:       "ALTER TABLE `users` ADD COLUMN retry_note VARCHAR(255)",
				Operation: "alter",
			}}},
		},
	}
	planID, err := cpStor.Plans().Create(ctx, plan)
	require.NoError(t, err)
	apply := &storage.Apply{
		ApplyIdentifier: fmt.Sprintf("apply-cp-retryable-%d", nano),
		PlanID:          planID,
		Database:        "testdb",
		DatabaseType:    storage.DatabaseTypeMySQL,
		Deployment:      "testdb",
		Engine:          storage.EngineSpirit,
		State:           state.Apply.Pending,
		Options:         storage.MarshalApplyOptions(storage.ApplyOptions{}),
		Environment:     localClientTestEnvironment,
		CreatedAt:       now,
		UpdatedAt:       now,
	}
	applyID, err := cpStor.Applies().Create(ctx, apply)
	require.NoError(t, err)
	task := &storage.Task{
		TaskIdentifier: fmt.Sprintf("task-cp-retryable-%d", nano),
		ApplyID:        applyID,
		PlanID:         planID,
		Database:       "testdb",
		DatabaseType:   storage.DatabaseTypeMySQL,
		Engine:         storage.EngineSpirit,
		State:          state.Task.Pending,
		TableName:      "users",
		Namespace:      "testdb",
		DDL:            "ALTER TABLE `users` ADD COLUMN retry_note VARCHAR(255)",
		DDLAction:      "alter",
		Options:        []byte("{}"),
		Environment:    localClientTestEnvironment,
		CreatedAt:      now,
		UpdatedAt:      now,
	}
	_, err = cpStor.Tasks().Create(ctx, task)
	require.NoError(t, err)

	// The control-plane driver claims the queued apply and drives it: dispatch
	// to the data plane, then poll to terminal.
	owner := "cp-driver-" + t.Name()
	var claimed *storage.Apply
	require.Eventually(t, func() bool {
		var claimErr error
		claimed, claimErr = cpStor.Applies().ClaimApplyByID(ctx, applyID, owner)
		return claimErr == nil && claimed != nil
	}, 10*time.Second, 50*time.Millisecond, "the queued control-plane apply never became claimable")
	driveCtx := storage.WithApplyLease(ctx, claimed.Lease())
	var driveErr error
	driveExited := make(chan struct{})
	go func() {
		driveErr = cpClient.ResumeApply(driveCtx, claimed)
		close(driveExited)
	}()
	t.Cleanup(func() {
		select {
		case <-driveExited:
		case <-time.After(30 * time.Second):
			// Proceeding would close the client and storage under a live
			// goroutine; there is no way to reclaim it, so mark the stuck
			// drive as a failure rather than tearing down beneath it.
			t.Errorf("control-plane drive did not exit during cleanup; teardown will race the live drive goroutine")
		}
	})

	// Wait for the dispatch to record the data plane's apply id.
	var remoteID string
	waitutil.Poll(t, 30*time.Second, 100*time.Millisecond,
		func() bool {
			select {
			case <-driveExited:
				require.NoError(t, driveErr, "control-plane drive exited before dispatch recorded a remote apply id")
				t.Fatal("control-plane drive exited before dispatch recorded a remote apply id")
			default:
			}
			stored, getErr := cpStor.Applies().Get(ctx, applyID)
			if getErr != nil || stored == nil {
				return false
			}
			remoteID = stored.ExternalID
			return remoteID != ""
		},
		func() string { return "control-plane apply never recorded a remote apply id" },
	)

	// Data-plane recovery attempt 1: the engine fails retryably and the data
	// plane parks its apply in failed_retryable. The pause lasts until this
	// test drives the next recovery attempt, so the control plane is
	// guaranteed to poll the paused (wire: STATE_FAILED) snapshot.
	driveQueuedApply(t, dpStor, dpClient, remoteID)
	dpApply := resolveDispatchedApply(t, dpStor, remoteID)
	require.Equal(t, state.Apply.FailedRetryable, dpApply.State,
		"the data plane must park the failed attempt for its own retry")

	// The control plane must mirror the pause on the task row without
	// terminalizing the stored apply.
	waitutil.Poll(t, 30*time.Second, 100*time.Millisecond,
		func() bool {
			tasks, tasksErr := cpStor.Tasks().GetByApplyID(ctx, applyID)
			if tasksErr != nil || len(tasks) != 1 {
				return false
			}
			return state.IsState(tasks[0].State, state.Task.FailedRetryable, state.Task.Failed)
		},
		func() string { return "control plane never observed the data-plane pause" },
	)
	pausedTasks, err := cpStor.Tasks().GetByApplyID(ctx, applyID)
	require.NoError(t, err)
	require.Len(t, pausedTasks, 1)
	assert.Equal(t, state.Task.FailedRetryable, pausedTasks[0].State,
		"the stored task must mirror the retryable pause, not a terminal failure")
	pausedApply, err := cpStor.Applies().Get(ctx, applyID)
	require.NoError(t, err)
	require.NotNil(t, pausedApply)
	require.False(t, state.IsTerminalApplyState(pausedApply.State),
		"a data-plane retryable pause must not terminalize the stored control-plane apply, got %s", pausedApply.State)

	// Data-plane recovery attempt 2 succeeds; the control-plane drive must
	// observe the completion and land the stored apply completed.
	driveQueuedApply(t, dpStor, dpClient, remoteID)

	select {
	case <-driveExited:
		require.NoError(t, driveErr, "control-plane drive")
	case <-time.After(30 * time.Second):
		t.Fatal("control-plane drive did not finish after the data-plane retry succeeded")
	}
	finalApply, err := cpStor.Applies().Get(ctx, applyID)
	require.NoError(t, err)
	require.NotNil(t, finalApply)
	assert.Equal(t, state.Apply.Completed, finalApply.State)
	require.NotNil(t, finalApply.CompletedAt)
	finalTasks, err := cpStor.Tasks().GetByApplyID(ctx, applyID)
	require.NoError(t, err)
	require.Len(t, finalTasks, 1)
	assert.Equal(t, state.Task.Completed, finalTasks[0].State)
}
