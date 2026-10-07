//go:build integration

package integration

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"os"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/block/spirit/pkg/utils"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/block/schemabot/e2e/testutil"
	schemabotapi "github.com/block/schemabot/pkg/api"
	"github.com/block/schemabot/pkg/state"
	"github.com/block/schemabot/pkg/storage"
	"github.com/block/schemabot/pkg/storage/mysqlstore"
	"github.com/block/schemabot/pkg/tern"
)

// leaseTakeoverPollDeadline bounds each wait in the lease takeover scenario.
const leaseTakeoverPollDeadline = 30 * time.Second

// leaseTakeoverSeedRows sizes the table so the original copy is still running
// when the peer claims the apply and for some time after.
const leaseTakeoverSeedRows = 1_000_000

// errHeartbeatUnreachable stands in for storage that the original driver's
// heartbeat writes cannot reach.
var errHeartbeatUnreachable = errors.New("heartbeat write did not reach storage")

// heartbeatGatedStorage is one instance's view of shared storage whose heartbeat
// writes fail while the gate is closed. Every other write lands, so the drive
// and its engine keep running while the lease the heartbeat renews goes stale.
type heartbeatGatedStorage struct {
	storage.Storage
	closed *atomic.Bool
}

func (s heartbeatGatedStorage) Applies() storage.ApplyStore {
	return heartbeatGatedApplyStore{ApplyStore: s.Storage.Applies(), closed: s.closed}
}

func (s heartbeatGatedStorage) ApplyOperations() storage.ApplyOperationStore {
	return heartbeatGatedOperationStore{ApplyOperationStore: s.Storage.ApplyOperations(), closed: s.closed}
}

type heartbeatGatedApplyStore struct {
	storage.ApplyStore
	closed *atomic.Bool
}

func (s heartbeatGatedApplyStore) Heartbeat(ctx context.Context, applyID int64) error {
	if s.closed.Load() {
		return errHeartbeatUnreachable
	}
	return s.ApplyStore.Heartbeat(ctx, applyID)
}

type heartbeatGatedOperationStore struct {
	storage.ApplyOperationStore
	closed *atomic.Bool
}

func (s heartbeatGatedOperationStore) Heartbeat(ctx context.Context, id int64) error {
	if s.closed.Load() {
		return errHeartbeatUnreachable
	}
	return s.ApplyOperationStore.Heartbeat(ctx, id)
}

// leaseTakeoverInstance is one SchemaBot process: its own operator and its own
// in-process Spirit engine, over storage it shares with its peer.
type leaseTakeoverInstance struct {
	addr    string
	service *schemabotapi.Service
}

func startLeaseTakeoverInstance(t *testing.T, name string, store storage.Storage, appDBName, appDSN string) *leaseTakeoverInstance {
	t.Helper()

	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo})).With("instance", name)
	localClient, err := tern.NewLocalClient(tern.LocalConfig{
		Database:  appDBName,
		Type:      "mysql",
		TargetDSN: appDSN,
	}, store, logger)
	require.NoError(t, err, "create local client for instance %s", name)

	svc := schemabotapi.New(store, &schemabotapi.ServerConfig{
		Databases: map[string]schemabotapi.DatabaseConfig{
			appDBName: {
				Type:         "mysql",
				Environments: map[string]schemabotapi.EnvironmentConfig{"staging": {DSN: appDSN}},
			},
		},
	}, map[string]tern.Client{appDBName + "/staging": localClient}, logger)
	require.NoError(t, svc.SetOperatorPollInterval(200*time.Millisecond))
	require.NoError(t, svc.SetRetryableExpiryInterval(200*time.Millisecond))

	mux := http.NewServeMux()
	svc.ConfigureRoutes(mux)
	listener, err := (&net.ListenConfig{}).Listen(t.Context(), "tcp", "localhost:0")
	require.NoError(t, err, "listen for instance %s", name)
	server := &http.Server{Handler: mux}
	go func() { _ = server.Serve(listener) }()
	addr := "http://" + listener.Addr().String()
	waitForHTTP(t, addr+"/health", 5*time.Second)

	t.Cleanup(func() {
		svc.StopOperator()
		_ = server.Close()
		_ = svc.Close()
	})
	return &leaseTakeoverInstance{addr: addr, service: svc}
}

// spiritLockHolder returns the target connection holding Spirit's advisory lock
// on the table, or 0 when no connection holds it.
func spiritLockHolder(t *testing.T, ctx context.Context, target *sql.DB, appDBName, table string) int64 {
	t.Helper()
	schemaPart := appDBName
	if len(schemaPart) > 20 {
		schemaPart = schemaPart[:20]
	}
	var holder sql.NullInt64
	err := target.QueryRowContext(ctx, `
		SELECT t.PROCESSLIST_ID
		FROM performance_schema.metadata_locks l
		JOIN performance_schema.threads t ON t.THREAD_ID = l.OWNER_THREAD_ID
		WHERE l.OBJECT_TYPE = 'USER LEVEL LOCK' AND l.LOCK_STATUS = 'GRANTED' AND l.OBJECT_NAME LIKE ?
		LIMIT 1`, schemaPart+"."+table+"-%").Scan(&holder)
	if errors.Is(err, sql.ErrNoRows) {
		return 0
	}
	require.NoError(t, err, "read Spirit's advisory lock holder for %s.%s", appDBName, table)
	return holder.Int64
}

// A driver is copying a large table through Spirit when its heartbeat writes
// stop reaching storage. Spirit itself is healthy and keeps copying. The lease
// goes stale and a second SchemaBot instance, with its own engine, reclaims the
// apply; the original instance then learns it was displaced.
//
// The work must never run twice, and the apply must never be recorded failed
// while a copy is still writing to the target: the displaced driver brings its
// copy down rather than leaving it running with no owner, and the reclaiming
// driver does not spend the apply's recovery budget against the copy it is
// waiting for. The apply finishes completed from a single copy, and nothing is
// left holding the table afterwards. The same holds for an apply that defers
// its cutover, which drives every table together and is cut over by an
// operator once the copy is done.
func TestOperator_LeaseTakeoverDoesNotOrphanTheEngineRun(t *testing.T) {
	t.Run("sequential", func(t *testing.T) { testLeaseTakeoverDoesNotOrphanTheEngineRun(t, false) })
	t.Run("deferred cutover", func(t *testing.T) { testLeaseTakeoverDoesNotOrphanTheEngineRun(t, true) })
}

func testLeaseTakeoverDoesNotOrphanTheEngineRun(t *testing.T, deferCutover bool) {
	ctx := t.Context()
	const table = "orders"

	appDBName, appDSN := createTestDB(t, "lease_takeover_")
	target, err := sql.Open("block-mysql", appDSN)
	require.NoError(t, err)
	require.NoError(t, target.PingContext(ctx))
	defer utils.CloseAndLog(target)
	_, err = target.ExecContext(ctx, "CREATE TABLE `"+table+"` ("+
		"`id` bigint unsigned NOT NULL AUTO_INCREMENT, "+
		"`name` varchar(50) NOT NULL, "+
		"`payload` varchar(255) NOT NULL, "+
		"PRIMARY KEY (`id`)"+
		") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci")
	require.NoError(t, err)
	testutil.SeedRows(t, appDSN, "`"+table+"`", "name, payload", "CONCAT('name-', seq), REPEAT('x', 200)", leaseTakeoverSeedRows)

	storageDB, err := sql.Open("block-mysql", schemabotDSN)
	require.NoError(t, err)
	require.NoError(t, storageDB.PingContext(ctx))
	clearStorageDB(t, storageDB)
	t.Cleanup(func() { _ = storageDB.Close() })
	shared := mysqlstore.New(storageDB)

	var heartbeatsBlocked atomic.Bool
	original := startLeaseTakeoverInstance(t, "original", heartbeatGatedStorage{Storage: shared, closed: &heartbeatsBlocked}, appDBName, appDSN)
	peer := startLeaseTakeoverInstance(t, "peer", shared, appDBName, appDSN)

	desired := "CREATE TABLE `" + table + "` (" +
		"`id` bigint unsigned NOT NULL AUTO_INCREMENT, " +
		"`name` varchar(50) NOT NULL, " +
		"`payload` varchar(255) NOT NULL, " +
		"PRIMARY KEY (`id`), " +
		"KEY `idx_name` (`name`)" +
		") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;"
	planResp := postJSON(t, original.addr+"/api/plan", map[string]any{
		"database": appDBName, "environment": "staging", "type": "mysql",
		"schema_files": map[string]any{"default": map[string]any{"files": map[string]string{table + ".sql": desired}}},
	})
	planID, _ := planResp["plan_id"].(string)
	require.NotEmpty(t, planID, "plan response: %v", planResp)

	original.service.StartOperator(ctx)
	applyReq := map[string]any{"plan_id": planID, "environment": "staging"}
	if deferCutover {
		applyReq["options"] = map[string]string{"defer_cutover": "true"}
	}
	applyResp := postJSON(t, original.addr+"/api/apply", applyReq)
	require.Equal(t, true, applyResp["accepted"], "apply response: %v", applyResp)
	applyIdentifier, _ := applyResp["apply_id"].(string)
	require.NotEmpty(t, applyIdentifier)

	// The original instance's copy is underway and holds Spirit's lock on the table.
	var apply *storage.Apply
	var originalRun int64
	testutil.Poll(t, leaseTakeoverPollDeadline, 100*time.Millisecond, func() bool {
		apply, err = shared.Applies().GetByApplyIdentifier(ctx, applyIdentifier)
		require.NoError(t, err)
		if apply == nil {
			return false
		}
		tasks, err := shared.Tasks().GetByApplyID(ctx, apply.ID)
		require.NoError(t, err)
		if len(tasks) != 1 || tasks[0].RowsCopied == 0 {
			return false
		}
		originalRun = spiritLockHolder(t, ctx, target, appDBName, table)
		return originalRun != 0
	}, func() string { return "the original instance's copy never started holding the table" })
	originalLease := apply.LeaseToken
	t.Logf("original copy holds the table on connection %d under lease %s", originalRun, originalLease)

	// The original instance stops claiming, so the peer is the one that reclaims.
	original.service.StopClaiming()

	// Heartbeats stop landing and the lease ages past the staleness window. The
	// window is a fixed minute, so it is aged in storage rather than waited out.
	// Any write to the rows refreshes their updated_at, including a heartbeat
	// that passed the gate just before it closed, so the rows are aged again on
	// every poll until the peer holds the apply.
	heartbeatsBlocked.Store(true)
	peer.service.StartOperator(ctx)
	testutil.Poll(t, leaseTakeoverPollDeadline, 50*time.Millisecond, func() bool {
		current, err := shared.Applies().Get(ctx, apply.ID)
		require.NoError(t, err)
		if current != nil && current.LeaseToken != originalLease {
			return true
		}
		ageLeaseTakeoverRows(t, storageDB, apply.ID)
		return false
	}, func() string { return "the peer never reclaimed the stale apply" })

	// The original instance's storage is reachable again, so its next heartbeat
	// proves it was displaced.
	heartbeatsBlocked.Store(false)

	// While the original copy still holds the table, no claim may spend the
	// apply's recovery budget or record it failed.
	var last *storage.Apply
	originalAlive := true
	cutoverRequested := false
	refusedClaimAged := false
	testutil.Poll(t, leaseTakeoverPollDeadline, 50*time.Millisecond, func() bool {
		holder := spiritLockHolder(t, ctx, target, appDBName, table)
		current, err := shared.Applies().Get(ctx, apply.ID)
		require.NoError(t, err)
		require.NotNil(t, current)
		last = current
		if holder == originalRun {
			require.NotEqualf(t, state.Apply.Failed, current.State,
				"apply %s was recorded failed while the original copy (connection %d) is still writing to the target: %s",
				applyIdentifier, originalRun, current.ErrorMessage)
			require.Zerof(t, current.Attempt,
				"apply %s spent %d recovery attempts against the original copy (connection %d) that still holds the table; state %s: %s",
				applyIdentifier, current.Attempt, originalRun, current.State, current.ErrorMessage)
		} else {
			originalAlive = false
		}
		// The original learns it was displaced from its next lease-guarded
		// write, so the peer's first start can come before or after the
		// original copy lets go. A deferred cutover drives every table
		// together, and a drive refused the table hands the apply back under
		// its claim; the next drive starts the work again once that claim
		// goes stale. That window is aged in storage too, once the original
		// copy has let go.
		if deferCutover && !originalAlive && !refusedClaimAged &&
			applyLogMentions(t, shared, apply.ID, "the apply is handed back to start again once it lets go") {
			ageLeaseTakeoverRows(t, storageDB, apply.ID)
			refusedClaimAged = true
		}
		if deferCutover && !originalAlive && !cutoverRequested && state.IsState(current.State, state.Apply.WaitingForCutover) {
			cutoverResp := postJSON(t, peer.addr+"/api/cutover", map[string]any{"apply_id": applyIdentifier, "environment": "staging"})
			require.Equal(t, true, cutoverResp["accepted"], "cutover response: %v", cutoverResp)
			cutoverRequested = true
		}
		return !originalAlive && state.IsTerminalApplyState(current.State)
	}, func() string {
		return fmt.Sprintf("apply %s did not settle after the takeover: original copy alive=%t, last state %q attempt %d: %s",
			applyIdentifier, originalAlive, last.State, last.Attempt, last.ErrorMessage)
	})

	assert.Equal(t, state.Apply.Completed, last.State, "the reclaiming driver finishes the schema change: %s", last.ErrorMessage)
	assert.Zero(t, spiritLockHolder(t, ctx, target, appDBName, table), "no copy still holds the table once the apply has settled")
	var indexCount int
	require.NoError(t, target.QueryRowContext(ctx,
		"SELECT COUNT(*) FROM information_schema.statistics WHERE table_schema = ? AND table_name = ? AND index_name = 'idx_name'",
		appDBName, table).Scan(&indexCount))
	assert.Positive(t, indexCount, "the reviewed index landed on %s", table)
	var shadowTables int
	require.NoError(t, target.QueryRowContext(ctx,
		"SELECT COUNT(*) FROM information_schema.tables WHERE table_schema = ? AND table_name LIKE '\\_"+table+"\\_%'",
		appDBName).Scan(&shadowTables))
	assert.Zero(t, shadowTables, "no shadow or checkpoint table is left on the target")
}

// applyLogMentions reports whether any entry in the apply's durable log
// contains text.
func applyLogMentions(t *testing.T, store storage.Storage, applyID int64, text string) bool {
	t.Helper()
	logs, err := store.ApplyLogs().List(t.Context(), storage.ApplyLogFilter{ApplyID: applyID})
	require.NoError(t, err)
	for _, entry := range logs {
		if strings.Contains(entry.Message, text) {
			return true
		}
	}
	return false
}

// ageLeaseTakeoverRows moves the apply's and its operations' last write past
// the lease staleness window, so a claim treats their leases as stale without
// the test waiting the window out.
func ageLeaseTakeoverRows(t *testing.T, storageDB *sql.DB, applyID int64) {
	t.Helper()
	_, err := storageDB.ExecContext(t.Context(), "UPDATE applies SET updated_at = NOW() - INTERVAL 2 MINUTE WHERE id = ?", applyID)
	require.NoError(t, err)
	_, err = storageDB.ExecContext(t.Context(), "UPDATE apply_operations SET updated_at = NOW() - INTERVAL 2 MINUTE WHERE apply_id = ?", applyID)
	require.NoError(t, err)
}
