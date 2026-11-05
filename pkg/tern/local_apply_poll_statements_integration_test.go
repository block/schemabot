//go:build integration

package tern

import (
	"context"
	"database/sql"
	"fmt"
	"log/slog"
	"os"
	"regexp"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	drivermysql "github.com/block/mysql"
	"github.com/block/spirit/pkg/utils"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/block/schemabot/pkg/engine"
	"github.com/block/schemabot/pkg/state"
	"github.com/block/schemabot/pkg/storage"
)

// runningForTicksEngine reports its work running for a fixed number of
// Progress calls, with the copied row count moving on every call the way a
// copy in flight does, and then reports it completed.
type runningForTicksEngine struct {
	engine.Engine
	mu      sync.Mutex
	running int
	calls   int
}

func (e *runningForTicksEngine) Name() string { return "running-for-ticks" }

func (e *runningForTicksEngine) Progress(context.Context, *engine.ProgressRequest) (*engine.ProgressResult, error) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.calls++
	if e.calls > e.running {
		return &engine.ProgressResult{State: engine.StateCompleted}, nil
	}
	return &engine.ProgressResult{
		State: engine.StateRunning,
		Tables: []engine.TableProgress{{
			Namespace:  "testdb",
			Table:      "users",
			State:      "copyRows",
			RowsCopied: int64(e.calls) * 1000,
			RowsTotal:  1_000_000,
		}},
	}, nil
}

// statementCounts reads how many times each statement shape has run against
// the storage schema, keyed by its normalized digest text. The reading
// connection's own performance_schema queries are left out.
func statementCounts(t *testing.T, db *sql.DB, schemaName string) map[string]int64 {
	t.Helper()
	rows, err := db.QueryContext(t.Context(), `
		SELECT DIGEST_TEXT, COUNT_STAR
		FROM performance_schema.events_statements_summary_by_digest
		WHERE SCHEMA_NAME = ? AND DIGEST_TEXT IS NOT NULL`, schemaName)
	require.NoError(t, err, "read statement digests for schema %s", schemaName)
	defer utils.CloseAndLog(rows)
	counts := map[string]int64{}
	for rows.Next() {
		var digest string
		var count int64
		require.NoError(t, rows.Scan(&digest, &count))
		if strings.Contains(digest, "performance_schema") {
			continue
		}
		counts[digest] = count
	}
	require.NoError(t, rows.Err())
	return counts
}

// statementsForDrive seeds a running apply with one task, claims it, and polls
// the task until the engine reports it completed after runningTicks running
// polls, writing new progress figures at most once per writeCap. It returns
// how many times each statement shape ran during the poll.
func statementsForDrive(t *testing.T, dsn, schemaName string, runningTicks int, writeCap time.Duration) map[string]int64 {
	t.Helper()
	// The poll settles the task but leaves its apply running, so clear the
	// previous drive's rows before seeding another apply for the same target.
	cleanupTasks(t, dsn)
	ctx := t.Context()
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelError}))
	stor := createStorage(t, dsn)
	defer utils.CloseAndLog(stor)

	eng := &runningForTicksEngine{running: runningTicks}
	client, err := NewLocalClient(LocalConfig{
		Database:  "testdb",
		Type:      storage.DatabaseTypeStrata,
		TargetDSN: dsn,
		EngineFactories: map[string]EngineFactory{
			storage.DatabaseTypeStrata: func(LocalConfig, *slog.Logger) (engine.Engine, error) {
				return eng, nil
			},
		},
	}, stor, logger)
	require.NoError(t, err)
	defer utils.CloseAndLog(client)
	client.taskPollIntervalOverride = 10 * time.Millisecond
	client.progressWriteCapOverride = writeCap
	client.progressWriteFloorOverride = time.Hour

	apply, _ := seedRunningApplyWithTask(t, stor, storage.DatabaseTypeStrata, storage.EngineStrata)
	expireApplyLease(t, dsn, apply.ID)
	claimed := claimApplyForDrive(t, stor, apply.ID)
	driveCtx := storage.WithApplyLease(ctx, claimed.Lease())
	tasks, err := stor.Tasks().GetByApplyID(ctx, apply.ID)
	require.NoError(t, err)
	require.Len(t, tasks, 1)

	reader, err := sql.Open("block-mysql", dsn)
	require.NoError(t, err)
	defer utils.CloseAndLog(reader)
	before := statementCounts(t, reader, schemaName)

	action := client.pollTaskToCompletion(driveCtx, claimed, tasks[0], nil, nil)

	after := statementCounts(t, reader, schemaName)
	require.Equal(t, taskContinue, action, "the task completes once the engine reports it completed")
	require.Equal(t, state.Task.Completed, tasks[0].State)
	ran := map[string]int64{}
	for digest, count := range after {
		if delta := count - before[digest]; delta > 0 {
			ran[digest] = delta
		}
	}
	return ran
}

// A drive polls its task every tick while the engine copies, and every driver
// in a wide apply does the same against one storage writer, so the storage
// statements one tick runs are multiplied by the number of shards. Two drives
// that differ only in how many running polls they make isolate that per-tick
// cost from the one-off work of starting and settling the task. A running
// poll writes the task row only when its write cap comes due.
func TestPollTaskToCompletion_StorageStatementsPerTick(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	_, dsn := setupMySQLContainer(t)
	setupStorageSchema(t, dsn)
	cleanupTasks(t, dsn)
	cleanupTestTables(t, dsn)
	cfg, err := drivermysql.ParseDSN(dsn)
	require.NoError(t, err)

	tests := []struct {
		name     string
		writeCap time.Duration
		want     map[string]float64
	}{
		{
			// Each running poll reads the apply's pending control requests and
			// the task row. No poll here reaches the write cap, so the moving
			// progress figures add no write.
			name:     "progress inside the write cap",
			writeCap: time.Hour,
			want: map[string]float64{
				"SELECT apply_control_requests": 1,
				"SELECT tasks":                  1,
			},
		},
		{
			// A cap shorter than a poll interval is due on every poll, so every
			// running poll also writes the task row's progress.
			name:     "write cap due on every poll",
			writeCap: time.Nanosecond,
			want: map[string]float64{
				"SELECT apply_control_requests": 1,
				"SELECT tasks":                  1,
				"UPDATE tasks":                  1,
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// A change that adds a statement here adds it once per shard per
			// tick on the storage writer.
			byShape, report := statementsPerRunningPoll(t, dsn, cfg.DBName, tt.writeCap)
			assert.Equal(t, tt.want, byShape, "storage statements per running poll:\n%s", report)
		})
	}
}

// statementsPerRunningPoll runs two drives that differ only in how many
// running polls they make, and returns the statements one running poll adds,
// grouped by shape, with a per-digest report for a failure message.
func statementsPerRunningPoll(t *testing.T, dsn, schemaName string, writeCap time.Duration) (map[string]float64, string) {
	t.Helper()
	const shortTicks, longTicks = 5, 25
	short := statementsForDrive(t, dsn, schemaName, shortTicks, writeCap)
	long := statementsForDrive(t, dsn, schemaName, longTicks, writeCap)

	perTick := map[string]float64{}
	byShape := map[string]float64{}
	for digest, count := range long {
		if delta := float64(count-short[digest]) / float64(longTicks-shortTicks); delta > 0 {
			perTick[digest] = delta
			byShape[statementShape(digest)] += delta
		}
	}
	digests := make([]string, 0, len(perTick))
	for digest := range perTick {
		digests = append(digests, digest)
	}
	sort.Strings(digests)
	var report strings.Builder
	for _, digest := range digests {
		fmt.Fprintf(&report, "%6.2f  %s\n", perTick[digest], digest)
	}
	return byShape, report.String()
}

// statementTable matches the first table a statement digest names.
var statementTable = regexp.MustCompile("(?:FROM|UPDATE|INTO) `([a-z_]+)`")

// statementShape reduces a statement digest to its verb and first table, such
// as "UPDATE tasks", so a per-tick count does not depend on its column list.
func statementShape(digest string) string {
	verb, _, _ := strings.Cut(digest, " ")
	if m := statementTable.FindStringSubmatch(digest); m != nil {
		return verb + " " + m[1]
	}
	return verb
}
