//go:build integration

package tern

import (
	"database/sql"
	"io"
	"log/slog"
	"testing"
	"time"

	"github.com/block/spirit/pkg/utils"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	ternv1 "github.com/block/schemabot/pkg/proto/ternv1"
	"github.com/block/schemabot/pkg/state"
	"github.com/block/schemabot/pkg/storage"
)

// An apply changes hands whenever a driver dies mid-drive and a peer reclaims
// it, and the drive that finishes the work is a resumed one. The engine's own
// log lines have to keep reaching the apply log stream across that handover:
// that stream is what the CLI and the PR summary render, so an apply resumed by
// a second driver would otherwise go quiet for the whole drive that actually
// ran it.
func TestLocalClient_ResumedDriveCapturesEngineLogs(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	_, dsn := setupMySQLContainer(t)
	setupStorageSchema(t, dsn)
	cleanupTasks(t, dsn)
	cleanupTestTables(t, dsn)

	ctx := t.Context()
	db, err := sql.Open("block-mysql", dsn)
	require.NoError(t, err, "open target database")
	defer utils.CloseAndLog(db)
	_, err = db.ExecContext(ctx, "CREATE TABLE users (id INT PRIMARY KEY)")
	require.NoError(t, err, "create target table")
	_, err = db.ExecContext(ctx, "INSERT INTO users (id) VALUES (1), (2), (3)")
	require.NoError(t, err, "seed the target table")

	// The engine routes a Spirit line into the apply log stream from its log
	// handler, so the client's logger has to admit info records for the routing
	// to run at all — the same level a server runs at. Discard the output; the
	// assertion reads the stored stream, not stdout.
	logger := slog.New(slog.NewTextHandler(io.Discard, &slog.HandlerOptions{Level: slog.LevelInfo}))
	stor := createStorage(t, dsn)
	client, err := NewLocalClient(LocalConfig{
		Database:  "testdb",
		Type:      storage.DatabaseTypeMySQL,
		TargetDSN: dsn,
	}, stor, logger)
	require.NoError(t, err, "create local client")
	defer utils.CloseAndLog(client)

	// A column-type change rebuilds the table, so the engine runs a real copy
	// and emits the log lines this test is about.
	schemaFiles := buildSchemaWithAllTables(t, dsn, map[string]string{
		"users": "CREATE TABLE users (id BIGINT PRIMARY KEY, email VARCHAR(255))",
	})
	planResp, err := client.Plan(ctx, &ternv1.PlanRequest{
		Type:     "mysql",
		Database: "testdb",
		SchemaFiles: map[string]*ternv1.SchemaFiles{
			"testdb": {Files: schemaFiles},
		},
	})
	require.NoError(t, err, "plan the schema change")
	applyResp, err := client.Apply(ctx, &ternv1.ApplyRequest{
		PlanId:      planResp.PlanId,
		Environment: localClientTestEnvironment,
	})
	require.NoError(t, err, "dispatch the apply")
	require.True(t, applyResp.Accepted, "dispatch rejected: %s", applyResp.ErrorMessage)

	// Put the apply in the shape a drive that failed part-way leaves behind:
	// started, paused for operator retry, with its table work still to do. The
	// next claim is a resume, not a first drive.
	apply := resolveDispatchedApply(t, stor, applyResp.ApplyId)
	startedAt := time.Now()
	apply.State = state.Apply.FailedRetryable
	apply.StartedAt = &startedAt
	require.NoError(t, stor.Applies().Update(ctx, apply), "pause the apply for operator retry")

	driveQueuedApply(t, stor, client, applyResp.ApplyId)

	settled, err := stor.Applies().Get(ctx, apply.ID)
	require.NoError(t, err, "reload the resumed apply")
	require.NotNil(t, settled)
	require.Equal(t, state.Apply.Completed, settled.State, "the resumed drive must finish the schema change")

	logs, err := stor.ApplyLogs().GetRecentByApply(ctx, apply.ID, 200)
	require.NoError(t, err, "load the apply's log stream")
	var engineLines int
	for _, entry := range logs {
		if entry.Source == storage.LogSourceSpirit {
			engineLines++
		}
	}
	assert.Positive(t, engineLines,
		"a resumed drive must route the engine's log lines into the apply log stream")
}
