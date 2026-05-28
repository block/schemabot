//go:build integration

package integration

import (
	"database/sql"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"os"
	"regexp"
	"testing"
	"time"

	"github.com/block/spirit/pkg/utils"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	schemabotapi "github.com/block/schemabot/pkg/api"
	"github.com/block/schemabot/pkg/state"
	"github.com/block/schemabot/pkg/storage"
	schemabotmysql "github.com/block/schemabot/pkg/storage/mysqlstore"
)

var statusApplyRowRE = regexp.MustCompile(`(?m)^  apply-\S+`)
var statusFollowUpCommandRE = regexp.MustCompile(`(?m)^schemabot status apply-\S+`)

// TestCLI_Status_DefaultLimitShowsTwentyMostRecent verifies that the no-flag
// recent status view uses the operator-facing default and tells users how to
// request more rows when history is truncated.
func TestCLI_Status_DefaultLimitShowsTwentyMostRecent(t *testing.T) {
	binPath := buildBinary(t, "schemabot", "./pkg/cmd")
	endpoint, store := startStatusOnlySchemaBot(t)

	for i := 1; i <= 21; i++ {
		seedStatusApply(t, store, statusApplySeed{
			ApplyID:  fmt.Sprintf("apply-status-default-%02d", i),
			Database: "status_default",
			State:    state.Apply.Completed,
		})
	}

	out := runCLI(t, binPath, "status", "--endpoint", endpoint)
	stripped := stripANSI(out)

	assert.Equal(t, 20, statusApplyRowCount(out))
	assert.Contains(t, stripped, "apply-status-default-21")
	assert.Contains(t, stripped, "apply-status-default-02")
	assert.NotContains(t, stripped, "apply-status-default-01")
	assert.Contains(t, stripped, "Showing the 20 most recent schema changes. Use --limit N to show more.")
}

// TestCLI_Status_LimitShowsRequestedRecentApplies verifies that --limit changes
// the recent status window without changing the newest-first ordering.
func TestCLI_Status_LimitShowsRequestedRecentApplies(t *testing.T) {
	binPath := buildBinary(t, "schemabot", "./pkg/cmd")
	endpoint, store := startStatusOnlySchemaBot(t)

	for i := 1; i <= 5; i++ {
		seedStatusApply(t, store, statusApplySeed{
			ApplyID:  fmt.Sprintf("apply-status-limit-%02d", i),
			Database: "status_limit",
			State:    state.Apply.Completed,
		})
	}

	out := runCLI(t, binPath, "status", "--endpoint", endpoint, "--limit", "3")
	stripped := stripANSI(out)

	assert.Equal(t, 3, statusApplyRowCount(out))
	assert.Contains(t, stripped, "apply-status-limit-05")
	assert.Contains(t, stripped, "apply-status-limit-04")
	assert.Contains(t, stripped, "apply-status-limit-03")
	assert.NotContains(t, stripped, "apply-status-limit-02")
	assert.Contains(t, stripped, "Showing the 3 most recent schema changes. Use --limit N to show more.")
}

// TestCLI_Status_FailedShowsOnlyFailedApplies verifies that --failed switches
// to the failure-focused view with follow-up commands and excludes successful
// terminal history.
func TestCLI_Status_FailedShowsOnlyFailedApplies(t *testing.T) {
	binPath := buildBinary(t, "schemabot", "./pkg/cmd")
	endpoint, store := startStatusOnlySchemaBot(t)

	seedStatusApply(t, store, statusApplySeed{
		ApplyID:      "apply-status-failed-success",
		Database:     "status_failed_success",
		State:        state.Apply.Completed,
		ErrorMessage: "this should not render",
	})
	seedStatusApply(t, store, statusApplySeed{
		ApplyID:      "apply-status-failed-permanent",
		Database:     "status_failed_permanent",
		State:        state.Apply.Failed,
		ExternalID:   "remote-failed-permanent",
		ErrorMessage: "syntax error near column definition",
	})
	seedStatusApply(t, store, statusApplySeed{
		ApplyID:      "apply-status-failed-retryable",
		Database:     "status_failed_retryable",
		State:        state.Apply.FailedRetryable,
		ErrorMessage: "temporary engine failure",
	})

	out := runCLI(t, binPath, "status", "--endpoint", endpoint, "--failed")
	stripped := stripANSI(out)

	assert.Equal(t, 2, statusFollowUpCommandCount(out))
	assert.Contains(t, stripped, "Recent failed schema changes")
	assert.Contains(t, stripped, "status_failed_permanent staging: Failed (github:tester) [")
	assert.Contains(t, stripped, "syntax error near column definition")
	assert.Contains(t, stripped, "schemabot status apply-status-failed-permanent")
	assert.Contains(t, stripped, "status_failed_retryable staging: Retrying (github:tester) [")
	assert.Contains(t, stripped, "temporary engine failure")
	assert.Contains(t, stripped, "schemabot status apply-status-failed-retryable")
	assert.NotContains(t, stripped, "REASON")
	assert.NotContains(t, stripped, "apply-status-failed-success")
	assert.NotContains(t, stripped, "this should not render")

	externalOut := runCLI(t, binPath, "status", "--endpoint", endpoint, "--failed", "--external-id")
	external := stripANSI(externalOut)

	assert.Equal(t, 2, statusFollowUpCommandCount(externalOut))
	assert.Contains(t, external, "status_failed_permanent staging: Failed (github:tester; external_id=remote-failed-permanent) [")
}

// TestCLI_Status_ExternalIDShowsRemoteApplyIDs verifies that --external-id adds
// the remote apply identifier column to the regular status table.
func TestCLI_Status_ExternalIDShowsRemoteApplyIDs(t *testing.T) {
	binPath := buildBinary(t, "schemabot", "./pkg/cmd")
	endpoint, store := startStatusOnlySchemaBot(t)

	seedStatusApply(t, store, statusApplySeed{
		ApplyID:    "apply-status-external-local",
		Database:   "status_external_local",
		State:      state.Apply.Completed,
		ExternalID: "",
	})
	seedStatusApply(t, store, statusApplySeed{
		ApplyID:    "apply-status-external-remote",
		Database:   "status_external_remote",
		State:      state.Apply.Completed,
		ExternalID: "remote-apply-123",
	})

	regularOut := runCLI(t, binPath, "status", "--endpoint", endpoint, "--limit", "2")
	regular := stripANSI(regularOut)
	assert.NotContains(t, regular, "EXTERNAL ID")
	assert.NotContains(t, regular, "remote-apply-123")

	out := runCLI(t, binPath, "status", "--endpoint", endpoint, "--limit", "2", "--external-id")
	stripped := stripANSI(out)

	assert.Equal(t, 2, statusApplyRowCount(out))
	assert.Contains(t, stripped, "EXTERNAL ID")
	assert.Contains(t, stripped, "apply-status-external-remote")
	assert.Contains(t, stripped, "remote-apply-123")
	assert.Contains(t, stripped, "apply-status-external-local")
}

// TestCLI_Status_MaxLimitFooterDoesNotSuggestHigherLimit verifies that a
// clamped request tells operators they are at the server maximum instead of
// suggesting an impossible larger --limit value.
func TestCLI_Status_MaxLimitFooterDoesNotSuggestHigherLimit(t *testing.T) {
	binPath := buildBinary(t, "schemabot", "./pkg/cmd")
	endpoint, store := startStatusOnlySchemaBot(t)

	for i := 1; i <= 1001; i++ {
		seedStatusApply(t, store, statusApplySeed{
			ApplyID:  fmt.Sprintf("apply-status-cap-%04d", i),
			Database: "status_cap",
			State:    state.Apply.Completed,
		})
	}

	out := runCLI(t, binPath, "status", "--endpoint", endpoint, "--limit", "2000")
	stripped := stripANSI(out)

	assert.Equal(t, 1000, statusApplyRowCount(out))
	assert.Contains(t, stripped, "apply-status-cap-1001")
	assert.Contains(t, stripped, "Showing the 1000 most recent schema changes. This server caps status history at 1000.")
	assert.NotContains(t, stripped, "Use --limit N to show more.")
}

type statusApplySeed struct {
	ApplyID      string
	Database     string
	Environment  string
	State        string
	ExternalID   string
	ErrorMessage string
}

func startStatusOnlySchemaBot(t *testing.T) (string, *schemabotmysql.Storage) {
	t.Helper()

	db, err := sql.Open("mysql", schemabotDSN)
	require.NoError(t, err, "open schemabot db")
	clearStorageDB(t, db)

	store := schemabotmysql.New(db)
	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelError}))
	svc := schemabotapi.New(store, &schemabotapi.ServerConfig{}, nil, logger)
	t.Cleanup(func() { utils.CloseAndLog(svc) })

	mux := http.NewServeMux()
	svc.ConfigureRoutes(mux)

	listener, err := (&net.ListenConfig{}).Listen(t.Context(), "tcp", "127.0.0.1:0")
	require.NoError(t, err, "listen")

	server := &http.Server{Handler: mux}
	go func() { _ = server.Serve(listener) }()
	t.Cleanup(func() { utils.CloseAndLog(server) })

	addr := listener.Addr().String()
	waitForHTTP(t, "http://"+addr+"/health", 5*time.Second)

	return "http://" + addr, store
}

func seedStatusApply(t *testing.T, store *schemabotmysql.Storage, seed statusApplySeed) {
	t.Helper()

	environment := seed.Environment
	if environment == "" {
		environment = "staging"
	}
	applyState := seed.State
	if applyState == "" {
		applyState = state.Apply.Completed
	}

	ctx := t.Context()
	lock := &storage.Lock{
		DatabaseName: seed.Database,
		DatabaseType: storage.DatabaseTypeMySQL,
		Repository:   "example/repo",
		PullRequest:  123,
		Owner:        "status-cli-test",
	}
	require.NoError(t, store.Locks().Acquire(ctx, lock))

	storedLock, err := store.Locks().Get(ctx, seed.Database, storage.DatabaseTypeMySQL)
	require.NoError(t, err)
	require.NotNil(t, storedLock)

	planID, err := store.Plans().Create(ctx, &storage.Plan{
		PlanIdentifier: "plan-" + seed.ApplyID,
		Database:       seed.Database,
		DatabaseType:   storage.DatabaseTypeMySQL,
		Deployment:     seed.Database,
		Target:         seed.Database,
		Repository:     "example/repo",
		PullRequest:    123,
		SchemaPath:     "schema",
		Environment:    environment,
		HeadSHA:        "status-test-head",
		CreatedAt:      time.Now(),
	})
	require.NoError(t, err)

	_, err = store.Applies().Create(ctx, &storage.Apply{
		ApplyIdentifier: seed.ApplyID,
		LockID:          storedLock.ID,
		PlanID:          planID,
		Database:        seed.Database,
		DatabaseType:    storage.DatabaseTypeMySQL,
		Repository:      "example/repo",
		PullRequest:     123,
		Environment:     environment,
		Deployment:      seed.Database,
		Caller:          "github:tester",
		ExternalID:      seed.ExternalID,
		Engine:          storage.EngineSpirit,
		State:           applyState,
		ErrorMessage:    seed.ErrorMessage,
		Options:         []byte("{}"),
	})
	require.NoError(t, err)
}

func statusApplyRowCount(output string) int {
	return len(statusApplyRowRE.FindAllString(stripANSI(output), -1))
}

func statusFollowUpCommandCount(output string) int {
	return len(statusFollowUpCommandRE.FindAllString(stripANSI(output), -1))
}
