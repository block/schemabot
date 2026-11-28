package api

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/block/schemabot/pkg/storage"
)

// retentionApplyStore elects this instance unless busy is set, and counts how
// often the pass asked.
type retentionApplyStore struct {
	storage.ApplyStore
	busy      bool
	electErr  error
	elections int
}

func (s *retentionApplyStore) WithStorageRetentionElection(ctx context.Context, fn func(context.Context) error) error {
	s.elections++
	if s.electErr != nil {
		return s.electErr
	}
	if s.busy {
		return storage.ErrStorageRetentionBusy
	}
	return fn(ctx)
}

// retentionWebhookStore answers delete calls from a script of batch sizes and
// remembers what it was asked, so a test can see how a pass paced itself.
type retentionWebhookStore struct {
	storage.WebhookEventStore

	// batches is the number of rows each successive delete call reports. A call
	// past the end of the script reports zero.
	batches   []int64
	deleteErr error
	// failAfter makes the delete call with this zero-based index fail.
	failAfter int
	counted   int64
	countErr  error
	// blockCount makes a count wait until its context ends, as a count over a
	// large backlog would.
	blockCount bool
	// onDelete runs inside each delete call, before it answers.
	onDelete func(call int)

	deleteCalls []deleteCall
	countCalls  []time.Duration
}

type deleteCall struct {
	age   time.Duration
	limit int
}

func (s *retentionWebhookStore) DeleteTerminalOlderThan(_ context.Context, age time.Duration, limit int) (int64, error) {
	call := len(s.deleteCalls)
	s.deleteCalls = append(s.deleteCalls, deleteCall{age: age, limit: limit})
	if s.onDelete != nil {
		s.onDelete(call)
	}
	if s.deleteErr != nil && call == s.failAfter {
		return 0, s.deleteErr
	}
	if call < len(s.batches) {
		return s.batches[call], nil
	}
	return 0, nil
}

func (s *retentionWebhookStore) CountTerminalOlderThan(ctx context.Context, age time.Duration) (int64, error) {
	s.countCalls = append(s.countCalls, age)
	if s.blockCount {
		<-ctx.Done()
		return 0, ctx.Err()
	}
	return s.counted, s.countErr
}

// retentionApplyLogStore answers apply log purge calls from a script of batch
// sizes, the same way retentionWebhookStore does for the webhook inbox.
type retentionApplyLogStore struct {
	storage.ApplyLogStore

	batches   []int64
	deleteErr error
	// failAfter makes the delete call with this zero-based index fail.
	failAfter int
	counted   int64
	countErr  error

	deleteCalls []deleteCall
	countCalls  []time.Duration
}

func (s *retentionApplyLogStore) DeleteForSettledAppliesOlderThan(_ context.Context, age time.Duration, limit int) (int64, error) {
	call := len(s.deleteCalls)
	s.deleteCalls = append(s.deleteCalls, deleteCall{age: age, limit: limit})
	if s.deleteErr != nil && call == s.failAfter {
		return 0, s.deleteErr
	}
	if call < len(s.batches) {
		return s.batches[call], nil
	}
	return 0, nil
}

func (s *retentionApplyLogStore) CountForSettledAppliesOlderThan(_ context.Context, age time.Duration) (int64, error) {
	s.countCalls = append(s.countCalls, age)
	return s.counted, s.countErr
}

func newRetentionService(t *testing.T, cfg StorageRetentionConfig, applies *retentionApplyStore, events *retentionWebhookStore) (*Service, *bytes.Buffer) {
	t.Helper()
	return newRetentionServiceWithApplyLogs(t, cfg, applies, events, &retentionApplyLogStore{})
}

func newRetentionServiceWithApplyLogs(t *testing.T, cfg StorageRetentionConfig, applies *retentionApplyStore, events *retentionWebhookStore, applyLogs *retentionApplyLogStore) (*Service, *bytes.Buffer) {
	t.Helper()
	store := &mockStorageWithApplyStores{mockStorage: mockStorage{webhookEvents: events}, applies: applies, applyLogs: applyLogs}
	svc := New(store, &ServerConfig{StorageRetention: cfg}, nil, slog.New(slog.DiscardHandler))
	var logs bytes.Buffer
	svc.logger = slog.New(slog.NewJSONHandler(&logs, &slog.HandlerOptions{Level: slog.LevelDebug}))
	return svc, &logs
}

// logEntries returns the JSON log lines whose message starts with prefix.
func logEntries(t *testing.T, logs *bytes.Buffer, prefix string) []map[string]any {
	t.Helper()
	var entries []map[string]any
	for line := range strings.SplitSeq(strings.TrimSpace(logs.String()), "\n") {
		if line == "" {
			continue
		}
		var entry map[string]any
		require.NoError(t, json.Unmarshal([]byte(line), &entry), "log line must be JSON: %s", line)
		if msg, _ := entry["msg"].(string); strings.HasPrefix(msg, prefix) {
			entries = append(entries, entry)
		}
	}
	return entries
}

func deletingSettings() storageRetentionSettings {
	return storageRetentionSettings{
		batchSize:     3,
		webhookEvents: 168 * time.Hour,
		maxBatches:    10,
		countTimeout:  time.Minute,
	}
}

func deletingSettingsWithApplyLogs() storageRetentionSettings {
	settings := deletingSettings()
	settings.applyLogs = 2160 * time.Hour
	settings.purgeApplyLogs = true
	return settings
}

// A pass keeps deleting full batches and stops at the first short one, since a
// short batch means the table has nothing older left. Every call carries the
// configured window and batch size.
func TestStorageRetentionPassDeletesUntilABatchComesBackShort(t *testing.T) {
	events := &retentionWebhookStore{batches: []int64{3, 3, 1}}
	svc, logs := newRetentionService(t, StorageRetentionConfig{}, &retentionApplyStore{}, events)

	svc.runStorageRetentionPass(t.Context(), deletingSettings())

	assert.Equal(t, []deleteCall{
		{age: 168 * time.Hour, limit: 3},
		{age: 168 * time.Hour, limit: 3},
		{age: 168 * time.Hour, limit: 3},
	}, events.deleteCalls)
	assert.Empty(t, events.countCalls, "a real pass does not count")

	complete := logEntries(t, logs, "storage retention pass complete")
	require.Len(t, complete, 1, logs.String())
	assert.Equal(t, "webhook_events", complete[0]["table"])
	assert.InDelta(t, 7, complete[0]["rows_deleted"], 0)
	assert.InDelta(t, 3, complete[0]["batches"], 0)
	assert.Equal(t, false, complete[0]["dry_run"])
	assert.Equal(t, (168 * time.Hour).String(), complete[0]["window"])
	assert.Empty(t, logEntries(t, logs, "storage retention pass failed"))
}

// A batch that comes back exactly full is not proof the backlog is gone, so the
// pass asks once more and ends on the empty answer.
func TestStorageRetentionPassAsksAgainAfterAFullFinalBatch(t *testing.T) {
	events := &retentionWebhookStore{batches: []int64{3, 3}}
	svc, logs := newRetentionService(t, StorageRetentionConfig{}, &retentionApplyStore{}, events)

	svc.runStorageRetentionPass(t.Context(), deletingSettings())

	assert.Len(t, events.deleteCalls, 3)
	complete := logEntries(t, logs, "storage retention pass complete")
	require.Len(t, complete, 1)
	assert.InDelta(t, 6, complete[0]["rows_deleted"], 0)
}

// A backlog larger than one pass can clear stops at the batch cap and says so,
// leaving the rest for the next interval.
func TestStorageRetentionPassStopsAtTheBatchCap(t *testing.T) {
	events := &retentionWebhookStore{batches: []int64{3, 3, 3, 3, 3, 3}}
	svc, logs := newRetentionService(t, StorageRetentionConfig{}, &retentionApplyStore{}, events)
	settings := deletingSettings()
	settings.maxBatches = 4

	svc.runStorageRetentionPass(t.Context(), settings)

	assert.Len(t, events.deleteCalls, 4)
	capped := logEntries(t, logs, "storage retention pass stopped at its batch cap")
	require.Len(t, capped, 1, logs.String())
	assert.InDelta(t, 12, capped[0]["rows_deleted"], 0)
	assert.InDelta(t, 4, capped[0]["batches"], 0)
	assert.Equal(t, true, capped[0]["stopped_at_batch_cap"])
	assert.Empty(t, logEntries(t, logs, "storage retention pass failed"))
}

// A dry run reports how many rows a real pass would delete and deletes none.
func TestStorageRetentionPassDryRunCountsAndDeletesNothing(t *testing.T) {
	events := &retentionWebhookStore{counted: 1234, batches: []int64{3}}
	svc, logs := newRetentionService(t, StorageRetentionConfig{}, &retentionApplyStore{}, events)
	settings := deletingSettings()
	settings.dryRun = true

	svc.runStorageRetentionPass(t.Context(), settings)

	assert.Empty(t, events.deleteCalls, "a dry run never deletes")
	assert.Equal(t, []time.Duration{168 * time.Hour}, events.countCalls)
	complete := logEntries(t, logs, "storage retention pass complete")
	require.Len(t, complete, 1, logs.String())
	assert.Equal(t, "webhook_events", complete[0]["table"])
	assert.InDelta(t, 1234, complete[0]["rows_would_delete"], 0)
	assert.NotContains(t, complete[0], "rows_deleted")
	assert.Equal(t, true, complete[0]["dry_run"])
	assert.Equal(t, (168 * time.Hour).String(), complete[0]["window"])
}

// A store error ends the pass at that point and logs it with the table, the
// window, and how far the pass got. The error is logged once, and nothing is
// attempted after it.
func TestStorageRetentionPassEndsAndLogsOnAStoreError(t *testing.T) {
	boom := errors.New("storage unavailable")
	events := &retentionWebhookStore{batches: []int64{3, 3, 3}, deleteErr: boom, failAfter: 1}
	svc, logs := newRetentionService(t, StorageRetentionConfig{}, &retentionApplyStore{}, events)

	svc.runStorageRetentionPass(t.Context(), deletingSettings())

	assert.Len(t, events.deleteCalls, 2, "the pass ends at the failing batch")
	failed := logEntries(t, logs, "storage retention pass failed")
	require.Len(t, failed, 1, logs.String())
	assert.Equal(t, "ERROR", failed[0]["level"])
	assert.Equal(t, "webhook_events", failed[0]["table"])
	assert.Equal(t, (168 * time.Hour).String(), failed[0]["window"])
	assert.Equal(t, false, failed[0]["dry_run"])
	assert.InDelta(t, 3, failed[0]["rows_deleted"], 0, "the first batch had already been deleted")
	assert.InDelta(t, 1, failed[0]["batches"], 0)
	assert.Contains(t, failed[0]["error"], "storage unavailable")
	assert.Empty(t, logEntries(t, logs, "storage retention pass complete"))
}

// A failed dry-run count is logged the same way.
func TestStorageRetentionPassLogsAFailedDryRunCount(t *testing.T) {
	events := &retentionWebhookStore{countErr: errors.New("count timed out")}
	svc, logs := newRetentionService(t, StorageRetentionConfig{}, &retentionApplyStore{}, events)
	settings := deletingSettings()
	settings.dryRun = true

	svc.runStorageRetentionPass(t.Context(), settings)

	failed := logEntries(t, logs, "storage retention pass failed")
	require.Len(t, failed, 1, logs.String())
	assert.Equal(t, "webhook_events", failed[0]["table"])
	assert.Equal(t, true, failed[0]["dry_run"])
}

// A dry-run count that runs past its deadline gives up on its own, so a slow
// count over a large backlog cannot hold the election. The pass reports no
// figure for that table, never zero, and still counts the other table.
func TestStorageRetentionDryRunCountGivesUpAtItsDeadline(t *testing.T) {
	events := &retentionWebhookStore{blockCount: true}
	applyLogs := &retentionApplyLogStore{counted: 4}
	svc, logs := newRetentionServiceWithApplyLogs(t, StorageRetentionConfig{}, &retentionApplyStore{}, events, applyLogs)
	settings := deletingSettingsWithApplyLogs()
	settings.dryRun = true
	settings.countTimeout = 10 * time.Millisecond

	svc.runStorageRetentionPass(t.Context(), settings)

	timedOut := logEntries(t, logs, "storage retention dry run could not count")
	require.Len(t, timedOut, 1, logs.String())
	assert.Equal(t, "webhook_events", timedOut[0]["table"])
	assert.Equal(t, "WARN", timedOut[0]["level"])
	assert.Equal(t, "10ms", timedOut[0]["count_timeout"])
	complete := logEntries(t, logs, "storage retention pass complete")
	require.Len(t, complete, 1, logs.String())
	assert.Equal(t, "apply_logs", complete[0]["table"])
	assert.InDelta(t, 4, complete[0]["rows_would_delete"], 0)
}

// Losing the election is the expected outcome on every instance but one, so
// the pass deletes nothing and logs no failure.
func TestStorageRetentionPassDoesNothingWhenAnotherInstanceIsElected(t *testing.T) {
	applies := &retentionApplyStore{busy: true}
	events := &retentionWebhookStore{batches: []int64{3}, counted: 5}
	svc, logs := newRetentionService(t, StorageRetentionConfig{}, applies, events)

	svc.runStorageRetentionPass(t.Context(), deletingSettings())

	assert.Equal(t, 1, applies.elections)
	assert.Empty(t, events.deleteCalls)
	assert.Empty(t, events.countCalls)
	assert.Empty(t, logEntries(t, logs, "storage retention pass failed"))
	assert.Empty(t, logEntries(t, logs, "storage retention pass could not"))
}

// A failure to reach the election at all is logged, because no purge ran to
// log it.
func TestStorageRetentionPassLogsAnElectionFailure(t *testing.T) {
	applies := &retentionApplyStore{electErr: errors.New("acquire lock: connection refused")}
	events := &retentionWebhookStore{}
	svc, logs := newRetentionService(t, StorageRetentionConfig{}, applies, events)

	svc.runStorageRetentionPass(t.Context(), deletingSettings())

	failed := logEntries(t, logs, "storage retention pass could not take part in the election")
	require.Len(t, failed, 1, logs.String())
	assert.Equal(t, "ERROR", failed[0]["level"])
	assert.Empty(t, events.deleteCalls)
}

// Shutdown mid-pass stops the pass at once instead of waiting out the pause
// between batches, and is a routine stop rather than a logged failure.
func TestStorageRetentionPassStopsOnShutdown(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	events := &retentionWebhookStore{batches: []int64{3, 3, 3, 3}}
	// Shutdown arrives while the pass is pausing after its first batch.
	events.onDelete = func(int) {
		time.AfterFunc(50*time.Millisecond, cancel)
	}
	svc, logs := newRetentionService(t, StorageRetentionConfig{}, &retentionApplyStore{}, events)
	settings := deletingSettings()
	settings.batchPause = time.Hour

	done := make(chan struct{})
	go func() {
		defer close(done)
		svc.runStorageRetentionPass(ctx, settings)
	}()
	select {
	case <-done:
	case <-time.After(10 * time.Second):
		t.Fatal("the pass kept waiting between batches after shutdown")
	}

	assert.Len(t, events.deleteCalls, 1, "no batch starts after shutdown")
	assert.Empty(t, logEntries(t, logs, "storage retention pass failed"))
	assert.Empty(t, logEntries(t, logs, "storage retention pass complete"))
	assert.Len(t, logEntries(t, logs, "storage retention pass interrupted by shutdown"), 1, logs.String())
}

// The pass starts only when it is enabled with valid values, and it carries the
// configured values, defaulting to a dry run so enabling it alone deletes
// nothing.
func TestStorageRetentionPassStartsOnlyWhenEnabled(t *testing.T) {
	boolPtr := func(b bool) *bool { return &b }
	tests := []struct {
		name         string
		cfg          StorageRetentionConfig
		starts       bool
		wantInterval time.Duration
		wantCount    int
		wantDeletes  int
		wantLimit    int
	}{
		{name: "disabled", cfg: StorageRetentionConfig{}},
		{name: "invalid interval", cfg: StorageRetentionConfig{Enabled: true, Interval: "hourly"}},
		{name: "invalid batch size", cfg: StorageRetentionConfig{Enabled: true, BatchSize: -1}},
		{name: "invalid window", cfg: StorageRetentionConfig{Enabled: true, WebhookEvents: "a week"}},
		{name: "window below GitHub's redelivery limit", cfg: StorageRetentionConfig{Enabled: true, DryRun: boolPtr(false), WebhookEvents: "24h"}},
		{name: "invalid apply logs window", cfg: StorageRetentionConfig{Enabled: true, ApplyLogs: "ninety days"}},
		{
			name: "enabled alone is a dry run", cfg: StorageRetentionConfig{Enabled: true},
			starts: true, wantInterval: time.Hour, wantCount: 1,
		},
		{
			name:   "dry run turned off deletes with the configured batch size",
			cfg:    StorageRetentionConfig{Enabled: true, DryRun: boolPtr(false), Interval: "30m", BatchSize: 250},
			starts: true, wantInterval: 30 * time.Minute, wantDeletes: 1, wantLimit: 250,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			events := &retentionWebhookStore{}
			svc, _ := newRetentionService(t, tt.cfg, &retentionApplyStore{}, events)

			pass, interval, ok := svc.storageRetentionPass()

			require.Equal(t, tt.starts, ok)
			if !ok {
				return
			}
			assert.Equal(t, tt.wantInterval, interval)
			pass(t.Context())
			assert.Len(t, events.countCalls, tt.wantCount)
			require.Len(t, events.deleteCalls, tt.wantDeletes)
			if tt.wantDeletes > 0 {
				assert.Equal(t, tt.wantLimit, events.deleteCalls[0].limit)
				assert.Equal(t, DefaultStorageRetentionWebhookEvents, events.deleteCalls[0].age)
			}
		})
	}
}

// retentionSignallingApplyStore reports the first storage retention election on
// a channel and declines it, so a test can see that the operator ran the pass.
type retentionSignallingApplyStore struct {
	*capturingApplyStore
	once sync.Once
	ran  chan struct{}
}

func (s *retentionSignallingApplyStore) WithStorageRetentionElection(context.Context, func(context.Context) error) error {
	s.once.Do(func() { close(s.ran) })
	return storage.ErrStorageRetentionBusy
}

// Starting the operator starts the storage retention pass when it is enabled.
func TestStartOperatorStartsTheStorageRetentionPass(t *testing.T) {
	store := &retentionSignallingApplyStore{capturingApplyStore: &capturingApplyStore{}, ran: make(chan struct{})}
	svc, _ := newQueueApplyTestService(trustedQueueApplyTestPlan(), &mockTernClient{}, store)
	svc.config.StorageRetention = StorageRetentionConfig{Enabled: true}
	require.NoError(t, svc.SetOperatorPollInterval(time.Hour))

	svc.StartOperator(t.Context())
	defer svc.StopOperator()

	select {
	case <-store.ran:
	case <-time.After(10 * time.Second):
		t.Fatal("the operator never ran the storage retention pass")
	}
}

// A disabled pass never asks for the election, so a deployment that has not
// opted in pays nothing for the feature.
func TestStorageRetentionPassDoesNothingWhenDisabled(t *testing.T) {
	applies := &retentionApplyStore{}
	svc, _ := newRetentionService(t, StorageRetentionConfig{}, applies, &retentionWebhookStore{})

	_, _, ok := svc.storageRetentionPass()

	assert.False(t, ok)
	assert.Zero(t, applies.elections)
}

// With no apply_logs window the pass never touches apply logs, in a dry run or a
// real pass, and says at debug level why it skipped them.
func TestStorageRetentionPassNeverPurgesApplyLogsWhenTheWindowIsUnset(t *testing.T) {
	for _, dryRun := range []bool{false, true} {
		t.Run(map[bool]string{false: "real pass", true: "dry run"}[dryRun], func(t *testing.T) {
			events := &retentionWebhookStore{batches: []int64{1}, counted: 1}
			applyLogs := &retentionApplyLogStore{batches: []int64{3}, counted: 9}
			svc, logs := newRetentionServiceWithApplyLogs(t, StorageRetentionConfig{}, &retentionApplyStore{}, events, applyLogs)
			settings := deletingSettings()
			settings.dryRun = dryRun

			svc.runStorageRetentionPass(t.Context(), settings)

			assert.Empty(t, applyLogs.deleteCalls)
			assert.Empty(t, applyLogs.countCalls)
			skipped := logEntries(t, logs, "storage retention: apply logs are not purged")
			require.Len(t, skipped, 1, logs.String())
			assert.Equal(t, "DEBUG", skipped[0]["level"])
			assert.Equal(t, "apply_logs", skipped[0]["table"])
			for _, entry := range logEntries(t, logs, "storage retention pass complete") {
				assert.Equal(t, "webhook_events", entry["table"])
			}
		})
	}
}

// With an apply_logs window the pass purges the webhook inbox and then apply
// logs, each with its own window, and logs one completion line per table.
func TestStorageRetentionPassPurgesApplyLogsAfterWebhookEvents(t *testing.T) {
	events := &retentionWebhookStore{batches: []int64{3, 1}}
	applyLogs := &retentionApplyLogStore{batches: []int64{3, 3, 2}}
	svc, logs := newRetentionServiceWithApplyLogs(t, StorageRetentionConfig{}, &retentionApplyStore{}, events, applyLogs)

	svc.runStorageRetentionPass(t.Context(), deletingSettingsWithApplyLogs())

	assert.Len(t, events.deleteCalls, 2)
	assert.Equal(t, []deleteCall{
		{age: 2160 * time.Hour, limit: 3},
		{age: 2160 * time.Hour, limit: 3},
		{age: 2160 * time.Hour, limit: 3},
	}, applyLogs.deleteCalls)
	assert.Empty(t, applyLogs.countCalls, "a real pass does not count")

	complete := logEntries(t, logs, "storage retention pass complete")
	require.Len(t, complete, 2, logs.String())
	assert.Equal(t, "webhook_events", complete[0]["table"])
	assert.Equal(t, (168 * time.Hour).String(), complete[0]["window"])
	assert.InDelta(t, 4, complete[0]["rows_deleted"], 0)
	assert.Equal(t, "apply_logs", complete[1]["table"])
	assert.Equal(t, (2160 * time.Hour).String(), complete[1]["window"])
	assert.InDelta(t, 8, complete[1]["rows_deleted"], 0)
	assert.InDelta(t, 3, complete[1]["batches"], 0)
	assert.Equal(t, false, complete[1]["dry_run"])
	assert.Empty(t, logEntries(t, logs, "storage retention: apply logs are not purged"))
}

// A dry run counts apply logs and deletes none.
func TestStorageRetentionPassDryRunCountsApplyLogs(t *testing.T) {
	events := &retentionWebhookStore{counted: 5}
	applyLogs := &retentionApplyLogStore{counted: 77, batches: []int64{3}}
	svc, logs := newRetentionServiceWithApplyLogs(t, StorageRetentionConfig{}, &retentionApplyStore{}, events, applyLogs)
	settings := deletingSettingsWithApplyLogs()
	settings.dryRun = true

	svc.runStorageRetentionPass(t.Context(), settings)

	assert.Empty(t, applyLogs.deleteCalls, "a dry run never deletes")
	assert.Equal(t, []time.Duration{2160 * time.Hour}, applyLogs.countCalls)
	complete := logEntries(t, logs, "storage retention pass complete")
	require.Len(t, complete, 2, logs.String())
	assert.Equal(t, "apply_logs", complete[1]["table"])
	assert.InDelta(t, 77, complete[1]["rows_would_delete"], 0)
	assert.NotContains(t, complete[1], "rows_deleted")
	assert.Equal(t, true, complete[1]["dry_run"])
}

// Apply logs honor the same batch cap as the webhook inbox.
func TestStorageRetentionPassStopsApplyLogsAtTheBatchCap(t *testing.T) {
	applyLogs := &retentionApplyLogStore{batches: []int64{3, 3, 3, 3, 3, 3}}
	svc, logs := newRetentionServiceWithApplyLogs(t, StorageRetentionConfig{}, &retentionApplyStore{}, &retentionWebhookStore{}, applyLogs)
	settings := deletingSettingsWithApplyLogs()
	settings.maxBatches = 4

	svc.runStorageRetentionPass(t.Context(), settings)

	assert.Len(t, applyLogs.deleteCalls, 4)
	capped := logEntries(t, logs, "storage retention pass stopped at its batch cap")
	require.Len(t, capped, 1, logs.String())
	assert.Equal(t, "apply_logs", capped[0]["table"])
	assert.InDelta(t, 12, capped[0]["rows_deleted"], 0)
	assert.Equal(t, true, capped[0]["stopped_at_batch_cap"])
}

// A failure purging the webhook inbox does not skip apply logs, and the pass
// returns the failure after logging it once.
func TestStorageRetentionPassPurgesApplyLogsAfterAWebhookEventsFailure(t *testing.T) {
	boom := errors.New("webhook purge failed")
	events := &retentionWebhookStore{deleteErr: boom}
	applyLogs := &retentionApplyLogStore{batches: []int64{2}}
	svc, logs := newRetentionServiceWithApplyLogs(t, StorageRetentionConfig{}, &retentionApplyStore{}, events, applyLogs)

	err := svc.purgeTables(t.Context(), deletingSettingsWithApplyLogs())

	require.ErrorIs(t, err, boom)
	assert.Len(t, applyLogs.deleteCalls, 1, "apply logs are still purged")
	failed := logEntries(t, logs, "storage retention pass failed")
	require.Len(t, failed, 1, logs.String())
	assert.Equal(t, "webhook_events", failed[0]["table"])
	complete := logEntries(t, logs, "storage retention pass complete")
	require.Len(t, complete, 1, logs.String())
	assert.Equal(t, "apply_logs", complete[0]["table"])
	assert.InDelta(t, 2, complete[0]["rows_deleted"], 0)
}

// A failure purging apply logs leaves the webhook inbox purge already done, and
// the pass returns the failure after logging it once.
func TestStorageRetentionPassReportsAnApplyLogsFailureAfterPurgingWebhookEvents(t *testing.T) {
	boom := errors.New("apply log purge failed")
	events := &retentionWebhookStore{batches: []int64{2}}
	applyLogs := &retentionApplyLogStore{deleteErr: boom}
	svc, logs := newRetentionServiceWithApplyLogs(t, StorageRetentionConfig{}, &retentionApplyStore{}, events, applyLogs)

	err := svc.purgeTables(t.Context(), deletingSettingsWithApplyLogs())

	require.ErrorIs(t, err, boom)
	assert.Len(t, events.deleteCalls, 1)
	failed := logEntries(t, logs, "storage retention pass failed")
	require.Len(t, failed, 1, logs.String())
	assert.Equal(t, "apply_logs", failed[0]["table"])
	complete := logEntries(t, logs, "storage retention pass complete")
	require.Len(t, complete, 1, logs.String())
	assert.Equal(t, "webhook_events", complete[0]["table"])
}

// When both tables fail, each failure is logged and the pass returns both.
func TestStorageRetentionPassReturnsBothTableFailures(t *testing.T) {
	webhookErr := errors.New("webhook purge failed")
	applyLogErr := errors.New("apply log purge failed")
	events := &retentionWebhookStore{deleteErr: webhookErr}
	applyLogs := &retentionApplyLogStore{deleteErr: applyLogErr}
	svc, logs := newRetentionServiceWithApplyLogs(t, StorageRetentionConfig{}, &retentionApplyStore{}, events, applyLogs)

	err := svc.purgeTables(t.Context(), deletingSettingsWithApplyLogs())

	require.ErrorIs(t, err, webhookErr)
	require.ErrorIs(t, err, applyLogErr)
	failed := logEntries(t, logs, "storage retention pass failed")
	require.Len(t, failed, 2, logs.String())
	assert.Equal(t, "webhook_events", failed[0]["table"])
	assert.Equal(t, "apply_logs", failed[1]["table"])
}

// Shutdown that arrives while the webhook inbox is purging ends the pass before
// apply logs start, and is a routine stop rather than a logged failure.
func TestStorageRetentionPassStopsBetweenTablesOnShutdown(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	events := &retentionWebhookStore{batches: []int64{1}}
	events.onDelete = func(int) { cancel() }
	applyLogs := &retentionApplyLogStore{batches: []int64{1}, counted: 1}
	svc, logs := newRetentionServiceWithApplyLogs(t, StorageRetentionConfig{}, &retentionApplyStore{}, events, applyLogs)

	svc.runStorageRetentionPass(ctx, deletingSettingsWithApplyLogs())

	assert.Len(t, events.deleteCalls, 1)
	assert.Empty(t, applyLogs.deleteCalls, "apply logs do not start after shutdown")
	assert.Empty(t, applyLogs.countCalls)
	assert.Empty(t, logEntries(t, logs, "storage retention pass failed"))
	assert.Len(t, logEntries(t, logs, "storage retention: pass interrupted by shutdown"), 1, logs.String())
}

// The startup line reports the apply logs window, or that none is set, and the
// pass built from the config carries the window to the store.
func TestStorageRetentionPassCarriesTheApplyLogsWindow(t *testing.T) {
	tests := []struct {
		name       string
		applyLogs  string
		wantWindow string
		wantCalls  []deleteCall
	}{
		{name: "unset", wantWindow: "unset"},
		{
			name: "set", applyLogs: "2160h", wantWindow: (2160 * time.Hour).String(),
			wantCalls: []deleteCall{{age: 2160 * time.Hour, limit: 250}},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := StorageRetentionConfig{Enabled: true, DryRun: new(false), BatchSize: 250, ApplyLogs: tt.applyLogs}
			applyLogs := &retentionApplyLogStore{}
			svc, logs := newRetentionServiceWithApplyLogs(t, cfg, &retentionApplyStore{}, &retentionWebhookStore{}, applyLogs)

			pass, _, ok := svc.storageRetentionPass()
			require.True(t, ok)
			pass(t.Context())

			enabled := logEntries(t, logs, "storage retention pass enabled")
			require.Len(t, enabled, 1, logs.String())
			assert.Equal(t, tt.wantWindow, enabled[0]["apply_logs_window"])
			assert.Equal(t, tt.wantCalls, applyLogs.deleteCalls)
		})
	}
}
