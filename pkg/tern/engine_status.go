package tern

import (
	"context"
	"log/slog"
	"strings"
	"sync"

	"github.com/block/schemabot/pkg/metrics"
	"github.com/block/schemabot/pkg/state"
	"github.com/block/schemabot/pkg/storage"
)

// maxObservedStatusLen bounds an engine-controlled status string before it is
// used as a structured log value, so a pathological engine response cannot
// flood the log with one enormous line. Real statuses are short single tokens.
//
// This bounds the size of each value, not how many distinct ones there are.
// Nothing keyed on the status may assume a small set of them.
const maxObservedStatusLen = 128

// unrecognizedStatusReporter surfaces engine- and data-plane-reported task
// statuses that have no mapping in pkg/state. NormalizeTaskStatus falls back
// to Running for an unknown status so the work stays visible and blocking;
// that fallback is a guess, and this reporter is what makes the guess
// operable: every sighting increments the unrecognized-status counter, and
// the first sighting of each task (or shard) and status pair warns with the
// task's triage identifiers so the missing mapping can be added. Recognized
// statuses record nothing, so observing on every poll tick is cheap.
//
// The zero value is ready to use. The warn-dedupe set is keyed by task (or
// shard) and status, so it grows with tasks times distinct statuses and is
// never evicted: entries accumulate over the process lifetime, not just over
// the tasks being driven now. It is left unbounded because the gate in front
// of it is what limits growth — nothing is recorded at all while every status
// maps, so the set only grows during a mapping gap, which a mapping added to
// pkg/state closes.
type unrecognizedStatusReporter struct {
	mu     sync.Mutex
	warned map[string]struct{}
}

// observeTaskStatus reports raw when it has no task-state mapping. Call it
// wherever an engine- or remote-reported task status enters the control
// plane, before the status is normalized away.
func (r *unrecognizedStatusReporter) observeTaskStatus(ctx context.Context, logger *slog.Logger, task *storage.Task, raw string) {
	r.observe(ctx, logger, task, "", raw)
}

// observeShardStatus is observeTaskStatus for one shard of a table task. The
// shard joins the warn line and its dedupe key, so each shard of a table gets
// its own first-sighting warn.
func (r *unrecognizedStatusReporter) observeShardStatus(ctx context.Context, logger *slog.Logger, task *storage.Task, shard, raw string) {
	r.observe(ctx, logger, task, shard, raw)
}

func (r *unrecognizedStatusReporter) observe(ctx context.Context, logger *slog.Logger, task *storage.Task, shard, raw string) {
	if state.RecognizedTaskStatus(raw) {
		return
	}
	metrics.RecordUnrecognizedEngineTaskStatus(ctx, task.Database, task.DatabaseType, task.Engine, task.Environment)
	status := clampObservedStatus(raw)
	if !r.firstSighting(task.TaskIdentifier + "\x00" + shard + "\x00" + status) {
		// The counter above recorded the sighting; only the warn is deduped so
		// a poll loop cannot emit one line per tick for the same status.
		return
	}
	attrs := task.LogAttrs()
	if shard != "" {
		attrs = append(attrs, "shard", shard)
	}
	logger.WarnContext(ctx, "engine reported a task status with no state mapping; the work renders as Running until a mapping is added in pkg/state",
		append(attrs, "raw_status", status)...)
}

// firstSighting records the key and reports whether it was previously unseen.
func (r *unrecognizedStatusReporter) firstSighting(key string) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, ok := r.warned[key]; ok {
		return false
	}
	if r.warned == nil {
		r.warned = make(map[string]struct{})
	}
	r.warned[key] = struct{}{}
	return true
}

// clampObservedStatus collapses whitespace runs (including newlines) to single
// spaces and clamps the length, keeping an engine-controlled status safe to use
// as a structured log value. An empty-after-collapse status becomes a visible
// placeholder so the sighting is still attributable.
func clampObservedStatus(raw string) string {
	collapsed := strings.Join(strings.Fields(raw), " ")
	if collapsed == "" {
		return "(empty)"
	}
	runes := []rune(collapsed)
	if len(runes) > maxObservedStatusLen {
		return string(runes[:maxObservedStatusLen-1]) + "…"
	}
	return collapsed
}
