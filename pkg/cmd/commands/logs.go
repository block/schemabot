package commands

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/block/schemabot/pkg/apitypes"
	"github.com/block/schemabot/pkg/cmd/client"
	"github.com/block/schemabot/pkg/cmd/internal/templates"
	"github.com/block/schemabot/pkg/ui"
	webhooktemplates "github.com/block/schemabot/pkg/webhook/templates"
)

// followInterval is how often --follow polls the logs endpoint for new entries.
const followInterval = 5 * time.Second

// followFetchLimit bounds each follow poll's read. A poll fetches the newest
// followFetchLimit entries (per data-plane source, for deployment tails) and
// prints the ones not yet shown; a poll whose bounded window no longer
// overlaps what was already printed reports a possible gap (see
// followState.missedEntries), so entries pushed past the bound between polls
// are announced rather than silently dropped.
const followFetchLimit = 500

// LogsCmd views apply logs for a database or specific apply.
type LogsCmd struct {
	ApplyIDArg  string `arg:"" optional:"" help:"Apply ID (positional)" name:"apply_id"`
	Database    string `short:"d" help:"Database name (required unless apply_id provided)"`
	Environment string `short:"e" help:"Target environment (required unless apply_id provided)"`
	ApplyID     string `short:"a" help:"Apply ID (e.g., apply_abc123)" name:"apply-id"`
	Limit       int    `short:"n" help:"Show the newest N log entries" default:"50"`
	Follow      bool   `short:"f" help:"Print the newest N entries, then poll for new ones until interrupted"`
	Deployment  string `help:"Read logs from the selected data-plane deployment"`
	Target      string `help:"Show only one target's entries, plus the entries about the whole apply"`
	Shard       string `help:"Show only one shard's entries of a sharded apply, plus the entries about the whole apply; with --target, that target's shard. Join a shard that starts with - to the flag: --shard=-80"`
	JSON        bool   `help:"Output as JSON"`
}

// Run executes the logs command.
func (cmd *LogsCmd) Run(ctx context.Context, g *Globals) error {
	// Merge positional apply_id into flag
	if cmd.ApplyIDArg != "" && cmd.ApplyID == "" {
		cmd.ApplyID = cmd.ApplyIDArg
	}

	// When apply ID is provided, database is not required
	if cmd.ApplyID == "" {
		if cmd.Database == "" {
			return fmt.Errorf("--database is required (or provide an apply_id)")
		}
		if cmd.Environment == "" {
			return fmt.Errorf("--environment is required (or provide an apply_id)")
		}
	}
	if cmd.Deployment != "" && cmd.ApplyID == "" {
		return fmt.Errorf("--deployment requires an explicit apply_id")
	}
	if cmd.JSON && cmd.Follow {
		return fmt.Errorf("--json is incompatible with --follow: the tail renders human-readable lines; run without --follow for a JSON snapshot")
	}

	ep, err := resolveEndpoint(g.Endpoint, g.Profile)
	if err != nil {
		return err
	}

	scope := client.LogScope{Target: cmd.Target, Shard: cmd.Shard}
	switch {
	case cmd.Follow && cmd.Deployment != "":
		return followDeploymentLogs(ctx, ep, cmd.ApplyID, cmd.Deployment, scope, cmd.Limit)
	case cmd.Follow:
		return followLogs(ctx, ep, cmd.Database, cmd.Environment, cmd.ApplyID, scope, cmd.Limit)
	case cmd.Deployment != "":
		return showDeploymentLogs(ctx, ep, cmd.ApplyID, cmd.Deployment, scope, cmd.Limit, cmd.JSON)
	default:
		return showLogs(ctx, ep, cmd.Database, cmd.Environment, cmd.ApplyID, scope, cmd.Limit, cmd.JSON)
	}
}

func showDeploymentLogs(ctx context.Context, endpoint, applyID, deployment string, scope client.LogScope, limit int, outputJSON bool) error {
	var result *apitypes.DeploymentLogsResponse
	err := withLoading("Loading data-plane logs...", !outputJSON, func() error {
		var loadErr error
		result, loadErr = client.GetDeploymentLogsCtx(ctx, endpoint, applyID, deployment, scope, limit)
		return loadErr
	})
	if endedByInterrupt(ctx, err) {
		return interrupted(ctx)
	}
	if err != nil {
		return err
	}
	if outputJSON {
		return writeJSON(result)
	}
	if len(result.Sources) == 0 {
		fmt.Println("No data-plane logs found.")
	}
	for i, source := range result.Sources {
		if len(result.Sources) > 1 {
			fmt.Printf("%s%s%s\n", templates.ANSIDim, deploymentLogSourceLabel("", source.Operations, source.ExternalID), templates.ANSIReset)
		}
		printLogTruncationNotice(source.Truncated, len(source.Logs))
		printLogs(source.Logs)
		if i+1 < len(result.Sources) {
			fmt.Println()
		}
	}
	for _, sourceErr := range result.Errors {
		fmt.Printf("Warning: %s: %s\n", deploymentLogSourceLabel(sourceErr.Target, sourceErr.Operations, sourceErr.ExternalID), ui.TerminalSafeInline(sourceErr.Message))
	}
	return nil
}

// deploymentLogSourceLabel names a data-plane log source for the line that
// heads its entries or its warning. The target, the operation kinds and the
// external ID all come from the response, so the label has its control runes
// removed; deploymentFollowSourceKey, not this label, identifies the source.
func deploymentLogSourceLabel(target string, operations []*apitypes.LogOperationProvenance, externalID string) string {
	if target == "" && len(operations) > 0 {
		target = operations[0].Target
	}
	parts := []string{target}
	seenKinds := make(map[string]bool)
	for _, op := range operations {
		if op.OperationKind != "" && !seenKinds[op.OperationKind] {
			seenKinds[op.OperationKind] = true
			parts = append(parts, op.OperationKind)
		}
	}
	label := strings.Join(parts, " / ") + " (" + externalID + ")"
	if len(parts) == 1 && parts[0] == "" {
		label = externalID
	}
	return ui.TerminalSafeLine(label)
}

// showLogs displays logs once and exits. Ctrl-C abandons the read and exits
// with ExitInterrupted, since the command never reached its output.
func showLogs(ctx context.Context, endpoint, database, environment, applyID string, scope client.LogScope, limit int, outputJSON bool) error {
	var result *apitypes.LogsResponse
	err := withLoading("Loading logs...", !outputJSON, func() error {
		var loadErr error
		result, loadErr = client.GetLogsCtx(ctx, endpoint, database, environment, applyID, scope, limit)
		return loadErr
	})
	if endedByInterrupt(ctx, err) {
		return interrupted(ctx)
	}
	if err != nil {
		return err
	}
	if outputJSON {
		return writeJSON(result)
	}

	if len(result.Logs) == 0 {
		fmt.Println("No logs found.")
		return nil
	}

	printLogTruncationNotice(result.Truncated, len(result.Logs))
	printLogs(result.Logs)
	return nil
}

// printLogTruncationNotice reports that the window shown starts partway through
// the apply's history. Entries print oldest to newest, so the notice leads: an
// operator reading a full window with no signal takes it for the whole
// lifecycle, and during triage that is exactly the wrong conclusion.
func printLogTruncationNotice(truncated bool, shown int) {
	if !truncated {
		return
	}
	fmt.Printf("%sShowing the newest %d entries; older entries exist. Raise -n for a wider window.%s\n", templates.ANSIDim, shown, templates.ANSIReset)
}

// printFollowGapNotice reports a tail that fell behind: a poll's window filled
// entirely with entries newer than the last one printed, so entries arriving
// between polls may have been pushed past the window without being shown.
func printFollowGapNotice() {
	fmt.Printf("%sPossible gap: the poll window filled with new entries; entries between polls may have been skipped.%s\n", templates.ANSIDim, templates.ANSIReset)
}

// followLogs prints the newest initialLimit entries, then polls for new ones
// until the command context is cancelled (Ctrl+C).
func followLogs(ctx context.Context, endpoint, database, environment, applyID string, scope client.LogScope, initialLimit int) error {
	header := scopeHeader(scope)
	switch {
	case applyID != "" && database == "":
		fmt.Printf("Following logs for %s%s... (Ctrl+C to stop)\n\n", applyID, header)
	case applyID != "":
		fmt.Printf("Following logs for %s (apply %s)%s... (Ctrl+C to stop)\n\n", database, applyID, header)
	default:
		fmt.Printf("Following logs for %s/%s%s... (Ctrl+C to stop)\n\n", database, environment, header)
	}

	state := &followState{}
	fetch := func(limit int) (*apitypes.LogsResponse, error) {
		return client.GetLogsCtx(ctx, endpoint, database, environment, applyID, scope, limit)
	}
	emit := func(result *apitypes.LogsResponse, initial bool) error {
		switch {
		case initial:
			printLogTruncationNotice(result.Truncated, len(result.Logs))
		case state.missedEntries(result.Logs, result.Truncated):
			printFollowGapNotice()
		}
		printLogs(state.advance(result.Logs))
		return nil
	}
	return runFollowLoop(ctx, fetch, emit, initialLimit, defaultFollowPacing)
}

// scopeHeader names the target and shard a followed read is narrowed to, for
// the header that opens the tail.
func scopeHeader(scope client.LogScope) string {
	var header string
	if scope.Target != "" {
		header += ", target " + scope.Target
	}
	if scope.Shard != "" {
		header += ", shard " + scope.Shard
	}
	return header
}

// followDeploymentLogs tails data-plane logs: it prints the newest
// initialLimit entries from every remote source, then polls for new ones
// until the command context is cancelled (Ctrl+C).
func followDeploymentLogs(ctx context.Context, endpoint, applyID, deployment string, scope client.LogScope, initialLimit int) error {
	fmt.Printf("Following data-plane logs for %s on %s%s... (Ctrl+C to stop)\n\n", applyID, deployment, scopeHeader(scope))

	fetch := func(limit int) (*apitypes.DeploymentLogsResponse, error) {
		return client.GetDeploymentLogsCtx(ctx, endpoint, applyID, deployment, scope, limit)
	}
	return runFollowLoop(ctx, fetch, newDeploymentFollowEmit(), initialLimit, defaultFollowPacing)
}

// newDeploymentFollowEmit returns the render step of a data-plane tail: each
// poll's new entries per source on stdout and each source's new failure once
// on stderr. A poll with no readable source prints nothing and returns the
// error the follow loop judges like any other failed poll.
func newDeploymentFollowEmit() func(result *apitypes.DeploymentLogsResponse, initial bool) error {
	state := newDeploymentFollowState()
	// The loop-wide initial flag is unused here: each source owes the
	// truncation notice to its own first successful read, which for a source
	// that failed the initial fan-out arrives on a later poll.
	return func(result *apitypes.DeploymentLogsResponse, _ bool) error {
		batches, warnings, err := state.advance(result)
		if err != nil {
			return err
		}
		for _, batch := range batches {
			if batch.label != "" {
				fmt.Printf("%s%s%s\n", templates.ANSIDim, batch.label, templates.ANSIReset)
			}
			switch {
			case batch.firstRender:
				printLogTruncationNotice(batch.truncated, len(batch.logs))
			case batch.possibleGap:
				printFollowGapNotice()
			}
			printLogs(batch.logs)
		}
		for _, warning := range warnings {
			fmt.Fprintf(os.Stderr, "Warning: %s\n", warning)
		}
		return nil
	}
}

// followState tracks the newest log entry already printed so each poll prints
// only entries that arrived since. Entries are deduplicated on the log id —
// the write-ordered key — not on timestamps, which collide at second
// precision.
type followState struct {
	lastID int64
}

// advance returns the entries newer than the last printed one, in the
// chronological order the server delivered them, and records the newest as
// printed.
func (s *followState) advance(logs []*client.LogEntry) []*client.LogEntry {
	var fresh []*client.LogEntry
	for _, log := range logs {
		if log.ID > s.lastID {
			fresh = append(fresh, log)
			s.lastID = log.ID
		}
	}
	return fresh
}

// missedEntries reports whether a poll's window may have skipped entries: the
// read hit its own bound and even its oldest entry is newer than the newest
// one already printed. Everything older was pushed out of the window, and the
// response does not say whether any of it arrived after the last poll, so the
// tail can no longer prove it printed every entry. A window that overlaps
// what was printed, a window under its bound, or a first render with nothing
// printed yet misses nothing. Evaluate before advance, which moves lastID
// past the window.
func (s *followState) missedEntries(logs []*client.LogEntry, truncated bool) bool {
	return truncated && s.lastID > 0 && len(logs) > 0 && logs[0].ID > s.lastID
}

// deploymentFollowState tracks, per data-plane source, the newest log entry
// already printed. A source is one remote apply on one target; its log ids
// are that data plane's write-ordered keys, monotonic within the source but
// unrelated across sources, so each source dedupes independently on its own
// followState.
type deploymentFollowState struct {
	bySource map[string]*followState
	// seen holds every source key ever observed, including sources that have
	// only ever failed, so the labeling decision reflects the apply's real
	// fan-out rather than which sources happened to be readable this poll.
	seen map[string]bool
	// lastWarned records, per failing source, the failure already reported so
	// an unchanged failure is not repeated on every poll. A source that
	// recovers is cleared, so a relapse warns again.
	lastWarned map[string]string
}

func newDeploymentFollowState() *deploymentFollowState {
	return &deploymentFollowState{bySource: make(map[string]*followState), seen: make(map[string]bool), lastWarned: make(map[string]string)}
}

// deploymentFollowBatch is one source's not-yet-printed entries from a poll.
// The label identifies the source and is empty while only one source has been
// observed, matching the unadorned single-source one-shot view.
type deploymentFollowBatch struct {
	label string
	logs  []*client.LogEntry
	// firstRender marks the source's first successful read — the one render
	// whose window reaches as deep as the tail ever will for that source, so
	// the one that owes the operator the truncation notice. A source that
	// fails the initial fan-out read gets its first render on the poll it
	// recovers.
	firstRender bool
	// truncated carries the source's window signal: the read hit its own bound
	// and older entries exist beyond it. On the first render it drives the
	// truncation notice; on later polls it feeds the possible-gap check.
	truncated bool
	// possibleGap marks a later poll whose window filled entirely with entries
	// newer than the last printed one (see followState.missedEntries): entries
	// arriving between polls may have been pushed past the window unseen.
	possibleGap bool
}

// deploymentFollowSourceKey identifies a data-plane log source (one remote
// apply on one target) across polls, for both successful sources and per-source
// read errors.
func deploymentFollowSourceKey(target string, operations []*apitypes.LogOperationProvenance, externalID string) string {
	if target == "" && len(operations) > 0 {
		target = operations[0].Target
	}
	return target + "\x00" + externalID
}

// advance returns each source's entries newer than the last printed ones, in
// the chronological order the server delivered them, plus the warnings to
// surface for sources that could not be read this poll. A poll in which no
// source could be read returns a *deploymentSourcesUnreadableError instead,
// so the follow loop treats it as a failed poll rather than a quiet one.
func (s *deploymentFollowState) advance(result *apitypes.DeploymentLogsResponse) ([]deploymentFollowBatch, []string, error) {
	firstRender := make(map[string]bool)
	for _, source := range result.Sources {
		key := deploymentFollowSourceKey("", source.Operations, source.ExternalID)
		s.seen[key] = true
		if s.bySource[key] == nil {
			s.bySource[key] = &followState{}
			firstRender[key] = true
		}
		// A successful read clears the source's warning suppression so a
		// relapse after recovery is reported again.
		delete(s.lastWarned, key)
	}
	for _, sourceErr := range result.Errors {
		s.seen[deploymentFollowSourceKey(sourceErr.Target, sourceErr.Operations, sourceErr.ExternalID)] = true
	}
	if unreadable := unreadableSourcesError(result); unreadable != nil {
		// The per-source warnings are left unrecorded: the failed-poll warning
		// already names every source, and a source that fails on beside a
		// recovered one is then still reported once.
		return nil, nil, unreadable
	}

	// Label batches once more than one source exists, so interleaved tails
	// stay attributable; a single-source tail stays unadorned.
	labelSources := len(s.seen) > 1

	var batches []deploymentFollowBatch
	for _, source := range result.Sources {
		key := deploymentFollowSourceKey("", source.Operations, source.ExternalID)
		sourceState := s.bySource[key]
		possibleGap := sourceState.missedEntries(source.Logs, source.Truncated)
		fresh := sourceState.advance(source.Logs)
		if len(fresh) == 0 {
			// Nothing new for this source since the last poll.
			continue
		}
		batch := deploymentFollowBatch{logs: fresh, firstRender: firstRender[key], truncated: source.Truncated, possibleGap: possibleGap}
		if labelSources {
			batch.label = deploymentLogSourceLabel("", source.Operations, source.ExternalID)
		}
		batches = append(batches, batch)
	}

	var warnings []string
	for _, sourceErr := range result.Errors {
		key := deploymentFollowSourceKey(sourceErr.Target, sourceErr.Operations, sourceErr.ExternalID)
		warning := fmt.Sprintf("%s: %s", deploymentLogSourceLabel(sourceErr.Target, sourceErr.Operations, sourceErr.ExternalID), ui.TerminalSafeInline(sourceErr.Message))
		if s.lastWarned[key] == warning {
			// This exact failure was already reported; repeating it on every
			// poll would drown the tail.
			continue
		}
		s.lastWarned[key] = warning
		warnings = append(warnings, warning)
	}
	return batches, warnings, nil
}

// deploymentSourcesUnreadableError is a poll the server answered without a
// single readable data-plane source. It is permanent when every source was
// refused for a reason a retry cannot change, and transient otherwise.
type deploymentSourcesUnreadableError struct {
	permanent bool
	// failures names each source with its message and reason, cleaned of
	// control runes, since all of it comes from the response.
	failures []string
}

func (e *deploymentSourcesUnreadableError) Error() string {
	return "no data-plane log source could be read: " + strings.Join(e.failures, "; ")
}

// unreadableSourcesError returns the failure of a poll in which every source
// the server tried failed, or nil when at least one source was read. One source
// that may still recover keeps the whole poll transient, so only a tail whose
// every source is refused for good ends at once.
func unreadableSourcesError(result *apitypes.DeploymentLogsResponse) *deploymentSourcesUnreadableError {
	if len(result.Sources) > 0 || len(result.Errors) == 0 {
		return nil
	}
	unreadable := &deploymentSourcesUnreadableError{permanent: true}
	for _, sourceErr := range result.Errors {
		if !isPermanentDeploymentLogError(sourceErr) {
			unreadable.permanent = false
		}
		unreadable.failures = append(unreadable.failures, fmt.Sprintf("%s: %s (%s)",
			deploymentLogSourceLabel(sourceErr.Target, sourceErr.Operations, sourceErr.ExternalID),
			ui.TerminalSafeInline(sourceErr.Message), ui.TerminalSafeInline(sourceErr.Reason)))
	}
	return unreadable
}

// isPermanentDeploymentLogError reports whether a source's read failure, as
// the server's logs endpoint classifies it, will come back the same on every
// poll. A source only reaches the data-plane read once the data plane has
// accepted its apply and returned the apply's id, so a data plane that then
// finds no log for that apply, target or shard has lost it rather than not
// reached it yet. A data plane that predates log, target or shard reads needs
// an upgrade, and records SchemaBot could not decode come back on the next
// read. A read that failed in transport or storage, and any code or reason
// this CLI does not know, may recover and is retried.
func isPermanentDeploymentLogError(sourceErr *apitypes.DeploymentLogError) bool {
	switch {
	case sourceErr.Code == apitypes.DeploymentLogCodeNotFound && sourceErr.Reason == apitypes.DeploymentLogReasonNotFound:
		return true
	case sourceErr.Code == apitypes.DeploymentLogCodeUnsupported && sourceErr.Reason == apitypes.DeploymentLogReasonUpgradeRequired:
		return true
	case sourceErr.Code == apitypes.DeploymentLogCodeMalformed && sourceErr.Reason == apitypes.DeploymentLogReasonMalformed:
		return true
	default:
		return false
	}
}

// followPacing sets how long a tail waits before its next poll: interval after
// a poll that succeeded, and retryWait after one that failed transiently, given
// that failure and how many polls in a row have failed. sleep performs the
// wait and returns early once the tail is cancelled.
type followPacing struct {
	interval  time.Duration
	retryWait func(err error, consecutiveFailures int) time.Duration
	sleep     func(ctx context.Context, d time.Duration)
}

// defaultFollowPacing polls at followInterval, and backs off after a transient
// failure on the same schedule the progress watches use, so a tail rides out a
// server restart for as long as a watch does.
var defaultFollowPacing = followPacing{interval: followInterval, retryWait: progressRetryWait, sleep: sleepUnlessDone}

// runFollowLoop fetches the newest initialLimit entries and hands them to
// emit, then polls fetch after each pacing wait until ctx is cancelled. emit
// owns dedupe and printing, so each log mode decides how to track what was
// already shown; it is told whether it is rendering the initial window, which
// is the only one bounded by the operator's own limit. The initial fetch must
// succeed — a broken endpoint should fail the command, not tail nothing.
// emit returns an error for an answer that carried nothing readable, and that
// poll, the initial one included, counts as failed under the rule below.
// fetch is expected to abandon its read once ctx is cancelled, so an interrupt
// ends the tail at once rather than after the in-flight read answers.
//
// Later polls follow the rule the progress watches use. A transient failure
// warns and is retried with backoff, so a tail during an incident survives a
// server blip, up to maxConsecutiveProgressFailures polls in a row. A permanent
// failure, such as the apply no longer existing or the credentials being
// refused, or one transient failure too many, ends the tail with an error:
// retrying cannot change the answer, and a tail that prints nothing looks like
// an apply with nothing to say.
func runFollowLoop[T any](ctx context.Context, fetch func(limit int) (T, error), emit func(result T, initial bool) error, initialLimit int, pacing followPacing) error {
	result, err := fetch(initialLimit)
	if err != nil {
		if ctx.Err() != nil {
			// Ctrl-C during the first read ends the tail like any other
			// interrupt, not as a failed read.
			return nil
		}
		return fmt.Errorf("fetch initial log window: %w", err)
	}

	wait := pacing.interval
	failures := 0
	if err := emit(result, true); err != nil {
		if ctx.Err() != nil {
			// A read the operator's Ctrl-C landed on is part of the interrupt,
			// even when its answer had nothing readable in it.
			return nil
		}
		failures++
		var stopErr error
		wait, stopErr = followFailureOutcome(err, failures, pacing)
		if stopErr != nil {
			return stopErr
		}
	}
	for {
		pacing.sleep(ctx, wait)
		if ctx.Err() != nil {
			// Interrupting a tail is its normal exit, not a failure.
			return nil
		}
		result, err := fetch(followFetchLimit)
		if err == nil {
			err = emit(result, false)
		}
		if err != nil && ctx.Err() != nil {
			// A poll the operator's Ctrl-C landed on is part of the interrupt,
			// not a failure of the endpoint, whether the read failed or its
			// answer had nothing readable in it.
			return nil
		}
		if err != nil {
			failures++
			var stopErr error
			wait, stopErr = followFailureOutcome(err, failures, pacing)
			if stopErr != nil {
				return stopErr
			}
			continue
		}
		failures = 0
		wait = pacing.interval
	}
}

// followFailureOutcome decides what a failed follow poll means. A failure the
// watches treat as permanent, or the maxConsecutiveProgressFailures-th
// transient failure in a row, is returned as the error that ends the tail.
// Otherwise it warns on stderr, which keeps stdout to the log lines, and
// returns the wait before the next poll.
func followFailureOutcome(err error, failures int, pacing followPacing) (time.Duration, error) {
	if !isRetryableFollowFailure(err) {
		return 0, fmt.Errorf("stopped following logs: %w", err)
	}
	if failures >= maxConsecutiveProgressFailures {
		return 0, fmt.Errorf("stopped following logs after %d consecutive failed polls; following logs does not affect the apply; run the same logs command again to keep following: %w", failures, err)
	}
	wait := pacing.retryWait(err, failures)
	fmt.Fprintf(os.Stderr, "Warning: failed to fetch logs (attempt %d/%d), retrying in %s: %s\n",
		failures, maxConsecutiveProgressFailures, wait, ui.TerminalSafeInline(err.Error()))
	return wait, nil
}

// isRetryableFollowFailure reports whether a failed follow poll is worth
// retrying. A poll the server answered with no readable data-plane source is
// judged by why its sources failed; any other failure is judged the way the
// watches judge a failed fetch.
func isRetryableFollowFailure(err error) bool {
	var unreadable *deploymentSourcesUnreadableError
	if errors.As(err, &unreadable) {
		return !unreadable.permanent
	}
	return isRetryableFetchError(err)
}

// printLogs formats and prints log entries. An entry's level, message and
// states are relayed from the engine and the database, so control runes are
// removed from them; the colors around the timestamp and the level are the
// CLI's own and are added afterwards.
func printLogs(logs []*client.LogEntry) {
	for _, log := range logs {
		ts := webhooktemplates.LogEntryTimestamp(log.CreatedAt)

		// Format level with color
		level := formatLogLevel(ui.TerminalSafeLine(log.Level))

		// Build the message
		msg := log.Message

		// Add state transition info if present
		if log.OldState != "" && log.NewState != "" {
			msg = fmt.Sprintf("%s [%s -> %s]", msg, log.OldState, log.NewState)
		}

		// Print formatted log line
		fmt.Printf("%s%s%s %s %s\n", templates.ANSIDim, ts, templates.ANSIReset, level, ui.TerminalSafe(msg))
	}
}

// formatLogLevel returns a colored log level indicator, wrapping the shared
// tag text so the terminal output and the PR-comment log fold stay identical
// apart from color.
func formatLogLevel(level string) string {
	tag := webhooktemplates.LogLevelTag(level)
	switch strings.ToLower(level) {
	case "error":
		return "\033[31m" + tag + templates.ANSIReset // Red
	case "warn":
		return templates.ANSIYellow + tag + templates.ANSIReset
	case "info":
		return templates.ANSIGreen + tag + templates.ANSIReset
	case "debug":
		return templates.ANSIDim + tag + templates.ANSIReset
	default:
		return tag
	}
}
