package commands

import (
	"context"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/block/schemabot/pkg/apitypes"
	"github.com/block/schemabot/pkg/cmd/client"
	"github.com/block/schemabot/pkg/cmd/internal/templates"
	webhooktemplates "github.com/block/schemabot/pkg/webhook/templates"
)

// followInterval is how often --follow polls the logs endpoint for new entries.
const followInterval = 5 * time.Second

// followFetchLimit bounds each follow poll's read. A poll fetches the newest
// followFetchLimit entries (per data-plane source, for deployment tails) and
// prints the ones not yet shown; an apply that writes more than this within
// one poll interval produces more output than a terminal tail can usefully
// render anyway.
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

	switch {
	case cmd.Follow && cmd.Deployment != "":
		return followDeploymentLogs(ctx, ep, cmd.ApplyID, cmd.Deployment, cmd.Limit)
	case cmd.Follow:
		return followLogs(ctx, ep, cmd.Database, cmd.Environment, cmd.ApplyID, cmd.Limit)
	case cmd.Deployment != "":
		return showDeploymentLogs(ep, cmd.ApplyID, cmd.Deployment, cmd.Limit, cmd.JSON)
	default:
		return showLogs(ep, cmd.Database, cmd.Environment, cmd.ApplyID, cmd.Limit, cmd.JSON)
	}
}

func showDeploymentLogs(endpoint, applyID, deployment string, limit int, outputJSON bool) error {
	var result *apitypes.DeploymentLogsResponse
	err := withLoading("Loading data-plane logs...", !outputJSON, func() error {
		var loadErr error
		result, loadErr = client.GetDeploymentLogs(endpoint, applyID, deployment, limit)
		return loadErr
	})
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
		printLogs(source.Logs)
		if i+1 < len(result.Sources) {
			fmt.Println()
		}
	}
	for _, sourceErr := range result.Errors {
		fmt.Printf("Warning: %s: %s\n", deploymentLogSourceLabel(sourceErr.Target, sourceErr.Operations, sourceErr.ExternalID), sourceErr.Message)
	}
	return nil
}

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
	if len(parts) == 1 && parts[0] == "" {
		return externalID
	}
	return strings.Join(parts, " / ") + " (" + externalID + ")"
}

// showLogs displays logs once and exits.
func showLogs(endpoint, database, environment, applyID string, limit int, outputJSON bool) error {
	var logs []*client.LogEntry
	err := withLoading("Loading logs...", !outputJSON, func() error {
		var loadErr error
		logs, loadErr = client.GetLogs(endpoint, database, environment, applyID, limit)
		return loadErr
	})
	if err != nil {
		return err
	}
	if outputJSON {
		return writeJSON(&apitypes.LogsResponse{ApplyID: applyID, Logs: logs})
	}

	if len(logs) == 0 {
		fmt.Println("No logs found.")
		return nil
	}

	printLogs(logs)
	return nil
}

// followLogs prints the newest initialLimit entries, then polls for new ones
// until the command context is cancelled (Ctrl+C).
func followLogs(ctx context.Context, endpoint, database, environment, applyID string, initialLimit int) error {
	switch {
	case applyID != "" && database == "":
		fmt.Printf("Following logs for %s... (Ctrl+C to stop)\n\n", applyID)
	case applyID != "":
		fmt.Printf("Following logs for %s (apply %s)... (Ctrl+C to stop)\n\n", database, applyID)
	default:
		fmt.Printf("Following logs for %s/%s... (Ctrl+C to stop)\n\n", database, environment)
	}

	state := &followState{}
	fetch := func(limit int) ([]*client.LogEntry, error) {
		return client.GetLogs(endpoint, database, environment, applyID, limit)
	}
	emit := func(logs []*client.LogEntry) {
		printLogs(state.advance(logs))
	}
	return runFollowLoop(ctx, fetch, emit, initialLimit, followInterval)
}

// followDeploymentLogs tails data-plane logs: it prints the newest
// initialLimit entries from every remote source, then polls for new ones
// until the command context is cancelled (Ctrl+C).
func followDeploymentLogs(ctx context.Context, endpoint, applyID, deployment string, initialLimit int) error {
	fmt.Printf("Following data-plane logs for %s on %s... (Ctrl+C to stop)\n\n", applyID, deployment)

	state := newDeploymentFollowState()
	fetch := func(limit int) (*apitypes.DeploymentLogsResponse, error) {
		return client.GetDeploymentLogs(endpoint, applyID, deployment, limit)
	}
	emit := func(result *apitypes.DeploymentLogsResponse) {
		batches, warnings := state.advance(result)
		for _, batch := range batches {
			if batch.label != "" {
				fmt.Printf("%s%s%s\n", templates.ANSIDim, batch.label, templates.ANSIReset)
			}
			printLogs(batch.logs)
		}
		for _, warning := range warnings {
			fmt.Fprintf(os.Stderr, "Warning: %s\n", warning)
		}
	}
	return runFollowLoop(ctx, fetch, emit, initialLimit, followInterval)
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
// surface for sources that could not be read this poll.
func (s *deploymentFollowState) advance(result *apitypes.DeploymentLogsResponse) ([]deploymentFollowBatch, []string) {
	for _, source := range result.Sources {
		key := deploymentFollowSourceKey("", source.Operations, source.ExternalID)
		s.seen[key] = true
		if s.bySource[key] == nil {
			s.bySource[key] = &followState{}
		}
		// A successful read clears the source's warning suppression so a
		// relapse after recovery is reported again.
		delete(s.lastWarned, key)
	}
	for _, sourceErr := range result.Errors {
		s.seen[deploymentFollowSourceKey(sourceErr.Target, sourceErr.Operations, sourceErr.ExternalID)] = true
	}

	// Label batches once more than one source exists, so interleaved tails
	// stay attributable; a single-source tail stays unadorned.
	labelSources := len(s.seen) > 1

	var batches []deploymentFollowBatch
	for _, source := range result.Sources {
		key := deploymentFollowSourceKey("", source.Operations, source.ExternalID)
		fresh := s.bySource[key].advance(source.Logs)
		if len(fresh) == 0 {
			// Nothing new for this source since the last poll.
			continue
		}
		batch := deploymentFollowBatch{logs: fresh}
		if labelSources {
			batch.label = deploymentLogSourceLabel("", source.Operations, source.ExternalID)
		}
		batches = append(batches, batch)
	}

	var warnings []string
	for _, sourceErr := range result.Errors {
		key := deploymentFollowSourceKey(sourceErr.Target, sourceErr.Operations, sourceErr.ExternalID)
		warning := fmt.Sprintf("%s: %s", deploymentLogSourceLabel(sourceErr.Target, sourceErr.Operations, sourceErr.ExternalID), sourceErr.Message)
		if s.lastWarned[key] == warning {
			// This exact failure was already reported; repeating it on every
			// poll would drown the tail.
			continue
		}
		s.lastWarned[key] = warning
		warnings = append(warnings, warning)
	}
	return batches, warnings
}

// runFollowLoop fetches the newest initialLimit entries and hands them to
// emit, then polls fetch on every interval tick until ctx is cancelled. emit
// owns dedupe and printing, so each log mode decides how to track what was
// already shown. The initial fetch must succeed — a broken endpoint should
// fail the command, not tail nothing. Later polls warn and retry on the next
// tick instead: a tail during an incident must survive transient server blips.
func runFollowLoop[T any](ctx context.Context, fetch func(limit int) (T, error), emit func(T), initialLimit int, interval time.Duration) error {
	result, err := fetch(initialLimit)
	if err != nil {
		return fmt.Errorf("fetch initial log window: %w", err)
	}
	emit(result)

	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			// Interrupting a tail is its normal exit, not a failure.
			return nil
		case <-ticker.C:
		}
		result, err := fetch(followFetchLimit)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Warning: failed to fetch logs, retrying next poll: %v\n", err)
			continue
		}
		emit(result)
	}
}

// printLogs formats and prints log entries.
func printLogs(logs []*client.LogEntry) {
	for _, log := range logs {
		// Format timestamp
		ts := log.CreatedAt.Local().Format("15:04:05")

		// Format level with color
		level := formatLogLevel(log.Level)

		// Build the message
		msg := log.Message

		// Add state transition info if present
		if log.OldState != "" && log.NewState != "" {
			msg = fmt.Sprintf("%s [%s -> %s]", msg, log.OldState, log.NewState)
		}

		// Print formatted log line
		fmt.Printf("%s%s%s %s %s\n", templates.ANSIDim, ts, templates.ANSIReset, level, msg)
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
