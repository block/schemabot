package commands

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"sort"
	"strings"
	"time"

	"github.com/block/schemabot/pkg/apitypes"
	"github.com/block/schemabot/pkg/cmd/client"
	"github.com/block/schemabot/pkg/cmd/templates"
	"github.com/block/schemabot/pkg/ddl"
	"github.com/block/schemabot/pkg/state"
	"github.com/block/schemabot/pkg/ui"
)

// ApplyCmd plans and applies schema changes.
type ApplyCmd struct {
	SchemaDir    string        `short:"s" required:"" help:"Schema directory with schemabot.yaml and .sql files" name:"schema_dir"`
	Environment  string        `short:"e" required:"" help:"Target environment"`
	Repository   string        `help:"Repository name (optional, for tracking)"`
	PullRequest  int           `help:"Pull request number (optional, for tracking)" name:"pull-request"`
	AutoApprove  bool          `short:"y" help:"Skip confirmation prompt" name:"auto-approve"`
	Watch        bool          `short:"w" help:"Watch progress until completion" default:"true" negatable:""`
	DeferCutover bool          `help:"Defer cutover until manual trigger (use 'schemabot cutover')" name:"defer-cutover"`
	DeferDeploy  bool          `help:"Defer deploy until manual trigger (holds at waiting_for_deploy)" name:"defer-deploy"`
	SkipRevert   bool          `help:"Skip revert window after completion (Vitess only)" name:"skip-revert"`
	Branch       string        `help:"Reuse existing PlanetScale branch (syncs with main, skips branch creation)" name:"branch"`
	AllowUnsafe  bool          `help:"Allow destructive changes (DROP TABLE, DROP COLUMN, etc.)" name:"allow-unsafe"`
	Force        bool          `help:"Force acquire lock (breaks existing lock from another owner)"`
	Yield        bool          `help:"Yield lock after successful completion"`
	NoLock       bool          `help:"Don't hold a database lock during the operation" name:"no-lock"`
	Output       OutputFormat  `short:"o" help:"Output format" default:"interactive" enum:"interactive,log,json"`
	LogHeartbeat time.Duration `help:"Interval between progress heartbeats in log mode" default:"10s" name:"log-heartbeat"`
}

// Run executes the apply command.
func (cmd *ApplyCmd) Run(g *Globals) error {
	if cmd.NoLock && cmd.Force {
		return fmt.Errorf("--force requires locking to break an existing lock; remove --no-lock to use it")
	}
	if cmd.NoLock && cmd.Yield {
		return fmt.Errorf("--yield requires locking to release after completion; remove --no-lock to use it")
	}

	// Load config from schema directory
	cfg, err := LoadCLIConfig(cmd.SchemaDir)
	if err != nil {
		return err
	}

	ep, err := resolveEndpoint(g.Endpoint, g.Profile)
	if err != nil {
		return err
	}

	// Generate owner for locking (used later if we have changes to apply)
	owner := client.GenerateCLIOwner()

	// Check for existing active schema change
	active, err := client.CheckActiveSchemaChange(ep, cfg.Database, cmd.Environment)
	if err != nil {
		// Ignore errors - progress API may fail if no schema change exists
	} else if active != nil && active.State != "" && !state.IsState(active.State, state.NoActiveChange) {
		progressCmd := fmt.Sprintf("schemabot status %s", active.ApplyID)
		if active.ApplyID == "" {
			progressCmd = fmt.Sprintf("schemabot progress -d %s -e %s", cfg.Database, cmd.Environment)
		}
		var stateMsg string
		switch {
		case state.IsState(active.State, state.Apply.WaitingForDeploy):
			stateMsg = "A schema change is waiting for deploy."
		case state.IsState(active.State, state.Apply.WaitingForCutover):
			stateMsg = "A schema change is waiting for cutover."
		case state.IsState(active.State, state.Apply.Running):
			stateMsg = "A schema change is already running."
		case state.IsState(active.State, state.Apply.CuttingOver):
			stateMsg = "A schema change is currently cutting over."
		}
		if stateMsg != "" {
			fmt.Println()
			fmt.Println("⏳ Schema Change In Progress")
			fmt.Println()
			fmt.Printf("Database: %s\n", cfg.Database)
			fmt.Printf("Environment: %s\n", cmd.Environment)
			fmt.Println()
			fmt.Println(stateMsg)
			fmt.Println()
			if state.IsState(active.State, state.Apply.WaitingForDeploy, state.Apply.WaitingForCutover) {
				if active.ApplyID != "" {
					fmt.Printf("To trigger cutover:  schemabot cutover --apply-id %s\n", active.ApplyID)
				} else {
					fmt.Printf("To trigger cutover:  schemabot cutover -d %s -e %s\n", cfg.Database, cmd.Environment)
				}
			}
			fmt.Printf("To watch and manage: %s\n", progressCmd)
			return fmt.Errorf("schema change already in progress")
		}
	}

	// Step 1: Generate plan
	opts := client.PlanOptions{Target: cfg.GetTarget(cmd.Environment), Deployment: cfg.Deployment}
	planResult, err := client.CallPlanAPI(ep, cfg.Database, cfg.Type, cmd.Environment, cfg.SchemaDir, cmd.Repository, cmd.PullRequest, opts)
	if err != nil {
		return err
	}

	// Validate engine-specific options
	if cmd.SkipRevert && planResult.Engine != "" && !state.IsPlanetScaleEngine(planResult.Engine) {
		return fmt.Errorf("--skip-revert is only supported for Vitess/PlanetScale databases")
	}
	if cmd.Branch != "" && planResult.Engine != "" && !state.IsPlanetScaleEngine(planResult.Engine) {
		return fmt.Errorf("--branch is only supported for Vitess/PlanetScale databases")
	}
	if err := validateBranchFlag(cmd.Branch); err != nil {
		return err
	}

	// Check for errors
	if len(planResult.Errors) > 0 {
		fmt.Println("Errors:")
		for _, e := range planResult.Errors {
			fmt.Printf("  - %s\n", e)
		}
		return fmt.Errorf("plan has errors")
	}

	// Check if there are any changes (DDL or VSchema)
	hasChanges := len(planResult.FlatTables()) > 0
	if !hasChanges {
		for _, sc := range planResult.Changes {
			if sc.Metadata["vschema"] != "" {
				hasChanges = true
				break
			}
		}
	}
	if !hasChanges {
		fmt.Println("No changes. Your schema is up-to-date.")
		return nil
	}

	// Check for unsafe changes
	if planResult.HasErrors() && !cmd.AllowUnsafe {
		return blockUnsafeApply(planResult, cfg.Database, cmd.Environment, cfg.SchemaDir)
	}

	// Check lock availability before showing plan (unless --force will break it anyway or --no-lock skips locking)
	if !cmd.NoLock && !cmd.Force {
		existingLock, err := client.GetLock(ep, cfg.Database, cfg.Type)
		if err != nil {
			return fmt.Errorf("check lock: %w", err)
		}
		if existingLock != nil && existingLock.Owner != owner {
			templates.WriteLockConflict(templates.LockConflictData{
				Database:     cfg.Database,
				DatabaseType: cfg.Type,
				Owner:        existingLock.Owner,
				Repository:   existingLock.Repository,
				PullRequest:  existingLock.PullRequest,
				CreatedAt:    existingLock.CreatedAt,
			})
			return fmt.Errorf("database is locked")
		}
	}

	// Step 2: Show the plan
	OutputPlanResult(planResult, cfg.Database, cmd.Environment, cfg.SchemaDir, true)

	// Show unsafe warning if --allow-unsafe was used
	if planResult.HasErrors() && cmd.AllowUnsafe {
		templates.WriteUnsafeWarningAllowed(planResult.UnsafeChanges())
	}

	// Show options if any flags are set
	templates.WriteOptions(cmd.DeferCutover, cmd.SkipRevert)

	// Step 3: Prompt for confirmation (unless auto-approve)
	if !cmd.AutoApprove {
		confirmed, err := confirmAction(
			"\nDo you want to apply these changes? Only 'yes' will be accepted: ",
			"\nApply cancelled.",
		)
		if err != nil {
			return err
		}
		if !confirmed {
			return nil
		}
	}

	// Step 4: Acquire lock and apply the plan

	if !cmd.NoLock {
		// If --force, break any existing lock first
		if cmd.Force {
			existingLock, err := client.GetLock(ep, cfg.Database, cfg.Type)
			if err != nil {
				return fmt.Errorf("check existing lock: %w", err)
			}
			if existingLock != nil && existingLock.Owner != owner {
				if err := client.ForceReleaseLock(ep, cfg.Database, cfg.Type); err != nil {
					return fmt.Errorf("force release lock: %w", err)
				}
				templates.WriteLockForceReleased(cfg.Database, cfg.Type, existingLock.Owner)
			}
		}

		// Acquire the lock
		_, err = client.AcquireLock(ep, cfg.Database, cfg.Type, owner, cmd.Repository, cmd.PullRequest)
		if errors.Is(err, client.ErrLockHeld) {
			// Lock is held by someone else - show conflict message
			existingLock, getErr := client.GetLock(ep, cfg.Database, cfg.Type)
			if getErr != nil || existingLock == nil {
				return fmt.Errorf("database is locked by another user")
			}
			templates.WriteLockConflict(templates.LockConflictData{
				Database:     cfg.Database,
				DatabaseType: cfg.Type,
				Owner:        existingLock.Owner,
				Repository:   existingLock.Repository,
				PullRequest:  existingLock.PullRequest,
				CreatedAt:    existingLock.CreatedAt,
			})
			return fmt.Errorf("database is locked")
		}
		if err != nil {
			return fmt.Errorf("acquire lock: %w", err)
		}
		templates.WriteLockAcquired(templates.LockData{
			Database:     cfg.Database,
			DatabaseType: cfg.Type,
			Owner:        owner,
		})
	}

	fmt.Println("\nApplying changes...")

	if _, err := applyAndWatch(ep, planResult, cfg.Database, cmd.Environment, owner, "apply", cmd.DeferCutover, cmd.DeferDeploy, cmd.SkipRevert, cmd.Branch, cmd.Watch, cmd.Output, cmd.LogHeartbeat, opts); err != nil {
		return err
	}

	// Yield lock if requested and apply was successful
	if cmd.Yield && !cmd.NoLock {
		if err := client.ReleaseLock(ep, cfg.Database, cfg.Type, owner); err != nil {
			fmt.Printf("Warning: failed to release lock: %v\n", err)
		} else {
			templates.WriteLockReleased(cfg.Database, cfg.Type)
		}
	}

	return nil
}

// OutputFormat specifies how progress output is rendered during apply watch.
//
// Three modes are available:
//
//   - interactive (default): Full-screen TUI with progress bars, spinners, and
//     keyboard controls (stop, volume, cutover). Requires a TTY. Provides a
//     rich, real-time view of progress. Best when a human is watching — local
//     development, production operations, etc.
//
//   - log: Structured logfmt lines (timestamp + key=value pairs).
//     Emits on state transitions (started, completed, failed, cutover,
//     stopped) and periodic heartbeats for tables copying rows (default
//     10s, configurable via --log-heartbeat). First heartbeat fires after
//     2s for quick feedback. Small/instant tables only get start and
//     complete lines. Each line includes apply_id and task_id for
//     correlation. Best for CI, server logs, and non-TTY environments.
//
//     Example:
//
//     2026-03-18T10:00:01Z apply_id=apply-abc msg="Table started" table=users ...
//     2026-03-18T10:00:03Z apply_id=apply-abc msg="Copying rows" table=orders progress=12% ...
//     2026-03-18T10:00:13Z apply_id=apply-abc msg="Copying rows" table=orders progress=45% ...
//     2026-03-18T10:01:30Z apply_id=apply-abc msg="Apply completed" duration=1m29s succeeded=3 failed=0
//
//   - json: One JSON object per poll. Includes full state for programmatic
//     consumption. Best for scripts and tooling that parse output.
type OutputFormat string

const (
	OutputFormatInteractive OutputFormat = "interactive"
	OutputFormatLog         OutputFormat = "log"
	OutputFormatJSON        OutputFormat = "json"
)

// WatchApplyProgressWithFormat polls the progress API by apply ID with the
// specified output format. logHeartbeat controls the interval between progress
// heartbeats in log mode (0 = default 10s).
func WatchApplyProgressWithFormat(endpoint, applyID string, allowCutoverPrompt bool, format OutputFormat, logHeartbeat time.Duration) error {
	// Use log format for CI/server environments
	if format == OutputFormatLog {
		if logHeartbeat <= 0 {
			logHeartbeat = logHeartbeatDefault
		}
		return watchApplyProgressLog(endpoint, applyID, logHeartbeat)
	}
	if format == OutputFormatJSON {
		return watchApplyProgressJSON(endpoint, applyID)
	}

	// Interactive format: use Bubbletea TUI
	return WatchApplyProgressByApplyID(endpoint, applyID, allowCutoverPrompt)
}

// WatchApplyProgressAfterCutover polls the progress API after cutover has been triggered.
// It waits for completion without showing the "waiting for cutover" instructions.
func WatchApplyProgressAfterCutover(endpoint, applyID string) error {
	maxTableNameLen := 0
	headerPrinted := false

	for {
		result, err := client.GetProgress(endpoint, applyID)
		if err != nil {
			return err
		}

		curState := result.State
		if curState == "" {
			return fmt.Errorf("unexpected response: missing state")
		}

		// Get tables for display
		tables := ddl.FilterInternalTablesTyped(result.Tables)
		if len(tables) > 0 && !headerPrinted {
			// Calculate max table name length for alignment
			for _, tbl := range tables {
				if len(tbl.TableName) > maxTableNameLen {
					maxTableNameLen = len(tbl.TableName)
				}
			}
			headerPrinted = true
		}

		// Check for terminal states
		if state.IsState(curState, state.Apply.Completed) {
			// Show green completion bar
			for _, tbl := range tables {
				bar := ui.ProgressBarComplete()
				fmt.Printf("%*s: %s ✓ Complete\n", maxTableNameLen, tbl.TableName, bar)
			}
			fmt.Printf("\n\n%s\n", templates.FormatApplyComplete())
			return nil
		}

		if state.IsState(curState, state.Apply.Failed) {
			if result.ErrorMessage != "" {
				return fmt.Errorf("cutover failed: %s", result.ErrorMessage)
			}
			return fmt.Errorf("cutover failed")
		}

		if state.IsState(curState, state.Apply.Stopped) {
			return fmt.Errorf("schema change was stopped during cutover")
		}

		// Still processing - just wait (don't show waiting instructions since cutover was already triggered)
		time.Sleep(2 * time.Second)
	}
}

// blockUnsafeApply displays the plan and an error when unsafe changes are detected without --allow-unsafe.
func blockUnsafeApply(planResult *apitypes.PlanResponse, database, environment, schemaDir string) error {
	// First show the plan so user can see what changes are proposed
	OutputPlanResult(planResult, database, environment, schemaDir, true)

	// Then show the unsafe changes warning
	unsafeChanges := planResult.UnsafeChanges()
	templates.WriteUnsafeChangesBlocked(unsafeChanges, database, environment, schemaDir)
	return ErrSilent
}

const (
	// logFirstHeartbeat is the delay before the first progress heartbeat for a table.
	// Shorter than the regular interval so operators see early confirmation of progress.
	logFirstHeartbeat = 2 * time.Second

	// logHeartbeatDefault is the default interval between progress heartbeats per active table.
	logHeartbeatDefault = 10 * time.Second
)

// tableLogState tracks the last-emitted state for a single table to detect changes.
type tableLogState struct {
	status         string    // last emitted status (normalized)
	lastEmit       time.Time // last time a line was emitted for this table
	announced      bool      // whether the "started" line was emitted
	startedAt      time.Time // when we first saw this table (for duration)
	taskID         string    // task ID for this table (for correlation)
	heartbeatCount int       // number of progress heartbeats emitted
}

// ANSI color codes for log output.
const (
	ansiReset  = "\033[0m"
	ansiBold   = "\033[1m"
	ansiDim    = "\033[2m"
	ansiRed    = "\033[31m"
	ansiGreen  = "\033[32m"
	ansiYellow = "\033[33m"
	ansiCyan   = "\033[36m"
)

// logEmitter writes structured logfmt lines with an apply_id prefix.
type logEmitter struct {
	applyID          string
	deployRequestURL string           // emitted once when first seen
	nowFunc          func() time.Time // override for testing/preview; defaults to time.Now().UTC()
}

// msgColor returns the ANSI color for a log message based on its content.
func msgColor(msg string) string {
	switch {
	case strings.Contains(msg, "complete"), strings.Contains(msg, "completed"):
		return ansiGreen
	case strings.Contains(msg, "failed"), strings.Contains(msg, "stopped"):
		return ansiRed
	case strings.Contains(msg, "started"), strings.Contains(msg, "Cutting"):
		return ansiYellow
	default:
		return ""
	}
}

// kvColor returns the ANSI color for a key-value pair based on the key name.
func kvColor(key string) string {
	switch key {
	case "table", "keyspace":
		return ansiCyan
	case "progress", "rows", "eta", "duration":
		return ansiBold
	case "status":
		return ansiYellow
	case "error":
		return ansiRed
	default:
		return ansiDim
	}
}

// emit writes a colored structured log line: timestamp apply_id=... key=value...
func (e *logEmitter) emit(kvs ...string) {
	now := time.Now().UTC()
	if e.nowFunc != nil {
		now = e.nowFunc()
	}
	ts := now.Format("15:04:05")
	var line []byte
	line = append(line, ansiDim...)
	line = append(line, ts...)
	line = append(line, ansiReset...)

	for i := 0; i+1 < len(kvs); i += 2 {
		key, val := kvs[i], kvs[i+1]
		// Skip noisy fields that aren't useful for human readers
		if key == "task_id" {
			continue
		}
		// apply_id is emitted once at start, not on every line
		if key == "apply_id" && e.applyID != "" {
			continue
		}
		line = append(line, ' ')
		if key == "msg" {
			// Message is rendered as just the value, with color
			c := msgColor(val)
			if c != "" {
				line = append(line, c...)
			}
			line = append(line, val...)
			if c != "" {
				line = append(line, ansiReset...)
			}
			continue
		}
		c := kvColor(key)
		line = append(line, c...)
		line = append(line, key...)
		line = append(line, '=')
		if logfmtNeedsQuoting(val) {
			line = append(line, '"')
			line = logfmtEscape(line, val)
			line = append(line, '"')
		} else {
			line = append(line, val...)
		}
		line = append(line, ansiReset...)
	}
	fmt.Println(string(line))
}

// logfmtNeedsQuoting returns true if the value needs quoting in logfmt output.
func logfmtNeedsQuoting(s string) bool {
	if s == "" {
		return true
	}
	for _, c := range s {
		if c == ' ' || c == '"' || c == '=' || c == '\\' || c < 0x20 {
			return true
		}
	}
	return false
}

// collapseDDL collapses a multi-line DDL statement into a single line for log output.
func collapseDDL(ddl string) string {
	var b strings.Builder
	b.Grow(len(ddl))
	prevSpace := false
	for _, c := range ddl {
		if c == '\n' || c == '\r' || c == '\t' {
			c = ' '
		}
		if c == ' ' && prevSpace {
			continue
		}
		prevSpace = c == ' '
		b.WriteRune(c)
	}
	return strings.TrimSpace(b.String())
}

// logfmtEscape appends val to b, escaping backslashes, quotes, and control characters.
func logfmtEscape(b []byte, val string) []byte {
	for i := range len(val) {
		c := val[i]
		switch c {
		case '\\':
			b = append(b, '\\', '\\')
		case '"':
			b = append(b, '\\', '"')
		case '\n':
			b = append(b, '\\', 'n')
		case '\r':
			b = append(b, '\\', 'r')
		case '\t':
			b = append(b, '\\', 't')
		default:
			b = append(b, c)
		}
	}
	return b
}

// watchApplyProgressLog outputs structured, event-based log lines.
//
// Design:
//   - State transitions (started, completed, failed, cutover, stopped) are emitted immediately.
//   - Progress heartbeats fire after 2s (first) then every --log-heartbeat interval (default 10s), only during row copy.
//   - Small/instant tables only get start + complete lines — no progress noise.
//   - A summary line is emitted on terminal states.
func watchApplyProgressLog(endpoint, applyID string, heartbeatInterval time.Duration) error {
	log := &logEmitter{}
	tableStates := make(map[string]*tableLogState)
	var lastGlobalState string
	var lastSummary string
	var applyStart time.Time
	var revertWindowStart time.Time
	var lastRevertHeartbeat time.Time
	pollInterval := 500 * time.Millisecond

	for {
		result, err := client.GetProgress(endpoint, applyID)
		if err != nil {
			return err
		}

		curState := result.State

		// Capture apply ID and start time from first response
		if log.applyID == "" && result.ApplyID != "" {
			// Emit once before setting — subsequent lines filter apply_id
			log.emit("msg", "Apply started", "apply_id", result.ApplyID)
			log.applyID = result.ApplyID
		}
		// Emit deploy request URL once when it first appears
		if log.deployRequestURL == "" {
			if url := result.Metadata["deploy_request_url"]; url != "" {
				log.deployRequestURL = url
				log.emit("msg", "Deploy request", "url", url)
			}
		}
		if applyStart.IsZero() {
			if result.StartedAt != "" {
				if t, err := time.Parse(time.RFC3339, result.StartedAt); err == nil {
					applyStart = t
				}
			}
			if applyStart.IsZero() {
				applyStart = time.Now()
			}
		}

		if state.IsState(curState, state.NoActiveChange) {
			// The background poller may not have updated task states yet.
			// Keep polling unless we've already seen a terminal state.
			if !state.IsTerminalApplyState(lastGlobalState) {
				time.Sleep(pollInterval)
				continue
			}
			log.emit("msg", "No active schema change")
			return nil
		}

		tables := ddl.FilterInternalTablesTyped(result.Tables)

		// Emit per-table events
		for _, tbl := range tables {
			ts, ok := tableStates[tbl.TableName]
			if !ok {
				ts = &tableLogState{startedAt: time.Now()}
				tableStates[tbl.TableName] = ts
			}

			tblStatus := state.NormalizeState(tbl.Status)

			// Announce table on first sight
			if !ts.announced {
				ts.announced = true
				ts.taskID = tbl.TaskID
				ts.lastEmit = time.Now()
				msg := "Table started"
				if isVSchemaTask(tbl) {
					msg = "VSchema update started"
				}
				kvs := tableKVs(msg, tbl, ts)
				if tbl.DDL != "" {
					kvs = append(kvs, "ddl", collapseDDL(tbl.DDL))
				}
				log.emit(kvs...)

				// If already in a terminal state (instant DDL), emit completion immediately
				if !isActiveStatus(tblStatus) {
					ts.status = tblStatus
					log.emitTableStateChange(tbl, tblStatus, ts)
				} else {
					ts.status = tblStatus
				}
				continue
			}

			// Detect state transitions
			if tblStatus != ts.status {
				ts.status = tblStatus
				ts.lastEmit = time.Now()
				log.emitTableStateChange(tbl, tblStatus, ts)
				continue
			}

			// Time-based heartbeat during active execution.
			// With row data: shows rows copied / total. Without: just status confirmation.
			// First heartbeat fires quickly (2s); subsequent at --log-heartbeat interval.
			if tblStatus == state.Apply.Running {
				interval := heartbeatInterval
				if ts.heartbeatCount == 0 {
					interval = logFirstHeartbeat
				}
				if time.Since(ts.lastEmit) >= interval {
					ts.lastEmit = time.Now()
					ts.heartbeatCount++
					if tbl.RowsTotal > 0 {
						log.emitProgressHeartbeat(tbl, ts)
					} else {
						kvs := append(tableKVs("Table running", tbl, ts), "status", "in progress")
						kvs = appendShardSummary(kvs, tbl.Shards)
						log.emit(kvs...)
					}
				}
			}
		}

		// Emit engine status messages (e.g., "Preparing branch sb-...")
		if result.Summary != "" && result.Summary != lastSummary {
			lastSummary = result.Summary
			log.emit("msg", result.Summary)
		}

		// Detect global state transitions
		globalNorm := state.NormalizeState(curState)

		if globalNorm != lastGlobalState {
			// Emit global state changes that aren't covered by per-table events
			switch {
			case state.IsState(curState, state.Apply.WaitingForDeploy) && lastGlobalState != "":
				log.emit("msg", "Deploy request ready — waiting for deploy")
			case state.IsState(curState, state.Apply.WaitingForCutover) && lastGlobalState != "":
				log.emit("msg", "Waiting for cutover")
			case state.IsState(curState, state.Apply.CuttingOver):
				log.emit("msg", "Cutting over")
			case state.IsState(curState, state.Apply.RevertWindow):
				revertWindowStart = time.Now()
				lastRevertHeartbeat = time.Now()
				log.emit("msg", "Revert window open — run 'schemabot revert' to undo or 'schemabot skip-revert' to finalize")
			}
			lastGlobalState = globalNorm
		}

		// Revert window heartbeat — emit elapsed time every heartbeat interval
		if state.IsState(curState, state.Apply.RevertWindow) && !revertWindowStart.IsZero() {
			if time.Since(lastRevertHeartbeat) >= heartbeatInterval {
				lastRevertHeartbeat = time.Now()
				elapsed := ui.FormatHumanDuration(time.Since(revertWindowStart))
				if result.Metadata["revert_skipped"] == "true" {
					log.emit("msg", "Finalizing", "elapsed", elapsed)
				} else {
					log.emit("msg", "Revert window open", "elapsed", elapsed)
				}
			}
		}

		// Terminal states — emit summary and exit
		if state.IsState(curState, state.Apply.Completed) {
			log.emitApplySummary("completed", tableStates, applyStart, "")
			return nil
		}
		if state.IsState(curState, state.Apply.Failed) {
			log.emitApplySummary("failed", tableStates, applyStart, result.ErrorMessage)
			return ErrSilent
		}
		if state.IsState(curState, state.Apply.Stopped) {
			log.emitApplySummary("stopped", tableStates, applyStart, "")
			return nil
		}

		time.Sleep(pollInterval)
		// Ramp up to 5s over the first few polls to avoid hammering the API on long schema changes
		if pollInterval < 5*time.Second {
			pollInterval *= 2
			if pollInterval > 5*time.Second {
				pollInterval = 5 * time.Second
			}
		}
	}
}

// tableKVs returns the common key-value pairs for a table log line (table name + task_id if known).
func tableKVs(msg string, tbl *apitypes.TableProgressResponse, ts *tableLogState) []string {
	kvs := []string{"msg", msg}
	if isVSchemaTask(tbl) {
		// VSchema tasks use keyspace as the identifier, not "table"
		kvs = append(kvs, "keyspace", tbl.Keyspace)
	} else {
		kvs = append(kvs, "table", tbl.TableName)
	}
	taskID := ts.taskID
	if taskID == "" {
		taskID = tbl.TaskID
	}
	if taskID != "" {
		kvs = append(kvs, "task_id", taskID)
	}
	if !isVSchemaTask(tbl) && tbl.Keyspace != "" {
		kvs = append(kvs, "keyspace", tbl.Keyspace)
	}
	return kvs
}

// appendShardSummary appends shard progress info to a log line's key-value pairs.
// For small shard counts, lists each shard. For large counts, shows a summary.
func appendShardSummary(kvs []string, shards []*apitypes.ShardProgressResponse) []string {
	if len(shards) == 0 {
		return kvs
	}

	kvs = append(kvs, "shards", fmt.Sprintf("%d", len(shards)))

	// Summarize by status
	counts := make(map[string]int)
	for _, s := range shards {
		counts[s.Status]++
	}

	var parts []string
	for status, count := range counts {
		parts = append(parts, fmt.Sprintf("%s=%d", status, count))
	}
	sort.Strings(parts)
	kvs = append(kvs, "shard_status", strings.Join(parts, ","))

	return kvs
}

// isVSchemaTask returns true if this is a synthetic VSchema update task.
func isVSchemaTask(tbl *apitypes.TableProgressResponse) bool {
	return strings.HasPrefix(tbl.TableName, "vschema:")
}

// emitTableStateChange emits a log line for a table state transition.
func (e *logEmitter) emitTableStateChange(tbl *apitypes.TableProgressResponse, tblStatus string, ts *tableLogState) {
	dur := ui.FormatHumanDuration(time.Since(ts.startedAt))
	vs := isVSchemaTask(tbl)

	switch tblStatus {
	case state.Apply.Completed:
		msg := "Table complete"
		if vs {
			msg = "VSchema update complete"
		}
		kvs := tableKVs(msg, tbl, ts)
		kvs = append(kvs, "duration", dur)
		kvs = appendShardSummary(kvs, tbl.Shards)
		e.emit(kvs...)
	case state.Apply.RevertWindow:
		msg := "Table deployed — revert window open"
		if vs {
			msg = "VSchema deployed — revert window open"
		}
		kvs := tableKVs(msg, tbl, ts)
		kvs = append(kvs, "duration", dur)
		kvs = appendShardSummary(kvs, tbl.Shards)
		e.emit(kvs...)
	case state.Apply.Failed:
		msg := "Table failed"
		if vs {
			msg = "VSchema update failed"
		}
		kvs := tableKVs(msg, tbl, ts)
		e.emit(append(kvs, "duration", dur)...)
	case state.Apply.WaitingForCutover:
		kvs := tableKVs("Waiting for cutover", tbl, ts)
		kvs = appendShardSummary(kvs, tbl.Shards)
		e.emit(kvs...)
	case state.Apply.CuttingOver:
		kvs := tableKVs("Cutting over", tbl, ts)
		kvs = appendShardSummary(kvs, tbl.Shards)
		e.emit(kvs...)
	case state.Apply.Stopped:
		kvs := tableKVs("Table stopped", tbl, ts)
		if tbl.PercentComplete > 0 {
			kvs = append(kvs, "progress", fmt.Sprintf("%d%%", min(int(tbl.PercentComplete), 100)))
		}
		e.emit(kvs...)
	default:
		kvs := tableKVs("Table status changed", tbl, ts)
		kvs = append(kvs, "status", tblStatus)
		kvs = appendShardSummary(kvs, tbl.Shards)
		e.emit(kvs...)
	}
}

// emitProgressHeartbeat emits a progress line for a table actively copying rows.
func (e *logEmitter) emitProgressHeartbeat(tbl *apitypes.TableProgressResponse, ts *tableLogState) {
	pct := min(int(tbl.PercentComplete), 100)
	kvs := tableKVs("Copying rows", tbl, ts)
	kvs = append(kvs,
		"progress", fmt.Sprintf("%d%%", pct),
		"rows", fmt.Sprintf("%s/%s", ui.FormatNumber(ui.ClampRows(tbl.RowsCopied, tbl.RowsTotal)), ui.FormatNumber(tbl.RowsTotal)),
	)

	// Try to extract ETA from Spirit progress detail
	if tbl.ProgressDetail != "" {
		if info := templates.ParseSpiritProgress(tbl.ProgressDetail); info != nil && info.ETA != "" && info.ETA != "TBD" {
			kvs = append(kvs, "eta", info.ETA)
		}
	} else if tbl.ETASeconds > 0 {
		kvs = append(kvs, "eta", ui.FormatETA(tbl.ETASeconds))
	}

	kvs = appendShardSummary(kvs, tbl.Shards)
	e.emit(kvs...)
}

// emitApplySummary emits the final summary line when an apply reaches a terminal state.
func (e *logEmitter) emitApplySummary(outcome string, tableStates map[string]*tableLogState, applyStart time.Time, errorMsg string) {
	var succeeded, failed, stopped int
	for _, ts := range tableStates {
		switch ts.status {
		case state.Apply.Completed:
			succeeded++
		case state.Apply.Failed:
			failed++
		case state.Apply.Stopped:
			stopped++
		}
	}

	dur := ui.FormatHumanDuration(time.Since(applyStart))

	total := succeeded + failed + stopped
	kvs := []string{
		"msg", "Apply " + outcome,
		"duration", dur,
		"tables", fmt.Sprintf("%d/%d succeeded", succeeded, total),
	}
	if failed > 0 {
		kvs = append(kvs, "failed", fmt.Sprintf("%d", failed))
	}
	if stopped > 0 {
		kvs = append(kvs, "stopped", fmt.Sprintf("%d", stopped))
	}
	if errorMsg != "" {
		kvs = append(kvs, "error", errorMsg)
	}

	e.emit(kvs...)
}

// isActiveStatus returns true if the table status represents an active (non-terminal) state.
func isActiveStatus(status string) bool {
	switch status {
	case state.Apply.Completed, state.Apply.Failed, state.Apply.Stopped, state.Apply.Reverted, state.Apply.RevertWindow:
		return false
	default:
		return true
	}
}

// watchApplyProgressJSON outputs JSON lines for programmatic consumption.
func watchApplyProgressJSON(endpoint, applyID string) error {
	for {
		result, err := client.GetProgress(endpoint, applyID)
		if err != nil {
			return err
		}

		curState := result.State

		// Output JSON on each poll
		line := map[string]string{"state": curState}
		if result.ErrorMessage != "" {
			line["error"] = result.ErrorMessage
		}
		if result.Summary != "" {
			line["summary"] = result.Summary
		}
		if err := json.NewEncoder(os.Stdout).Encode(line); err != nil {
			return err
		}

		// Check for terminal states
		if state.IsState(curState, state.Apply.Completed) {
			return nil
		}
		if state.IsState(curState, state.Apply.Failed) {
			return ErrSilent
		}
		if state.IsState(curState, state.Apply.Stopped) {
			return nil
		}
		if state.IsState(curState, state.NoActiveChange) {
			return nil
		}

		time.Sleep(2 * time.Second)
	}
}

// validateBranchFlag checks that the --branch flag value is safe to use.
func validateBranchFlag(branch string) error {
	if branch == "main" {
		return fmt.Errorf("cannot reuse the main branch — use a development branch")
	}
	return nil
}
