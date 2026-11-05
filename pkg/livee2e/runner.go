package livee2e

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"path"
	"strings"
	"time"
)

// ErrInconclusive reports a run that proved nothing either way, such as a
// copy that finished before a stop could land. A gate retries it rather than
// failing the release.
var ErrInconclusive = errors.New("inconclusive")

// BranchPrefix starts every branch the runner creates. The sweep closes open
// pull requests from these branches, and only these.
const BranchPrefix = "schemabot-e2e/"

// Config describes the deployment and test database a run drives.
type Config struct {
	// Repo is the test repository, in owner/name form.
	Repo string
	// BaseBranch is the branch test pull requests target.
	BaseBranch string
	// TableDir is the repository directory that holds the test database's
	// table files; the run adds its table there.
	TableDir string
	// Database is the test database SchemaBot manages.
	Database string
	// Environment is the SchemaBot environment the run applies to.
	Environment string
	// CheckName is the Check Run SchemaBot posts for Environment.
	CheckName string
	// AppSlug is the slug of SchemaBot's GitHub App. The runner reads only
	// that App's comments and Check Runs, so another App's comment or
	// same-named Check Run cannot pass a step.
	AppSlug string
	// CommandPrefix starts every PR command.
	CommandPrefix string
	// SeedTable is an existing table in Database to copy rows from. Its rows
	// make the run's copy long enough to stop and cancel part way.
	SeedTable string
	// SeedRows is how many rows the run copies from SeedTable.
	SeedRows int64
	// PollInterval is how often the runner reads the pull request.
	PollInterval time.Duration
	// WaitTimeout bounds each wait for SchemaBot to act on a command, and
	// the cleanup after a run. The wait for the deferred apply to reach
	// Waiting for Cutover runs the whole copy and checksum of SeedRows rows,
	// so WaitTimeout must cover that copy, and RunTimeout several waits:
	// raise both with SeedRows.
	WaitTimeout time.Duration
	// RunTimeout bounds a whole run, database calls included. A run is over
	// once RunTimeout and then WaitTimeout have passed since it started. The
	// run writes that time into its name, so a sweep with other timeouts
	// still waits for it.
	RunTimeout time.Duration
}

func (c Config) validate() error {
	var missing []string
	for name, v := range map[string]string{
		"repository":     c.Repo,
		"base branch":    c.BaseBranch,
		"table dir":      c.TableDir,
		"database":       c.Database,
		"environment":    c.Environment,
		"check name":     c.CheckName,
		"app slug":       c.AppSlug,
		"command prefix": c.CommandPrefix,
		"seed table":     c.SeedTable,
	} {
		if v == "" {
			missing = append(missing, name)
		}
	}
	if len(missing) > 0 {
		return fmt.Errorf("missing configuration: %s", strings.Join(missing, ", "))
	}
	if c.SeedRows <= 0 {
		return errors.New("seed rows must be positive")
	}
	if c.PollInterval <= 0 || c.WaitTimeout <= 0 || c.RunTimeout <= 0 {
		return errors.New("poll interval, wait timeout, and run timeout must be positive")
	}
	return nil
}

// Runner runs one live end-to-end test.
type Runner struct {
	cfg    Config
	gh     GitHub
	target Target
	logger *slog.Logger
	now    func() time.Time
}

// NewRunner returns a runner for cfg.
func NewRunner(cfg Config, gh GitHub, target Target, logger *slog.Logger) (*Runner, error) {
	if err := cfg.validate(); err != nil {
		return nil, err
	}
	return &Runner{cfg: cfg, gh: gh, target: target, logger: logger, now: time.Now}, nil
}

// A run names its table runTablePrefix plus the time it is over in
// runTimeLayout, UTC: its start plus RunTimeout and WaitTimeout. The sweep
// reads that time from the name, so it never reaps a run that is still
// within its own limits.
const (
	runTablePrefix = "e2e_"
	runTimeLayout  = "20060102150405"
)

// run is one test's names, and the applies it has started.
type run struct {
	table   string
	branch  string
	pr      int
	applies map[string]bool
	// ownsBranch is set once the run created its branch. Another run that
	// picked the same name owns everything under it, so a run that never
	// created its branch cleans up nothing.
	ownsBranch bool
}

// The test table's two schemas. The change between them rewrites every row,
// so the apply copies the table and has a window to stop and cancel in.
const (
	createTableDDL = "CREATE TABLE `%s` (\n" +
		"  `id` bigint unsigned NOT NULL AUTO_INCREMENT,\n" +
		"  `payload` varchar(255) NOT NULL,\n" +
		"  `created_at` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP,\n" +
		"  PRIMARY KEY (`id`)\n" +
		");\n"
	alteredTableDDL = "CREATE TABLE `%s` (\n" +
		"  `id` bigint unsigned NOT NULL AUTO_INCREMENT,\n" +
		"  `payload` varchar(255) NOT NULL,\n" +
		"  `created_at` datetime(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),\n" +
		"  PRIMARY KEY (`id`)\n" +
		");\n"
	columnBefore = "datetime"
	columnAfter  = "datetime(6)"
)

// Run sweeps up after earlier runs, then runs the test. It returns nil when
// every step passed, an error wrapping ErrInconclusive when the run could not
// decide, and any other error when a step failed. A GitHub rate limit or
// server error leaves the run undecided, so it wraps ErrInconclusive too. The
// run's pull request, branch, and table are removed whatever the outcome, and
// a cleanup that fails fails the run, even an inconclusive one: what it left
// behind may hold the lock, so a retry would only come back inconclusive.
//
// The sweep counts against RunTimeout, so the time in the run's name bounds
// the whole call. A sweep that fails does not stop the run: a leftover that
// still holds the lock makes the run inconclusive at its first apply, and a
// run that fails names the sweep's error too.
func (r *Runner) Run(ctx context.Context) (err error) {
	ctx, cancel := context.WithTimeout(ctx, r.cfg.RunTimeout)
	defer cancel()
	sweepErr := r.Sweep(ctx)
	if sweepErr != nil {
		r.logger.Warn("sweep before the run failed; running anyway", "repo", r.cfg.Repo, "error", sweepErr)
	}
	table := runTablePrefix + r.now().Add(r.cfg.RunTimeout+r.cfg.WaitTimeout).UTC().Format(runTimeLayout)
	cur := &run{table: table, branch: BranchPrefix + table, applies: map[string]bool{}}
	r.logger.Info("starting live e2e run", "repo", r.cfg.Repo, "database", r.cfg.Database,
		"environment", r.cfg.Environment, "table", cur.table, "branch", cur.branch)
	defer func() {
		cleanupCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), r.cfg.WaitTimeout)
		defer cancel()
		if cerr := r.cleanup(cleanupCtx, cur); cerr != nil {
			cleanupErr := fmt.Errorf("clean up run %s: %w", cur.table, cerr)
			if err == nil {
				err = cleanupErr
				return
			}
			// The steps' error keeps its message but not its chain, so an
			// inconclusive run whose cleanup failed no longer reads as
			// inconclusive.
			err = fmt.Errorf("%s; %w", err.Error(), cleanupErr)
		}
	}()
	// Classified before the cleanup error joins it, so a rate-limited cleanup
	// cannot turn a failed step into an inconclusive run.
	err = inconclusiveWhenGitHubUnavailable(r.steps(ctx, cur))
	if err != nil && sweepErr != nil {
		err = fmt.Errorf("%w (the sweep before the run also failed: %s)", err, sweepErr.Error())
	}
	return err
}

// inconclusiveWhenGitHubUnavailable wraps ErrInconclusive around err when
// GitHub, not SchemaBot, failed the call.
func inconclusiveWhenGitHubUnavailable(err error) error {
	if err == nil || errors.Is(err, ErrInconclusive) || !githubUnavailable(err) {
		return err
	}
	return fmt.Errorf("%w: %w", err, ErrInconclusive)
}

func (r *Runner) steps(ctx context.Context, cur *run) error {
	base, err := r.gh.BranchHead(ctx, r.cfg.BaseBranch)
	if err != nil {
		return err
	}
	if err := r.gh.CreateBranch(ctx, cur.branch, base); err != nil {
		if _, headErr := r.gh.BranchHead(ctx, cur.branch); headErr == nil {
			// Another run started in the same second and took the name.
			return fmt.Errorf("branch %s already exists: %w", cur.branch, ErrInconclusive)
		}
		return err
	}
	cur.ownsBranch = true
	file := path.Join(r.cfg.TableDir, cur.table+".sql")
	sha, err := r.gh.PutFile(ctx, cur.branch, file, "Create "+cur.table, fmt.Appendf(nil, createTableDDL, cur.table))
	if err != nil {
		return err
	}
	cur.pr, err = r.gh.OpenPullRequest(ctx, cur.branch, r.cfg.BaseBranch,
		"SchemaBot live e2e "+cur.table,
		"An automated SchemaBot end-to-end test. The runner closes this pull request and drops its table when it finishes.")
	if err != nil {
		return err
	}
	r.logger.Info("opened test pull request", "repo", r.cfg.Repo, "pr", cur.pr, "head_sha", sha)

	// 1. Create the table and fill it.
	if err := r.waitForPlan(ctx, sha); err != nil {
		return err
	}
	create, err := r.applyCommand(ctx, cur, "apply")
	if err != nil {
		return err
	}
	if err := r.waitForStatus(ctx, cur, create, StatusApplied, appliedFails, 0); err != nil {
		return err
	}
	if err := r.expectTable(ctx, cur.table); err != nil {
		return err
	}
	if err := r.target.Seed(ctx, cur.table, r.cfg.SeedTable, r.cfg.SeedRows); err != nil {
		return err
	}

	// 2. Start a copy, stop it, start it again, and cancel it.
	sha, err = r.gh.PutFile(ctx, cur.branch, file, "Alter "+cur.table, fmt.Appendf(nil, alteredTableDDL, cur.table))
	if err != nil {
		return err
	}
	if err := r.waitForPlan(ctx, sha); err != nil {
		return err
	}
	copyApply, err := r.applyCommand(ctx, cur, "apply")
	if err != nil {
		return err
	}
	if err := r.waitForStatus(ctx, cur, copyApply, StatusInProgress, appliedInconclusive, 0); err != nil {
		return err
	}
	if err := r.controlCommand(ctx, cur, "stop", copyApply, StatusStopped, appliedInconclusive); err != nil {
		return err
	}
	if err := r.expectColumn(ctx, cur.table, columnBefore); err != nil {
		return err
	}
	if err := r.controlCommand(ctx, cur, "start", copyApply, StatusInProgress, appliedInconclusive); err != nil {
		return err
	}
	if err := r.controlCommand(ctx, cur, "cancel", copyApply, StatusCancelled, appliedInconclusive); err != nil {
		return err
	}
	if err := r.expectColumn(ctx, cur.table, columnBefore); err != nil {
		return err
	}
	if err := r.expectNoLeftoverCopy(ctx, cur.table); err != nil {
		return err
	}

	// 3. Apply the same change with a deferred cutover, then run the cutover.
	deferred, err := r.applyCommand(ctx, cur, "apply", "--defer-cutover")
	if err != nil {
		return err
	}
	if err := r.waitForStatus(ctx, cur, deferred, StatusWaitingForCutover, appliedFails, 0); err != nil {
		return err
	}
	if err := r.expectColumn(ctx, cur.table, columnBefore); err != nil {
		return err
	}
	if err := r.controlCommand(ctx, cur, "cutover", deferred, StatusApplied, appliedFails); err != nil {
		return err
	}
	if err := r.expectColumn(ctx, cur.table, columnAfter); err != nil {
		return err
	}
	if err := r.expectRows(ctx, cur.table, r.cfg.SeedRows); err != nil {
		return err
	}

	// 4. The Check Run passes once the head commit's schema is applied.
	if err := r.waitForCheckSuccess(ctx, sha); err != nil {
		return err
	}
	r.logger.Info("live e2e run passed", "repo", r.cfg.Repo, "pr", cur.pr, "table", cur.table)
	return nil
}

// command posts one PR command and returns the comment's ID.
func (r *Runner) command(ctx context.Context, cur *run, verb string, args ...string) (int64, error) {
	parts := append([]string{r.cfg.CommandPrefix, verb}, args...)
	parts = append(parts, "-e", r.cfg.Environment)
	body := strings.Join(parts, " ")
	id, err := r.gh.CreateComment(ctx, cur.pr, body)
	if err != nil {
		return 0, err
	}
	r.logger.Info("posted command", "repo", r.cfg.Repo, "pr", cur.pr, "command", body, "comment_id", id)
	return id, nil
}

// applyCommand posts an apply command and returns the ID of the apply it
// started.
func (r *Runner) applyCommand(ctx context.Context, cur *run, verb string, args ...string) (string, error) {
	commentID, err := r.command(ctx, cur, verb, args...)
	if err != nil {
		return "", err
	}
	var started ApplyStatus
	var lastHeadline string
	err = r.poll(ctx, func(ctx context.Context) (bool, error) {
		comments, err := r.botComments(ctx, cur.pr)
		if err != nil {
			return false, err
		}
		lastHeadline = lastHeadlineAfter(comments, commentID)
		if blockedAfter(comments, commentID) {
			return false, fmt.Errorf("another pull request or CLI session holds the lock on %s: %w", r.cfg.Database, ErrInconclusive)
		}
		var ok bool
		started, ok = firstApplyAfter(withoutApplies(comments, cur.applies), commentID)
		if ok {
			return true, nil
		}
		if refusal, refused := refusalAfter(comments, commentID); refused {
			if strings.Contains(refusal, pendingHeadline) {
				// Another check, or an earlier environment, is still running:
				// a matter of timing, not of SchemaBot.
				return false, fmt.Errorf("SchemaBot held %s until other work finishes (%q): %w", verb, refusal, ErrInconclusive)
			}
			return false, fmt.Errorf("SchemaBot refused %s: %q", verb, refusal)
		}
		r.logger.Debug("no apply started yet", "pr", cur.pr, "command_id", commentID)
		return false, nil
	})
	if err != nil {
		return "", fmt.Errorf("wait for %s on %s#%d to start an apply (last reply %q): %w", verb, r.cfg.Repo, cur.pr, lastHeadline, err)
	}
	cur.applies[started.ApplyID] = true
	r.logger.Info("apply started", "repo", r.cfg.Repo, "pr", cur.pr, "apply_id", started.ApplyID, "status", started.Status)
	return started.ApplyID, nil
}

// controlCommand posts a control command for applyID and waits for want.
func (r *Runner) controlCommand(ctx context.Context, cur *run, verb, applyID, want string, applied onApplied) error {
	commentID, err := r.command(ctx, cur, verb, applyID)
	if err != nil {
		return err
	}
	return r.waitForStatus(ctx, cur, applyID, want, applied, commentID)
}

// onApplied says what an apply that finishes before reaching the awaited
// status means.
type onApplied int

const (
	// appliedInconclusive: the copy outran the command, which proves nothing
	// about the command either way.
	appliedInconclusive onApplied = iota
	// appliedFails: the apply should never have finished on its own, such as
	// a deferred cutover that ran without the cutover command.
	appliedFails
)

// refusedStatuses maps the state SchemaBot names when it refuses a command for
// a finished apply to the status label that state renders as.
var refusedStatuses = map[string]string{
	"completed": StatusApplied,
	"failed":    StatusFailed,
	"cancelled": StatusCancelled,
	"reverted":  StatusReverted,
}

// waitForStatus waits until applyID's comment shows want. Any other terminal
// status fails the run; Applied is judged by applied. A nonzero command is the
// control command's comment: SchemaBot's refusal of it because the apply had
// already finished counts as that final status.
func (r *Runner) waitForStatus(ctx context.Context, cur *run, applyID, want string, applied onApplied, command int64) error {
	var last string
	err := r.poll(ctx, func(ctx context.Context) (bool, error) {
		comments, err := r.botComments(ctx, cur.pr)
		if err != nil {
			return false, err
		}
		s, ok := latestApplyStatus(comments, applyID)
		if final, finished := finishedStatus(comments, applyID); finished {
			// A summary can land while the status comment still shows an
			// earlier state.
			s, ok = ApplyStatus{ApplyID: applyID, Status: final}, true
		}
		if command != 0 {
			// The copy can finish before the command lands, and the refusal
			// can arrive while the status comment still shows an earlier state.
			if finished, refused := refusedAsFinished(comments, command); refused {
				label, known := refusedStatuses[finished]
				if !known {
					return false, fmt.Errorf("SchemaBot refused the command for apply %s, which finished in unknown state %q", applyID, finished)
				}
				s, ok = ApplyStatus{ApplyID: applyID, Status: label}, true
			}
		}
		if !ok {
			r.logger.Debug("no status comment yet", "pr", cur.pr, "apply_id", applyID)
			return false, nil
		}
		if s.Status != last {
			r.logger.Info("apply status", "pr", cur.pr, "apply_id", applyID, "status", s.Status, "want", want)
			last = s.Status
		}
		switch {
		case s.Status == want:
			return true, nil
		case s.Status == StatusApplied && applied == appliedInconclusive:
			return false, fmt.Errorf("apply %s finished before it reached %s: %w", applyID, want, ErrInconclusive)
		case isTerminalStatus(s.Status):
			return false, fmt.Errorf("apply %s ended %s while waiting for %s", applyID, s.Status, want)
		}
		return false, nil
	})
	if err != nil {
		return fmt.Errorf("wait for apply %s on %s#%d to reach %s (last status %q): %w", applyID, r.cfg.Repo, cur.pr, want, last, err)
	}
	return nil
}

// waitForPlan waits for SchemaBot to finish planning sha, so the next apply
// command runs against that commit rather than an earlier one.
func (r *Runner) waitForPlan(ctx context.Context, sha string) error {
	err := r.poll(ctx, func(ctx context.Context) (bool, error) {
		check, err := r.gh.CheckRun(ctx, sha, r.cfg.CheckName, r.cfg.AppSlug)
		if err != nil {
			return false, err
		}
		if check == nil || check.Status != "completed" {
			r.logger.Debug("plan not finished yet", "sha", sha, "check", r.cfg.CheckName)
			return false, nil
		}
		if check.Conclusion == "failure" {
			return false, fmt.Errorf("SchemaBot's plan failed: %q", check.Title)
		}
		return true, nil
	})
	if err != nil {
		return fmt.Errorf("wait for check %q on %s: %w", r.cfg.CheckName, sha, err)
	}
	return nil
}

func (r *Runner) waitForCheckSuccess(ctx context.Context, sha string) error {
	var last CheckRun
	err := r.poll(ctx, func(ctx context.Context) (bool, error) {
		check, err := r.gh.CheckRun(ctx, sha, r.cfg.CheckName, r.cfg.AppSlug)
		if err != nil {
			return false, err
		}
		if check == nil {
			r.logger.Debug("no check run yet", "sha", sha, "check", r.cfg.CheckName)
			return false, nil
		}
		last = *check
		return check.Status == "completed" && check.Conclusion == "success", nil
	})
	if err != nil {
		return fmt.Errorf("wait for check %q on %s to pass (last %s/%s %q): %w",
			r.cfg.CheckName, sha, last.Status, last.Conclusion, last.Title, err)
	}
	return nil
}

func (r *Runner) expectTable(ctx context.Context, table string) error {
	exists, err := r.target.TableExists(ctx, table)
	if err != nil {
		return err
	}
	if !exists {
		return fmt.Errorf("SchemaBot reported %s applied, but %s.%s does not exist", table, r.cfg.Database, table)
	}
	return nil
}

func (r *Runner) expectColumn(ctx context.Context, table, want string) error {
	got, err := r.target.ColumnType(ctx, table, "created_at")
	if err != nil {
		return err
	}
	if got != want {
		return fmt.Errorf("%s.%s.created_at is %s; expected %s", r.cfg.Database, table, got, want)
	}
	return nil
}

func (r *Runner) expectRows(ctx context.Context, table string, want int64) error {
	got, err := r.target.RowCount(ctx, table)
	if err != nil {
		return err
	}
	if got != want {
		return fmt.Errorf("%s.%s has %d rows after the cutover; expected %d", r.cfg.Database, table, got, want)
	}
	return nil
}

// expectNoLeftoverCopy waits for a cancelled copy's tables to be removed.
func (r *Runner) expectNoLeftoverCopy(ctx context.Context, table string) error {
	var leftover []string
	err := r.poll(ctx, func(ctx context.Context) (bool, error) {
		var err error
		leftover, err = r.target.LeftoverCopyTables(ctx, table)
		return len(leftover) == 0, err
	})
	if err != nil {
		return fmt.Errorf("cancelled copy of %s.%s left %v behind: %w", r.cfg.Database, table, leftover, err)
	}
	return nil
}

// Sweep removes what earlier runs left behind: it cancels their unfinished
// applies, closes their pull requests and deletes their branches, then drops
// their tables. It reaps only runs that are over (see Config.RunTimeout), so
// it never touches a run another runner is still driving. It drops no table
// while its run's pull request stays open, or while a closed pull request
// for it shows an apply that may not have finished.
func (r *Runner) Sweep(ctx context.Context) error {
	prs, err := r.gh.OpenPullRequests(ctx, BranchPrefix)
	if err != nil {
		return err
	}
	var errs []error
	kept := map[string]bool{}
	closed := map[string]bool{}
	for _, pr := range prs {
		table := strings.TrimPrefix(pr.Branch, BranchPrefix)
		if !r.over(table) {
			r.logger.Info("leaving a run that may still be in progress", "repo", r.cfg.Repo, "pr", pr.Number, "branch", pr.Branch)
			kept[table] = true
			continue
		}
		r.logger.Info("sweeping earlier run", "repo", r.cfg.Repo, "pr", pr.Number, "branch", pr.Branch)
		if err := r.closeRun(ctx, &run{table: table, branch: pr.Branch, pr: pr.Number}); err != nil {
			errs = append(errs, fmt.Errorf("leaving table %s in place: %w", table, err))
			kept[table] = true
			continue
		}
		closed[table] = true
	}
	tables, err := r.target.RunTables(ctx)
	if err != nil {
		return errors.Join(append(errs, err)...)
	}
	for _, table := range tables {
		if kept[table] || !r.over(table) {
			continue
		}
		if !closed[table] {
			// Closing a pull request leaves an unfinished apply running and
			// holding its lock, so a closed run's table stays until its
			// applies are proven finished.
			if err := r.closedRunsFinished(ctx, BranchPrefix+table); err != nil {
				errs = append(errs, fmt.Errorf("leaving table %s in place: %w", table, err))
				continue
			}
		}
		if err := r.target.DropTable(ctx, table); err != nil {
			errs = append(errs, err)
		}
	}
	return errors.Join(errs...)
}

// sweepGrace is how long after the time in a run's name the sweep still
// leaves it alone, so a sweeper whose clock runs ahead of the run's host
// cannot reach a cleanup still finishing.
const sweepGrace = time.Minute

// over reports whether the run that named its table table has ended: the time
// in its name, and then sweepGrace, have passed. A name that does not parse is
// not a run's, so it never counts as over.
func (r *Runner) over(table string) bool {
	end, err := time.Parse(runTimeLayout, strings.TrimPrefix(table, runTablePrefix))
	if err != nil || !strings.HasPrefix(table, runTablePrefix) {
		return false
	}
	return r.now().After(end.Add(sweepGrace))
}

// closedRunsFinished returns an error unless every pull request from branch
// shows each of its applies finished. The sweep cannot cancel through a
// closed pull request, so it leaves the table for someone to check by hand.
func (r *Runner) closedRunsFinished(ctx context.Context, branch string) error {
	prs, err := r.gh.PullRequestsForBranch(ctx, branch)
	if err != nil {
		return err
	}
	if len(prs) == 0 {
		// Only a pull request's apply creates a run's table, so finding none
		// means the lookup missed it, not that nothing ran.
		return fmt.Errorf("found no pull request from branch %s in %s, so its applies cannot be proven finished; check it by hand", branch, r.cfg.Repo)
	}
	for _, pr := range prs {
		all, err := r.gh.Comments(ctx, pr.Number)
		if err != nil {
			return err
		}
		if unansweredApply(all, r.isBot, r.cfg.CommandPrefix) {
			return fmt.Errorf("closed pull request %s#%d has an apply command with no reply from SchemaBot, so an apply may be running; check it by hand", r.cfg.Repo, pr.Number)
		}
		for _, applyID := range inFlightApplies(r.fromBot(all)) {
			if !r.cancelRefusedAsFinished(all, applyID) {
				return fmt.Errorf("closed pull request %s#%d shows apply %s unfinished; cancel it by hand", r.cfg.Repo, pr.Number, applyID)
			}
		}
	}
	return nil
}

// cancelRefusedAsFinished reports whether SchemaBot refused a cancel of
// applyID because the apply had already finished, as closeRun accepts. Only
// the first SchemaBot reply after the cancel counts, so a refusal of a later
// command for another apply cannot pass for it.
func (r *Runner) cancelRefusedAsFinished(all []Comment, applyID string) bool {
	bot := r.fromBot(all)
	for _, c := range all {
		fields := strings.Fields(c.Body)
		if r.isBot(c) || len(fields) < 3 || fields[0] != r.cfg.CommandPrefix || fields[1] != "cancel" || fields[2] != applyID {
			continue
		}
		reply, ok := firstCommentAfter(bot, c.ID)
		if !ok {
			continue
		}
		if _, refused := refusedAsFinished([]Comment{reply}, c.ID); refused {
			return true
		}
	}
	return false
}

// firstCommentAfter returns the earliest comment posted after the comment
// with ID after.
func firstCommentAfter(comments []Comment, after int64) (Comment, bool) {
	var first Comment
	for _, c := range comments {
		if c.ID > after && (first.ID == 0 || c.ID < first.ID) {
			first = c
		}
	}
	return first, first.ID != 0
}

// cleanup removes one run's pull request, branch, and table.
func (r *Runner) cleanup(ctx context.Context, cur *run) error {
	if !cur.ownsBranch {
		r.logger.Info("run created no branch; leaving its names to the run that owns them", "branch", cur.branch)
		return nil
	}
	if cur.pr == 0 {
		r.logger.Info("run opened no pull request; deleting its branch", "branch", cur.branch)
		if err := r.gh.DeleteBranch(ctx, cur.branch); err != nil {
			return err
		}
	} else if err := r.closeRun(ctx, cur); err != nil {
		return fmt.Errorf("leaving table %s in place for the next sweep: %w", cur.table, err)
	}
	return r.target.DropTable(ctx, cur.table)
}

// closeRun cancels a run's unfinished applies, then closes its pull request
// and deletes its branch. Closing a pull request releases its database lock
// only once no apply is in flight, so the cancels come first.
func (r *Runner) closeRun(ctx context.Context, cur *run) error {
	all, err := r.gh.Comments(ctx, cur.pr)
	if err != nil {
		return err
	}
	comments := r.fromBot(all)
	if unansweredApply(all, r.isBot, r.cfg.CommandPrefix) {
		// SchemaBot can accept an apply and fail to post its progress
		// comment, so an unanswered apply may be running. Closing the pull
		// request would not stop it.
		return fmt.Errorf("an apply command on %s#%d has no reply from SchemaBot, so an apply may be running; check the pull request by hand", r.cfg.Repo, cur.pr)
	}
	for _, applyID := range inFlightApplies(comments) {
		r.logger.Info("cancelling unfinished apply", "repo", r.cfg.Repo, "pr", cur.pr, "apply_id", applyID)
		cancelID, err := r.command(ctx, cur, "cancel", applyID)
		if err != nil {
			return err
		}
		if err := r.waitForTerminal(ctx, cur, applyID, cancelID); err != nil {
			return err
		}
	}
	// Deleting the branch first closes the pull request with it. If the
	// delete fails, the pull request stays open, and the next sweep finds it
	// and retries.
	if err := r.gh.DeleteBranch(ctx, cur.branch); err != nil {
		return err
	}
	if err := r.gh.ClosePullRequest(ctx, cur.pr); err != nil {
		return err
	}
	r.logger.Info("closed run pull request", "repo", r.cfg.Repo, "pr", cur.pr, "branch", cur.branch)
	return nil
}

// waitForTerminal waits for applyID to finish after the cancel posted as
// comment cancelID. SchemaBot refusing the cancel because the apply already
// finished counts: the apply holds nothing, even though its comments never
// showed it finishing.
func (r *Runner) waitForTerminal(ctx context.Context, cur *run, applyID string, cancelID int64) error {
	var last, refusedState string
	err := r.poll(ctx, func(ctx context.Context) (bool, error) {
		comments, err := r.botComments(ctx, cur.pr)
		if err != nil {
			return false, err
		}
		if _, finished := finishedStatus(comments, applyID); finished {
			return true, nil
		}
		s, _ := latestApplyStatus(comments, applyID)
		last = s.Status
		refusedState, _ = refusedAsFinished(comments, cancelID)
		return refusedState != "", nil
	})
	if err != nil {
		return fmt.Errorf("wait for apply %s on %s#%d to finish (last status %q): %w", applyID, r.cfg.Repo, cur.pr, last, err)
	}
	if refusedState != "" {
		r.logger.Warn("SchemaBot refused the cancel because the apply already finished, but the pull request never showed it finishing",
			"repo", r.cfg.Repo, "pr", cur.pr, "apply_id", applyID, "state", refusedState, "last_status", last)
	}
	return nil
}

// botComments returns the pull request's comments from SchemaBot.
func (r *Runner) botComments(ctx context.Context, pr int) ([]Comment, error) {
	all, err := r.gh.Comments(ctx, pr)
	if err != nil {
		return nil, err
	}
	return r.fromBot(all), nil
}

// fromBot keeps the comments SchemaBot's App posted.
func (r *Runner) fromBot(all []Comment) []Comment {
	var out []Comment
	for _, c := range all {
		if r.isBot(c) {
			out = append(out, c)
		}
	}
	return out
}

// isBot reports whether SchemaBot's App posted c. An App comments as its
// slug with a [bot] suffix.
func (r *Runner) isBot(c Comment) bool {
	return c.IsBot && c.Author == r.cfg.AppSlug+"[bot]"
}

// pendingHeadline marks the refusal SchemaBot posts while other checks on the
// commit, or an apply in an earlier environment, are still running.
const pendingHeadline = "⏳ Apply Blocked"

// refusalAfter returns the headline of the first reply after the comment with
// ID after that refuses an apply before it reaches the engine.
func refusalAfter(comments []Comment, after int64) (string, bool) {
	var first Comment
	for _, c := range comments {
		if c.ID > after && isApplyRejection(c.Body) && (first.ID == 0 || c.ID < first.ID) {
			first = c
		}
	}
	return firstLine(first.Body), first.ID != 0
}

// withoutApplies drops the status comments for applies in known, so a
// rotated comment for an earlier apply is never read as a new one.
func withoutApplies(comments []Comment, known map[string]bool) []Comment {
	var out []Comment
	for _, c := range comments {
		if s, ok := ParseApplyStatus(c.Body); ok && known[s.ApplyID] {
			continue
		}
		out = append(out, c)
	}
	return out
}

// lastHeadlineAfter returns the first line of the newest bot comment posted
// after the comment with ID after, so a command SchemaBot refused names the
// refusal in the error.
func lastHeadlineAfter(comments []Comment, after int64) string {
	var newest Comment
	for _, c := range comments {
		if c.ID > after && c.ID > newest.ID {
			newest = c
		}
	}
	return firstLine(newest.Body)
}

// poll calls check every PollInterval until it reports done, returns an
// error, or WaitTimeout passes. A GitHub rate limit or server error from
// check is retried on the next tick: polls only read, so a retry cannot post
// anything twice.
func (r *Runner) poll(ctx context.Context, check func(context.Context) (bool, error)) error {
	run := ctx
	ctx, cancel := context.WithTimeout(ctx, r.cfg.WaitTimeout)
	defer cancel()
	ticker := time.NewTicker(r.cfg.PollInterval)
	defer ticker.Stop()
	var transient error
	for {
		done, err := check(ctx)
		switch {
		case err != nil && ctx.Err() != nil:
			// The read failed because the wait ended mid-call.
			return r.pollExpired(run, ctx, transient)
		case err != nil && githubUnavailable(err):
			r.logger.Warn("GitHub call failed; retrying", "error", err)
			transient = err
		case err != nil:
			return err
		case done:
			return nil
		default:
			transient = nil
		}
		select {
		case <-ctx.Done():
			return r.pollExpired(run, ctx, transient)
		case <-ticker.C:
		}
	}
}

// pollExpired names the bound that ended a poll: the run's own deadline or
// cancellation, or WaitTimeout. A GitHub error the poll was still retrying
// marks the wait inconclusive.
func (r *Runner) pollExpired(run, wait context.Context, transient error) error {
	var err error
	switch {
	case errors.Is(run.Err(), context.Canceled):
		err = fmt.Errorf("run cancelled: %w", run.Err())
	case run.Err() != nil:
		err = fmt.Errorf("run deadline passed: %w", run.Err())
	default:
		err = fmt.Errorf("timed out after %s: %w", r.cfg.WaitTimeout, wait.Err())
	}
	if transient != nil {
		return fmt.Errorf("%w, while GitHub was failing (%w): %w", err, transient, ErrInconclusive)
	}
	return err
}
