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
	// the cleanup after a run.
	WaitTimeout time.Duration
	// RunTimeout bounds a whole run, database calls included. A run is over
	// once RunTimeout and then WaitTimeout have passed since it started, so
	// the sweep reaps only runs older than that.
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

// A run names its table runTablePrefix plus its start time in runTimeLayout,
// UTC.
const (
	runTablePrefix = "e2e_"
	runTimeLayout  = "20060102150405"
)

// run is one test's names.
type run struct {
	table  string
	branch string
	pr     int
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

// Run runs the test. It returns nil when every step passed, an error
// wrapping ErrInconclusive when the run could not decide, and any other error
// when a step failed. A GitHub rate limit or server error leaves the run
// undecided, so it wraps ErrInconclusive too.
func (r *Runner) Run(ctx context.Context) error {
	ctx, cancel := context.WithTimeout(ctx, r.cfg.RunTimeout)
	defer cancel()
	table := runTablePrefix + r.now().UTC().Format(runTimeLayout)
	cur := &run{table: table, branch: BranchPrefix + table}
	r.logger.Info("starting live e2e run", "repo", r.cfg.Repo, "database", r.cfg.Database,
		"environment", r.cfg.Environment, "table", cur.table, "branch", cur.branch)
	return inconclusiveWhenGitHubUnavailable(r.steps(ctx, cur))
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
		return err
	}
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
		started, ok = firstApplyAfter(comments, commentID)
		return ok, nil
	})
	if err != nil {
		return "", fmt.Errorf("wait for %s on %s#%d to start an apply (last reply %q): %w", verb, r.cfg.Repo, cur.pr, lastHeadline, err)
	}
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
	"reverted":  "Reverted",
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
		return check != nil && check.Status == "completed", nil
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
// error, or WaitTimeout passes.
func (r *Runner) poll(ctx context.Context, check func(context.Context) (bool, error)) error {
	ctx, cancel := context.WithTimeout(ctx, r.cfg.WaitTimeout)
	defer cancel()
	ticker := time.NewTicker(r.cfg.PollInterval)
	defer ticker.Stop()
	for {
		done, err := check(ctx)
		if err != nil {
			return err
		}
		if done {
			return nil
		}
		select {
		case <-ctx.Done():
			return fmt.Errorf("timed out after %s: %w", r.cfg.WaitTimeout, ctx.Err())
		case <-ticker.C:
		}
	}
}
