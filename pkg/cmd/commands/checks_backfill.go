package commands

import (
	"context"
	"fmt"
	"io"
	"os"
	"slices"
	"sort"
	"strconv"
	"strings"
	"text/tabwriter"
	"time"

	"github.com/block/schemabot/pkg/apitypes"
	cmdclient "github.com/block/schemabot/pkg/cmd/client"
	"github.com/block/schemabot/pkg/ui"
)

// ChecksCmd groups SchemaBot Check Run operator commands.
type ChecksCmd struct {
	Backfill ChecksBackfillCmd `cmd:"" help:"Find open PRs with missing or stuck SchemaBot Check Runs; recreate the missing ones" group:"commands"`
	Show     ChecksShowCmd     `cmd:"" help:"Show one PR's stored check state and Check Run, and what each row is waiting on" group:"commands"`
}

// ChecksBackfillCmd walks open PRs and asks the two questions that matter
// for the check gate: is the expected SchemaBot Check Run present, and if
// present, did it complete? Missing checks are recreated by replaying the
// auto-plan flow server-side; existing-but-uncompleted checks aged past
// --stuck-after are reported for investigation, never acted on, because an
// uncompleted check can belong to a genuinely in-flight apply. For the same
// reason, a missing-check PR whose head also carries an uncompleted check is
// held out of the backfill (and the server independently refuses any PR with
// a non-terminal apply).
//
// --all-repos sweeps every repository declared on the server, and --last
// bounds the sweep to PRs updated within a window — together they make an
// incident sweep cost O(incident) instead of O(every open PR at the org).
// Without --last the scan covers every open PR, which is the one-time
// backfill for a repository that predates check enablement. --dry-run
// reports without acting.
//
// A sweep skips and reports a declared repository the server's GitHub App is
// not installed on, so one repository mid-onboarding does not hide the rest of
// the fleet; the skip still makes the run exit nonzero. Every other scan
// failure stops the sweep, since a repository that could not be read is not
// one with nothing to backfill. --skip-repo leaves named repositories out of a
// sweep, so an operator can do the largest repositories first, on their own.
//
// A PR that changes managed schema is deferred unless --plan is given:
// planning reads each changed database's schema once per environment, so one
// large schema PR can cost more of the GitHub budget than every other PR in
// the sweep. The report lists each deferred PR with its databases and prints
// the command that plans them, which names them with --pr so that run scans
// and plans those PRs alone.
type ChecksBackfillCmd struct {
	Repo           string   `arg:"" optional:"" help:"Repository to backfill, in owner/name form; omit with --all-repos"`
	AllRepos       bool     `help:"Scan every repository declared in the server's repos config, skipping and reporting any the server's GitHub App is not installed on"`
	SkipRepo       []string `name:"skip-repo" sep:"," help:"With --all-repos, leave out these repositories (owner/name); repeat the flag or separate them with commas"`
	PR             []int    `name:"pr" sep:"," help:"Only scan these PR numbers in the named repository; repeat the flag or separate them with commas"`
	Plan           bool     `help:"Plan the PRs that change managed schema; without it they are deferred and listed. Requires --pr, because each plan reads every changed database's schema once per environment"`
	Last           string   `help:"Only scan PRs updated within this window, for example 1h or 1d; by default every open PR is scanned"`
	Environment    string   `short:"e" help:"Only backfill this environment's SchemaBot Check Run name"`
	CheckName      string   `help:"Override the SchemaBot Check Run name to look for"`
	Limit          int      `help:"Maximum open PRs to consider across all scanned repositories; 0 considers all" default:"0"`
	StuckAfter     string   `help:"Report an existing but uncompleted Check Run as stuck once it has been sitting this long, for example 1h or 2d" default:"1h"`
	RateLimitFloor int      `help:"Pause whenever the GitHub budget drops below this percentage of its limit, resuming after it resets, so live webhook traffic keeps headroom; 0 disables pacing" default:"20"`
	DryRun         bool     `help:"Report missing and stuck Check Runs without synthesizing"`
	JSON           bool     `help:"Output as JSON"`

	// waitForBudget replaces pauseForRateLimit in tests, so a run that pauses
	// for the budget reset does not wait for a real one.
	waitForBudget func(ctx context.Context, rate *apitypes.GitHubRateLimit, floorPct int, updateProgress func(string)) error `kong:"-"`
}

// pauseForBudget waits for the GitHub budget reset when rate is below the
// --rate-limit-floor.
func (cmd *ChecksBackfillCmd) pauseForBudget(ctx context.Context, rate *apitypes.GitHubRateLimit, updateProgress func(string)) error {
	if cmd.waitForBudget != nil {
		return cmd.waitForBudget(ctx, rate, cmd.RateLimitFloor, updateProgress)
	}
	return pauseForRateLimit(ctx, rate, cmd.RateLimitFloor, updateProgress)
}

// checksSynthesizeBatchSize matches the server's per-request PR cap.
const checksSynthesizeBatchSize = 10

// checksSynthesizeMergeQueueBatchSize matches the server's per-request cap on
// merge queue heads.
const checksSynthesizeMergeQueueBatchSize = 20

// checksBackfillMergeQueueDryRunStatus is a dry run's action for a merge group
// missing a check.
const checksBackfillMergeQueueDryRunStatus = "would post the passing merge queue check if the group is still queued"

// checksScanNamedBatchSize matches the server's cap on named PRs per scan.
const checksScanNamedBatchSize = 30

// checksBackfillDryRunStatus is a dry run's planned action for a PR that is
// not held. Without --plan the server defers a PR that changes managed schema,
// which a dry run cannot tell apart before discovery, so it says both.
const (
	checksBackfillDryRunStatus     = "recreate via auto-plan; a PR that changes managed schema is deferred"
	checksBackfillDryRunPlanStatus = "recreate via auto-plan, planning managed schema"
)

// checksBackfillHeldStatus explains a held PR in the action table. The server
// refuses these too (a non-terminal apply refuses its PR's backfill), but the
// hold works from the scan's own signal — an uncompleted Check Run on the same
// head — so the PR never even reaches the server, and the operator sees why.
const checksBackfillHeldStatus = "held: an uncompleted Check Run sits on this head (a started apply may own it); investigate before backfilling"

// checksBackfillUnconfirmedStatus is the outcome of a PR an interrupted run
// sent for recreation but never heard back about. The request may have
// reached the server, so it claims neither way whether the Check Run was
// recreated.
const checksBackfillUnconfirmedStatus = "not confirmed: interrupted before the server answered"

// checksBackfillNotAttemptedStatus is the outcome of a PR an interrupted run
// never sent for recreation. The server did not hear of it, so its Check Run
// is still missing.
const checksBackfillNotAttemptedStatus = "not attempted: interrupted before this PR was sent"

// checksBackfillInterruptedScanNote is the note under the headline of a
// report whose scan was interrupted.
const checksBackfillInterruptedScanNote = "Interrupted during the scan: only the PRs scanned so far are listed, and no Check Run was recreated. Run the command again for a full sweep."

// checksBackfillInterruptedRecreateNote is the note under the headline of a
// report whose recreation was interrupted. It explains only the interrupted
// outcomes the report shows.
func checksBackfillInterruptedRecreateNote(report *checksBackfillReport) string {
	var unconfirmed, notAttempted bool
	outcomes := make([]string, 0, len(report.Actions)+len(report.MergeQueue))
	for _, action := range report.Actions {
		outcomes = append(outcomes, action.Outcome)
	}
	for _, group := range report.MergeQueue {
		outcomes = append(outcomes, group.Outcome)
	}
	for _, outcome := range outcomes {
		switch outcome {
		case checksBackfillUnconfirmedStatus:
			unconfirmed = true
		case checksBackfillNotAttemptedStatus:
			notAttempted = true
		}
	}
	var explained []string
	if unconfirmed {
		explained = append(explained, `a PR whose outcome is "not confirmed" may or may not have its Check Run yet`)
	}
	if notAttempted {
		explained = append(explained, `a PR whose outcome is "not attempted" was never sent to the server`)
	}
	note := "Interrupted while recreating Check Runs"
	if len(explained) > 0 {
		note += ": " + strings.Join(explained, ", and ")
	}
	return note + ". Run the command again to finish; it recreates only the Check Runs still missing."
}

// What stderr says about how far an interrupted run got when its report could
// not be written to stdout, one per phase the interrupt can land in.
const (
	checksBackfillUnwrittenScanReportNote     = "No Check Run was recreated. Run the command again for a full sweep."
	checksBackfillUnwrittenRecreateReportNote = "Some Check Runs may already be recreated. Run the command again to finish; it recreates only the Check Runs still missing."
)

// checksBackfillAction is one PR's classification and (after acting) outcome.
type checksBackfillAction struct {
	Repo         string   `json:"repo"`
	PR           int      `json:"pr"`
	URL          string   `json:"url"`
	Title        string   `json:"title"`
	HeadSHA      string   `json:"head_sha"`
	MissingNames []string `json:"missing_names"`
	// UntrustedConflictNames are missing checks whose slot is already held by
	// an untrusted app's Check Run; backfill recreates the trusted check but
	// the operator likely also needs to resolve the conflicting one.
	UntrustedConflictNames []string `json:"untrusted_conflict_names,omitempty"`
	// Held marks a missing-check PR the backfill refuses to act on: the same
	// head also carries an uncompleted Check Run, which a genuinely in-flight
	// apply may own, and re-planning the PR could replace an apply-owned merge
	// block with a fresh passing plan. Held PRs are reported, never sent to
	// the server.
	Held    bool   `json:"held,omitempty"`
	Outcome string `json:"outcome,omitempty"`
	Error   string `json:"error,omitempty"`
	// Deferred marks a PR the server did not plan because it changes managed
	// schema and the run did not pass --plan. Nothing was written for it, so
	// its missing check still blocks the merge.
	Deferred bool `json:"deferred,omitempty"`
	// Databases are the managed databases a deferred PR would plan.
	Databases []string `json:"databases,omitempty"`
	// ChangedFiles counts the files a deferred PR proposes, as a size hint.
	ChangedFiles int `json:"changed_files,omitempty"`
	// sent records that this PR was in a batch handed to the server, and
	// answered that the server sent its result. An interrupted run uses them
	// to tell the PRs it heard back about from the batch in flight, and that
	// batch from the PRs it never sent.
	sent     bool
	answered bool
}

// checksMergeQueueAction is one merge group missing a SchemaBot check, and
// (after acting) the outcome of posting the passing check on its head. Until
// the check exists, the group blocks every PR queued behind it.
type checksMergeQueueAction struct {
	Repo string `json:"repo"`
	// Ref is the merge group's temporary branch, gh-readonly-queue/<base>/pr-<n>-<sha>.
	Ref     string `json:"ref"`
	BaseRef string `json:"base_ref,omitempty"`
	// PR is the pull request whose queue entry created the group; 0 when its
	// branch name does not say.
	PR                     int      `json:"pr,omitempty"`
	URL                    string   `json:"url,omitempty"`
	HeadSHA                string   `json:"head_sha"`
	MissingNames           []string `json:"missing_names"`
	UntrustedConflictNames []string `json:"untrusted_conflict_names,omitempty"`
	Outcome                string   `json:"outcome,omitempty"`
	Error                  string   `json:"error,omitempty"`
	// sent and answered track the request as they do for a PR.
	sent     bool
	answered bool
}

// label names the merge group in the report: the PR that created it when its
// branch says, otherwise the branch itself.
func (a checksMergeQueueAction) label() string {
	if a.URL != "" {
		return a.URL
	}
	return a.Ref
}

// checksStuckCheck is one existing-but-uncompleted Check Run on an open PR,
// aged past --stuck-after. Reported for investigation only; the backfill
// never acts on a check that already exists.
type checksStuckCheck struct {
	Repo       string `json:"repo"`
	PR         int    `json:"pr"`
	URL        string `json:"url"`
	Title      string `json:"title"`
	HeadSHA    string `json:"head_sha"`
	CheckName  string `json:"check_name"`
	CheckRunID int64  `json:"check_run_id"`
	Status     string `json:"status"`
	StartedAt  string `json:"started_at,omitempty"`
	// Age is how long the run has been sitting uncompleted at scan time;
	// "unknown" when GitHub did not report a start time.
	Age string `json:"age"`
	// WaitingOn says whether the stored state behind this run resolves on its
	// own ("schemabot") or needs a person ("operator").
	//
	// Empty means no stored row explains the run, which covers more than an
	// empty read: rows may exist and none of them block once scoped to this
	// run's environment, or the only one that does is the aggregate, which
	// restates the rows beside it rather than naming a cause. Empty also
	// means unannotated when the scan was given a stuck_after this run is too
	// young for. A reader that takes it for a failed storage read draws a
	// conclusion the field does not support.
	WaitingOn string `json:"waiting_on,omitempty"`
	// Reasons are the distinct dispositions of the blocking stored rows, in
	// the order the server reported them. They are what makes a stuck row
	// triageable from the sweep instead of only from the PR.
	Reasons []string `json:"reasons,omitempty"`
}

type checksBackfillReport struct {
	Repos []string `json:"repos"`
	// SkippedDisabled are repositories the sweep did not scan because their
	// Check Run publishing is turned off (enable_checks: false) — the server
	// refuses to create their checks, so every open PR would read as missing.
	SkippedDisabled []string `json:"skipped_checks_disabled,omitempty"`
	// SkippedAppNotInstalled are repositories the sweep did not scan because
	// GitHub has no installation of their configured GitHub App on them, so
	// the App can neither read nor publish their Check Runs. Unlike a disabled
	// repository this is a misconfiguration, so it counts toward the run's
	// nonzero exit.
	SkippedAppNotInstalled []string `json:"skipped_app_not_installed,omitempty"`
	// SkippedByOperator are repositories --skip-repo left out of the sweep.
	SkippedByOperator []string `json:"skipped_by_operator,omitempty"`
	CheckNames        []string `json:"check_names"`
	Scanned           int      `json:"scanned"`
	// Last echoes the --last window bounding the sweep; empty means every
	// open PR was scanned.
	Last   string `json:"last,omitempty"`
	DryRun bool   `json:"dry_run"`
	// Plan echoes --plan: whether PRs that change managed schema were planned
	// rather than deferred.
	Plan bool `json:"plan"`
	// Environment and CheckName echo -e and --check-name, the filters that
	// chose which missing checks the run acted on.
	Environment string `json:"environment,omitempty"`
	CheckName   string `json:"check_name,omitempty"`
	// Interrupted says an operator's Ctrl-C or SIGTERM cut the run short, so
	// the report holds only what the run learned before it. It is always
	// present, false on a run that finished.
	Interrupted bool                   `json:"interrupted"`
	StuckAfter  string                 `json:"stuck_after"`
	Actions     []checksBackfillAction `json:"actions"`
	Stuck       []checksStuckCheck     `json:"stuck,omitempty"`
	// MergeQueueScanned counts the merge groups the scan found queued across
	// the scanned repositories, and MergeQueue lists those missing a check.
	// A scan of named PRs does not read the merge queue.
	MergeQueueScanned int                      `json:"merge_queue_scanned,omitempty"`
	MergeQueue        []checksMergeQueueAction `json:"merge_queue,omitempty"`
	// MergeQueueUnsupported lists the scanned repositories whose merge queue
	// went unread because the server does not support reading it.
	MergeQueueUnsupported []string `json:"merge_queue_unsupported,omitempty"`
	// MergeQueueUnread lists the scanned repositories whose merge queue the
	// server failed to read, and why. Their PRs were still scanned.
	MergeQueueUnread []checksMergeQueueUnread `json:"merge_queue_unread,omitempty"`
	// interruptedScan says the interrupt landed before the scan finished, so
	// nothing was sent for recreation and the table shows what the backfill
	// would do rather than an outcome.
	interruptedScan bool
}

// markUnanswered sets the outcome of every PR an interrupted act phase did
// not hear back about: unconfirmed when the PR was sent, since the server may
// have acted on it, and not attempted when it was never sent. Held PRs never
// reach the server and keep the reason they were held.
func (r *checksBackfillReport) markUnanswered() {
	for i := range r.Actions {
		action := &r.Actions[i]
		if action.Held || action.answered {
			continue
		}
		if action.sent {
			action.Outcome = checksBackfillUnconfirmedStatus
			continue
		}
		action.Outcome = checksBackfillNotAttemptedStatus
	}
	for i := range r.MergeQueue {
		group := &r.MergeQueue[i]
		if group.answered {
			continue
		}
		if group.sent {
			group.Outcome = checksBackfillUnconfirmedStatus
			continue
		}
		group.Outcome = checksBackfillNotAttemptedStatus
	}
}

func (cmd *ChecksBackfillCmd) Run(ctx context.Context, g *Globals) error {
	if cmd.Repo == "" && !cmd.AllRepos {
		return fmt.Errorf("name a repository in owner/name form, or pass --all-repos to sweep every repository declared on the server")
	}
	if cmd.Repo != "" && cmd.AllRepos {
		return fmt.Errorf("use either a repository argument or --all-repos, not both")
	}
	if cmd.Limit < 0 {
		return fmt.Errorf("--limit must be non-negative")
	}
	if cmd.RateLimitFloor < 0 || cmd.RateLimitFloor > 99 {
		return fmt.Errorf("--rate-limit-floor must be between 0 and 99")
	}
	if err := cmd.validateScope(); err != nil {
		return err
	}
	stuckAfter, err := parseOperatorDuration(cmd.StuckAfter)
	if err != nil {
		return fmt.Errorf("parse --stuck-after: %w", err)
	}
	var updatedSince string
	if cmd.Last != "" {
		window, err := parseOperatorDuration(cmd.Last)
		if err != nil {
			return fmt.Errorf("parse --last: %w", err)
		}
		updatedSince = operatorNow().Add(-window).Format(time.RFC3339)
	}
	endpoint, err := g.Resolve()
	if err != nil {
		return err
	}

	updateProgress, stopProgress := startLiveProgress(!cmd.JSON)
	defer stopProgress()

	repos := []string{cmd.Repo}
	var skippedDisabled []string
	if cmd.AllRepos {
		updateProgress("listing the repositories declared on the server...")
		declared, err := cmdclient.ChecksRepos(ctx, endpoint)
		if canceled := backfillCanceledError(ctx, err, stopProgress); canceled != nil {
			return canceled
		}
		if err != nil {
			return fmt.Errorf("list the server's declared repositories: %w", err)
		}
		if len(declared.Repos) == 0 && len(declared.Disabled) == 0 {
			return fmt.Errorf("the server declared no repositories; name a repository explicitly")
		}
		// Disabled repos are skipped, not scanned: the server refuses to
		// create their checks, so every open PR would read as missing. An
		// empty enabled list leaves nothing to scan; the report says so.
		repos = declared.Repos
		skippedDisabled = declared.Disabled
		repos, err = withoutSkippedRepos(repos, declared.Disabled, cmd.SkipRepo)
		if err != nil {
			return err
		}
	}

	// Scan phase: page through each repository's open PRs and ask the two
	// questions per PR — is the expected Check Run present, and did it
	// complete? The listing is newest-updated first, so a --last window stops
	// each repo's paging as soon as it crosses the window start.
	report := &checksBackfillReport{
		Repos:             repos,
		SkippedDisabled:   skippedDisabled,
		SkippedByOperator: cmd.SkipRepo,
		Last:              cmd.Last,
		DryRun:            cmd.DryRun,
		Plan:              cmd.Plan,
		Environment:       cmd.Environment,
		CheckName:         cmd.CheckName,
		StuckAfter:        cmd.StuckAfter,
	}
	namedPRs := chunkPRNumbers(cmd.PR, checksScanNamedBatchSize)
	named := make(map[int]bool, len(cmd.PR))
	for _, pr := range cmd.PR {
		named[pr] = true
	}
	checkNamesSeen := map[string]bool{}
	skippedByServer := map[string]bool{}
	held := 0
	// reached counts the leading repositories at least one scan page came back
	// for, so an interrupted scan reports only the repositories it read.
	reached := 0
	var scanCanceled error
scanning:
	for i, repo := range repos {
		page := 1
		repoScanned := 0
		repoEstimate := 0
		repoStart := report.mark()
		for {
			scanPage, scanPRs := page, []int(nil)
			if len(namedPRs) > 0 {
				// A named scan reads its PRs in server-sized groups; page
				// counts the groups here and is not sent.
				scanPage, scanPRs = 0, namedPRs[page-1]
			}
			chunk, err := cmdclient.ChecksScan(ctx, endpoint, apitypes.ChecksScanRequest{
				Repo:         repo,
				Environment:  cmd.Environment,
				CheckName:    cmd.CheckName,
				Page:         scanPage,
				PRs:          scanPRs,
				UpdatedSince: updatedSince,
				// Sent normalized, not as typed: this flag accepts operator
				// spellings the server's own parser does not, so forwarding
				// "2d" verbatim would fail the first page and abort the
				// sweep. The report keeps the spelling the operator used.
				StuckAfter: stuckAfter.String(),
				// This command handles the not-installed skip below, so the
				// server may answer with it instead of an error.
				ReportAppNotInstalled: true,
				// A blocked merge queue holds every PR queued behind it, so
				// a sweep of the repository reads it too. The server reads it
				// on the first page only; named PRs leave it out.
				MergeQueue: len(namedPRs) == 0,
			})
			if canceled := backfillCanceledError(ctx, err, stopProgress); canceled != nil {
				scanCanceled = canceled
				break scanning
			}
			if err != nil {
				return fmt.Errorf("scan %s open PRs for missing or stuck Check Runs: %w", repo, err)
			}
			if err := cmd.requireServerSupport(chunk.Supports); err != nil {
				return err
			}
			// The server skips repos whose Check Run publishing is turned
			// off rather than reporting every open PR as missing; record the
			// skip for the report and move to the next repo. A skip can land
			// on a later page (the config or installation changed mid-scan),
			// so it also drops whatever this repo's earlier pages added: a
			// skipped repo has no findings, and none of them reach synthesis.
			if chunk.ChecksDisabled {
				held -= report.rollback(repoStart, checkNamesSeen)
				skippedByServer[repo] = true
				report.SkippedDisabled = append(report.SkippedDisabled, repo)
				continue scanning
			}
			// GitHub confirmed the repo's App is not installed on it. A named
			// repo is the operator asking about that one repo, so say what to
			// fix; a sweep records the skip and moves on, so one repo whose
			// onboarding is still in progress does not hide every repo after
			// it. Any other scan failure stopped the sweep above.
			if chunk.AppNotInstalled {
				if !cmd.AllRepos {
					return appNotInstalledError(repo, chunk.GitHubApp)
				}
				held -= report.rollback(repoStart, checkNamesSeen)
				skippedByServer[repo] = true
				report.SkippedAppNotInstalled = append(report.SkippedAppNotInstalled, repo)
				continue scanning
			}
			reached = i + 1
			// Reading the merge queue only adds findings, so a server that
			// cannot read it does not stop the sweep; the report says which
			// queues went unread, and the run exits nonzero for them.
			if page == 1 && len(namedPRs) == 0 && !chunk.Supports.MergeQueue {
				report.MergeQueueUnsupported = append(report.MergeQueueUnsupported, repo)
			}
			if chunk.MergeQueueError != "" {
				report.MergeQueueUnread = append(report.MergeQueueUnread, checksMergeQueueUnread{Repo: repo, Error: chunk.MergeQueueError})
			}
			for _, name := range chunk.CheckNames {
				if !checkNamesSeen[name] {
					checkNamesSeen[name] = true
					report.CheckNames = append(report.CheckNames, name)
				}
			}
			repoScanned += chunk.Scanned
			report.Scanned += chunk.Scanned
			report.MergeQueueScanned += chunk.MergeQueueScanned
			for _, group := range chunk.MergeQueue {
				report.MergeQueue = append(report.MergeQueue, checksMergeQueueAction{
					Repo:                   repo,
					Ref:                    group.Ref,
					BaseRef:                group.BaseRef,
					PR:                     group.PR,
					URL:                    group.URL,
					HeadSHA:                group.HeadSHA,
					MissingNames:           group.MissingNames,
					UntrustedConflictNames: group.UntrustedConflictNames,
				})
			}
			if chunk.EstimatedOpenPRs > 0 {
				repoEstimate = chunk.EstimatedOpenPRs
			}
			// Every uncompleted run holds its PR, regardless of how long it has
			// been sitting — --stuck-after shapes the stuck *report*, but an
			// uncompleted run of any age could belong to an in-flight apply.
			if len(named) > 0 {
				// The server scans only the named PRs; filtering here too
				// keeps a server that predates named scans from widening the
				// run to every PR on the page.
				chunk.Missing = slices.DeleteFunc(chunk.Missing, func(pr apitypes.MissingCheckPR) bool { return !named[pr.Number] })
				chunk.Stuck = slices.DeleteFunc(chunk.Stuck, func(pr apitypes.StuckCheckPR) bool { return !named[pr.Number] })
			}
			uncompleted := make(map[int]bool, len(chunk.Stuck))
			for _, stuckPR := range chunk.Stuck {
				uncompleted[stuckPR.Number] = true
			}
			for _, missing := range chunk.Missing {
				action := checksBackfillAction{
					Repo:                   repo,
					PR:                     missing.Number,
					URL:                    missing.URL,
					Title:                  missing.Title,
					HeadSHA:                missing.HeadSHA,
					MissingNames:           missing.MissingNames,
					UntrustedConflictNames: missing.UntrustedConflictNames,
					Held:                   uncompleted[missing.Number],
				}
				if action.Held {
					held++
				}
				report.Actions = append(report.Actions, action)
			}
			report.Stuck = append(report.Stuck, stuckChecksPastThreshold(repo, chunk.Stuck, stuckAfter, scanObservedAt(chunk))...)
			updateProgress(checksScanProgressLine(i+1, len(repos), repo, repoScanned, repoEstimate, report.Scanned, len(report.Actions), held, len(report.Stuck)))
			if cmd.Limit > 0 && report.Scanned >= cmd.Limit {
				break scanning
			}
			nextPage := chunk.NextPage
			if len(namedPRs) > 0 {
				nextPage = 0
				if page < len(namedPRs) {
					nextPage = page + 1
				}
			}
			if nextPage == 0 {
				break
			}
			if err := cmd.pauseForBudget(ctx, chunk.RateLimit, updateProgress); err != nil {
				if canceled := backfillCanceledError(ctx, err, stopProgress); canceled != nil {
					scanCanceled = canceled
					break scanning
				}
				return err
			}
			page = nextPage
		}
	}
	sort.Strings(report.CheckNames)
	if scanCanceled != nil {
		// The repositories after the one the interrupt cut off were never
		// read, so they are not part of the report.
		report.Repos = report.Repos[:reached]
	}

	// The report's scanned-repo list and its skip lists stay disjoint: a repo
	// the server skipped was never scanned, so it belongs only in a skip list.
	if len(skippedByServer) > 0 {
		report.Repos = slices.DeleteFunc(report.Repos, func(r string) bool { return skippedByServer[r] })
	}

	if scanCanceled != nil {
		// Every finding came from a scan page the server answered in full, so
		// each listed PR was fully inspected; the list is partial, and the
		// report says so. Nothing was sent for recreation. A scan that read
		// nothing has nothing to report.
		if reached > 0 || len(skippedByServer) > 0 {
			report.Interrupted = true
			report.interruptedScan = true
			cmd.writeInterrupted(report)
		}
		return scanCanceled
	}

	if !cmd.DryRun {
		// Act phase: synthesize the missing checks in server-bounded batches
		// per repository, skipping held PRs.
		err := cmd.act(ctx, endpoint, report, updateProgress)
		if canceled := backfillCanceledError(ctx, err, stopProgress); canceled != nil {
			// Print what completed before the interrupt, then exit with the
			// interrupt status.
			stopProgress()
			report.Interrupted = true
			report.markUnanswered()
			cmd.writeInterrupted(report)
			return canceled
		}
		if err != nil {
			return err
		}
	}
	stopProgress()
	if err := cmd.write(report); err != nil {
		return err
	}
	if cmd.JSON {
		// The report on stdout lists everything that needs attention.
		return reportedInJSONResponse(attentionExitError(report))
	}
	return attentionExitError(report)
}

// validateScope checks that the flags choosing which repositories and PRs to
// scan, and whether to plan, fit together.
func (cmd *ChecksBackfillCmd) validateScope() error {
	if len(cmd.SkipRepo) > 0 && !cmd.AllRepos {
		return fmt.Errorf("--skip-repo only applies with --all-repos")
	}
	if len(cmd.PR) > 0 {
		if cmd.AllRepos {
			return fmt.Errorf("--pr needs a repository argument; PR numbers belong to one repository")
		}
		if cmd.Last != "" {
			return fmt.Errorf("--pr and --last cannot be used together; --pr already names the PRs to scan")
		}
		for _, pr := range cmd.PR {
			if pr <= 0 {
				return fmt.Errorf("--pr numbers must be positive; got %d", pr)
			}
		}
	}
	if cmd.Plan && len(cmd.PR) == 0 {
		return fmt.Errorf("--plan requires --pr: each plan reads every changed database's schema once per environment, so name the PRs to plan")
	}
	return nil
}

// requireServerSupport refuses a run that relies on a backfill behavior the
// server does not confirm. A server that predates one ignores the request
// field asking for it, so the run would do something other than what the
// operator asked for rather than fail.
func (cmd *ChecksBackfillCmd) requireServerSupport(supports apitypes.ChecksBackfillSupport) error {
	if len(cmd.PR) > 0 && !supports.NamedPRs {
		return fmt.Errorf("the server does not support --pr: it would scan the repository's open PRs page by page instead of the PRs you named. Upgrade the server, or run without --pr")
	}
	if !cmd.DryRun && !supports.DeferSchemaPRs {
		return fmt.Errorf("the server does not defer PRs that change managed schema: it would plan every one and skip the --rate-limit-floor check between PRs, which can drain the GitHub budget live webhooks share. Upgrade the server, or run with --dry-run to report missing Check Runs without recreating them")
	}
	return nil
}

// withoutSkippedRepos removes the repositories named by --skip-repo from the
// declared ones. A name the server does not declare is an error, so a typo
// cannot quietly sweep the repository the operator meant to leave out.
func withoutSkippedRepos(enabled, disabled, skip []string) ([]string, error) {
	declared := func(repo string) bool {
		match := func(r string) bool { return strings.EqualFold(r, repo) }
		return slices.ContainsFunc(enabled, match) || slices.ContainsFunc(disabled, match)
	}
	for _, repo := range skip {
		if !declared(repo) {
			return nil, fmt.Errorf("--skip-repo names %s, which the server does not declare; check the spelling against the server's repos config", repo)
		}
	}
	return slices.DeleteFunc(slices.Clone(enabled), func(repo string) bool {
		return slices.ContainsFunc(skip, func(s string) bool { return strings.EqualFold(s, repo) })
	}), nil
}

// chunkPRNumbers splits prs into groups of at most size, in order.
func chunkPRNumbers(prs []int, size int) [][]int {
	var chunks [][]int
	for start := 0; start < len(prs); start += size {
		chunks = append(chunks, prs[start:min(start+size, len(prs))])
	}
	return chunks
}

// checksMergeQueueUnread is a repository whose merge queue the server failed
// to read, so its merge groups were not checked.
type checksMergeQueueUnread struct {
	Repo  string `json:"repo"`
	Error string `json:"error"`
}

// checksScanMark is how much of a report existed before one repository's scan
// began.
type checksScanMark struct {
	actions, stuck, scanned, checkNames, mergeQueue, mergeQueueScanned, mergeQueueUnsupported, mergeQueueUnread int
}

func (r *checksBackfillReport) mark() checksScanMark {
	return checksScanMark{
		actions:               len(r.Actions),
		stuck:                 len(r.Stuck),
		scanned:               r.Scanned,
		checkNames:            len(r.CheckNames),
		mergeQueue:            len(r.MergeQueue),
		mergeQueueScanned:     r.MergeQueueScanned,
		mergeQueueUnsupported: len(r.MergeQueueUnsupported),
		mergeQueueUnread:      len(r.MergeQueueUnread),
	}
}

// rollback removes everything added to the report since m, for a repository
// the server skipped after some of its pages were already scanned, and
// returns how many held PRs it removed. A repository's findings are appended
// contiguously, so truncating to the mark removes exactly that repository's.
func (r *checksBackfillReport) rollback(m checksScanMark, checkNamesSeen map[string]bool) int {
	held := 0
	for _, action := range r.Actions[m.actions:] {
		if action.Held {
			held++
		}
	}
	for _, name := range r.CheckNames[m.checkNames:] {
		delete(checkNamesSeen, name)
	}
	r.Actions = r.Actions[:m.actions]
	r.Stuck = r.Stuck[:m.stuck]
	r.Scanned = m.scanned
	r.CheckNames = r.CheckNames[:m.checkNames]
	r.MergeQueue = r.MergeQueue[:m.mergeQueue]
	r.MergeQueueScanned = m.mergeQueueScanned
	r.MergeQueueUnsupported = r.MergeQueueUnsupported[:m.mergeQueueUnsupported]
	r.MergeQueueUnread = r.MergeQueueUnread[:m.mergeQueueUnread]
	return held
}

// appNotInstalledError tells the operator who named repo that its GitHub App
// is not installed there, and the two ways to resolve it.
func appNotInstalledError(repo, app string) error {
	appName := "the server's GitHub App"
	if app != "" {
		appName = fmt.Sprintf("GitHub App %q", app)
	}
	return fmt.Errorf("%s is not installed on %s, so SchemaBot cannot read or create its Check Runs; install the App on %s, or remove %s from the server's repos config", appName, repo, repo, repo)
}

// attentionExitError makes the exit code mean "nothing needs an operator
// after this run": it converts anything the run left unresolved into a
// nonzero exit, so scheduled sweeps can alert on the exit code instead of
// parsing the output. In a dry-run nothing was acted on, so every finding
// remains. After an act phase, successfully recreated checks are resolved;
// held PRs, failed recreations, and stuck Check Runs remain — the backfill
// never acts on those. It fires only after the full report has been printed;
// the report itself is unchanged. A run that leaves nothing behind returns
// nil.
func attentionExitError(report *checksBackfillReport) error {
	var parts []string
	if report.DryRun {
		if n := len(report.MergeQueue); n > 0 {
			parts = append(parts, fmt.Sprintf("%d %s missing Check Runs", n, ui.PluralizeLabel("merge group", "merge groups", n)))
		}
		if n := len(report.Actions); n > 0 {
			part := fmt.Sprintf("%d %s with missing Check Runs", n, ui.Pluralize("PR", n))
			held := 0
			for _, action := range report.Actions {
				if action.Held {
					held++
				}
			}
			if held > 0 {
				part += fmt.Sprintf(" (%d held)", held)
			}
			parts = append(parts, part)
		}
	} else {
		held, failed, deferred := 0, 0, 0
		for _, action := range report.Actions {
			switch {
			case action.Held:
				held++
			case action.Error != "":
				failed++
			case action.Deferred:
				deferred++
			}
		}
		if held > 0 {
			parts = append(parts, fmt.Sprintf("%d held %s", held, ui.Pluralize("PR", held)))
		}
		// A deferred PR's check is still missing, so its merge stays blocked
		// until someone plans it.
		if deferred > 0 {
			parts = append(parts, fmt.Sprintf("%d deferred schema %s", deferred, ui.Pluralize("PR", deferred)))
		}
		if failed > 0 {
			parts = append(parts, fmt.Sprintf("%d failed Check Run %s", failed, ui.Pluralize("recreation", failed)))
		}
		// A merge group whose check was not posted still blocks its queue.
		// One the queue no longer holds needs nothing.
		if n := mergeGroupsStillBlocked(report.MergeQueue); n > 0 {
			parts = append(parts, fmt.Sprintf("%d %s still missing Check Runs", n, ui.PluralizeLabel("merge group", "merge groups", n)))
		}
	}
	if n := len(report.Stuck); n > 0 {
		parts = append(parts, fmt.Sprintf("%d stuck %s", n, ui.Pluralize("Check Run", n)))
	}
	// An unchecked merge queue may be blocked, so the run cannot call it clear.
	if n := len(report.MergeQueueUnsupported) + len(report.MergeQueueUnread); n > 0 {
		parts = append(parts, fmt.Sprintf("%d %s with an unchecked merge queue", n, ui.PluralizeLabel("repository", "repositories", n)))
	}
	// A repo the App is not installed on was never scanned, so the run cannot
	// say it has nothing to backfill.
	if n := len(report.SkippedAppNotInstalled); n > 0 {
		parts = append(parts, fmt.Sprintf("%d %s without the GitHub App installed", n, ui.PluralizeLabel("repository", "repositories", n)))
	}
	if len(parts) == 0 {
		return nil
	}
	return fmt.Errorf("checks backfill left %s needing attention", strings.Join(parts, " and "))
}

// checksScanProgressLine renders one scan progress update. The denominator is
// GitHub's own open-PR count for the repository, recomputed every page, so a
// long sweep always reads as progress toward a bound instead of an endless
// loop; the counts after the dash say what the sweep has found so far. The
// denominator is an upper bound until the repo's final page (a --last window
// legitimately stops short of it).
func checksScanProgressLine(repoIndex, repoCount int, repo string, repoScanned, repoEstimate, totalScanned, missing, held, stuck int) string {
	var b strings.Builder
	if repoCount > 1 {
		fmt.Fprintf(&b, "repo %d/%d ", repoIndex, repoCount)
	}
	fmt.Fprintf(&b, "%s: %d", repo, repoScanned)
	if repoEstimate > repoScanned {
		fmt.Fprintf(&b, "/~%d", repoEstimate)
	}
	b.WriteString(" PRs scanned")
	if repoCount > 1 {
		fmt.Fprintf(&b, " (%d across all repos)", totalScanned)
	}
	fmt.Fprintf(&b, " — %d missing", missing)
	if held > 0 {
		fmt.Fprintf(&b, " (%d held)", held)
	}
	fmt.Fprintf(&b, ", %d stuck Check Runs", stuck)
	return b.String()
}

// backfillCanceledError maps an operator cancellation (Ctrl+C) to a clean
// stop: it clears the progress line, prints a notice, and returns a silent
// error so the CLI exits with the interrupt status without a raw "context
// canceled" error line. It returns nil for any other error (including none)
// so the caller handles it normally.
func backfillCanceledError(ctx context.Context, err error, stopProgress func()) error {
	if !endedByInterrupt(ctx, err) {
		return nil
	}
	stopProgress()
	fmt.Fprintln(os.Stderr, "checks backfill canceled")
	return interrupted(ctx)
}

// pauseForRateLimit sleeps until the GitHub budget resets when the remaining
// budget has fallen below floorPct percent of its limit. The backfill shares
// its installation budget with live webhook serving — check creation for PRs
// being pushed right now — so a large sweep must leave headroom rather than
// drain the budget and starve the live path. A nil snapshot (the server could
// not read the rate state) proceeds without pausing; GitHub's reactive
// secondary-rate-limit handling still applies underneath.
func pauseForRateLimit(ctx context.Context, rate *apitypes.GitHubRateLimit, floorPct int, updateProgress func(string)) error {
	wait, ok := rateLimitPauseDuration(rate, floorPct, operatorNow())
	if !ok {
		return nil
	}
	updateProgress(fmt.Sprintf("GitHub budget below %d%% (%d/%d requests left); pausing %s until it resets so live webhook traffic keeps headroom", floorPct, rate.Remaining, rate.Limit, wait.Truncate(time.Second)))
	timer := time.NewTimer(wait)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

// rateLimitPauseDuration decides whether the budget snapshot is below the
// floor and how long to wait for the reset. It returns false — no pause —
// when pacing is disabled, the snapshot is missing, the budget is above the
// floor, or the reset time is unparseable or already past (the next request
// sees a replenished budget, so waiting would only slow the sweep).
func rateLimitPauseDuration(rate *apitypes.GitHubRateLimit, floorPct int, now time.Time) (time.Duration, bool) {
	if rate == nil || floorPct <= 0 || rate.Limit <= 0 {
		return 0, false
	}
	if rate.Remaining*100 >= rate.Limit*floorPct {
		return 0, false
	}
	resetAt, err := time.Parse(time.RFC3339, rate.ResetAt)
	if err != nil || !resetAt.After(now) {
		return 0, false
	}
	// A minute of slack lets GitHub's reset land before the sweep resumes.
	return resetAt.Sub(now) + time.Minute, true
}

// scanObservedAt is the clock this page's runs should be aged against: the
// server's, when it reported one.
//
// The threshold is applied on both sides — the server decides which runs to
// read stored rows for, this command decides which to render — and a client
// clock is a round trip ahead of the server's while the start time it reads
// back is truncated to whole seconds. Both errors point the same way, so a run
// the server judged just short of the threshold, and therefore left
// unannotated, is judged past it here and rendered with an empty WAITING ON
// and REASON. Those columns read as "no stored row was blocking", which is a
// finding; "the server did not look" is not one.
//
// A server reporting no clock is one that does not annotate either, so there
// the caller's own is all there is and applying the threshold locally is what
// keeps it applied at all. An unparseable one is treated the same way rather
// than trusted: the fallback renders more than it should, never less.
func scanObservedAt(chunk *apitypes.ChecksScanResponse) time.Time {
	if chunk.ObservedAt != "" {
		if observed, err := time.Parse(time.RFC3339, chunk.ObservedAt); err == nil {
			return observed
		}
	}
	return operatorNow()
}

// stuckChecksPastThreshold flattens the scan's uncompleted Check Runs to one
// row per (PR, check), keeping only runs that have been sitting longer than
// stuckAfter. A run whose start time is missing, unparseable, or in the
// future (clock skew) is always kept with an "unknown" age — a start time
// that cannot prove the run is young must not hide it.
func stuckChecksPastThreshold(repo string, prs []apitypes.StuckCheckPR, stuckAfter time.Duration, now time.Time) []checksStuckCheck {
	var out []checksStuckCheck
	for _, pr := range prs {
		for _, check := range pr.Checks {
			age := "unknown"
			if check.StartedAt != "" {
				startedAt, err := time.Parse(time.RFC3339, check.StartedAt)
				if err == nil && !startedAt.After(now) {
					sitting := now.Sub(startedAt)
					if sitting < stuckAfter {
						continue
					}
					age = sitting.Truncate(time.Minute).String()
				}
			}
			out = append(out, checksStuckCheck{
				Repo:       repo,
				PR:         pr.Number,
				URL:        pr.URL,
				Title:      pr.Title,
				HeadSHA:    pr.HeadSHA,
				CheckName:  check.Name,
				CheckRunID: check.CheckRunID,
				Status:     check.Status,
				StartedAt:  check.StartedAt,
				Age:        age,
				WaitingOn:  check.WaitingOn,
				Reasons:    blockingReasons(check.StoredRows),
			})
		}
	}
	return out
}

// blockingReasons lists the distinct dispositions of the rows that are
// holding the gate open, dropping the ones that already resolved. A resolved
// row explains nothing about why the run is still sitting, and listing it
// alongside the real cause is what makes a sweep unreadable.
//
// The rollup row is dropped for the same reason. It restates the rows beside
// it, so it adds a reason that names the symptom the operator is already
// looking at instead of the cause underneath it.
func blockingReasons(rows []apitypes.InspectedCheck) []string {
	var reasons []string
	for _, row := range rows {
		if row.Aggregate || !row.Blocking || row.Reason == "" {
			continue
		}
		if !slices.Contains(reasons, row.Reason) {
			reasons = append(reasons, row.Reason)
		}
	}
	return reasons
}

// repoPR keys a PR within a multi-repo report.
type repoPR struct {
	repo string
	pr   int
}

func (cmd *ChecksBackfillCmd) act(ctx context.Context, endpoint string, report *checksBackfillReport, updateProgress func(string)) error {
	// Merge groups go first, across every repository: each one holds every PR
	// queued behind it, so clearing it unblocks more than any single PR.
	if err := cmd.actOnMergeQueue(ctx, endpoint, report, updateProgress); err != nil {
		return err
	}
	// Synthesize per repository, in server-bounded batches. Held PRs never
	// reach the server; their outcome records why.
	synthesizeByRepo := make(map[string][]int)
	actionByPR := make(map[repoPR]*checksBackfillAction)
	for i := range report.Actions {
		action := &report.Actions[i]
		actionByPR[repoPR{repo: action.Repo, pr: action.PR}] = action
		if action.Held {
			action.Outcome = checksBackfillHeldStatus
			continue
		}
		synthesizeByRepo[action.Repo] = append(synthesizeByRepo[action.Repo], action.PR)
	}
	failed := 0
	for _, repo := range sortedKeys(synthesizeByRepo) {
		prs := synthesizeByRepo[repo]
		pending := prs
		for len(pending) > 0 {
			batch := pending[:min(checksSynthesizeBatchSize, len(pending))]
			pending = pending[len(batch):]
			progressLine := fmt.Sprintf("synthesizing Check Runs for %d/%d PRs in %s...", len(prs)-len(pending), len(prs), repo)
			if failed > 0 {
				// Surface failures as they happen so the operator can interrupt a
				// failing sweep instead of discovering it in the final table.
				progressLine += fmt.Sprintf(" (%d failed so far)", failed)
			}
			updateProgress(progressLine)
			// Mark the batch before the request: once it is handed over, an
			// interrupt cannot tell whether it reached the server.
			for _, pr := range batch {
				actionByPR[repoPR{repo: repo, pr: pr}].sent = true
			}
			resp, err := cmdclient.ChecksSynthesize(ctx, endpoint, apitypes.ChecksSynthesizeRequest{
				Repo:                  repo,
				PRs:                   batch,
				Plan:                  cmd.Plan,
				RateLimitFloorPercent: cmd.RateLimitFloor,
			})
			if err != nil {
				return fmt.Errorf("synthesize Check Runs for %s: %w", repo, err)
			}
			// The server stops a batch at the first PR it reaches with the
			// budget below the floor. Those PRs were not started, so they go
			// back on the queue to send after the pause below.
			_, pausing := rateLimitPauseDuration(resp.RateLimit, cmd.RateLimitFloor, operatorNow())
			var notStarted []int
			for _, result := range resp.Results {
				action, ok := actionByPR[repoPR{repo: repo, pr: result.PR}]
				if !ok {
					continue
				}
				if result.RateLimited && pausing {
					action.sent = false
					notStarted = append(notStarted, result.PR)
					continue
				}
				applySynthesizeResult(action, result)
				if action.Error != "" {
					failed++
				}
			}
			pending = append(notStarted, pending...)
			if err := cmd.pauseForBudget(ctx, resp.RateLimit, updateProgress); err != nil {
				return err
			}
		}
	}
	return nil
}

// actOnMergeQueue asks the server to post the passing merge queue check on
// each merge group head the scan found missing one, per repository, in
// server-bounded batches. The server lists the queue again and posts only on
// a head it still holds, so a group that merged or left the queue since the
// scan gets nothing.
func (cmd *ChecksBackfillCmd) actOnMergeQueue(ctx context.Context, endpoint string, report *checksBackfillReport, updateProgress func(string)) error {
	headsByRepo := make(map[string][]string)
	groupByHead := make(map[string]*checksMergeQueueAction)
	for i := range report.MergeQueue {
		group := &report.MergeQueue[i]
		headsByRepo[group.Repo] = append(headsByRepo[group.Repo], group.HeadSHA)
		groupByHead[group.Repo+"@"+group.HeadSHA] = group
	}
	for _, repo := range sortedKeys(headsByRepo) {
		heads := headsByRepo[repo]
		pending := heads
		for len(pending) > 0 {
			batch := pending[:min(checksSynthesizeMergeQueueBatchSize, len(pending))]
			pending = pending[len(batch):]
			updateProgress(fmt.Sprintf("posting merge queue checks for %d/%d merge groups in %s...", len(heads)-len(pending), len(heads), repo))
			for _, head := range batch {
				groupByHead[repo+"@"+head].sent = true
			}
			resp, err := cmdclient.ChecksSynthesize(ctx, endpoint, apitypes.ChecksSynthesizeRequest{
				Repo:                  repo,
				MergeQueueHeads:       batch,
				Environment:           cmd.Environment,
				CheckName:             cmd.CheckName,
				RateLimitFloorPercent: cmd.RateLimitFloor,
			})
			if err != nil {
				if endedByInterrupt(ctx, err) {
					return fmt.Errorf("post merge queue checks for %s: %w", repo, err)
				}
				// A failed request leaves this batch unconfirmed, since the
				// server may have posted some of its checks before failing.
				// The run keeps going: the PRs' own checks do not depend on
				// the merge queue, and the unconfirmed groups still make the
				// run exit nonzero.
				updateProgress(fmt.Sprintf("posting merge queue checks in %s failed; continuing with the rest of the sweep", repo))
				for _, head := range batch {
					group := groupByHead[repo+"@"+head]
					group.Error = fmt.Sprintf("request failed before the server answered, so the check may or may not have been posted: %v", err)
					group.answered = true
				}
				continue
			}
			// Heads the server did not start because the budget was below
			// the floor go back on the queue to send after the pause.
			_, pausing := rateLimitPauseDuration(resp.RateLimit, cmd.RateLimitFloor, operatorNow())
			var notStarted []string
			for _, result := range resp.MergeQueue {
				group, ok := groupByHead[repo+"@"+result.HeadSHA]
				if !ok {
					continue
				}
				if result.RateLimited && pausing {
					group.sent = false
					notStarted = append(notStarted, result.HeadSHA)
					continue
				}
				applyMergeQueueResult(group, result)
			}
			pending = append(notStarted, pending...)
			if err := cmd.pauseForBudget(ctx, resp.RateLimit, updateProgress); err != nil {
				return err
			}
		}
	}
	return nil
}

// applyMergeQueueResult records the server's answer for one merge group. A
// head not started for the budget, and not about to be resent, is a failure:
// its check is still missing.
func applyMergeQueueResult(group *checksMergeQueueAction, result apitypes.ChecksMergeQueueResult) {
	group.Outcome = result.Outcome
	group.Error = result.Error
	if result.RateLimited && group.Error == "" {
		group.Error = result.Outcome
	}
	group.answered = true
}

// mergeGroupsStillBlocked counts the merge groups the run did not confirm a
// check on: a failed post, or no answer from the server (an interrupted run).
// A group the queue no longer holds, or one skipped for the repository's
// configuration, needs nothing more.
func mergeGroupsStillBlocked(groups []checksMergeQueueAction) int {
	n := 0
	for _, group := range groups {
		if group.Error != "" || !group.answered {
			n++
		}
	}
	return n
}

// applySynthesizeResult records the server's answer for one PR on its action.
// A PR the server did not start because the budget was below the floor, and
// that the run is not about to resend, is a failure: its check is still
// missing, and the run cannot say when it would be recreated.
func applySynthesizeResult(action *checksBackfillAction, result apitypes.ChecksSynthesizeResult) {
	action.Outcome = result.Outcome
	action.Error = result.Error
	if result.RateLimited && action.Error == "" {
		action.Error = result.Outcome
	}
	action.Deferred = result.Deferred
	action.Databases = result.Databases
	action.ChangedFiles = result.ChangedFiles
	action.answered = true
}

func (cmd *ChecksBackfillCmd) write(report *checksBackfillReport) error {
	if cmd.JSON {
		return writeJSON(report)
	}
	return writeChecksBackfillReport(os.Stdout, report)
}

// writeInterrupted prints the report of a run the operator's interrupt cut
// short. The run ends with the interrupt status whether or not stdout took
// the report, so a report that was not written is named on stderr, along with
// what its note would have said about how far the run got.
func (cmd *ChecksBackfillCmd) writeInterrupted(report *checksBackfillReport) {
	err := cmd.write(report)
	if err == nil {
		return
	}
	note := checksBackfillUnwrittenRecreateReportNote
	if report.interruptedScan {
		note = checksBackfillUnwrittenScanReportNote
	}
	fmt.Fprintf(os.Stderr, "The report could not be written to stdout, so it is missing or incomplete: %v. %s\n", err, note)
}

// writeChecksBackfillReport prints the text report. Its error means w refused
// a write. A recreation that failed is a row of the report, and
// attentionExitError turns it into the exit status.
func writeChecksBackfillReport(w io.Writer, report *checksBackfillReport) error {
	// An interrupted scan that read no repository still names the ones it
	// skipped, rather than reading as a sweep with nothing to scan.
	if len(report.Repos) == 0 && (len(report.SkippedAppNotInstalled) > 0 || report.Interrupted) {
		if _, err := fmt.Fprintln(w, "No repositories scanned."); err != nil {
			return err
		}
		if err := writeChecksInterruptedNote(w, report); err != nil {
			return err
		}
		return writeChecksSkippedRepos(w, report)
	}
	if len(report.Repos) == 0 && len(report.SkippedDisabled) > 0 {
		if len(report.SkippedDisabled) == 1 {
			_, err := fmt.Fprintf(w, "Repository %s has Check Runs disabled (enable_checks: false); nothing to scan.\n", report.SkippedDisabled[0])
			return err
		}
		_, err := fmt.Fprintf(w, "All %d repositories have Check Runs disabled (enable_checks: false); nothing to scan: %s.\n", len(report.SkippedDisabled), strings.Join(report.SkippedDisabled, ", "))
		return err
	}
	repoScope := strings.Join(report.Repos, ", ")
	if len(report.Repos) > 3 {
		repoScope = fmt.Sprintf("%d repositories", len(report.Repos))
	}
	window := ""
	if report.Last != "" {
		window = fmt.Sprintf(" updated in the last %s", report.Last)
	}
	checkScope := ""
	if len(report.CheckNames) > 0 {
		checkScope = fmt.Sprintf(" for %s", strings.Join(report.CheckNames, ", "))
	}
	mergeGroups := ""
	if report.MergeQueueScanned > 0 {
		mergeGroups = fmt.Sprintf(" and %d queued %s", report.MergeQueueScanned, ui.PluralizeLabel("merge group", "merge groups", report.MergeQueueScanned))
	}
	if _, err := fmt.Fprintf(w, "Scanned %d open PRs%s%s in %s%s.\n", report.Scanned, window, mergeGroups, repoScope, checkScope); err != nil {
		return err
	}
	if err := writeChecksInterruptedNote(w, report); err != nil {
		return err
	}
	if err := writeChecksSkippedRepos(w, report); err != nil {
		return err
	}
	if len(report.MergeQueueUnsupported) > 0 {
		if _, err := fmt.Fprintf(w, "The server cannot read the merge queue, so queued merge groups in %s were not checked. Upgrade the server to check them.\n", strings.Join(report.MergeQueueUnsupported, ", ")); err != nil {
			return err
		}
	}
	for _, unread := range report.MergeQueueUnread {
		if _, err := fmt.Fprintf(w, "Could not read the merge queue of %s, so its queued merge groups were not checked: %s. Run the backfill for %s again to check them.\n", unread.Repo, unread.Error, unread.Repo); err != nil {
			return err
		}
	}
	if err := writeChecksStuckSection(w, report); err != nil {
		return err
	}
	// An interrupted scan sent nothing for recreation, so like a dry run it
	// lists what the backfill would do.
	acted := !report.DryRun && !report.interruptedScan
	if err := writeChecksMergeQueueSection(w, report, acted); err != nil {
		return err
	}
	if len(report.Actions) == 0 {
		if len(report.MergeQueue) > 0 {
			return nil
		}
		_, err := fmt.Fprintln(w, "No missing SchemaBot Check Runs found.")
		return err
	}

	tw := tabwriter.NewWriter(w, 0, 0, 2, ' ', 0)
	header := "PR\tMISSING CHECKS\tACTION"
	if acted {
		header = "PR\tMISSING CHECKS\tOUTCOME"
	}
	if _, err := fmt.Fprintln(tw, header); err != nil {
		return err
	}
	for _, action := range report.Actions {
		status := checksBackfillDryRunStatus
		if report.Plan {
			status = checksBackfillDryRunPlanStatus
		}
		if action.Held {
			status = checksBackfillHeldStatus
		}
		if acted {
			status = action.Outcome
			if action.Deferred {
				status = deferredStatus(action)
			}
			if action.Error != "" {
				status = "failed: " + action.Error
			}
		}
		// The status (server outcome/error) is a tab-separated cell: strip
		// tabs/newlines so a value containing them cannot break the layout.
		if _, err := fmt.Fprintf(tw, "%s\t%s\t%s\n", action.URL, strings.Join(action.MissingNames, ", "), sanitizeCell(status)); err != nil {
			return err
		}
	}
	if err := tw.Flush(); err != nil {
		return err
	}
	for _, action := range report.Actions {
		if len(action.UntrustedConflictNames) > 0 {
			if _, err := fmt.Fprintf(w, "Note: %s#%d has checks held by an untrusted app (%s); backfill recreates the trusted check, but resolve the conflicting one too.\n", action.Repo, action.PR, strings.Join(action.UntrustedConflictNames, ", ")); err != nil {
				return err
			}
		}
	}
	return writeChecksDeferredHint(w, report)
}

// writeChecksMergeQueueSection lists the merge groups missing a SchemaBot
// check, ahead of the PRs: each one blocks every PR queued behind it.
func writeChecksMergeQueueSection(w io.Writer, report *checksBackfillReport, acted bool) error {
	if len(report.MergeQueue) == 0 {
		return nil
	}
	n := len(report.MergeQueue)
	if _, err := fmt.Fprintf(w, "%d %s missing SchemaBot Check Runs, blocking the PRs queued behind %s:\n",
		n, ui.PluralizeLabel("merge group is", "merge groups are", n), ui.PluralizeLabel("it", "them", n)); err != nil {
		return err
	}
	tw := tabwriter.NewWriter(w, 0, 0, 2, ' ', 0)
	header := "MERGE GROUP\tBASE\tMISSING CHECKS\tACTION"
	if acted {
		header = "MERGE GROUP\tBASE\tMISSING CHECKS\tOUTCOME"
	}
	if _, err := fmt.Fprintln(tw, header); err != nil {
		return err
	}
	for _, group := range report.MergeQueue {
		status := checksBackfillMergeQueueDryRunStatus
		if acted {
			status = group.Outcome
			if group.Error != "" {
				status = "failed: " + group.Error
			}
		}
		if _, err := fmt.Fprintf(tw, "%s\t%s\t%s\t%s\n", group.label(), group.BaseRef, strings.Join(group.MissingNames, ", "), sanitizeCell(status)); err != nil {
			return err
		}
	}
	if err := tw.Flush(); err != nil {
		return err
	}
	for _, group := range report.MergeQueue {
		if len(group.UntrustedConflictNames) > 0 {
			if _, err := fmt.Fprintf(w, "Note: merge group %s has checks held by an untrusted app (%s); backfill posts the trusted check, but resolve the conflicting one too.\n", group.Ref, strings.Join(group.UntrustedConflictNames, ", ")); err != nil {
				return err
			}
		}
	}
	// A blank line keeps the merge queue table apart from the PR table.
	if len(report.Actions) > 0 {
		if _, err := fmt.Fprintln(w); err != nil {
			return err
		}
	}
	return nil
}

// deferredStatus is the outcome cell of a PR deferred because it changes
// managed schema: the databases its plan would read, and how many files it
// changes, so the operator can tell a one-table change from a sweeping one.
func deferredStatus(action checksBackfillAction) string {
	databases := strings.Join(action.Databases, ", ")
	if len(action.Databases) > 3 {
		databases = fmt.Sprintf("%s and %d more", strings.Join(action.Databases[:3], ", "), len(action.Databases)-3)
	}
	return fmt.Sprintf("deferred: changes schema in %d %s (%s); %d changed %s",
		len(action.Databases), ui.PluralizeLabel("database", "databases", len(action.Databases)), databases,
		action.ChangedFiles, ui.PluralizeLabel("file", "files", action.ChangedFiles))
}

// writeChecksDeferredHint prints, per repository, the command that plans the
// PRs this run deferred.
func writeChecksDeferredHint(w io.Writer, report *checksBackfillReport) error {
	deferredByRepo := make(map[string][]string)
	total := 0
	for _, action := range report.Actions {
		if action.Deferred && action.Error == "" {
			deferredByRepo[action.Repo] = append(deferredByRepo[action.Repo], strconv.Itoa(action.PR))
			total++
		}
	}
	if total == 0 {
		return nil
	}
	if _, err := fmt.Fprintf(w, "\n%d %s managed schema and %s not planned, so %s Check %s still missing and the merge stays blocked. Each plan reads every changed database's schema once per environment. To plan them:\n",
		total, ui.PluralizeLabel("PR changes", "PRs change", total), ui.PluralizeLabel("was", "were", total),
		ui.PluralizeLabel("its", "their", total), ui.PluralizeLabel("Run is", "Runs are", total)); err != nil {
		return err
	}
	for _, repo := range sortedKeys(deferredByRepo) {
		if _, err := fmt.Fprintf(w, "  schemabot checks backfill %s%s --plan --pr %s\n", repo, checksBackfillFilterFlags(report), strings.Join(deferredByRepo[repo], ",")); err != nil {
			return err
		}
	}
	return nil
}

// checksBackfillFilterFlags repeats the run's -e and --check-name filters for
// a follow-up command, so it acts on the same missing checks this run found.
// Without them, a PR missing only its production check could be held for an
// uncompleted staging check and never planned.
func checksBackfillFilterFlags(report *checksBackfillReport) string {
	var flags strings.Builder
	if report.Environment != "" {
		fmt.Fprintf(&flags, " -e %s", shellQuoteArg(report.Environment))
	}
	if report.CheckName != "" {
		fmt.Fprintf(&flags, " --check-name %s", shellQuoteArg(report.CheckName))
	}
	return flags.String()
}

// shellQuoteArg quotes arg for a POSIX shell when it holds anything beyond
// characters a shell reads literally, so a printed command pastes as one word.
func shellQuoteArg(arg string) string {
	plain := arg != "" && strings.IndexFunc(arg, func(r rune) bool {
		return (r < 'a' || r > 'z') && (r < 'A' || r > 'Z') && (r < '0' || r > '9') && !strings.ContainsRune("@%_+=:,./-", r)
	}) < 0
	if plain {
		return arg
	}
	return "'" + strings.ReplaceAll(arg, "'", `'\''`) + "'"
}

// writeChecksInterruptedNote says, under the headline, that an interrupt cut
// the run short and what the report therefore leaves out.
func writeChecksInterruptedNote(w io.Writer, report *checksBackfillReport) error {
	if !report.Interrupted {
		return nil
	}
	note := checksBackfillInterruptedRecreateNote(report)
	if report.interruptedScan {
		note = checksBackfillInterruptedScanNote
	}
	_, err := fmt.Fprintln(w, note)
	return err
}

// writeChecksSkippedRepos names the repositories the sweep did not scan, one
// line per reason.
func writeChecksSkippedRepos(w io.Writer, report *checksBackfillReport) error {
	if len(report.SkippedDisabled) > 0 {
		if _, err := fmt.Fprintf(w, "Skipped repositories with Check Runs disabled (enable_checks: false): %s.\n", strings.Join(report.SkippedDisabled, ", ")); err != nil {
			return err
		}
	}
	if len(report.SkippedAppNotInstalled) > 0 {
		if _, err := fmt.Fprintf(w, "Skipped repositories the server's GitHub App is not installed on: %s. Install the App on each, or remove it from the server's repos config.\n", strings.Join(report.SkippedAppNotInstalled, ", ")); err != nil {
			return err
		}
	}
	if len(report.SkippedByOperator) > 0 {
		if _, err := fmt.Fprintf(w, "Skipped repositories named by --skip-repo: %s.\n", strings.Join(report.SkippedByOperator, ", ")); err != nil {
			return err
		}
	}
	return nil
}

// writeChecksStuckSection renders the stuck Check Runs the scan found: runs
// that exist on an open PR's head but have sat uncompleted past --stuck-after.
// Backfill never acts on these — an existing run may belong to an in-flight
// apply, and overwriting it could convert real uncertainty into a passing
// check — so the section tells the operator to investigate instead.
func writeChecksStuckSection(w io.Writer, report *checksBackfillReport) error {
	if len(report.Stuck) == 0 {
		return nil
	}
	if _, err := fmt.Fprintf(w, "\nStuck Check Runs — uncompleted for over %s (backfill does not act on existing Check Runs; investigate the apply or plan that owns each):\n", report.StuckAfter); err != nil {
		return err
	}
	tw := tabwriter.NewWriter(w, 0, 0, 2, ' ', 0)
	if _, err := fmt.Fprintln(tw, "PR\tCHECK\tSTATUS\tAGE\tWAITING ON\tREASON"); err != nil {
		return err
	}
	for _, stuck := range report.Stuck {
		if _, err := fmt.Fprintf(tw, "%s\t%s\t%s\t%s\t%s\t%s\n",
			stuck.URL, stuck.CheckName, stuck.Status, stuck.Age,
			orDash(stuck.WaitingOn), orDash(strings.Join(stuck.Reasons, ","))); err != nil {
			return err
		}
	}
	if err := tw.Flush(); err != nil {
		return err
	}
	if _, err := fmt.Fprintln(w, "\nA run waiting on an operator will not clear on its own. Read the stored rows behind one with `sq schemabot checks show <owner/repo> <pr>`."); err != nil {
		return err
	}
	_, err := fmt.Fprintln(w)
	return err
}

// sortedKeys returns the map keys in deterministic order, so per-repo output
// (synthesize progress, batch order) does not depend on map iteration order.
func sortedKeys[V any](m map[string]V) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}

// sanitizeCell collapses tabs and newlines to spaces so a caller-influenced
// value (a server outcome or error string) cannot break the tab-separated
// table layout.
func sanitizeCell(s string) string {
	return strings.NewReplacer("\t", " ", "\n", " ", "\r", " ").Replace(s)
}
