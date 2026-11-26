package webhook

import (
	"cmp"
	"context"
	"fmt"
	"slices"
	"strings"

	"github.com/block/schemabot/pkg/api"
	"github.com/block/schemabot/pkg/checkstate"
	ghclient "github.com/block/schemabot/pkg/github"
	"github.com/block/schemabot/pkg/state"
	"github.com/block/schemabot/pkg/storage"
)

// aggregateCheckNameForEnv returns the environment-scoped aggregate check name.
// e.g., "SchemaBot (staging)" or "Custom SchemaBot (production)".
func aggregateCheckNameForEnv(baseName, env string) string {
	return fmt.Sprintf("%s (%s)", baseName, env)
}

func (h *Handler) serverConfig() (*api.ServerConfig, bool) {
	if h == nil {
		return nil, false
	}
	if h.service == nil {
		return nil, false
	}
	config := h.service.Config()
	if config == nil {
		return nil, false
	}
	return config, true
}

// isAggregateParticipant reports whether this deployment is configured as an
// aggregate participant for repo — a repo where the aggregate leader owns the
// single required check and this deployment's own check is informational. A
// participant has nothing to publish on a PR that touches none of its schema,
// so it stays silent there rather than adding a passing per-tenant row near the
// merge button. A deployment that is the sole SchemaBot on a repo has no
// aggregate config (role ""), so this returns false and it keeps posting.
func (h *Handler) isAggregateParticipant(repo string) bool {
	config, ok := h.serverConfig()
	if !ok {
		return false
	}
	return config.AggregateRoleForRepo(repo) == api.AggregateRoleParticipant
}

// hasAggregateRole reports whether this deployment is the aggregate leader or
// an aggregate participant for repo.
func (h *Handler) hasAggregateRole(repo string) bool {
	config, ok := h.serverConfig()
	if !ok {
		return false
	}
	role := config.AggregateRoleForRepo(repo)
	return role == api.AggregateRoleLeader || role == api.AggregateRoleParticipant
}

// leaderParticipantsForPR returns the participants this deployment, as the
// aggregate leader for repo, must fold for the PR at headSHA. A PR that selects
// any carries schema work owned by another deployment even when the leader
// itself has none, so the leader's aggregate must fold the participants' Check
// Runs (fail-closed) rather than pass on "no managed schema changes".
//
// listed is the PR's raw changed-file listing and proposed is that listing
// narrowed to what the PR proposes against the default branch. Every path that
// decides whether to fold (the fold itself, auto-plan, and the plan command's
// refresh) asks this one question, so a participant that kept a non-passing run
// on the head after narrowing dropped it gates all of them alike. Non-leaders
// get no participants: the expected set only exists on the leader. A failed
// participant read is returned as an error so the caller publishes nothing.
//
// An environment-scoped leader publishes every aggregate through its own fold,
// which reads the participants it expects under their environment-scoped
// names, so its callers only use the answer to choose how to describe that
// fold. It returns every participant the listed files select without reading
// any Check Run: participants publish no run under the unscoped name there,
// and selecting too many only ever routes to the fold, which fails closed.
func (h *Handler) leaderParticipantsForPR(
	ctx context.Context, client checkRunFinder,
	repo string, pr int, headSHA string,
	listed, proposed []ghclient.PRFile,
) ([]api.ExpectedTenant, error) {
	config, ok := h.serverConfig()
	if !ok {
		return nil, nil
	}
	if len(config.AllowedEnvironments) > 0 {
		h.logger.Debug("environment-scoped leader selects participants from the listed files without reading their Check Runs; its fold reads them per environment",
			"repo", repo, "pr", pr, "head_sha", headSHA)
		return config.ExpectedParticipantChecksForPR(repo, prFilePaths(listed)), nil
	}
	return h.participantsExpectedOnHead(ctx, client, repo, pr, aggregateSentinel, headSHA,
		config.ExpectedParticipantChecksForPR(repo, prFilePaths(listed)),
		config.ExpectedParticipantChecksForPR(repo, prFilePaths(proposed)))
}

// participantPathsTouched reports whether the PR selects this participant's
// managed directories even though no schema config resolved (for example a
// documentation-only change inside a participant's paths).
func (h *Handler) participantPathsTouched(repo string, files []ghclient.PRFile) bool {
	config, ok := h.serverConfig()
	if !ok || !config.RepoHasSchemaDirAllowlist(repo) {
		return false
	}
	for _, file := range prFilePaths(files) {
		if config.SchemaPathAllowedForRepo(repo, file) {
			return true
		}
	}
	return false
}

func (h *Handler) aggregateCheckNameForRepo(repo string) string {
	config, ok := h.serverConfig()
	if !ok {
		return aggregateCheckName
	}
	return config.GitHubCheckNameBaseForRepo(repo)
}

// aggregateCheckTarget pairs an aggregate Check Run name with the environment
// it represents (aggregateSentinel when this instance is not environment-scoped).
type aggregateCheckTarget struct {
	name        string
	environment string
}

// aggregateCheckTargetsForRepo returns the aggregate Check Run name(s) this
// instance publishes for repo: one per allowed environment, or a single
// non-environment-scoped name when AllowedEnvironments is empty. Callers that
// publish aggregates on any commit (PR head or merge-group head) must use these
// names so branch protection's required-check names always match.
func (h *Handler) aggregateCheckTargetsForRepo(repo string) []aggregateCheckTarget {
	base := h.aggregateCheckNameForRepo(repo)
	config, ok := h.serverConfig()
	if !ok || len(config.AllowedEnvironments) == 0 {
		return []aggregateCheckTarget{{name: base, environment: aggregateSentinel}}
	}
	environments, _ := config.CheckEnvironmentsForRepo(context.Background(), nil, repo, "")
	targets := make([]aggregateCheckTarget, 0, len(environments))
	for _, env := range environments {
		targets = append(targets, aggregateCheckTarget{
			name:        aggregateCheckNameForEnv(base, env),
			environment: env,
		})
	}
	return targets
}

func (h *Handler) configuredDatabaseEnvironments(database string) ([]string, error) {
	config, ok := h.serverConfig()
	if !ok {
		return nil, fmt.Errorf("server config is unavailable")
	}
	return config.DatabaseEnvironments(database)
}

func (h *Handler) allowedDatabaseEnvironments(database string) ([]string, error) {
	config, ok := h.serverConfig()
	if !ok {
		return nil, fmt.Errorf("server config is unavailable")
	}
	environments, err := config.DatabaseEnvironments(database)
	if err != nil {
		return nil, fmt.Errorf("resolve configured environments for database %q: %w", database, err)
	}
	if len(config.AllowedEnvironments) == 0 {
		return environments, nil
	}
	allowed := make([]string, 0, len(environments))
	for _, environment := range environments {
		if config.IsEnvironmentAllowed(environment) {
			allowed = append(allowed, environment)
		}
	}
	return allowed, nil
}

func (h *Handler) attachServerEnvironments(schemaResult *ghclient.SchemaRequestResult, environment string) error {
	if schemaResult == nil {
		return fmt.Errorf("schema request result is nil")
	}
	environments, err := h.configuredDatabaseEnvironments(schemaResult.Database)
	if err != nil {
		return fmt.Errorf("resolve configured environments for database %q: %w", schemaResult.Database, err)
	}
	if environment != "" && !slices.Contains(environments, environment) {
		return &environmentNotConfiguredError{Database: schemaResult.Database, Environment: environment}
	}
	schemaResult.Environments = environments
	return nil
}

// filterChecksByEnvironment returns only stored check state for the given environment.
// Aggregate state is excluded.
func filterChecksByEnvironment(checks []*storage.Check, env string) []*storage.Check {
	var filtered []*storage.Check
	for _, c := range checks {
		if isAggregateCheck(c) {
			continue
		}
		if c.Environment == env {
			filtered = append(filtered, c)
		}
	}
	return filtered
}

// isAggregateCheck returns true if stored check state is aggregate, not per-database.
// Aggregate state uses the sentinel value for DatabaseType and DatabaseName.
// The Environment field is either aggregateSentinel (global aggregate) or a real
// environment name (per-environment aggregate when allowed_environments is configured).
func isAggregateCheck(c *storage.Check) bool {
	return checkstate.IsAggregate(c)
}

// checkHasStartedApply returns true once work may already have reached, or may
// still reach, the live database. ApplyID remains set on terminal apply-owned
// rows so later PR commits cannot clean them up as plan-only state.
func checkHasStartedApply(c *storage.Check) bool {
	return c.Status == checkStatusInProgress || c.ApplyID != 0
}

func checkBlockedByRemovedSchemaAfterApply(c *storage.Check) bool {
	return c.BlockingReason == schemaRemovedAfterApplyBlock.blockingReason
}

func checkBlocksPassingAggregate(c *storage.Check) bool {
	if isAggregateCheck(c) {
		return false
	}
	if checkHasStartedApply(c) {
		return true
	}
	return c.Conclusion == checkConclusionFailure || c.Conclusion == checkConclusionActionRequired
}

func hasBlockingCheckForEnvironment(checks []*storage.Check, environment string) bool {
	for _, c := range checks {
		if environment != aggregateSentinel && c.Environment != environment {
			continue
		}
		if checkBlocksPassingAggregate(c) {
			return true
		}
	}
	return false
}

// awaitingCurrentCommitTitle is the aggregate title shown when only results
// recorded for another commit hold the aggregate open — nothing is running for
// the commit the Check Run is published on, and the current commit's own plan
// or apply results are still pending.
const awaitingCurrentCommitTitle = "Awaiting results for the latest commit"

// normalizeStaleContributions returns the fold contributions for headSHA.
// A row stored for a different commit contributes a blocking in-progress
// placeholder instead of its stored status and conclusion: results computed
// for another commit say nothing about the current one, so they must hold the
// aggregate open until the current commit's plan or apply results replace
// them. Rows for the current commit pass through unchanged, and so does a
// failed apply's row on any commit (see settledApplyFailure).
func normalizeStaleContributions(checks []*storage.Check, headSHA string) (contributions []*storage.Check, staleCount int) {
	contributions = make([]*storage.Check, 0, len(checks))
	for _, c := range checks {
		if c.HeadSHA == headSHA || settledApplyFailure(c) {
			contributions = append(contributions, c)
			continue
		}
		stale := *c
		stale.Status = checkStatusInProgress
		stale.Conclusion = ""
		contributions = append(contributions, &stale)
		staleCount++
	}
	return contributions, staleCount
}

// settledApplyFailure reports whether c is the outcome of an apply that
// settled failed. The row keeps naming the commit the apply started on, which
// is older than the PR head whenever a push landed while the apply ran. Its
// outcome still describes the target, so it folds as the failure it is rather
// than as a placeholder awaiting results for the newer commit: the Check Run
// says the apply failed, and a failure can only block the aggregate, never
// pass it.
func settledApplyFailure(c *storage.Check) bool {
	return c.ApplyID != 0 && c.Status == checkStatusCompleted && c.Conclusion == checkConclusionFailure
}

// failedPlan reports whether c is one of this deployment's own databases
// whose plan failed before any apply started: no schema change ran against the
// database.
func failedPlan(c *storage.Check) bool {
	if isAggregateCheck(c) || isParticipantCheck(c) {
		return false
	}
	return c.ApplyID == 0 && c.Status == checkStatusCompleted && c.Conclusion == checkConclusionFailure
}

// blockedPlan reports whether c is a plan that completed and named a change
// the engine refuses to run. It fails the aggregate as a plan error does, but
// nothing failed: the plan worked, and the change it found is one no apply can
// carry out. A plan that also reported errors stores planFailedCheckSummary in
// place of its change summary, and a durable block carries a reason of its
// own, so neither reads as blocked.
func blockedPlan(c *storage.Check) bool {
	return failedPlan(c) &&
		c.HasChanges &&
		c.BlockingReason == "" &&
		c.ChangeSummary != "" &&
		c.ChangeSummary != planFailedCheckSummary
}

// failedAggregateTitle names what failed in a failing aggregate. A failed
// apply means a schema change ran against a database, so it names the Check
// Run whenever one is present. When every failing row is a plan, the title
// says what happened to the plan: "Apply failed" would tell the reader their
// database was changed when nothing ran. A plan error outranks a blocked plan,
// since it is the one the reader has to investigate. Any other mix keeps
// "Apply failed".
func failedAggregateTitle(checks []*storage.Check) string {
	planFailures, planBlocks := 0, 0
	for _, c := range checks {
		switch {
		case settledApplyFailure(c):
			return "Apply failed"
		case blockedPlan(c):
			planBlocks++
		case failedPlan(c):
			planFailures++
		case failsAggregate(c):
			return "Apply failed"
		}
	}
	if planFailures > 0 {
		return planFailedCheckText
	}
	if planBlocks > 0 {
		return planBlockedCheckText
	}
	return "Apply failed"
}

// blockedPlanRemedy says what clears a blocked plan, or returns "" when no
// contribution is one. The plan comment already names each blocked change and
// why, so the remedy points there rather than repeating it.
func blockedPlanRemedy(checks []*storage.Check) string {
	if !slices.ContainsFunc(checks, blockedPlan) {
		return ""
	}
	return "\n\n" + blockedPlanRemedyText
}

// blockedPlanRemedyText is the remedy a blocked plan's aggregate ends with.
const blockedPlanRemedyText = "The plan comment names each blocked change and why. " +
	"Rewrite the change into a form SchemaBot runs, or take it out of the schema files, and push."

// failedApplyRemedyBudget bounds the failed-apply remedy, so a pull request
// with many failed databases still leaves the table its room under the Check
// Run text limit.
const failedApplyRemedyBudget = maxCheckRunTextLength / 8

// failedApplyRemedy names what clears a failed apply's block, or returns ""
// when no contribution is a failed apply. Each failed database gets its own
// plan, carrying the deployment's tenant so the pasted command reaches this
// deployment, and `checks show` names the same command for the same row
// (checkstate.PlanCommand). When any failed row's change was rolled back or
// is no longer in the pull request, the remedy does not offer to apply it
// again. The commands stop at failedApplyRemedyBudget; the rest are counted,
// and the table above still lists every failed database.
func failedApplyRemedy(checks []*storage.Check, tenant string) string {
	type target struct{ environment, database string }
	var failed []target
	remedy := checkstate.ReconciliationRemedy
	for _, c := range checks {
		if !settledApplyFailure(c) {
			continue
		}
		if checkstate.ReapplyIsNotTheRemedy(c.BlockingReason) {
			remedy = checkstate.MatchTargetRemedy
		}
		t := target{c.Environment, c.DatabaseName}
		if !slices.Contains(failed, t) {
			failed = append(failed, t)
		}
	}
	if len(failed) == 0 {
		return ""
	}
	slices.SortFunc(failed, func(a, b target) int {
		return cmp.Or(cmp.Compare(a.environment, b.environment), cmp.Compare(a.database, b.database))
	})
	var commands []string
	size := 0
	for _, t := range failed {
		command := checkstate.PlanCommand(t.environment, t.database, tenant)
		if len(commands) > 0 && size+len(command) > failedApplyRemedyBudget {
			break
		}
		commands = append(commands, command)
		size += len(command)
	}
	return "\n**To clear this:** " + remedy(commands...) + otherFailedDatabases(len(failed)-len(commands)) + "\n"
}

// otherFailedDatabases points at the failed databases the remedy had no room
// to name, or returns "" when it named them all.
func otherFailedDatabases(omitted int) string {
	switch {
	case omitted <= 0:
		return ""
	case omitted == 1:
		return " Plan the other failed database in the table the same way."
	default:
		return fmt.Sprintf(" Plan the other %d failed databases in the table the same way.", omitted)
	}
}

// anyInProgressOnCommit reports whether any check recorded for headSHA is
// genuinely in progress, as opposed to a stale-row placeholder.
func anyInProgressOnCommit(checks []*storage.Check, headSHA string) bool {
	for _, c := range checks {
		if c.HeadSHA == headSHA && c.Status == checkStatusInProgress {
			return true
		}
	}
	return false
}

// failsAggregate reports whether a completed check fails the aggregate: a
// success that still carries a blocking reason, or any conclusion other than
// success or action required.
func failsAggregate(c *storage.Check) bool {
	if c.Status != checkStatusCompleted {
		return false
	}
	if c.BlockingReason != "" && c.Conclusion == checkConclusionSuccess {
		return true
	}
	switch c.Conclusion {
	case checkConclusionSuccess, checkConclusionActionRequired:
		return false
	default:
		return true
	}
}

// computeAggregate determines the aggregate conclusion and status from
// per-database checks. stopped names the applies resting in the stopped state;
// nil reads as "nothing is stopped".
func computeAggregate(checks []*storage.Check, stopped stoppedApplyIDs) (conclusion, status string) {
	// in_progress takes precedence — the aggregate should show running. A row
	// held open by a stopped apply is not running work: it waits on an
	// operator to start or cancel the apply, so it folds as action_required
	// below, still blocking merge, rather than as a run that never finishes.
	heldByStoppedApply := false
	for _, c := range checks {
		if c.Status == checkStatusCompleted {
			continue
		}
		if c.Status == checkStatusInProgress && stopped.holds(c) {
			heldByStoppedApply = true
			continue
		}
		return "", checkStatusInProgress
	}

	// Every remaining check is completed or held by a stopped apply — compute
	// the conclusion.
	if slices.ContainsFunc(checks, failsAggregate) {
		return checkConclusionFailure, checkStatusCompleted
	}
	if heldByStoppedApply {
		return checkConclusionActionRequired, checkStatusCompleted
	}
	for _, c := range checks {
		if c.Conclusion == checkConclusionActionRequired {
			return checkConclusionActionRequired, checkStatusCompleted
		}
	}

	return checkConclusionSuccess, checkStatusCompleted
}

// aggregateSummary builds a human-readable title and markdown summary for the aggregate check.
// tenant is the deployment's own tenant, which the commands the summary names
// carry; pass "" for an untenanted deployment.
func aggregateSummary(checks []*storage.Check, conclusion string, stopped stoppedApplyIDs, progress rolloutProgress, tenant string) (title, summary string) {
	switch conclusion {
	case checkConclusionSuccess:
		if allChecksAreUpToDate(checks) {
			title = "Schema up to date"
		} else {
			title = "All applies complete"
		}
		summary = buildAggregateTable(checks, stopped, progress)
	case checkConclusionFailure:
		title = failedAggregateTitle(checks)
		remedy := failedApplyRemedy(checks, tenant) + blockedPlanRemedy(checks)
		summary = buildAggregateTableWithin(checks, stopped, progress, len(remedy)) + remedy
	case checkConclusionActionRequired:
		pending := 0
		unresolved := 0
		tenantApplies := 0
		stoppedApplies := 0
		var pendingCheck *storage.Check
		for _, c := range checks {
			if c.Status == checkStatusInProgress && stopped.holds(c) {
				stoppedApplies++
				continue
			}
			if c.Conclusion != checkConclusionActionRequired {
				continue
			}
			pending++
			pendingCheck = c
			switch {
			case isUnresolvedParticipantCheck(c):
				unresolved++
			case isParticipantCheck(c):
				tenantApplies++
			}
		}
		switch {
		case pending == 0 && stoppedApplies > 0:
			// Every row holding the aggregate is a stopped apply, so the PR
			// is waiting on an operator to start or cancel, not on work.
			title = stoppedAppliesTitle(stoppedApplies)
		case pending > 0 && unresolved == pending:
			// Every blocking row is a participant whose Check Run could not be
			// read; no apply is actually pending, the participants are silent.
			if pending == 1 {
				title = "1 participant deployment has not reported"
			} else {
				title = fmt.Sprintf("%d participant deployments have not reported", pending)
			}
		case unresolved > 0:
			// Real pending applies alongside participants whose Check Runs
			// could not be read. Surface both: an unread participant is not a
			// pending apply, and folding it into the apply count would hide
			// the reporting gap an operator needs to investigate.
			applies := pending - unresolved
			appliesPart := fmt.Sprintf("%d applies pending", applies)
			if applies == 1 {
				appliesPart = "1 apply pending"
			}
			unresolvedPart := fmt.Sprintf("%d participants have not reported", unresolved)
			if unresolved == 1 {
				unresolvedPart = "1 participant has not reported"
			}
			title = appliesPart + ", " + unresolvedPart
		case pending > 0 && tenantApplies == pending:
			// Every blocking row is a tenant deployment's folded check — say
			// so, since "apply pending" alone reads as this deployment's own
			// work when the pending applies belong to the tenants.
			if pending == 1 {
				title = "1 tenant apply pending"
			} else {
				title = fmt.Sprintf("%d tenant applies pending", pending)
			}
		case tenantApplies > 0:
			title = fmt.Sprintf("%d applies pending (%d tenant)", pending, tenantApplies)
		case pending == 1:
			title = "1 apply pending" + rolloutTargetsPhrase(pendingCheck)
		default:
			title = fmt.Sprintf("%d applies pending", pending)
		}
		if pending > 0 && stoppedApplies > 0 {
			title = fmt.Sprintf("%s (%d stopped)", title, stoppedApplies)
		}
		summary = buildAggregateTable(checks, stopped, progress)
	default:
		// in_progress — conclusion is empty
		inProgress := 0
		waiting := 0
		paused := 0
		var inProgressCheck *storage.Check
		for _, c := range checks {
			if c.Status != checkStatusInProgress {
				continue
			}
			inProgress++
			inProgressCheck = c
			switch {
			case isUnresolvedParticipantCheck(c):
				waiting++
			case stopped.holds(c):
				paused++
			}
		}
		switch {
		case inProgress > 0 && waiting == inProgress:
			// Nothing is applying; the fold is waiting for participant Check
			// Runs a scheduled re-fold will re-read shortly.
			if waiting == 1 {
				title = "Waiting for 1 participant deployment to report"
			} else {
				title = fmt.Sprintf("Waiting for %d participant deployments to report", waiting)
			}
		case paused > 0:
			title = fmt.Sprintf("Apply in progress (%d stopped)", paused)
		case inProgress == 1:
			title = "Apply in progress" + rolloutTargetsPhrase(inProgressCheck) + rolloutDoneSuffix(inProgressCheck, progress)
		default:
			title = "Apply in progress"
		}
		summary = buildAggregateTable(checks, stopped, progress)
	}
	return title, summary
}

// rolloutTargetsPhrase says how many of a rollout's targets a row's work is on,
// for a title that speaks for that one row, so the PR's merge box shows how far
// the change has rolled out. A row that names no rollout of several targets
// adds nothing. Titles over several rows leave it out: counts from different
// databases do not add up to anything a reader can act on.
func rolloutTargetsPhrase(c *storage.Check) string {
	if c.RolloutTargets <= 1 || c.RolloutTargetsWithWork <= 0 {
		return ""
	}
	if c.RolloutTargetsWithWork >= c.RolloutTargets {
		return fmt.Sprintf(" on all %d targets", c.RolloutTargets)
	}
	return fmt.Sprintf(" on %d of %d targets", c.RolloutTargetsWithWork, c.RolloutTargets)
}

// stoppedAppliesTitle names the stopped applies holding the aggregate.
func stoppedAppliesTitle(n int) string {
	if n == 1 {
		return "1 apply stopped"
	}
	return fmt.Sprintf("%d applies stopped", n)
}

// isStoppedAppliesAggregate reports whether an aggregate Check Run concluded
// because stopped applies are all that hold it, the case the fold titles with
// stoppedAppliesTitle. It is read off the title because that is the only place
// a deployment that cannot see the applies learns why the run concluded; the
// conclusion alone is the same as for changes no apply has run yet.
func isStoppedAppliesAggregate(run *ghclient.CheckRunResult) bool {
	if run.Status != checkStatusCompleted || run.Conclusion != checkConclusionActionRequired {
		return false
	}
	var n int
	if _, err := fmt.Sscanf(run.Title, "%d", &n); err != nil {
		return false
	}
	return n > 0 && run.Title == stoppedAppliesTitle(n)
}

// stoppedApplyIDs is the set of applies resting in the stopped state, so the
// fold can tell a check held open by a paused apply from one held open by work
// still running. The distinction is the operator's next move: a stopped apply
// holds its database until someone starts or cancels it, and calling that
// "Apply in progress" sends the reader off to watch progress that will never
// arrive. Resolved per fold rather than stored on the row, so a resume needs no
// second write to undo it. A nil set reads as "nothing is stopped".
type stoppedApplyIDs map[int64]bool

// holds reports whether the apply that owns this check is stopped.
func (s stoppedApplyIDs) holds(c *storage.Check) bool {
	return c.ApplyID != 0 && s[c.ApplyID]
}

// isUnresolvedParticipantCheck reports whether a synthesized participant row
// blocks only because the participant's Check Run could not be read (not
// reported yet, or a failed Checks API read) — the aggregate should present it
// as a reporting gap, never as a pending or failed apply.
func isUnresolvedParticipantCheck(c *storage.Check) bool {
	return c.BlockingReason == participantUnresolvedBlockingReason
}

// isParticipantCheck reports whether a check is a participant deployment's
// outcome folded into the leader's aggregate, rather than one of the leader's
// own per-database checks. Participant outcomes carry the aggregate sentinel as
// their database type and the tenant name as their database name.
func isParticipantCheck(c *storage.Check) bool {
	return c.DatabaseType == aggregateSentinel && c.DatabaseName != aggregateSentinel
}

// buildAggregateTable renders the aggregate check's summary: the leader's own
// per-database checks in a Database table, and — when the leader gates on
// participant deployments — each participant's rolled-up status in a separate
// Tenant deployments table, so a reader can tell "my databases" from "the other
// tenants I'm gating on" at a glance. Participant gating is the key information,
// so the Tenant deployments section is rendered first and its size reserved: it
// is never dropped, even when many per-database rows would otherwise fill the
// Check Run text limit — those rows truncate instead.
func buildAggregateTable(checks []*storage.Check, stopped stoppedApplyIDs, progress rolloutProgress) string {
	return buildAggregateTableWithin(checks, stopped, progress, 0)
}

// buildAggregateTableWithin is buildAggregateTable leaving reserved bytes of
// the Check Run text limit free for text the summary adds after the table.
func buildAggregateTableWithin(checks []*storage.Check, stopped stoppedApplyIDs, progress rolloutProgress, reserved int) string {
	var dbChecks, participantChecks []*storage.Check
	for _, c := range checks {
		if isParticipantCheck(c) {
			participantChecks = append(participantChecks, c)
		} else {
			dbChecks = append(dbChecks, c)
		}
	}

	tenantSection := renderParticipantSection(participantChecks)
	// The progress lines get a quarter of the text limit, so the Database
	// table always keeps room for its own rows.
	progressSection := renderRolloutProgress(dbChecks, progress, stopped, maxCheckRunTextLength/4)
	dbSection := renderDatabaseSection(dbChecks, maxCheckRunTextLength-1000-reserved-len(tenantSection)-len(progressSection), stopped, progress)

	var sb strings.Builder
	sb.WriteString(dbSection)
	sb.WriteString(progressSection)
	if tenantSection != "" {
		if sb.Len() > 0 {
			sb.WriteString("\n")
		}
		sb.WriteString(tenantSection)
	}
	return sb.String()
}

// changeSummaryLabel returns the per-database change summary for the aggregate
// check's Change column, or an em dash when no summary was recorded (aggregate
// rows, or rows recorded before the summary was stored).
func changeSummaryLabel(c *storage.Check) string {
	if c.ChangeSummary == "" {
		return "—"
	}
	return c.ChangeSummary
}

// renderDatabaseSection renders the leader's own per-database checks as a
// Database table with each database's type, change summary, and status,
// truncating to stay within budget bytes. Per-environment aggregates contain a
// single environment, so the environment is not repeated per row. The global
// aggregate (no allowed_environments) can fold databases from several
// environments into one table, so an Environment column is added when the rows
// span more than one environment — otherwise staging and production rows for the
// same database would be indistinguishable.
func renderDatabaseSection(dbChecks []*storage.Check, budget int, stopped stoppedApplyIDs, progress rolloutProgress) string {
	if len(dbChecks) == 0 {
		return ""
	}
	showEnv := hasMultipleEnvironments(dbChecks)
	var sb strings.Builder
	if showEnv {
		sb.WriteString("| Database | Environment | Type | Change | Status |\n")
		sb.WriteString("|----------|-------------|------|--------|--------|\n")
	} else {
		sb.WriteString("| Database | Type | Change | Status |\n")
		sb.WriteString("|----------|------|--------|--------|\n")
	}
	for i, c := range dbChecks {
		var row string
		if showEnv {
			row = fmt.Sprintf("| `%s` | %s | %s | %s | %s |\n", c.DatabaseName, c.Environment, c.DatabaseType, changeSummaryLabel(c), checkStatusLabel(c, stopped, progress))
		} else {
			row = fmt.Sprintf("| `%s` | %s | %s | %s |\n", c.DatabaseName, c.DatabaseType, changeSummaryLabel(c), checkStatusLabel(c, stopped, progress))
		}
		if sb.Len()+len(row) > budget {
			fmt.Fprintf(&sb, "\n... and %d more check(s)\n", len(dbChecks)-i)
			break
		}
		sb.WriteString(row)
	}
	return sb.String()
}

// hasMultipleEnvironments reports whether the per-database checks span more than
// one distinct environment, which happens for the global aggregate that is not
// scoped to a single environment.
func hasMultipleEnvironments(dbChecks []*storage.Check) bool {
	seen := make(map[string]struct{}, 2)
	for _, c := range dbChecks {
		seen[c.Environment] = struct{}{}
		if len(seen) > 1 {
			return true
		}
	}
	return false
}

// renderParticipantSection renders the folded participant deployments as a
// Tenant table keyed by tenant, truncating within the Check Run text limit.
func renderParticipantSection(participantChecks []*storage.Check) string {
	if len(participantChecks) == 0 {
		return ""
	}
	var sb strings.Builder
	sb.WriteString("**Tenant deployments**\n\n")
	sb.WriteString("| Tenant | Status |\n")
	sb.WriteString("|--------|--------|\n")
	for i, c := range participantChecks {
		row := fmt.Sprintf("| `%s` | %s |\n", c.DatabaseName, checkStatusLabel(c, nil, nil))
		if sb.Len()+len(row) > maxCheckRunTextLength-1000 {
			fmt.Fprintf(&sb, "\n... and %d more tenant(s)\n", len(participantChecks)-i)
			break
		}
		sb.WriteString(row)
	}
	return sb.String()
}

func allChecksAreUpToDate(checks []*storage.Check) bool {
	if len(checks) == 0 {
		return false
	}
	for _, c := range checks {
		if c.Status != checkStatusCompleted || c.Conclusion != checkConclusionSuccess || c.HasChanges {
			return false
		}
	}
	return true
}

func checkStatusLabel(c *storage.Check, stopped stoppedApplyIDs, progress rolloutProgress) string {
	if c.Status == checkStatusCompleted && c.Conclusion == checkConclusionSuccess && !c.HasChanges {
		return "Up to date"
	}
	// A stopped apply's check is still in progress so merge stays blocked; the
	// row says which so the table agrees with the title.
	if c.Status == checkStatusInProgress && stopped.holds(c) {
		return state.Label(state.Apply.Stopped)
	}
	if isUnresolvedParticipantCheck(c) {
		if c.Status == checkStatusInProgress {
			return "Waiting to report"
		}
		return "Not reported"
	}
	if c.Status == checkStatusInProgress {
		return rolloutStatusLabel(c, progress)
	}
	if blockedPlan(c) {
		return "Blocked"
	}
	return conclusionEmoji(c.Status, c.Conclusion)
}

// conclusionEmoji returns a short status label for a check.
func conclusionEmoji(status, conclusion string) string {
	if status == checkStatusInProgress {
		return "In progress"
	}
	switch conclusion {
	case checkConclusionSuccess:
		return "Applied"
	case checkConclusionFailure:
		return "Failed"
	case checkConclusionActionRequired:
		return "Pending"
	case checkConclusionNeutral:
		return "Cancelled"
	default:
		return conclusion
	}
}
