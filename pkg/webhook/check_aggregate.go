package webhook

import (
	"fmt"
	"slices"
	"strings"

	"github.com/block/schemabot/pkg/api"
	ghclient "github.com/block/schemabot/pkg/github"
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

// leaderExpectsParticipantsForPR reports whether this deployment is the
// aggregate leader for repo and the PR's changed files touch at least one
// expected participant's paths. Such a PR carries schema work owned by another
// deployment even when the leader itself has none, so the leader's aggregate
// must fold the participants' Check Runs (fail-closed) rather than pass on
// "no managed schema changes". Returns false for non-leaders — the expected set
// only exists on the leader.
func (h *Handler) leaderExpectsParticipantsForPR(repo string, files []ghclient.PRFile) bool {
	config, ok := h.serverConfig()
	if !ok {
		return false
	}
	return len(config.ExpectedParticipantChecksForPR(repo, prFilePaths(files))) > 0
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
	targets := make([]aggregateCheckTarget, 0, len(config.AllowedEnvironments))
	for _, env := range config.AllowedEnvironments {
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
		return fmt.Errorf("database %q environment %q is not configured on this server", schemaResult.Database, environment)
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
	return c.DatabaseType == aggregateSentinel &&
		c.DatabaseName == aggregateSentinel
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
// them. Rows for the current commit pass through unchanged.
func normalizeStaleContributions(checks []*storage.Check, headSHA string) (contributions []*storage.Check, staleCount int) {
	contributions = make([]*storage.Check, 0, len(checks))
	for _, c := range checks {
		if c.HeadSHA == headSHA {
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

// computeAggregate determines the aggregate conclusion and status from per-database checks.
func computeAggregate(checks []*storage.Check) (conclusion, status string) {
	// in_progress takes precedence — the aggregate should show running
	for _, c := range checks {
		if c.Status == checkStatusInProgress {
			return "", checkStatusInProgress
		}
	}

	// All checks are completed — compute conclusion
	for _, c := range checks {
		if c.Conclusion == checkConclusionFailure {
			return checkConclusionFailure, checkStatusCompleted
		}
	}
	for _, c := range checks {
		if c.Conclusion == checkConclusionActionRequired {
			return checkConclusionActionRequired, checkStatusCompleted
		}
	}

	return checkConclusionSuccess, checkStatusCompleted
}

// aggregateSummary builds a human-readable title and markdown summary for the aggregate check.
func aggregateSummary(checks []*storage.Check, conclusion string) (title, summary string) {
	switch conclusion {
	case checkConclusionSuccess:
		if allChecksAreUpToDate(checks) {
			title = "Schema up to date"
		} else {
			title = "All applies complete"
		}
		summary = buildAggregateTable(checks)
	case checkConclusionFailure:
		title = "Apply failed"
		summary = buildAggregateTable(checks)
	case checkConclusionActionRequired:
		pending := 0
		unresolved := 0
		for _, c := range checks {
			if c.Conclusion != checkConclusionActionRequired {
				continue
			}
			pending++
			if isUnresolvedParticipantCheck(c) {
				unresolved++
			}
		}
		switch {
		case pending > 0 && unresolved == pending:
			// Every blocking row is a participant whose Check Run could not be
			// read; no apply is actually pending, the participants are silent.
			if pending == 1 {
				title = "1 participant deployment has not reported"
			} else {
				title = fmt.Sprintf("%d participant deployments have not reported", pending)
			}
		case pending == 1:
			title = "1 apply pending"
		default:
			title = fmt.Sprintf("%d applies pending", pending)
		}
		summary = buildAggregateTable(checks)
	default:
		// in_progress — conclusion is empty
		inProgress := 0
		waiting := 0
		for _, c := range checks {
			if c.Status != checkStatusInProgress {
				continue
			}
			inProgress++
			if isUnresolvedParticipantCheck(c) {
				waiting++
			}
		}
		if inProgress > 0 && waiting == inProgress {
			// Nothing is applying; the fold is waiting for participant Check
			// Runs a scheduled re-fold will re-read shortly.
			if waiting == 1 {
				title = "Waiting for 1 participant deployment to report"
			} else {
				title = fmt.Sprintf("Waiting for %d participant deployments to report", waiting)
			}
		} else {
			title = "Apply in progress"
		}
		summary = buildAggregateTable(checks)
	}
	return title, summary
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
func buildAggregateTable(checks []*storage.Check) string {
	var dbChecks, participantChecks []*storage.Check
	for _, c := range checks {
		if isParticipantCheck(c) {
			participantChecks = append(participantChecks, c)
		} else {
			dbChecks = append(dbChecks, c)
		}
	}

	tenantSection := renderParticipantSection(participantChecks)
	dbSection := renderDatabaseSection(dbChecks, maxCheckRunTextLength-1000-len(tenantSection))

	var sb strings.Builder
	sb.WriteString(dbSection)
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
func renderDatabaseSection(dbChecks []*storage.Check, budget int) string {
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
			row = fmt.Sprintf("| `%s` | %s | %s | %s | %s |\n", c.DatabaseName, c.Environment, c.DatabaseType, changeSummaryLabel(c), checkStatusLabel(c))
		} else {
			row = fmt.Sprintf("| `%s` | %s | %s | %s |\n", c.DatabaseName, c.DatabaseType, changeSummaryLabel(c), checkStatusLabel(c))
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
		row := fmt.Sprintf("| `%s` | %s |\n", c.DatabaseName, checkStatusLabel(c))
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

func checkStatusLabel(c *storage.Check) string {
	if c.Status == checkStatusCompleted && c.Conclusion == checkConclusionSuccess && !c.HasChanges {
		return "Up to date"
	}
	if isUnresolvedParticipantCheck(c) {
		if c.Status == checkStatusInProgress {
			return "Waiting to report"
		}
		return "Not reported"
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
