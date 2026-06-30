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
		for _, c := range checks {
			if c.Conclusion == checkConclusionActionRequired {
				pending++
			}
		}
		if pending == 1 {
			title = "1 apply pending"
		} else {
			title = fmt.Sprintf("%d applies pending", pending)
		}
		summary = buildAggregateTable(checks)
	default:
		// in_progress — conclusion is empty
		title = "Apply in progress"
		summary = buildAggregateTable(checks)
	}
	return title, summary
}

// buildAggregateTable builds a markdown table showing the status of each per-database check.
// Truncates to stay within GitHub's check run output limits.
func buildAggregateTable(checks []*storage.Check) string {
	var sb strings.Builder
	sb.WriteString("| Database | Environment | Status |\n")
	sb.WriteString("|----------|-------------|--------|\n")

	for i, c := range checks {
		row := fmt.Sprintf("| `%s` | %s | %s |\n", c.DatabaseName, c.Environment, checkStatusLabel(c))
		if sb.Len()+len(row) > maxCheckRunTextLength-1000 {
			fmt.Fprintf(&sb, "\n... and %d more check(s)\n", len(checks)-i)
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
