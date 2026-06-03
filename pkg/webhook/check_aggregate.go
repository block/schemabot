package webhook

import (
	"fmt"
	"strings"

	"github.com/block/schemabot/pkg/storage"
)

// aggregateCheckNameForEnv returns the environment-scoped aggregate check name.
// e.g., "SchemaBot (staging)" or "Custom SchemaBot (production)".
func aggregateCheckNameForEnv(baseName, env string) string {
	return fmt.Sprintf("%s (%s)", baseName, env)
}

func (h *Handler) aggregateCheckNameForRepo(repo string) string {
	if h == nil || h.service == nil || h.service.Config() == nil {
		return aggregateCheckName
	}
	return h.service.Config().GitHubCheckNameBaseForRepo(repo)
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
		title = "All applies complete"
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
		row := fmt.Sprintf("| `%s` | %s | %s |\n", c.DatabaseName, c.Environment, conclusionEmoji(c.Status, c.Conclusion))
		if sb.Len()+len(row) > maxCheckRunTextLength-1000 {
			fmt.Fprintf(&sb, "\n... and %d more check(s)\n", len(checks)-i)
			break
		}
		sb.WriteString(row)
	}

	return sb.String()
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
