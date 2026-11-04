package webhook

import (
	"fmt"
	"net/url"
	"slices"
	"strings"

	"github.com/block/schemabot/pkg/storage"
)

const prerequisiteContribution = "local_environment_prerequisite"

// withLocalPrerequisites folds raw earlier outcomes, never the earlier rendered
// aggregate. The copies are ephemeral: a completed prerequisite unblocks a later
// clean plan without having to rewrite that plan's own stored result.
func (h *Handler) withLocalPrerequisites(checks, all []*storage.Check, environment, repo string, pr int, headSHA string) []*storage.Check {
	result := slices.Clone(checks)
	relevant := h.service.Config().CheckRowsForEnvironment(all, environment, repo, pr, headSHA)
	for _, evidence := range relevant {
		if evidence.Environment == environment {
			continue
		}
		contribution := *evidence
		contribution.BlockingReason = prerequisiteContribution
		result = append(result, &contribution)
	}
	return result
}

func prerequisiteSummary(checks []*storage.Check, base, repo, headSHA string) string {
	var lines []string
	for _, check := range checks {
		if check.BlockingReason != prerequisiteContribution {
			continue
		}
		name := aggregateCheckNameForEnv(base, check.Environment)
		if isParticipantCheck(check) && check.ChangeSummary != "" {
			name = check.ChangeSummary
		}
		link := fmt.Sprintf("https://github.com/%s/commit/%s/checks?check_name=%s", repo, headSHA, url.QueryEscape(name))
		lines = append(lines, fmt.Sprintf("Waiting for %s: %s. See [%s](%s).", sanitizeCheckRunErrorSummary(check.Environment), sanitizeCheckRunErrorSummary(check.DatabaseName), sanitizeCheckRunErrorSummary(name), link))
	}
	slices.Sort(lines)
	var summary strings.Builder
	const omitted = "More prerequisites remain; open the environment checks for all results."
	for _, line := range lines {
		if summary.Len()+len(line)+len(omitted)+4 > maxCheckRunTextLength/3 {
			summary.WriteString("\n\n" + omitted)
			break
		}
		if summary.Len() > 0 {
			summary.WriteString("\n\n")
		}
		summary.WriteString(line)
	}
	return summary.String()
}

// appendPrerequisiteSummary reserves space for the actionable notice while
// retaining complete rows from the later environment's own result table.
func appendPrerequisiteSummary(summary, notice string) string {
	const omitted = "\n\nMore database results omitted."
	limit := maxCheckRunTextLength - len(notice) - len(omitted) - 2
	if len(summary) > limit {
		if rowEnd := strings.LastIndex(summary[:limit], "\n"); rowEnd >= 0 {
			summary = summary[:rowEnd]
		} else {
			summary = summary[:limit]
		}
		summary += omitted
	}
	return summary + "\n\n" + notice
}
