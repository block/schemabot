package webhook

import (
	"fmt"
	"strings"

	"github.com/block/schemabot/pkg/api"
	"github.com/block/schemabot/pkg/storage"
	"github.com/block/schemabot/pkg/webhook/templates"
)

// changeGateRefusalDetail renders what a freeze refusal tells the commenter:
// what blocks the change and what to do next. command is the refused PR
// command; one that takes --override is told how to ask for one. tenant is the
// deployment's own, carried by the approve command the detail names, so a
// pasted command reaches the deployment that stores the request. Every reason
// is a person's text, so a mention in it notifies nobody.
//
// The request is approved by someone other than the commenter and the pull
// request's author, and then the commenter runs the command again: an approved
// override admits only the person who asked for it.
func changeGateRefusalDetail(refused *api.ChangeGateRefusedError, command, tenant string) string {
	// Reasons are clamped and at most maxListedBlockers are named, so the
	// detail stays inside templates.MaxChangeGateRefusalLen and the request
	// and its instructions at the end are never cut off.
	listed := *refused
	unlisted := 0
	if len(listed.Blockers) > maxListedBlockers {
		unlisted = len(listed.Blockers) - maxListedBlockers
		listed.Blockers = listed.Blockers[:maxListedBlockers]
	}
	detail := listed.BlockedWith(func(reason string) string {
		return templates.ErrorDetailReason(clampReason(reason, maxListedFreezeReason))
	}) + "."
	switch {
	case unlisted == 1:
		detail += " 1 more freeze also blocks it."
	case unlisted > 1:
		detail += fmt.Sprintf(" %d more freezes also block it.", unlisted)
	}
	switch request := refused.OverrideRequest; {
	case request != nil:
		requester := strings.TrimPrefix(request.RequestedBy, storage.GitHubIdentityPrefix)
		author := strings.TrimPrefix(request.PRAuthor, storage.GitHubIdentityPrefix)
		detail += fmt.Sprintf(" Override request `%s` was recorded.", request.Identifier)
		if reason := clampReason(strings.TrimSpace(request.Reason), maxShownOverrideReason); reason != "" {
			detail += " Reason the approver sees: " + templates.ErrorDetailReason(reason)
			if !strings.HasSuffix(reason, ".") {
				detail += "."
			}
		}
		if refused.OverrideReasonKept {
			detail += " You asked for it earlier, so it keeps the reason you gave then."
		}
		approver := fmt.Sprintf("A freeze approver other than @%s and other than the PR author (@%s)", requester, author)
		if strings.EqualFold(requester, author) {
			approver = fmt.Sprintf("A freeze approver other than @%s, who wrote this pull request,", requester)
		}
		detail += fmt.Sprintf(" %s must comment `%s`. Then @%s runs this command again.",
			approver, templates.TenantFlag("schemabot freeze approve "+request.Identifier, tenant), requester)
	case refused.OverrideUnrecorded:
		detail += " No override request was recorded, because SchemaBot could not confirm who is asking and who wrote this pull request. Run the command again; if this persists, see the SchemaBot server logs."
	case commandSupportsOverrideFlag(command):
		// The detail is HTML-escaped on its way into the comment, so the hint
		// names no quoted placeholder, which would show as entity text.
		detail += " To ask a freeze approver to let it through, run the same command again with `--override` followed by your reason in quotes."
	}
	return detail
}

const (
	// maxListedBlockers is how many blockers a refusal names; the rest are
	// counted.
	maxListedBlockers = 3
	// maxListedFreezeReason and maxShownOverrideReason bound, in runes, the
	// reasons a refusal quotes. The full reasons are on the freeze and the
	// request.
	maxListedFreezeReason  = 150
	maxShownOverrideReason = 300
)

// clampReason cuts reason to at most limit runes, marking a cut with "…".
func clampReason(reason string, limit int) string {
	runes := []rune(reason)
	if len(runes) <= limit {
		return reason
	}
	return strings.TrimSpace(string(runes[:limit-1])) + "…"
}
