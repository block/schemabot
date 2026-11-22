package webhook

import (
	"strings"

	"github.com/block/schemabot/pkg/api"
	"github.com/block/schemabot/pkg/storage"
	"github.com/block/schemabot/pkg/webhook/templates"
)

// changeGateRefusalData is what the comment on a freeze refusal names:
// command is the refused PR command, tenant the deployment's own, requestedBy
// the commenter, and approvers the teams of freeze.github_teams.
func changeGateRefusalData(refused *api.ChangeGateRefusedError, command, tenant, requestedBy string, approvers []string) templates.ChangeGateRefusalData {
	data := templates.ChangeGateRefusalData{
		CommandName:        command,
		TakesOverride:      commandSupportsOverrideFlag(command),
		Environment:        refused.Environment,
		Tenant:             tenant,
		RequestedBy:        requestedBy,
		Timestamp:          templates.NowFunc().UTC().Format("2006-01-02 15:04:05"),
		OverrideUnrecorded: refused.OverrideUnrecorded,
		Approvers:          approvers,
	}
	for _, blocker := range refused.Blockers {
		data.Freezes = append(data.Freezes, templates.RefusingFreeze{ID: blocker.ID, Reason: blocker.Reason, EndsAt: blocker.EndsAt})
	}
	if request := refused.OverrideRequest; request != nil {
		data.Override = &templates.RefusalOverrideRequest{
			ID:         request.Identifier,
			Requester:  strings.TrimPrefix(request.RequestedBy, storage.GitHubIdentityPrefix),
			Author:     strings.TrimPrefix(request.PRAuthor, storage.GitHubIdentityPrefix),
			Reason:     request.Reason,
			ReasonKept: refused.OverrideReasonKept,
		}
	}
	return data
}
