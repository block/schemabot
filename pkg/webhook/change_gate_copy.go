package webhook

import (
	"fmt"
	"strings"

	"github.com/block/schemabot/pkg/api"
	"github.com/block/schemabot/pkg/storage"
)

// changeGateRefusalDetail renders what a freeze refusal tells the commenter:
// what blocks the change and, when the command carried --override, what became
// of the request. Apply, start and cutover share it. The request is approved
// by someone other than the commenter and the pull request's author, then the
// same command runs again.
func changeGateRefusalDetail(refused *api.ChangeGateRefusedError) string {
	detail := refused.Blocked() + "."
	switch request := refused.OverrideRequest; {
	case request != nil:
		requester := strings.TrimPrefix(request.RequestedBy, storage.GitHubIdentityPrefix)
		author := strings.TrimPrefix(request.PRAuthor, storage.GitHubIdentityPrefix)
		detail += fmt.Sprintf(" Override request `%s` was recorded. A freeze approver other than @%s and other than the PR author (@%s) must comment `schemabot freeze approve %s`, then run this command again.",
			request.Identifier, requester, author, request.Identifier)
	case refused.OverrideUnrecorded:
		detail += " No override request was recorded, because SchemaBot could not confirm who is asking and who wrote this pull request. Run the command again; if this persists, see the SchemaBot server logs."
	}
	return detail
}
