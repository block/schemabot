package templates

import (
	"fmt"
	"time"

	"github.com/block/schemabot/pkg/glyph"
)

// FreezeOverrideApprovedData is what the reply to an approval names.
type FreezeOverrideApprovedData struct {
	ID          string
	Approver    string
	Action      string
	Database    string
	Environment string
	// RequestedBy is the requester's GitHub login, without any prefix.
	RequestedBy string
	Reason      string
	ExpiresAt   time.Time
}

// RenderFreezeOverrideApproved renders the reply to an approved override: who
// approved which change, and that the requester can run the command again.
// The reason is the requester's own text, so a mention in it notifies nobody.
func RenderFreezeOverrideApproved(data FreezeOverrideApprovedData) string {
	return fmt.Sprintf("✅ @%s approved `%s` for %s on `%s` in %s. Requested by @%s: %s. It expires at %s UTC. @%s can now re-run the command.",
		data.Approver, data.ID, data.Action, data.Database, data.Environment,
		data.RequestedBy, markdownReason(data.Reason), data.ExpiresAt.UTC().Format("2006-01-02 15:04:05"), data.RequestedBy)
}

// RenderFreezeOverrideApprovalRefused renders why a pull request approval
// approved nothing. The reason is the refusal's own text.
func RenderFreezeOverrideApprovalRefused(reason string) string {
	return glyph.Refused + " Nothing was approved. " + reason
}

// RenderFreezeOverrideApprovalFailed renders an approval that could not be
// completed, for a cause that is not the commenter's and may pass: a read
// failed. Nothing was approved.
func RenderFreezeOverrideApprovalFailed(problem string) string {
	return glyph.Attention + " Nothing was approved. " + problem + " Try again; if it persists, the SchemaBot server logs have the cause."
}
