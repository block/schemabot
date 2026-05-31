package templates

import "fmt"

// RenderRepositoryNotRegistered renders the message posted when a SchemaBot
// command arrives from a repository that is not in the configured allowlist.
func RenderRepositoryNotRegistered() string {
	return "**Repository not registered.** This repository is not in SchemaBot's configuration. " +
		"To onboard, add it to the `repos` section of SchemaBot's config and redeploy."
}

// RenderRollbackMissingArguments renders the message posted when `schemabot rollback`
// is invoked without an apply ID and without an `-e` flag.
func RenderRollbackMissingArguments() string {
	return "## Missing Arguments\n\n" +
		"Usage: `schemabot rollback <apply-id> -e <environment>`\n\n" +
		"Rollback requires both an apply ID and the `-e` flag to select the target environment."
}

// RenderRollbackMissingEnv renders the message posted when `schemabot rollback`
// is invoked with an apply ID but no `-e` flag. Distinct from RenderMissingEnv —
// the rollback variant tailors the example usage to rollback semantics.
func RenderRollbackMissingEnv() string {
	return "## Missing Environment\n\n" +
		"Usage: `schemabot rollback <apply-id> -e <environment>`\n\n" +
		"The `-e` flag is required to select the target environment."
}

// RenderUnsupportedAutoConfirm renders the message posted when the `-y` /
// `--yes` auto-confirm flag is supplied to a command that does not support it.
func RenderUnsupportedAutoConfirm(action string) string {
	return fmt.Sprintf("The `-y` flag is not supported for `%s`.", action)
}

// StopCommandAcceptedData contains data for a PR comment stop acknowledgement.
type StopCommandAcceptedData struct {
	ApplyID      string
	Environment  string
	RequestedBy  string
	Status       string
	StoppedCount int64
	SkippedCount int64
}

// RenderControlMissingApplyID renders the message posted when an apply-scoped
// control command is invoked without the required apply ID.
func RenderControlMissingApplyID(action string) string {
	return fmt.Sprintf("## Missing Apply ID\n\n"+
		"Usage: `schemabot %s <apply-id> -e <environment>`\n\n"+
		"Use `schemabot status -e <environment>` to find the apply ID.", action)
}

// RenderStopCommandAccepted renders the acknowledgement posted when a PR
// comment stop command records durable stop intent.
func RenderStopCommandAccepted(data StopCommandAcceptedData) string {
	statusLine := "Stop request accepted. SchemaBot will stop this schema change; status remains available from the PR progress comment or CLI."
	if data.Status == "already_requested" {
		statusLine = "Stop was already requested. SchemaBot will keep the existing stop request pending until the scheduler owner finishes it."
	}

	body := "## Stop Request Accepted\n\n" +
		fmt.Sprintf("**Apply**: `%s`\n", data.ApplyID) +
		fmt.Sprintf("**Environment**: `%s`\n", data.Environment)
	if data.RequestedBy != "" {
		body += fmt.Sprintf("**Requested by**: @%s\n", data.RequestedBy)
	}
	body += "\n" + statusLine + "\n"
	if data.StoppedCount > 0 || data.SkippedCount > 0 {
		body += fmt.Sprintf("\n**Tasks selected for stop**: %d stopped, %d skipped.\n", data.StoppedCount, data.SkippedCount)
	}
	return body
}

// RenderCommandNotYetAvailable renders the acknowledgement posted when a
// recognised but not-yet-implemented PR comment command is invoked. It points
// the user at the CLI fallback for the same action.
func RenderCommandNotYetAvailable(action, environment string) string {
	return fmt.Sprintf("The `%s` command is not yet available via PR comments. "+
		"Use the CLI instead:\n```\nschemabot %s -e %s\n```",
		action, action, environment)
}
