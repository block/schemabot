package templates

import (
	"fmt"
)

// RenderCutoverLookupError renders a comment when the apply lookup fails.
func RenderCutoverLookupError(applyID string, err error) string {
	return fmt.Sprintf("## Cutover Failed\n\nFailed to look up apply `%s`: %s", applyID, err)
}

// RenderCutoverApplyNotFound renders a comment when no apply matches the ID.
func RenderCutoverApplyNotFound(applyID string) string {
	return fmt.Sprintf("## Apply Not Found\n\nNo apply found with ID `%s`.", applyID)
}

// RenderCutoverNotAvailable renders a comment when the apply isn't in waiting_for_cutover.
func RenderCutoverNotAvailable(database, environment, applyID, applyState string) string {
	return fmt.Sprintf("## Cutover Not Available\n\n"+
		"**Database**: `%s` | **Environment**: `%s`\n\n"+
		"Apply `%s` is in state `%s`, not waiting for cutover.",
		database, environment, applyID, applyState)
}

// RenderCutoverTernError renders a comment when the tern client connection fails.
func RenderCutoverTernError(database, environment string, err error) string {
	return fmt.Sprintf("## Cutover Failed\n\n"+
		"**Database**: `%s` | **Environment**: `%s`\n\n"+
		"Failed to connect to tern: %s", database, environment, err)
}

// RenderCutoverFailed renders a comment when the cutover RPC fails.
func RenderCutoverFailed(database, environment string, err error) string {
	return fmt.Sprintf("## Cutover Failed\n\n"+
		"**Database**: `%s` | **Environment**: `%s`\n\n"+
		"Failed to trigger cutover: %s", database, environment, err)
}

// RenderCutoverNotAccepted renders a comment when the tern server rejects the cutover.
func RenderCutoverNotAccepted(database, environment, errorMessage string) string {
	return fmt.Sprintf("## Cutover Not Accepted\n\n"+
		"**Database**: `%s` | **Environment**: `%s`\n\n"+
		"%s", database, environment, errorMessage)
}

// RenderCutoverRecovering renders a comment when cutover is attempted during recovery.
func RenderCutoverRecovering(database, environment string) string {
	return fmt.Sprintf("## Cutover — Recovery In Progress\n\n"+
		"**Database**: `%s` | **Environment**: `%s`\n\n"+
		"The schema change is being recovered after a server restart. "+
		"The recovery worker will resume it shortly — try again in a moment.",
		database, environment)
}

// RenderCutoverMissingApplyID renders a comment when cutover is called without an apply ID.
func RenderCutoverMissingApplyID() string {
	return "## Missing Apply ID\n\n" +
		"Usage: `schemabot cutover <apply-id> -e <environment>`\n\n" +
		"You can find the apply ID in the progress comment above."
}

// RenderCutoverTriggered renders a comment acknowledging the cutover was triggered.
func RenderCutoverTriggered(database, environment, applyID string) string {
	return fmt.Sprintf("## Cutover Triggered\n\n"+
		"**Database**: `%s` | **Environment**: `%s`\n\n"+
		"Cutover has been triggered for apply `%s`. The table swap will complete shortly.",
		database, environment, applyID)
}

// RenderCutoverMissingEnvironment renders a comment when cutover is called without -e.
func RenderCutoverMissingEnvironment() string {
	return "## Missing Environment\n\n" +
		"Usage: `schemabot cutover <apply-id> -e <environment>`\n\n" +
		"The `-e` flag is required to specify the target environment."
}
