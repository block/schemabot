package webhook

import (
	"errors"
	"fmt"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/block/schemabot/pkg/api"
	"github.com/block/schemabot/pkg/apitypes"
	ghclient "github.com/block/schemabot/pkg/github"
)

// planControlRetryAdvice ends a failure line whose cause may clear on a later
// attempt. The failure's own text goes to the server log, so the line names
// only the step and what to do next.
const planControlRetryAdvice = "Run the command again; if it fails again, the SchemaBot server logs have the cause."

// closedPRRefreshError is a plan refused because the pull request is closed:
// its close-time cleanup owns the stored check state, so there is nothing to
// refresh. SchemaBot wrote the text, so a PR comment shows it as it stands.
type closedPRRefreshError struct {
	repo string
	pr   int
}

func (e *closedPRRefreshError) Error() string {
	return fmt.Sprintf("PR #%d in %s is closed; SchemaBot refreshes check state only for open PRs", e.pr, e.repo)
}

// planNotStartedMessage is the PR comment line for an internal failure that
// stopped a plan before it started. failedStep finishes the sentence
// "SchemaBot could not ...".
func planNotStartedMessage(failedStep string) string {
	return fmt.Sprintf("SchemaBot could not %s, so the plan did not start. %s", failedStep, planControlRetryAdvice)
}

// planReconciliationFailureMessage is the PR comment detail for a plan whose
// check of the pull request's schema files failed. A closed pull request is a
// refusal SchemaBot wrote, shown as it stands. A pull request past GitHub's
// changed-file cap fails the same way on every attempt, so its line says to
// split the pull request instead of running the command again, and a quota
// reserve refusal names when the reads go out again. Every other failure is a
// GitHub read or a storage read whose text stays in the server log. database
// is empty for a plan that names no database.
func planReconciliationFailureMessage(err error, database string) string {
	if closed, ok := errors.AsType[*closedPRRefreshError](err); ok {
		return closed.Error()
	}
	if errors.Is(err, ghclient.ErrPRFilesIncomplete) {
		return "SchemaBot cannot plan this pull request: it changes more files than GitHub will report for a single pull request, so the changed-file list SchemaBot reads is incomplete and running the command again returns the same list. " +
			"Split this pull request so GitHub reports the full changed-file list; if it carries schema changes, move them into their own smaller pull request."
	}
	if refusal, ok := errors.AsType[*ghclient.QuotaReservedError](err); ok {
		return "SchemaBot is holding back its remaining GitHub API quota for check runs, so the plan did not start. " +
			"Comment the command again after the quota resets at " + quotaResetTime(refusal.ResetAt) + "."
	}
	if database == "" {
		return planNotStartedMessage("check the schema files in this pull request")
	}
	return planNotStartedMessage(fmt.Sprintf("check the schema files in this pull request for `%s`", database))
}

// planHeadVerificationFailedMessage is the PR comment line for a plan that
// could not read the pull request's latest commit to confirm the schema files
// it loaded are still current. database is empty when the plan has none.
func planHeadVerificationFailedMessage(database string) string {
	if database == "" {
		return planNotStartedMessage("read the latest commit of this pull request")
	}
	return planNotStartedMessage(fmt.Sprintf("read the latest commit of this pull request for `%s`", database))
}

// controlOperationFailureMessage is the PR comment detail for a control command
// whose operation returned an error. A rejection or failure that carries text
// SchemaBot wrote for the requester is shown as it stands, and a remote data
// plane that did not answer is named as such. Any other failure's text can
// carry storage, driver, or engine detail, so it stays in the server log and
// the line names only the command and what to do next. The command may
// already be recorded and carried out by the apply's driver, so the line
// asks the requester to check the apply before running the command again.
func controlOperationFailureMessage(err error, command, applyID, environment string) string {
	if message, ok := stopUnsupportedMessage(api.ControlOperationErrorCode(err), applyID, environment); ok {
		return message
	}
	if message, ok := api.AuthoredControlFailureMessage(err); ok {
		return message
	}
	if remoteErr, ok := errors.AsType[*api.RemoteDeploymentUnavailableError](err); ok {
		return userFacingRemoteUnavailableError(remoteErr.Deployment, remoteErr.Target)
	}
	if status.Code(err) == codes.Unavailable {
		return userFacingRemoteUnavailableError("", "")
	}
	return fmt.Sprintf("SchemaBot could not complete the %s command for apply `%s`. "+
		"It may already be recorded, so check the progress comment for this apply before running the command again; "+
		"the SchemaBot server logs have the cause.", command, applyID)
}

// stopUnsupportedMessage is the PR comment line for a stop refused because
// the engine cannot pause the schema change, from the refusal's error code.
// The server's message names CLI commands, and progress is not a PR comment
// command, so the line names cancel as the comment to post and otherwise
// points at the progress comment, which follows the apply to its end.
func stopUnsupportedMessage(code, applyID, environment string) (string, bool) {
	cancel := fmt.Sprintf("`schemabot cancel %s -e %s`", applyID, environment)
	follow := fmt.Sprintf("the progress comment for apply `%s` follows it to the end", applyID)
	switch code {
	case apitypes.ErrCodeStopUnsupportedCancel:
		return "This schema change can't be paused. To end it, comment " + cancel + ".", true
	case apitypes.ErrCodeStopUnsupportedFinishes:
		return "This schema change can't be paused, and it finishes on its own; " + follow + ".", true
	case apitypes.ErrCodeStopUnsupported:
		return "This schema change can't be paused. If it is a single index build, end it by commenting " + cancel +
			". Anything else finishes on its own, and " + follow + ".", true
	default:
		return "", false
	}
}

// controlApplyLookupFailedMessage is the PR comment line for a control command
// whose apply could not be read from storage.
func controlApplyLookupFailedMessage(command, applyID string) string {
	return fmt.Sprintf("SchemaBot could not look up apply `%s`, so the %s command did not run. %s",
		applyID, command, planControlRetryAdvice)
}
