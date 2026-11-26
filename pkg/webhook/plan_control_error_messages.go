package webhook

import (
	"errors"
	"fmt"

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

// controlApplyLookupFailedMessage is the PR comment line for a control command
// whose apply could not be read from storage.
func controlApplyLookupFailedMessage(command, applyID string) string {
	return fmt.Sprintf("SchemaBot could not look up apply `%s`, so the %s command did not run. %s",
		applyID, command, planControlRetryAdvice)
}
