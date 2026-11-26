package webhook

import (
	"errors"
	"fmt"

	"github.com/block/schemabot/pkg/api"
	"github.com/block/schemabot/pkg/storage"
)

// rollbackRetryAdvice ends a failure line whose cause may clear on a later
// attempt.
const rollbackRetryAdvice = "Run the command again; if it fails again, the SchemaBot server logs have the cause."

// rollbackTerminalAdvice ends a failure line whose cause is the same on every
// attempt, such as an engine rejection or a deployment the server
// configuration gets wrong, so running the command again cannot help.
const rollbackTerminalAdvice = "Running the command again fails the same way until the cause is fixed; the SchemaBot server logs have the cause."

// rollbackNotStartedMessage is the PR comment line for an internal failure that
// stopped a rollback before it started. failedStep finishes the sentence
// "SchemaBot could not ...". The failure's own text goes to the server log, so
// the line names only the step and what to do next.
func rollbackNotStartedMessage(failedStep string) string {
	return fmt.Sprintf("SchemaBot could not %s, so the rollback did not start. %s", failedStep, rollbackRetryAdvice)
}

// rollbackStepFailureMessage is rollbackNotStartedMessage for a failure the
// control core classified. A terminal failure fails the same way on the next
// attempt, so its line does not ask for one.
func rollbackStepFailureMessage(err error, failedStep string) string {
	if api.IsTerminalControlError(err) {
		return fmt.Sprintf("SchemaBot could not %s, so the rollback did not start. %s", failedStep, rollbackTerminalAdvice)
	}
	return rollbackNotStartedMessage(failedStep)
}

// rollbackSourceFailureMessage is the PR comment detail for a failed check of
// the apply a rollback would reverse. A guardrail rejection or an invariant
// violation named by its identifiers carries text SchemaBot wrote, which is
// shown as it stands. Any other failure's text stays in the server log.
func rollbackSourceFailureMessage(err error) string {
	if message, ok := api.AuthoredControlFailureMessage(err); ok {
		return message
	}
	return rollbackStepFailureMessage(err, "check that the apply can be rolled back")
}

// rollbackPlanFailureMessage is the PR comment detail for a rollback plan that
// could not be generated. A message SchemaBot wrote, or the engine's own
// refusal of the plan, is shown as it stands. A remote deployment that could
// not be reached is named by its deployment and target. Every other failure is
// named by its database and environment only.
func rollbackPlanFailureMessage(err error, database, environment string) string {
	if message, ok := api.AuthoredControlFailureMessage(err); ok {
		return message
	}
	if unavailable, ok := errors.AsType[*api.RemoteDeploymentUnavailableError](err); ok {
		return remoteDeploymentUnavailableForRollbackMessage(unavailable)
	}
	return rollbackStepFailureMessage(err, fmt.Sprintf("create the rollback plan for `%s` in `%s`", database, environment))
}

// remoteDeploymentUnavailableForRollbackMessage names the deployment and target
// a rollback plan request could not reach, leaving out whichever the error does
// not carry. The transport's own error stays in the server log.
func remoteDeploymentUnavailableForRollbackMessage(err *api.RemoteDeploymentUnavailableError) string {
	service := "the remote schema change service"
	switch {
	case err.Deployment != "" && err.Target != "":
		service = "the remote deployment `" + err.Deployment + "` for target `" + err.Target + "`"
	case err.Deployment != "":
		service = "the remote deployment `" + err.Deployment + "`"
	}
	recovered := "that deployment is"
	if err.Deployment == "" {
		recovered = "the service is"
	}
	return "SchemaBot could not reach " + service + ", so the rollback did not start. Run the command again once " + recovered +
		" healthy; if it fails again, the SchemaBot server logs have the cause."
}

// rollbackLockFailureMessage is the PR comment line for a lock write that
// failed before the rollback started. failedStep finishes the sentence
// "SchemaBot could not ...". A lock someone else took while the command ran is
// a refusal, so it is named as one; every other failure is an internal error
// whose text stays in the server log.
func rollbackLockFailureMessage(err error, database, failedStep string) string {
	if errors.Is(err, storage.ErrLockHeld) {
		return fmt.Sprintf("Someone else took the lock for `%s` while this command ran, so the rollback did not start. "+
			"Run the command again to see who holds it.", database)
	}
	return rollbackNotStartedMessage(failedStep)
}

// rollbackConfirmPlanFailureMessage is the PR comment line for a rollback-confirm
// that could not resolve its pinned rollback plan. A deterministic rejection
// carries text SchemaBot wrote from the lock and plan identifiers, so it is shown
// as it stands. Any other failure is a storage read whose text stays in the
// server log.
func rollbackConfirmPlanFailureMessage(err error, environment string) string {
	if rejection, ok := errors.AsType[*rollbackConfirmRejectionError](err); ok {
		return rejection.Error()
	}
	return fmt.Sprintf("SchemaBot could not read the pending rollback for this pull request in `%s`, so nothing was confirmed and nothing ran. %s",
		environment, rollbackRetryAdvice)
}

// rollbackCheckUpdateFailedMessage is the PR comment line for an accepted
// rollback whose required status check could not be marked in progress. A
// refusal to clear a block the check holds is deterministic, so it is named in
// SchemaBot's own words with the step that clears it. Any other failure is a
// storage or GitHub error whose text stays in the server log. The rollback is
// stored and unaffected either way. The line promises no later refresh: when
// the check row was never created, the rollback's completion has no row to
// update, so it names the step that refreshes the check instead.
func rollbackCheckUpdateFailedMessage(err error, database, environment string) string {
	rollback := fmt.Sprintf("The rollback for `%s` in `%s` was accepted", database, environment)
	switch {
	case errors.Is(err, errApplyStartDriftBlocked):
		return rollback + ", but the required status check still shows the deployment drift found at review time, so SchemaBot left it as it was. " +
			"The rollback itself is not affected. Reconcile the deployments the check summary names, then run plan again."
	case errors.Is(err, errApplyStartPlacementBlocked):
		return rollback + ", but the required status check still shows that namespace placement refused the plan for this environment, so SchemaBot left it as it was. " +
			"The rollback itself is not affected. Fix the namespace placement, then run plan again."
	}
	return rollback + ", but SchemaBot could not update the required status check, so the check does not show it yet. The rollback itself is not affected. " +
		"Once the rollback finishes, run plan again to refresh the check; if it still does not match, the SchemaBot server logs have the cause."
}
