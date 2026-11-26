package webhook

import (
	"errors"

	"github.com/block/schemabot/pkg/api"
)

// rerunAdvice ends every fixed apply and unlock error line. The cause of an
// internal failure goes to the server log with its identifiers and is never
// rendered on the PR.
const rerunAdvice = "Run the command again; if it fails again, the SchemaBot server logs have the cause."

// applyPRChangesCheckFailedMessage is the PR comment line for an apply or
// apply-confirm that could not read the PR's changed files or its stored check
// state.
func applyPRChangesCheckFailedMessage(environment string) string {
	return "SchemaBot could not check the schema changes in this PR" + forEnvironment(environment) + ", so the apply did not start. " + rerunAdvice
}

// applyLockCheckFailedMessage is the PR comment line for an apply or
// apply-confirm that could not read the database's lock.
func applyLockCheckFailedMessage(database string) string {
	return "SchemaBot could not check the lock for `" + database + "`, so the apply did not start. " + rerunAdvice
}

// applyLockHeldAtAcquireMessage is the PR comment line for an apply whose
// pending lock changed while it was planning: another owner took it, or it was
// released.
func applyLockHeldAtAcquireMessage(database string) string {
	return "The lock for `" + database + "` changed while this command ran, so the apply did not start. Run the command again to see who holds it now."
}

// applyLockAcquireFailedMessage is the PR comment line for an apply whose lock
// acquire failed for a reason other than another owner holding the lock.
func applyLockAcquireFailedMessage(database string) string {
	return "SchemaBot could not take the lock for `" + database + "`, so the apply did not start. " + rerunAdvice
}

// applyCheckUpdateFailedMessage is the PR comment line for an apply that was
// accepted but whose stored check state could not be moved to in progress. The
// two refusals SchemaBot decides on its own name the fix their block calls for.
// Any other failure is a storage or GitHub error whose text stays in the server
// log. The line does not promise that the apply ending refreshes the check: a
// completed apply updates only a check row it owns, and a failed start leaves
// the row unowned. A plan writes an unowned row, so the line asks for one once
// the apply finishes.
func applyCheckUpdateFailedMessage(database, environment string, err error) string {
	subject := "The apply for `" + database + "` in `" + environment + "` was accepted, but "
	switch {
	case errors.Is(err, errApplyStartDriftBlocked):
		return subject + "the required status check still shows the deployment drift found at review time, so SchemaBot left it as it was. The apply itself is not affected. Reconcile the deployments the check summary names, then run plan again."
	case errors.Is(err, errApplyStartPlacementBlocked):
		return subject + "the required status check still shows that namespace placement refused the plan for this environment, so SchemaBot left it as it was. The apply itself is not affected. Fix the namespace placement, then run plan again."
	default:
		return subject + "SchemaBot could not update the required status check, so the check does not show this apply. The apply itself is not affected. Once the apply finishes, run plan again to refresh the check."
	}
}

// applyConfirmPlanLoadFailedMessage is the PR comment line for an
// apply-confirm that could not load the plan its pending lock points at. The
// lock stays in place.
func applyConfirmPlanLoadFailedMessage(database string) string {
	return "SchemaBot could not load the pending plan for `" + database + "`, so the apply did not start. " + rerunAdvice
}

// applyRemoteUnavailableMessage is the PR comment line for an apply whose plan
// could not reach a remote deployment. The deployment and target come from the
// typed error when it carries them; the transport error text stays in the
// server log.
func applyRemoteUnavailableMessage(err error) string {
	service, healthy := "remote schema change service", "the service"
	if remoteErr, ok := errors.AsType[*api.RemoteDeploymentUnavailableError](err); ok {
		switch {
		case remoteErr.Deployment != "" && remoteErr.Target != "":
			service, healthy = "remote deployment `"+remoteErr.Deployment+"` for target `"+remoteErr.Target+"`", "that deployment"
		case remoteErr.Deployment != "":
			service, healthy = "remote deployment `"+remoteErr.Deployment+"`", "that deployment"
		}
	}
	return "SchemaBot could not reach the " + service + ", so the apply did not start. Run the command again once " + healthy + " is healthy; if it fails again, the SchemaBot server logs have the cause."
}

// unlockInferenceFailedMessage is the PR comment line for a force unlock whose
// database could not be inferred from this PR.
const unlockInferenceFailedMessage = "SchemaBot could not work out which database to force unlock for this PR, so no lock was released. Run the command again, or add `-d` and the database name to `schemabot unlock --force`."

// unlockLookupFailedMessage is the PR comment line for an unlock that could
// not read the locks it targets. The database is empty when the command named
// none.
func unlockLookupFailedMessage(database string) string {
	target := "the locks for this PR"
	if database != "" {
		target = "the lock for `" + database + "`"
	}
	return "SchemaBot could not look up " + target + ", so no lock was released. " + rerunAdvice
}

func forEnvironment(environment string) string {
	if environment == "" {
		return ""
	}
	return " for `" + environment + "`"
}
