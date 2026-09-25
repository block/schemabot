package webhook

import (
	"context"
	"fmt"
	"strings"

	"github.com/block/schemabot/pkg/apitypes"
	ghclient "github.com/block/schemabot/pkg/github"
)

// rolloutStillPending reports whether a rollout whose reviewed primary is
// already at the desired schema still has something standing between it and
// done: a member that needs the change, or a member whose plan could not be
// confirmed.
//
// An empty primary plan speaks only for the primary. A member planned against a
// schema of its own can still need the change, and a member expected to mirror
// the primary can have drifted from it, so an apply cannot conclude there is
// nothing to do until the rollout round has planned every member.
func rolloutStillPending(outcome reviewDriftOutcome) bool {
	return outcome.blocks() || outcome.work.pending > 0
}

// refusePendingRollout answers an apply whose reviewed primary is already at
// the desired schema while the rest of the rollout is not known to be.
//
// A PR apply is built from the reviewed primary's plan and its comment shows
// only that plan's statements, so running other members' work from here would
// apply statements nobody was shown (RV-1). The apply does not run, and the
// check records what is pending so the PR cannot merge as if every target were
// up to date (MG-12).
func (h *Handler) refusePendingRollout(ctx context.Context, client *ghclient.InstallationClient, repo string, pr int, installationID int64, schemaResult *ghclient.SchemaRequestResult, planResp *apitypes.PlanResponse, environment, requestedBy string, actionName string, outcome reviewDriftOutcome) {
	h.logger.Info("apply refused: the reviewed target is already at the desired schema but the rest of the rollout is not",
		"repo", repo, "pr", pr, "database", schemaResult.Database, "database_type", schemaResult.Type,
		"environment", environment, "action", actionName, "plan_id", planResp.PlanID,
		"drift_blocked", outcome.blocks(), "targets_pending", outcome.work.pending, "targets", outcome.work.members,
		"pending_targets", outcome.work.names)
	headSHA, err := h.storePlanCheckRecord(ctx, client, repo, pr, schemaResult, planResp, environment, outcome)
	switch {
	case err != nil:
		h.logger.Error("failed to record the pending rollout on the check; publishing a failing aggregate from the rollout round instead",
			"repo", repo, "pr", pr, "head_sha", schemaResult.HeadSHA, "database", schemaResult.Database, "database_type", schemaResult.Type,
			"environment", environment, "error", err)
		h.failClosedOnUnstoredRollout(ctx, client, repo, pr, schemaResult.HeadSHA, environment, outcome)
	case headSHA != "":
		h.updateAggregateCheck(ctx, client, repo, pr, headSHA)
	}
	h.postCommandError(repo, pr, installationID, actionName, environment, requestedBy, pendingRolloutMessage(outcome))
}

// failClosedOnUnstoredRollout publishes a failing aggregate for an environment
// whose plan check record could not be stored after the rollout round proved
// its check must not pass. Refreshing the aggregate instead would recompute it
// from the stored row, which can still be a pass recorded before the pending
// work was found (MG-1, MG-12).
func (h *Handler) failClosedOnUnstoredRollout(ctx context.Context, client *ghclient.InstallationClient, repo string, pr int, headSHA, environment string, outcome reviewDriftOutcome) {
	if headSHA == "" {
		h.logger.Warn("the pending rollout was not stored and no head SHA is known; the fallback failing aggregate was not posted, so an operator must re-run plan to re-establish the merge-gate block",
			"repo", repo, "pr", pr, "environment", environment)
		return
	}
	if outcome.blocks() {
		h.postFailingAggregatesWithBlock(ctx, client, repo, pr, headSHA, map[string]string{environment: outcome.summary}, reviewTimeDeploymentDriftBlock)
		return
	}
	h.postFailingAggregates(ctx, client, repo, pr, headSHA, map[string]string{environment: outcome.work.summary()})
}

// pendingRolloutMessage explains a refused apply to the operator. The drift
// summary names only configured members and is already clamped for markdown,
// so it is safe to render; the raw causes stay in the server logs.
func pendingRolloutMessage(outcome reviewDriftOutcome) string {
	const nothingRan = "A PR apply cannot yet run a change on targets other than the reviewed one, so nothing was applied. The schema check stays pending until every target has the change."
	if outcome.blocks() {
		return fmt.Sprintf("The reviewed target already has this schema, but SchemaBot could not confirm that the other targets do (%s). %s", outcome.summary, nothingRan)
	}
	return fmt.Sprintf("The reviewed target already has this schema, but %s: %s. %s", outcome.work.summary(), strings.Join(outcome.work.names, ", "), nothingRan)
}
