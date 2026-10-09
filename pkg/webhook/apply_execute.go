package webhook

import (
	"cmp"
	"context"
	"errors"
	"fmt"
	"maps"
	"slices"
	"strings"

	"github.com/block/schemabot/pkg/api"
	"github.com/block/schemabot/pkg/apitypes"
	ghclient "github.com/block/schemabot/pkg/github"
	"github.com/block/schemabot/pkg/routing"
	"github.com/block/schemabot/pkg/schema"
	"github.com/block/schemabot/pkg/storage"
	"github.com/block/schemabot/pkg/ui"
	"github.com/block/schemabot/pkg/webhook/action"
	"github.com/block/schemabot/pkg/webhook/templates"
)

// executeApply re-plans for drift detection and executes the apply. This is the shared
// execution core used by both handleApplyConfirmCommand and handleApplyCommand.
//
// When storedPlan is non-nil (auto-confirm path), the re-plan DDL is compared against it.
// If the DDL differs, execution is downgraded to manual confirmation — a plan comment is
// posted with a warning and the user must run apply-confirm separately. The copy-discard
// gate below is not scoped that way: it stops an operator's own apply-confirm too,
// because a copy can appear after the comment they confirmed was posted.
//
// disclosedPlan is the plan whose comment the operator was last shown: the
// stored plan on the auto-confirm path, the pending confirmation's plan on
// apply-confirm. A re-plan that routes a statement to direct execution that
// disclosedPlan did not stops on either path, since no comment disclosed it.
//
// disclosedCopyDiscard is what the comment behind this apply told the operator
// about an unfinished copy on the target, read from the lock's pending
// confirmation. It is the consent this re-plan is checked against: the plan
// decided whether to stop, and the same decision is made again here, against the
// target as it is now.
func (h *Handler) executeApply(
	ctx context.Context, client *ghclient.InstallationClient,
	repo string, pr int, schemaResult *ghclient.SchemaRequestResult,
	environment string, installationID int64, requestedBy string,
	result CommandResult, storedPlan, disclosedPlan *storage.Plan, expectedPendingPlanID string,
	disclosedCopyDiscard bool,
) {
	database := schemaResult.Database
	dbType := schemaResult.Type

	// apply-confirm names no target: it confirms the plan its apply posted, so
	// a plan narrowed to one rollout member narrows the re-plan and the apply
	// to that same member.
	if result.Target == "" && disclosedPlan != nil && disclosedPlan.NarrowedTo != "" {
		result.Target = disclosedPlan.NarrowedTo
		h.logger.Info("apply narrowed to the rollout member its confirmed plan was made for",
			"repo", repo, "pr", pr, "database", database, "database_type", dbType, "environment", environment,
			"plan_id", disclosedPlan.PlanIdentifier, "narrowed_to", disclosedPlan.NarrowedTo)
	}
	// The apply a refusal asks the operator to re-run: the one they sent,
	// narrowed as its confirmed plan was, with the options they typed.
	recoveryCommand := templates.ApplyCommand(environment, result.Database, applyCommandOptionsOf(result))

	// Re-plan for drift detection
	prNumber := int32(pr)
	planReq := api.PlanRequest{
		Database:          schemaResult.Database,
		Environment:       environment,
		Type:              schemaResult.Type,
		SchemaFiles:       schemaResult.SchemaFiles,
		Repository:        repo,
		PullRequest:       &prNumber,
		HeadSHA:           &schemaResult.HeadSHA,
		SchemaPath:        schemaResult.SchemaPath,
		IgnoredNamespaces: schemaResult.IgnoredNamespaces,
		IgnoreTables:      schemaResult.IgnoreTables,
		SourceTrusted:     true,
		// This re-plan is what the copy-discard gate below reads, so it has to
		// predict the apply that is about to run, not the default shape. The
		// command carrying that decision is already resolved here.
		GroupedExecution: storage.GroupsEngineExecution(schemaResult.Type, result.DeferCutover),
		Target:           result.Target,
	}

	planProto, planResp, err := h.executePlanProtoWithTransientRetry(ctx, planReq, repo, pr)
	if err != nil {
		h.logger.Error("plan execution failed on confirm", "repo", repo, "pr", pr, "database", database, "database_type", dbType, "environment", environment, "error", err)
		h.postCommandError(repo, pr, installationID, action.Apply, environment, requestedBy, err.Error())
		return
	}

	// Revalidate the PR before interpreting the re-plan. The earlier handler
	// checks provide fast rejection, but the re-plan can be retried and the
	// base branch or PR HEAD can advance while it runs. A stale snapshot must
	// be rejected before any plan-derived response — the "no changes" release,
	// the drift downgrade, and especially the unsafe-change prompt would all
	// misread DDL that is only an artifact of the stale branch.
	actionName := action.ApplyConfirm
	if storedPlan != nil {
		actionName = action.Apply
	}
	freshPRInfo, err := client.FetchPullRequestNoCache(ctx, repo, pr)
	if err != nil {
		h.logger.Error("apply rejected: failed final PR freshness fetch",
			"repo", repo, "pr", pr, "database", database, "database_type", dbType,
			"environment", environment, "action", actionName, "error", err)
		h.postCommandError(repo, pr, installationID, actionName, environment, requestedBy,
			"SchemaBot could not verify the current PR state. The apply was rejected; retry the command.")
		h.releaseApplyLockIfIntentUnchanged(ctx, repo, pr, database, dbType, environment, expectedPendingPlanID, "final PR freshness fetch failure")
		return
	}
	if rejected := h.assertSchemaStillCurrent(ctx, repo, pr, installationID, schemaResult, freshPRInfo.HeadSHA, environment, requestedBy, actionName); rejected {
		h.releaseApplyLockIfIntentUnchanged(ctx, repo, pr, database, dbType, environment, expectedPendingPlanID, "final stale-schema rejection")
		return
	}
	// executeApply runs past the durable hand-off boundary, so a verification
	// failure and a verified-stale rejection both stop the apply here and
	// release the observed lock intent; the gate has already logged and
	// posted the distinction, and the user's recovery is re-issuing the
	// command.
	rejected, freshnessErr := h.assertBaseSchemaStillCurrent(ctx, client, repo, pr, installationID, schemaResult, freshPRInfo, environment, requestedBy, actionName)
	if freshnessErr != nil {
		h.releaseApplyLockIfIntentUnchanged(ctx, repo, pr, database, dbType, environment, expectedPendingPlanID, "final base-schema freshness verification failure")
		return
	}
	if rejected {
		h.releaseApplyLockIfIntentUnchanged(ctx, repo, pr, database, dbType, environment, expectedPendingPlanID, "final base-schema freshness rejection")
		return
	}

	// A confirmation given against the comment saying the primary target
	// already had this schema, which showed only the other targets' plans, does
	// not cover changes the primary target has gained since: none of them were
	// on that comment, so they never run on the strength of it. Nor does it
	// cover a primary target that is another member now, even one whose plan
	// that comment showed: consent given under one target does not transfer to
	// the target that leads the rollout since. Any other confirmation was given
	// against the primary target's own plan, and the gates below re-check that.
	if storedPlan == nil && planResp.HasChanges() {
		refusal, primary, roundErr := h.confirmedConvergedTargetRound(ctx, expectedPendingPlanID, planResp.PlanID, environment)
		if roundErr != nil {
			h.logger.Error("apply-confirm rejected: could not load the confirmed plan, its review round, or the re-plan to compare with the primary target's changes; the pending confirmation is preserved",
				"repo", repo, "pr", pr, "database", database, "database_type", dbType, "environment", environment,
				"pending_plan_id", expectedPendingPlanID, "plan_id", planResp.PlanID, "error", roundErr)
			h.postCommandError(repo, pr, installationID, actionName, environment, requestedBy,
				"SchemaBot could not verify the plan this confirmation covers, so nothing was applied. Retry the command, and see server logs if it persists.")
			return
		}
		switch refusal {
		case convergedRoundPrimaryMoved:
			reason := primaryTargetDifferenceReason(primary, workTarget)
			h.logger.Info("apply-confirm refused: the primary target is not the one the confirmed comment showed as converged",
				"repo", repo, "pr", pr, "database", database, "database_type", dbType, "environment", environment,
				"pending_plan_id", expectedPendingPlanID, "plan_id", planResp.PlanID, "reason", reason)
			h.releaseApplyLockIfIntentUnchanged(ctx, repo, pr, database, dbType, environment, expectedPendingPlanID, reason)
			h.postCommandError(repo, pr, installationID, actionName, environment, requestedBy,
				unconfirmedWorkMessage(memberWork{}, reason))
			return
		case convergedRoundPrimaryGainedWork:
			h.logger.Info("apply-confirm refused: the primary target has changes the confirmed comment did not show",
				"repo", repo, "pr", pr, "database", database, "database_type", dbType, "environment", environment,
				"pending_plan_id", expectedPendingPlanID, "plan_id", planResp.PlanID)
			h.releaseApplyLockIfIntentUnchanged(ctx, repo, pr, database, dbType, environment, expectedPendingPlanID, "the primary target has changes the confirmation did not cover")
			h.postCommandError(repo, pr, installationID, actionName, environment, requestedBy,
				fmt.Sprintf("The comment this confirmation acts on showed target `%s` already at the desired schema, but it now has changes of its own, so nothing was applied. Run apply again for this environment to review and confirm the current plans.", primary))
			return
		case convergedRoundAccepts:
		}
	}

	// No changes (neither table DDL nor a VSchema update) — release the lock
	// (keyed on the pending intent this handler observed, so a lock re-pinned by
	// a newer plan is preserved) and notify. An empty primary plan speaks only
	// for the primary where members hold schemas of their own, so the other
	// members are planned first and their work, if any, answers.
	//
	// The rollout round runs for a primary target with work too: the apply is
	// created from this re-plan, and each other target runs the plan this round
	// stores for it, bound to this re-plan.
	//
	// Other targets' work runs only when the comment behind this apply showed
	// exactly that work: on apply-confirm, the comment the confirmation was
	// given against, and on an automatic apply, the comment the apply command
	// posted with every target's plan just before this re-plan. Work that comment
	// did not show stops for a fresh confirmation or is refused.
	rollout, rolloutPreview := h.reviewTimeDrift(ctx, planReq, planProto, planResp, repo, pr)
	primaryTargetConverged := !planResp.HasChanges()
	refuseRollout := func(reason string) {
		h.releaseApplyLockIfIntentUnchanged(ctx, repo, pr, database, dbType, environment, expectedPendingPlanID, reason)
		h.refusePendingRollout(ctx, client, repo, pr, installationID, schemaResult, planResp, environment, requestedBy, actionName, rollout, primaryTargetConverged)
	}
	runsMemberWork := rolloutRunsMemberWork(rollout, rolloutPreview)
	// confirmedMemberWork records that this apply was checked against every
	// other target's work on the comment behind it, so apply creation may run
	// the direct changes that comment disclosed under those targets.
	confirmedMemberWork := false
	switch {
	case runsMemberWork:
		covered, reason, coverErr := h.confirmationCoversMemberWork(ctx, expectedPendingPlanID, planResp.PlanID, environment, func() string {
			return h.primaryTargetName(rollout.work, planResp, schemaResult.Database, environment)
		})
		if coverErr != nil {
			h.rejectUnverifiedMemberWork(ctx, repo, pr, installationID, schemaResult, environment, requestedBy, actionName, storedPlan != nil, expectedPendingPlanID, planResp.PlanID,
				"could not verify that the reviewed plans cover the other targets' work", coverErr,
				"the plans this apply covers")
			return
		}
		// Work the apply cannot run is refused before anything else, releasing
		// the lock, whether or not the comment showed it: a copy can appear on
		// a target after the comment was posted, a fresh confirmation could
		// never run it, and apply creation would refuse it with the lock still
		// pinned.
		// When the confirmation is also stale, the refusal is the reason the
		// operator is told: it is the one that blocks any confirmation, where
		// a stale one would only send them to confirm again into the same
		// refusal.
		refusal, refusalErr := h.memberWorkRefusal(ctx, planResp.PlanID, environment, rollout, primaryTargetConverged)
		if refusalErr != nil {
			h.rejectUnverifiedMemberWork(ctx, repo, pr, installationID, schemaResult, environment, requestedBy, actionName, storedPlan != nil, expectedPendingPlanID, planResp.PlanID,
				"could not verify that the other targets' plans can run from this apply", refusalErr,
				"the other targets' plans")
			return
		}
		if refusal.refuses() {
			h.logger.Info("apply refused: the other targets' work cannot run from this apply",
				"repo", repo, "pr", pr, "database", database, "database_type", dbType, "environment", environment,
				"pending_plan_id", expectedPendingPlanID, "plan_id", planResp.PlanID, "reason", refusal.String())
			h.releaseApplyLockIfIntentUnchanged(ctx, repo, pr, database, dbType, environment, expectedPendingPlanID, "the other targets' work cannot run from this apply")
			h.refuseRollout(ctx, client, repo, pr, installationID, schemaResult, planResp, environment, requestedBy, actionName, rollout, primaryTargetConverged, memberWorkRefusalMessage(refusal))
			return
		}
		// A primary plan the engine now blocks cannot run from any
		// confirmation, so an automatic apply rejects it here rather than
		// pausing on a confirmation that would only be rejected in turn.
		if storedPlan != nil && planResp.HasBlockedChanges() {
			h.rejectBlockedChanges(ctx, repo, pr, installationID, schemaResult, planResp, environment, requestedBy, actionName, result, expectedPendingPlanID)
			return
		}
		if !covered && storedPlan != nil {
			// The apply command posted every target's plan moments ago, and a
			// target's schema changed before this re-plan. As with a primary
			// plan whose DDL drifted, stop and ask against a comment that
			// shows each target's plan as it is now, naming each target whose
			// plan changed and how.
			cause, causeErr := h.rolloutPlansChangedCause(ctx, expectedPendingPlanID, planResp.PlanID, environment)
			if causeErr != nil {
				h.rejectUnverifiedMemberWork(ctx, repo, pr, installationID, schemaResult, environment, requestedBy, actionName, true, expectedPendingPlanID, planResp.PlanID,
					"could not read the targets' plans to say how they changed", causeErr,
					"the plans this apply covers")
				return
			}
			h.logger.Info("automatic apply downgraded: a target's plan changed after the apply posted it",
				"repo", repo, "pr", pr, "database", database, "database_type", dbType, "environment", environment,
				"posted_plan_id", expectedPendingPlanID, "plan_id", planResp.PlanID, "reason", reason)
			if err := h.postAutoConfirmDowngrade(ctx, client, repo, pr, installationID, schemaResult, planResp, environment, result, requestedBy,
				cause, rolloutPreview, runsMemberWork); err != nil {
				h.logger.Error("failed to post the comment showing the targets' changed plans, so the pending confirmation was not moved",
					"repo", repo, "pr", pr, "database", database, "database_type", dbType,
					"environment", environment, "plan_id", planResp.PlanID, "error", err)
				return
			}
			disclosesDiscard := len(planResp.DiscardedCopies()) > 0
			if err := h.repinPendingConfirmation(ctx, repo, pr, database, dbType, environment, actionName, expectedPendingPlanID, planResp.PlanID, disclosesDiscard); err != nil {
				if h.reportRepinRefused(err, repo, pr, installationID, actionName, database, environment, requestedBy, recoveryCommand) {
					return
				}
				h.logger.Error("failed to re-pin the pending confirmation onto the plan whose comment shows the targets' changed plans",
					"repo", repo, "pr", pr, "database", database, "database_type", dbType,
					"environment", environment, "plan_id", planResp.PlanID, "error", err)
				h.postCommandError(repo, pr, installationID, actionName, environment, requestedBy,
					"The targets' plans changed before this apply could start. SchemaBot stopped the apply but could not record the confirmation; re-run `schemabot apply -e "+environment+"` to review them.")
			}
			return
		}
		if !covered {
			h.logger.Info("apply-confirm refused: the confirmation does not cover the work the rollout would run",
				"repo", repo, "pr", pr, "database", database, "database_type", dbType, "environment", environment,
				"pending_plan_id", expectedPendingPlanID, "plan_id", planResp.PlanID, "reason", reason)
			h.releaseApplyLockIfIntentUnchanged(ctx, repo, pr, database, dbType, environment, expectedPendingPlanID, reason)
			h.refuseRollout(ctx, client, repo, pr, installationID, schemaResult, planResp, environment, requestedBy, actionName, rollout, primaryTargetConverged, unconfirmedWorkMessage(rollout.work, reason))
			return
		}
		confirmedMemberWork = true
		h.logger.Info("apply: running the reviewed plans of the other targets that still need the change",
			"repo", repo, "pr", pr, "database", database, "database_type", dbType, "environment", environment,
			"action", actionName, "plan_id", planResp.PlanID, "primary_target_converged", primaryTargetConverged, "pending_targets", rollout.work.names)
	case rollout.blocks():
		refuseRollout("the rollout round could not confirm every target's plan")
		return
	case primaryTargetConverged && rolloutStillPending(rollout):
		refuseRollout("the rest of the rollout is not at the desired schema")
		return
	case primaryTargetConverged:
		h.releaseApplyLockIfIntentUnchanged(ctx, repo, pr, database, dbType, environment, expectedPendingPlanID, "no changes to apply")
		// The target already matches the PR schema — apply found nothing to do.
		// Record the passing (no-change) check result and refresh the aggregate so
		// the schema check reflects that the target is up to date, the same as the
		// no-change plan path. A narrowed re-plan speaks for one target only, so
		// it records nothing.
		if planResp.NarrowedTo != "" {
			h.logger.Info("narrowed target already has the change; stored check state is left unchanged",
				"repo", repo, "pr", pr, "database", database, "database_type", dbType, "environment", environment,
				"plan_id", planResp.PlanID, "narrowed_to", planResp.NarrowedTo)
		} else if headSHA, checkErr := h.storePlanCheckRecord(ctx, client, repo, pr, schemaResult, planResp, environment, rollout); checkErr != nil {
			h.logger.Error("failed to record no-changes check after apply",
				"repo", repo, "pr", pr, "database", database, "database_type", dbType, "environment", environment, "error", checkErr)
		} else if headSHA != "" {
			h.updateAggregateCheck(ctx, client, repo, pr, headSHA)
		}
		h.postComment(repo, pr, installationID, templates.RenderApplyConfirmNoChanges(database, environment))
		return
	default:
		// The primary target's re-plan runs from this confirmation even when
		// no other target has work left, so in a rollout it must be the work
		// the confirmed comment showed. A rollout that has since shrunk to the
		// primary target is still checked against its confirmed round.
		if storedPlan == nil {
			difference, coverErr := h.confirmationCoversPrimaryTarget(ctx, expectedPendingPlanID, planResp.PlanID, environment, rollout.work.members > 1)
			if coverErr != nil {
				h.logger.Error("apply-confirm rejected: could not load the confirmed plan, its round, or the re-plan to compare the primary target's work; the pending confirmation is preserved",
					"repo", repo, "pr", pr, "database", database, "database_type", dbType, "environment", environment,
					"pending_plan_id", expectedPendingPlanID, "plan_id", planResp.PlanID, "error", coverErr)
				h.postCommandError(repo, pr, installationID, actionName, environment, requestedBy,
					"SchemaBot could not verify the plan this confirmation covers, so nothing was applied. Retry the command, and see server logs if it persists.")
				return
			}
			if difference != workUnchanged {
				reason := primaryTargetDifferenceReason(h.primaryTargetName(rollout.work, planResp, schemaResult.Database, environment), difference)
				h.logger.Info("apply-confirm refused: the primary target would run work the confirmed plan did not show",
					"repo", repo, "pr", pr, "database", database, "database_type", dbType, "environment", environment,
					"pending_plan_id", expectedPendingPlanID, "plan_id", planResp.PlanID, "reason", reason)
				h.releaseApplyLockIfIntentUnchanged(ctx, repo, pr, database, dbType, environment, expectedPendingPlanID, reason)
				h.postCommandError(repo, pr, installationID, actionName, environment, requestedBy,
					unconfirmedWorkMessage(rollout.work, reason))
				return
			}
		}
		h.logger.Debug("apply: only the primary target's own plan runs",
			"repo", repo, "pr", pr, "database", database, "environment", environment, "plan_id", planResp.PlanID)
	}
	// Engine-blocked changes reject the apply outright — the re-plan may have
	// resolved a change to blocked even if the primary plan had none (e.g.
	// the direct execution policy changed, or the table grew past its bound).
	// Release the lock: no retry of this command can succeed, so holding it
	// would only force a manual unlock after the schema is rewritten. An
	// apply-confirm given on a rollout's comment refuses a reviewed-target
	// statement newly resolved to blocked above instead, since how each
	// statement runs is part of the confirmed plan it is held to.
	if planResp.HasBlockedChanges() {
		h.rejectBlockedChanges(ctx, repo, pr, installationID, schemaResult, planResp, environment, requestedBy, actionName, result, expectedPendingPlanID)
		return
	}

	// Automatic apply DDL drift check: if the re-plan DDL differs from the stored auto-plan,
	// downgrade to manual confirmation so the user reviews the new plan.
	if storedPlan != nil && !ddlMatchesStoredPlan(planResp, storedPlan) {
		h.logger.Info("automatic apply downgraded: DDL drift detected",
			"repo", repo, "pr", pr, "database", database, "environment", environment)
		if err := h.postAutoConfirmDowngrade(ctx, client, repo, pr, installationID, schemaResult, planResp, environment, result, requestedBy,
			planDriftCause(planResp, storedPlan), rolloutPreview, runsMemberWork); err != nil {
			h.logger.Error("failed to post the DDL-drift downgrade comment",
				"repo", repo, "pr", pr, "database", database, "database_type", dbType,
				"environment", environment, "error", err)
		}
		return
	}

	// Engine refusals are judged against the live table, so the re-plan can
	// route a statement to direct execution that the plan behind the comment the
	// operator was last shown ran through the engine, without its DDL changing.
	// That comment never disclosed the native DDL, so stop and ask against one
	// that does.
	//
	// On a single target, the pending confirmation moves onto this re-plan, so
	// confirming it is checked against the comment that disclosed the direct
	// statements rather than stopping again on the old one. An apply-confirm
	// given on a rollout's comment never reaches here with such a change: the
	// primary target is held to its whole confirmed plan above, how each
	// statement runs included, so a statement newly routed to direct execution
	// refuses there and releases the lock, and the operator reviews the rollout
	// again with a fresh apply.
	if disclosedPlan != nil {
		if newlyDirect := newlyDirectChanges(planResp, disclosedPlan); len(newlyDirect) > 0 {
			h.logger.Info("apply stopped for confirmation: re-plan routes changes to direct execution that the disclosed plan did not",
				"repo", repo, "pr", pr, "database", database, "database_type", dbType,
				"environment", environment, "action", actionName,
				"plan_id", planResp.PlanID, "disclosed_plan_id", disclosedPlan.PlanIdentifier, "newly_direct", len(newlyDirect))
			if err := h.postAutoConfirmDowngrade(ctx, client, repo, pr, installationID, schemaResult, planResp, environment, result, requestedBy,
				newlyDirectCause(newlyDirect), rolloutPreview, runsMemberWork); err != nil {
				h.logger.Error("failed to post the comment disclosing the newly-direct changes, so the pending confirmation was not moved",
					"repo", repo, "pr", pr, "database", database, "database_type", dbType,
					"environment", environment, "plan_id", planResp.PlanID, "error", err)
				return
			}
			// The comment just posted renders this re-plan, so it discloses
			// whatever unfinished copy the re-plan would discard.
			disclosesDiscard := len(planResp.DiscardedCopies()) > 0
			if err := h.repinPendingConfirmation(ctx, repo, pr, database, dbType, environment, actionName, expectedPendingPlanID, planResp.PlanID, disclosesDiscard); err != nil {
				if h.reportRepinRefused(err, repo, pr, installationID, actionName, database, environment, requestedBy, recoveryCommand) {
					return
				}
				h.logger.Error("failed to re-pin pending confirmation onto the plan that discloses the newly-direct changes",
					"repo", repo, "pr", pr, "database", database, "database_type", dbType,
					"environment", environment, "plan_id", planResp.PlanID, "error", err)
				h.postCommandError(repo, pr, installationID, actionName, environment, requestedBy,
					"Some changes now run as direct execution. SchemaBot stopped the apply but could not record the confirmation; re-run `schemabot apply -e "+environment+"` to see how they will run.")
			}
			return
		}
	}

	// The copy on the target is read fresh on every plan, so this re-plan can
	// discover a discard the comment behind this apply never showed: another
	// apply can start a copy, or an adopted copy's checkpoint can age out,
	// between the disclosure and this moment. An apply that was allowed to reach
	// here because nothing was at stake must not become one that destroys hours
	// of work with nobody asked, so it stops and asks against a comment that
	// discloses the copy actually at stake.
	//
	// A discard the operator was already shown proceeds. They agreed to that
	// cost, and asking again on every confirm would make the stop unpassable.
	if discarded := planResp.DiscardedCopies(); len(discarded) > 0 && !disclosedCopyDiscard {
		h.logger.Info("apply stopped for confirmation: re-plan discards an existing copy the disclosure did not show",
			"repo", repo, "pr", pr, "database", database, "database_type", dbType,
			"environment", environment, "action", actionName,
			"plan_id", planResp.PlanID, "discarded_copies", len(discarded))
		// The disclosure is posted before it is recorded. The record's whole
		// claim is that the operator was shown the copy, so a post that never
		// lands must leave no consent behind: the next attempt stops and asks
		// again rather than dispatching over a disclosure nobody read.
		if err := h.postAutoConfirmDowngrade(ctx, client, repo, pr, installationID, schemaResult, planResp, environment, result, requestedBy,
			nil, rolloutPreview, runsMemberWork); err != nil {
			h.logger.Error("failed to post the comment disclosing the discard, so no consent was recorded",
				"repo", repo, "pr", pr, "database", database, "database_type", dbType,
				"environment", environment, "plan_id", planResp.PlanID, "error", err)
			return
		}
		if err := h.repinPendingConfirmation(ctx, repo, pr, database, dbType, environment, actionName, expectedPendingPlanID, planResp.PlanID, true); err != nil {
			if h.reportRepinRefused(err, repo, pr, installationID, actionName, database, environment, requestedBy, recoveryCommand) {
				return
			}
			// Without the re-pin the confirm command would load the disclosure
			// that showed no discard and stop again, so say what happened rather
			// than leaving a confirmation the operator cannot pass.
			h.logger.Error("failed to re-pin pending confirmation onto the plan that discloses the discard",
				"repo", repo, "pr", pr, "database", database, "database_type", dbType,
				"environment", environment, "plan_id", planResp.PlanID, "error", err)
			h.postCommandError(repo, pr, installationID, actionName, environment, requestedBy,
				"Applying would destroy work in progress on the target. SchemaBot stopped the apply but could not record the confirmation; re-run `schemabot apply -e "+environment+"` to see what is at stake.")
		}
		return
	}

	// --defer-cutover only affects engine-driven statements; an apply whose
	// every target runs only direct statements has no cutover to defer, so
	// reject the flag instead of silently ignoring it. Apply-confirm keeps the
	// lock: it still pins the plan the operator confirmed against, and
	// re-running apply-confirm without the flag executes it. An automatic
	// apply releases it, as on its other rejections, so the apply command
	// starts over. The apply command rejects the flag before locking on an
	// apply with nothing to defer, so an automatic apply normally rejects
	// here only when reading the targets' plans fails.
	if result.DeferCutover {
		automatic := storedPlan != nil
		nothingToDefer, deferErr := h.deferCutoverHasNothingToDefer(ctx, planResp, environment, runsMemberWork)
		if deferErr != nil {
			h.rejectUnverifiedMemberWork(ctx, repo, pr, installationID, schemaResult, environment, requestedBy, actionName, automatic, expectedPendingPlanID, planResp.PlanID,
				"could not read every target's plan to tell whether --defer-cutover has a cutover to defer", deferErr,
				"the other targets' plans")
			return
		}
		if nothingToDefer && automatic {
			h.logger.Info("automatic apply rejected: --defer-cutover on an apply whose every target runs only direct changes; releasing the lock",
				"repo", repo, "pr", pr, "database", database, "database_type", dbType, "environment", environment, "action", actionName,
				"posted_plan_id", expectedPendingPlanID, "plan_id", planResp.PlanID, "runs_other_targets", runsMemberWork)
			h.releaseApplyLockIfIntentUnchanged(ctx, repo, pr, database, dbType, environment, expectedPendingPlanID, "--defer-cutover has nothing to defer")
			h.postCommandError(repo, pr, installationID, actionName, environment, requestedBy, msgDeferCutoverAllDirect)
			return
		}
		if nothingToDefer {
			h.logger.Info("apply rejected: --defer-cutover on an apply whose every target runs only direct changes; the pending confirmation is preserved",
				"repo", repo, "pr", pr, "database", database, "database_type", dbType, "environment", environment, "action", actionName,
				"plan_id", planResp.PlanID, "runs_other_targets", runsMemberWork)
			h.postCommandError(repo, pr, installationID, actionName, environment, requestedBy,
				fmt.Sprintf(msgDeferCutoverAllDirectConfirm, environment))
			return
		}
	}

	// Block unsafe changes on confirm (re-plan may have detected new unsafe
	// changes), on every target the apply runs.
	if blocked := h.blockUnsafeWithoutOptIn(ctx, client, repo, pr, installationID, schemaResult, planResp, environment, requestedBy, result, runsMemberWork, rolloutPreview); blocked {
		return
	}

	// Build apply options
	options := make(map[string]string)
	if result.DeferCutover {
		options["defer_cutover"] = "true"
	}
	if result.SkipRevert {
		options["skip_revert"] = "true"
	}
	if result.AllowUnsafe {
		options["allow_unsafe"] = "true"
	}

	caller := formatGitHubCaller(requestedBy, repo, pr)

	// Resolve the App factory for this repo once so the observer captures
	// the correct App for all subsequent GitHub calls (comments, check runs).
	// Failure here is unrecoverable for outbound calls — the same error would
	// also block postComment — so log and return without attempting a comment.
	factory, factoryErr := h.factoryForRepo(repo)
	if factoryErr != nil {
		h.logger.Error("apply blocked: cannot resolve GitHub App client for repo",
			"repo", repo, "pr", pr, "database", database, "database_type", dbType, "environment", environment, "error", factoryErr)
		return
	}

	// Set observer before queuing the apply so ExecuteApply can register it on
	// the durable apply row before operator dispatch starts.
	observerCfg := h.commentObserverConfig(factory, repo, pr, installationID)
	observerCfg.DeferCutover = options["defer_cutover"] == "true"
	observerCfg.OnTerminalHook = func(apply *storage.Apply) {
		// refreshChecksForTerminalApply routes a completed rollback straight
		// to action_required. The observer registered here can be consumed by
		// a rollback apply (pending observers share a per-target key), so the
		// terminal ordering must honor the rollback intent from the durable
		// apply, not from the command that registered the observer.
		h.refreshChecksForTerminalApply(context.Background(), apply, "apply command")
	}
	observer := NewCommentObserver(observerCfg)
	pendingObserver := h.service.SetPendingObserver(database, "", environment, observer)

	applyReq := api.ApplyRequest{
		PlanID:                planResp.PlanID,
		Environment:           environment,
		Options:               options,
		Caller:                caller,
		InstallationID:        installationID,
		ExpectedLockOwner:     fmt.Sprintf("%s#%d", repo, pr),
		ExpectedPendingPlanID: expectedPendingPlanID,
		ConfirmedMemberWork:   confirmedMemberWork,
		Target:                result.Target,
	}

	applyResp, applyID, err := h.service.ExecuteApply(ctx, applyReq)
	if err != nil {
		h.service.ClearPendingObserver(pendingObserver)
		h.logger.Error("apply execution failed", "repo", repo, "pr", pr, "database", database, "database_type", dbType, "environment", environment, "error", err)
		h.postCommandError(repo, pr, installationID, actionName, environment, requestedBy, applyExecutionErrorMessage(actionName, environment, err))
		return
	}

	if !applyResp.Accepted {
		h.service.ClearPendingObserver(pendingObserver)
		h.logger.Info("apply rejected by engine", "repo", repo, "pr", pr, "database", database, "environment", environment, "error", applyResp.ErrorMessage)
		h.postCommandError(repo, pr, installationID, actionName, environment, requestedBy, "The apply was not accepted. See SchemaBot server logs for details.")
		return
	}

	// ExecuteApply rejects accepted applies unless SchemaBot stored its own
	// apply row. Keep this guard fail-closed in case that invariant changes.
	if applyID <= 0 {
		h.service.ClearPendingObserver(pendingObserver)
		h.logger.Error("accepted apply did not return an apply id",
			"repo", repo, "pr", pr, "database", database,
			"database_type", schemaResult.Type, "environment", environment,
			"apply_id", applyResp.ApplyID)
		h.postCommandError(repo, pr, installationID, action.Apply, environment, requestedBy, "Apply was accepted, but SchemaBot did not receive a stored apply ID. SchemaBot cannot safely track progress or update required status checks. An operator must reconcile the apply state before retrying.")
		return
	}

	apply, err := h.service.Storage().Applies().Get(ctx, applyID)
	if err != nil {
		h.logger.Error("failed to load apply after accepted apply",
			"repo", repo, "pr", pr, "database", database,
			"database_type", schemaResult.Type, "environment", environment,
			"apply_id", applyResp.ApplyID, "error", err)
		return
	}
	if apply == nil {
		h.logger.Error("apply missing after accepted apply",
			"repo", repo, "pr", pr, "database", database,
			"database_type", schemaResult.Type, "environment", environment,
			"apply_id", applyResp.ApplyID)
		return
	}

	// Post the progress comment immediately so the observer always has a
	// comment to edit. This must happen before any terminal check — otherwise
	// the apply could complete between the check and the post, leaving a
	// stale "In Progress" comment that the observer never edits.
	progressBody := templates.RenderApplyStarted(templates.ApplyStatusCommentData{
		ApplyID:     applyResp.ApplyID,
		Database:    database,
		Environment: environment,
		RequestedBy: requestedBy,
		State:       apply.State,
		Engine:      schemaResult.Type,
	})
	h.postInitialProgressComment(ctx, repo, pr, installationID, apply, progressBody)

	// Update stored check state to in_progress (transitions action_required to in_progress).
	if err := h.updateCheckRecordForApplyStart(ctx, client, repo, pr, schemaResult, environment, apply); err != nil {
		h.logger.Error("failed to mark check in_progress for apply",
			append(apply.LogAttrs(), "error", err)...)
		h.postCommandError(repo, pr, installationID, action.Apply, environment, requestedBy, "Apply was accepted, but SchemaBot could not update the required status check: "+err.Error())
		return
	}
}

// rolloutPlansChangedCause heads the comment an automatic apply posts when a
// target's plan changed between the comment the apply command posted and the
// re-plan the apply runs from. It names each target whose plan changed and,
// per table, how, so the reader sees what moved without diffing two comments.
// A target whose statements are unchanged but whose plan still differs, in how
// a statement runs, which statements are unsafe, the namespaces it finalizes,
// or the VSchema it writes, gets one entry naming that part.
func (h *Handler) rolloutPlansChangedCause(ctx context.Context, postedPlanID, planID, environment string) (*templates.PausedApplyCauseData, error) {
	postedPrimary, posted, err := h.reviewRoundPlans(ctx, postedPlanID, environment)
	if err != nil {
		return nil, fmt.Errorf("load the round the apply posted: %w", err)
	}
	currentPrimary, current, err := h.reviewRoundPlans(ctx, planID, environment)
	if err != nil {
		return nil, fmt.Errorf("load the round the apply re-planned: %w", err)
	}
	started := roundWithPrimary(postedPrimary, posted)
	now := roundWithPrimary(currentPrimary, current)
	members := make(map[string]struct{}, len(started)+len(now))
	for member := range started {
		members[member] = struct{}{}
	}
	for member := range now {
		members[member] = struct{}{}
	}

	type changedTarget struct {
		name    string
		was, is *storage.Plan
	}
	var changed []changedTarget
	for _, member := range slices.Sorted(maps.Keys(members)) {
		was, is := started[member], now[member]
		named, round := is, now
		if named == nil {
			named, round = was, started
		}
		target := changedTarget{name: roundMemberName(named, round), was: was, is: is}
		if len(targetPlanDriftEntries(target.was, target.is, "")) > 0 {
			changed = append(changed, target)
		}
	}
	// One changed target is named in the heading, so its entries need not
	// repeat it; with several, each entry says which target it is on.
	var entries []string
	for _, target := range changed {
		name := ""
		if len(changed) > 1 {
			name = target.name
		}
		entries = append(entries, targetPlanDriftEntries(target.was, target.is, name)...)
	}
	if len(entries) > planDriftEntryCap {
		remaining := len(entries) - planDriftEntryCap
		entries = append(entries[:planDriftEntryCap:planDriftEntryCap],
			fmt.Sprintf("and %d more %s", remaining, ui.PluralizeLabel("change", "changes", remaining)))
	}

	heading := "The targets' plans changed before this apply could start"
	switch {
	case len(changed) == 1:
		heading = fmt.Sprintf("The plan for target `%s` changed before this apply could start", changed[0].name)
	case len(changed) > 1:
		heading = fmt.Sprintf("The plans for %d targets changed before this apply could start", len(changed))
	}
	return &templates.PausedApplyCauseData{
		Heading: heading,
		Entries: entries,
	}, nil
}

// roundWithPrimary is a round's plans keyed by member, the primary target's
// own plan included.
func roundWithPrimary(primary *storage.Plan, members map[string]*storage.Plan) map[string]*storage.Plan {
	round := maps.Clone(members)
	if round == nil {
		round = make(map[string]*storage.Plan, 1)
	}
	round[qualifiedTargetName(routing.ExecutionTarget{Deployment: primary.Deployment, Target: primary.Target})] = primary
	return round
}

// targetPlanDriftEntries says how one target's plan now differs from the plan
// the apply was started from, either of which may be absent. A non-empty name
// starts each entry, for an apply whose entries span several targets.
func targetPlanDriftEntries(was, is *storage.Plan, name string) []string {
	var wasIDs, isIDs map[planChangeIdentity]int
	if was != nil {
		wasIDs = storedPlanIdentities(was)
	}
	if is != nil {
		isIDs = storedPlanIdentities(is)
	}
	tablePrefix, subject := "", "It"
	if name != "" {
		tablePrefix, subject = fmt.Sprintf("Target `%s`: ", name), fmt.Sprintf("Target `%s`", name)
	}
	if entries := planDriftEntries(planDriftStatements(isIDs), planDriftStatements(wasIDs), tablePrefix); len(entries) > 0 {
		return entries
	}
	switch {
	case was == nil && is != nil && is.HasWork():
		return []string{subject + " now has changes to apply"}
	case was != nil && is == nil && was.HasWork():
		return []string{subject + " no longer has changes to apply"}
	case was != nil && is != nil:
		if difference := memberWorkDifference(was, is); difference != workUnchanged {
			if name == "" {
				return []string{fmt.Sprintf("%s changed", ui.CapitalizeFirst(string(difference)))}
			}
			return []string{fmt.Sprintf("%s: %s changed", subject, difference)}
		}
	}
	return nil
}

// rejectUnverifiedMemberWork answers an apply that could not read what it needs
// to tell whether the other targets' work is what the comment behind it showed.
// Nothing is known to be wrong with that comment, so an apply-confirm keeps its
// pending confirmation for a retry. An automatic apply releases the lock it
// took, as its other failures past the hand-off do: the operator's recovery is
// re-issuing the command.
func (h *Handler) rejectUnverifiedMemberWork(
	ctx context.Context, repo string, pr int, installationID int64, schemaResult *ghclient.SchemaRequestResult,
	environment, requestedBy, actionName string, automatic bool, expectedPendingPlanID, planID string,
	what string, err error, unverified string,
) {
	database, dbType := schemaResult.Database, schemaResult.Type
	if automatic {
		h.logger.Error("automatic apply rejected: "+what+"; releasing the lock",
			"repo", repo, "pr", pr, "database", database, "database_type", dbType, "environment", environment,
			"posted_plan_id", expectedPendingPlanID, "plan_id", planID, "error", err)
		h.releaseApplyLockIfIntentUnchanged(ctx, repo, pr, database, dbType, environment, expectedPendingPlanID, what)
	} else {
		h.logger.Error("apply-confirm rejected: "+what+"; the pending confirmation is preserved",
			"repo", repo, "pr", pr, "database", database, "database_type", dbType, "environment", environment,
			"pending_plan_id", expectedPendingPlanID, "plan_id", planID, "error", err)
	}
	h.postCommandError(repo, pr, installationID, actionName, environment, requestedBy, unverifiedMemberWorkMessage(unverified, environment, automatic))
}

// rejectBlockedChanges rejects an apply whose re-plan the engine blocks and
// releases its lock: no retry of the command can succeed, so holding the lock
// would only force a manual unlock after the schema is rewritten.
func (h *Handler) rejectBlockedChanges(
	ctx context.Context, repo string, pr int, installationID int64, schemaResult *ghclient.SchemaRequestResult,
	planResp *apitypes.PlanResponse, environment, requestedBy, actionName string, result CommandResult, expectedPendingPlanID string,
) {
	database, dbType := schemaResult.Database, schemaResult.Type
	commentData := buildPlanCommentData(schemaResult, planResp, environment, result.Tenant, requestedBy, h.agentHint(), h.cliName())
	commentData.ScopedDatabase = result.Database
	commentData.Target = narrowedTarget(planResp, result.Target)
	h.logger.Info("apply rejected: re-plan contains engine-blocked changes",
		"repo", repo, "pr", pr, "database", database, "database_type", dbType, "environment", environment,
		"action", actionName, "plan_id", planResp.PlanID)
	h.postComment(repo, pr, installationID, templates.RenderBlockedChangesApplyRejected(commentData))
	h.releaseApplyLockIfIntentUnchanged(ctx, repo, pr, database, dbType, environment, expectedPendingPlanID, "engine-blocked changes rejection")
}

// unverifiedMemberWorkMessage tells the operator that nothing ran because
// SchemaBot could not verify the named plans, and how to recover from the
// state the rejection left: an automatic apply releases its lock unless the
// intent changed, so the apply command starts over, while apply-confirm kept the pending confirmation, so
// confirming again with the same flags retries against the same comment.
func unverifiedMemberWorkMessage(unverified, environment string, automatic bool) string {
	recovery := fmt.Sprintf("The pending confirmation is preserved; re-run `schemabot %s -e %s` with the same flags, and see server logs if it persists.", action.ApplyConfirm, environment)
	if automatic {
		recovery = "Run apply again, and see server logs if it persists."
	}
	return fmt.Sprintf("SchemaBot could not verify %s, so nothing was applied. %s", unverified, recovery)
}

func applyExecutionErrorMessage(command, environment string, err error) string {
	return dispatchErrorMessage(err, dispatchMessages{
		command:     command,
		environment: environment,
		lockIntentChanged: "The pending schema change changed while this command was running. " +
			"The apply was rejected; review the latest plan and run the command again.",
		internal: "Failed to execute apply. See SchemaBot server logs for details.",
	})
}

// dispatchMessages carries the command-specific words dispatchErrorMessage
// renders around the shared classification of a failed dispatch.
type dispatchMessages struct {
	// command and environment name the PR command to re-issue in a remedy,
	// for example `schemabot rollback-confirm -e staging`.
	command     string
	environment string
	// lockIntentChanged is the whole message for a lock intent change; its
	// recovery differs between apply and rollback.
	lockIntentChanged string
	// afterRefusal, when set, follows the remedy for a refused feature and
	// says what the refusal left in place for the re-issued command to use.
	afterRefusal string
	// internal is the fixed line for every failure whose text belongs in
	// server logs.
	internal string
}

// dispatchErrorMessage renders the PR-facing detail for a failed apply or
// rollback dispatch. Two failures are deterministic and carry their own
// recovery, so they are named rather than sanitized: a lock intent change is an
// expected race whose answer is lockIntentChanged, and an unsupported feature
// is rejected before anything is stored and would be refused the same way on
// retry, so the operator sees the feature error followed by the command to
// re-issue without the option that asked for it. A rollout member's refused
// plan is named by target and table from fields SchemaBot controls. Everything
// else is an internal error whose text stays in server logs behind the fixed
// line.
func dispatchErrorMessage(err error, msgs dispatchMessages) string {
	if errors.Is(err, storage.ErrLockIntentChanged) {
		return msgs.lockIntentChanged
	}
	var featureErr *api.UnsupportedFeatureError
	if errors.As(err, &featureErr) {
		parts := []string{featureErr.Error() + "."}
		if remedy := unsupportedFeatureRemedy(featureErr.Feature, msgs.command, msgs.environment); remedy != "" {
			parts = append(parts, remedy)
			if msgs.afterRefusal != "" {
				parts = append(parts, msgs.afterRefusal)
			}
		}
		return strings.Join(parts, " ")
	}
	if refused, ok := errors.AsType[*api.MemberPlanRefusedError](err); ok {
		switch refused.Refusal {
		case api.MemberPlanBlocked:
			return templates.MemberPlanBlockedDetail(refused.Target, refused.Table)
		case api.MemberPlanUndisclosedUnsafe:
			return templates.MemberPlanUndisclosedUnsafeDetail(refused.Target, refused.Table, refused.Namespace)
		case api.MemberPlanUnsafeWithoutOptIn:
			return templates.MemberPlanUnsafeWithoutOptInDetail(refused.Target, refused.Table, refused.Namespace)
		}
		// A refusal kind with no line of its own takes the generic one below,
		// never the error text.
	}
	return msgs.internal
}

// unsupportedFeatureRemedy names the command to re-issue without the option
// that asked for a feature the database type refused. Features no PR command
// option requests have no remedy: the operator cannot change the request.
func unsupportedFeatureRemedy(feature schema.Feature, command, environment string) string {
	if feature != schema.FeatureDeferredCutover {
		return ""
	}
	return fmt.Sprintf("Run `schemabot %s -e %s` again without `--defer-cutover`.", command, environment)
}

// postAutoConfirmDowngrade posts the locked plan comment that pauses an
// automatic apply for manual confirmation. It carries the original command's
// flags and the lock owner so the coached apply-confirm command re-issues the
// operator's full intent and the comment shows who holds the lock.
//
// The re-plan this downgrade acts on is the first time an automatic apply sees
// the live database, so it is also the first time it can see a destructive
// change to a table another pull request owns. The attributed-change disclosure
// belongs on this comment for the same reason as the direct-execution one: it
// must sit on the comment the confirmation acts on.
// It reports whether the comment landed, so a caller that records what the
// comment disclosed can decline to record it when the operator was shown
// nothing.
func (h *Handler) postAutoConfirmDowngrade(
	ctx context.Context, client *ghclient.InstallationClient,
	repo string, pr int, installationID int64, schemaResult *ghclient.SchemaRequestResult,
	planResp *apitypes.PlanResponse, environment string, result CommandResult, requestedBy string,
	cause *templates.PausedApplyCauseData, rolloutPreview *templates.DeploymentDriftData, runsMemberWork bool,
) error {
	commentData := buildPlanCommentData(schemaResult, planResp, environment, result.Tenant, requestedBy, h.agentHint(), h.cliName())
	commentData.ScopedDatabase = result.Database
	commentData.Target = narrowedTarget(planResp, result.Target)
	h.annotateAttributedChanges(ctx, client, &commentData, planResp, rolloutPreview, repo, pr, environment)
	commentData.IsLocked = true
	commentData.LockOwner = fmt.Sprintf("%s#%d", repo, pr)
	commentData.AllowUnsafe = result.AllowUnsafe
	commentData.DeferCutover = result.DeferCutover
	if result.DeferCutover {
		commentData.NoCutoverToDefer = h.confirmCommandHasNoCutoverToDefer(ctx, repo, pr, planResp, environment, runsMemberWork)
	}
	commentData.SkipRevert = result.SkipRevert
	commentData.PendingManualConfirmation = true
	commentData.PausedApplyCause = cause
	// A confirmation of this comment covers the other targets' work only if
	// the comment renders their plans.
	commentData.DeploymentDrift = rolloutPreview
	return h.postCommentReportingError(repo, pr, installationID, templates.RenderPlanComment(commentData))
}

// repinPendingConfirmation moves this PR's apply lock onto the plan whose
// comment is being posted now, together with what that comment discloses about
// an unfinished copy on the target. Both move in one storage write, so the
// record can never describe a plan other than the one apply-confirm loads.
//
// A stop that does not re-pin is a stop nobody can pass: the confirm command
// reads the lock to learn what the operator was shown, so a lock still pointing
// at the comment that disclosed nothing would stop the same apply again on every
// attempt.
// The re-pin is refused with storage.ErrLockIntentChanged when the lock no
// longer carries the pending intent this apply observed — a rollback the
// operator issued while the gate ran owns the lock now, and overwriting its pin
// would answer "no pending rollback" to the rollback-confirm they are about to
// send — or when the lock is gone, so a lock someone released stays released.
// The write itself is conditional on that intent, so the answer does not
// depend on when the lock changed. Declining leaves the copy gate armed, so
// the next apply-confirm stops and discloses again rather than proceeding on
// consent that was never recorded.
func (h *Handler) repinPendingConfirmation(ctx context.Context, repo string, pr int, database, dbType, environment, actionName, expectedPendingPlanID, planID string, disclosedCopyDiscard bool) error {
	// An empty observed intent asks the conditional acquire for a free lock,
	// which would re-create a released one, so there is nothing safe to move.
	if expectedPendingPlanID == "" {
		return fmt.Errorf("re-pin pending confirmation for %s (%s) onto plan %s: the apply observed no pending confirmation to move",
			database, dbType, planID)
	}
	err := h.service.Storage().Locks().AcquireIfPendingPlanID(ctx, &storage.Lock{
		DatabaseName:         database,
		DatabaseType:         dbType,
		Owner:                fmt.Sprintf("%s#%d", repo, pr),
		Repository:           repo,
		PullRequest:          pr,
		PendingPlanID:        planID,
		DisclosedCopyDiscard: disclosedCopyDiscard,
	}, expectedPendingPlanID)
	if errors.Is(err, storage.ErrLockIntentChanged) {
		h.logPreservedLockIntent(ctx, repo, pr, database, dbType, environment, actionName, expectedPendingPlanID, planID)
	}
	if err != nil {
		return fmt.Errorf("re-pin pending confirmation for %s (%s) onto plan %s from %s: %w",
			database, dbType, planID, expectedPendingPlanID, err)
	}
	return nil
}

// logPreservedLockIntent records that a re-pin left the lock as another
// command set it, with the lock's state after the refusal so the log says
// whether a newer intent holds it or it was released. The lock is keyed by
// database, so the environment and the command that was refused are named
// here for a recurring race to be placed.
func (h *Handler) logPreservedLockIntent(ctx context.Context, repo string, pr int, database, dbType, environment, actionName, expectedPendingPlanID, planID string) {
	attrs := []any{
		"repo", repo, "pr", pr, "database", database, "database_type", dbType,
		"environment", environment, "action", actionName,
		"expected_pending_plan_id", expectedPendingPlanID, "plan_id", planID,
	}
	current, err := h.service.Storage().Locks().Get(ctx, database, dbType)
	switch {
	case err != nil:
		attrs = append(attrs, "lock_read_error", err)
	case current == nil:
		attrs = append(attrs, "lock_present", false)
	default:
		attrs = append(attrs, "lock_present", true, "observed_pending_plan_id", current.PendingPlanID, "lock_owner", current.Owner)
	}
	h.logger.Warn("preserved the lock's current intent instead of re-pinning the confirmation onto the disclosing plan", attrs...)
}

// reportRepinRefused answers an apply whose pending confirmation could not be
// re-pinned because another command pinned or released the lock meanwhile. The
// stop comment just posted coaches a confirmation the lock no longer carries,
// so the operator is told the lock changed, given recoveryCommand to re-run,
// and told that a retry names the command holding the lock, if any. It reports
// whether err was that refusal; any other error is left for the caller to
// report. repinPendingConfirmation has already logged the refusal with the
// lock's state, so it is not logged again here.
func (h *Handler) reportRepinRefused(err error, repo string, pr int, installationID int64, actionName, database, environment, requestedBy, recoveryCommand string) bool {
	if !errors.Is(err, storage.ErrLockIntentChanged) {
		return false
	}
	h.postCommandError(repo, pr, installationID, actionName, environment, requestedBy, applyLockIntentChangedRefusal(database, recoveryCommand))
	return true
}

// releaseApplyLockIfIntentUnchanged releases this PR's apply lock after a
// pre-execution gate rejected (or obviated) the apply, but only while the lock
// still carries the exact pending intent the rejecting handler observed. A lock
// whose pending plan has since changed — e.g. a rollback plan re-pinned it while
// the gate ran — belongs to that newer intent and is preserved. The reason names
// the gate that triggered the release so logs distinguish the call sites.
func (h *Handler) releaseApplyLockIfIntentUnchanged(ctx context.Context, repo string, pr int, database, dbType, environment, expectedPendingPlanID, reason string) {
	lockOwner := fmt.Sprintf("%s#%d", repo, pr)
	released, relErr := h.service.Storage().Locks().ReleaseIfPendingPlanID(ctx, database, dbType, lockOwner, expectedPendingPlanID)
	if relErr != nil {
		h.logger.Error("failed to release apply lock after pre-execution rejection",
			"repo", repo, "pr", pr, "database", database, "database_type", dbType,
			"environment", environment, "reason", reason, "error", relErr)
		return
	}
	if !released {
		h.logger.Info("preserved apply lock after pre-execution rejection because its pending intent changed",
			"repo", repo, "pr", pr, "database", database, "database_type", dbType,
			"environment", environment, "reason", reason,
			"expected_pending_plan_id", expectedPendingPlanID)
	}
}

// planChangeIdentity is the drift-comparison key for a single table change. A
// bare DDL string is not enough: the same DDL text can move between namespaces,
// tables, or operations (e.g. one keyspace dropping a table and another creating
// it), which a DDL-only multiset would treat as unchanged and auto-apply. The
// full identity is what the operator reviewed, so drift must be judged on it.
type planChangeIdentity struct {
	namespace string
	table     string
	operation string
	ddl       string
}

// planDriftEntryCap bounds the drift disclosure. A re-plan that differs in
// dozens of changes has already told the reader what they need — the plan they
// are looking at is not the one the apply started from — and listing every one
// buries the statements above it.
const planDriftEntryCap = 8

// planDriftChange is one table's change without the DDL that realizes it. Drift
// is reported at this granularity because an entry names the table and the
// operation, so two identities that differ only in DDL are one line to the
// reader, not two.
type planDriftChange struct {
	namespace string
	table     string
	operation string
}

// planDriftStatements groups a plan's change identities by the table change
// they realize, keeping each distinct statement and how many times it appears.
func planDriftStatements(identities map[planChangeIdentity]int) map[planDriftChange]map[string]int {
	grouped := make(map[planDriftChange]map[string]int)
	for id, count := range identities {
		change := planDriftChange{namespace: id.namespace, table: id.table, operation: id.operation}
		if grouped[change] == nil {
			grouped[change] = make(map[string]int)
		}
		grouped[change][id.ddl] += count
	}
	return grouped
}

// planDriftCause describes how the re-plan differs from the plan the apply was
// started from, per table, so the reader can see what moved without diffing two
// comments themselves. The DDL itself is not repeated: the statements that will
// run are already fenced above this disclosure. That is why a table in both
// plans with an amended statement is one "differs" entry rather than an added
// and a removed one — the pair would name the same table and operation twice,
// once as present and once as absent, and the field that tells them apart is
// the one deliberately left out.
func planDriftCause(planResp *apitypes.PlanResponse, storedPlan *storage.Plan) *templates.PausedApplyCauseData {
	now := planDriftStatements(responsePlanIdentities(planResp))
	started := planDriftStatements(storedPlanIdentities(storedPlan))

	entries := planDriftEntries(now, started, "")

	if len(entries) > planDriftEntryCap {
		remaining := len(entries) - planDriftEntryCap
		entries = append(entries[:planDriftEntryCap:planDriftEntryCap],
			fmt.Sprintf("and %d more %s", remaining, ui.PluralizeLabel("change", "changes", remaining)))
	}

	return &templates.PausedApplyCauseData{
		Heading: "Schema changes differ from the plan this apply was started from",
		Entries: entries,
	}
}

// directChangeIdentity is one statement routed to direct execution, keyed by
// where it runs: a sharded plan routes each shard's statement on its own.
type directChangeIdentity struct {
	namespace string
	shard     string
	table     string
	ddl       string
}

// newlyDirectChanges returns the statements the re-plan routes to direct
// execution that disclosedPlan did not, sorted for a stable disclosure. A
// statement that stops being direct is not returned: it now runs through the
// engine, which the comment already overstated rather than hid.
//
// A sharded namespace's own rows summarize its shards, so where a namespace has
// per-shard rows those alone are compared: counting the summary too would name
// one table once for the namespace and again for every shard. DDL is compared
// trimmed, since the stored plan trims each shard's statement and a planner's
// response need not.
func newlyDirectChanges(planResp *apitypes.PlanResponse, disclosedPlan *storage.Plan) []directChangeIdentity {
	disclosedSharded := make(map[string]bool)
	for _, sp := range disclosedPlan.Shards {
		disclosedSharded[normalizePlanNamespace(sp.Namespace)] = true
	}
	disclosed := make(map[directChangeIdentity]struct{})
	for _, tc := range disclosedPlan.FlatDDLChanges() {
		namespace := normalizePlanNamespace(tc.Namespace)
		if tc.DirectExecution() && !disclosedSharded[namespace] {
			disclosed[directChangeIdentity{namespace: namespace, table: tc.Table, ddl: strings.TrimSpace(tc.DDL)}] = struct{}{}
		}
	}
	for _, sp := range disclosedPlan.Shards {
		for _, tc := range sp.Changes {
			if tc.DirectExecution() {
				disclosed[directChangeIdentity{namespace: normalizePlanNamespace(sp.Namespace), shard: sp.Shard, table: tc.Table, ddl: strings.TrimSpace(tc.DDL)}] = struct{}{}
			}
		}
	}

	replanSharded := make(map[string]bool)
	for _, sp := range planResp.Shards {
		if sp != nil {
			replanSharded[normalizePlanNamespace(sp.Namespace)] = true
		}
	}
	var newly []directChangeIdentity
	addIfNew := func(id directChangeIdentity) {
		if _, ok := disclosed[id]; !ok {
			newly = append(newly, id)
		}
	}
	for _, sc := range planResp.Changes {
		namespace := normalizePlanNamespace(sc.Namespace)
		if replanSharded[namespace] {
			continue
		}
		for _, tc := range sc.TableChanges {
			if tc.DirectExecution() {
				addIfNew(directChangeIdentity{namespace: namespace, table: tc.TableName, ddl: strings.TrimSpace(tc.DDL)})
			}
		}
	}
	for _, sp := range planResp.Shards {
		if sp == nil {
			continue
		}
		for _, tc := range sp.Changes {
			if tc.DirectExecution() {
				addIfNew(directChangeIdentity{namespace: normalizePlanNamespace(sp.Namespace), shard: sp.Shard, table: tc.TableName, ddl: strings.TrimSpace(tc.DDL)})
			}
		}
	}
	slices.SortFunc(newly, func(a, b directChangeIdentity) int {
		return cmp.Or(
			cmp.Compare(a.namespace, b.namespace),
			cmp.Compare(a.table, b.table),
			cmp.Compare(a.shard, b.shard),
			cmp.Compare(a.ddl, b.ddl),
		)
	})
	return newly
}

// newlyDirectCause names each table the re-plan newly routes to direct
// execution, once per table with the shards it moved on. How the statements
// run is disclosed in the direct execution section, so the entries only say
// which tables moved.
func newlyDirectCause(newly []directChangeIdentity) *templates.PausedApplyCauseData {
	type tableKey struct{ namespace, table string }
	var order []tableKey
	shards := make(map[tableKey][]string)
	for _, id := range newly {
		key := tableKey{id.namespace, id.table}
		if _, seen := shards[key]; !seen {
			order = append(order, key)
			shards[key] = nil
		}
		if id.shard != "" && !slices.Contains(shards[key], id.shard) {
			shards[key] = append(shards[key], id.shard)
		}
	}
	var entries []string
	for _, key := range order {
		subject := fmt.Sprintf("`%s`", key.table)
		switch names := shards[key]; len(names) {
		case 0:
		case 1:
			subject += fmt.Sprintf(" (shard `%s`)", names[0])
		default:
			subject += " (shards `" + strings.Join(names, "`, `") + "`)"
		}
		entries = append(entries, subject+" now runs as direct execution")
	}
	if len(entries) > planDriftEntryCap {
		remaining := len(entries) - planDriftEntryCap
		entries = append(entries[:planDriftEntryCap:planDriftEntryCap],
			fmt.Sprintf("and %d more %s", remaining, ui.PluralizeLabel("change", "changes", remaining)))
	}
	return &templates.PausedApplyCauseData{
		Heading: "Changes run differently from the plan this apply was started from",
		Entries: entries,
	}
}

// planDriftEntries lists, per table, how the changes now differ from the ones
// the apply was started from. prefix, when set, starts each entry, for example
// with the target the table is on.
func planDriftEntries(now, started map[planDriftChange]map[string]int, prefix string) []string {
	// The namespace distinguishes the same table under two keyspaces, so it is
	// named only when the drift spans more than one — on the single-namespace
	// database it is noise the rest of the comment does not carry either.
	namespaces := make(map[string]struct{})
	for change := range now {
		namespaces[change.namespace] = struct{}{}
	}
	for change := range started {
		namespaces[change.namespace] = struct{}{}
	}
	qualify := len(namespaces) > 1

	changes := slices.SortedFunc(
		maps.Keys(planDriftUnion(now, started)),
		func(a, b planDriftChange) int {
			return cmp.Or(
				cmp.Compare(a.namespace, b.namespace),
				cmp.Compare(a.table, b.table),
				cmp.Compare(a.operation, b.operation),
			)
		})

	var entries []string
	for _, change := range changes {
		phrase, drifted := planDriftPhrase(now[change], started[change])
		if !drifted {
			continue
		}
		subject := fmt.Sprintf("`%s`", change.table)
		if qualify {
			subject = fmt.Sprintf("`%s` in `%s`", change.table, change.namespace)
		}
		entries = append(entries, fmt.Sprintf("%s%s (%s) %s", prefix, subject, change.operation, phrase))
	}
	return entries
}

// planDriftUnion is every table change either plan carries, so one pass over it
// classifies each as added, dropped, or amended rather than two passes finding
// the same change from both ends.
func planDriftUnion(now, started map[planDriftChange]map[string]int) map[planDriftChange]struct{} {
	union := make(map[planDriftChange]struct{}, len(now)+len(started))
	for change := range now {
		union[change] = struct{}{}
	}
	for change := range started {
		union[change] = struct{}{}
	}
	return union
}

// planDriftPhrase says how one table change differs between the two plans, and
// whether it differs at all.
func planDriftPhrase(now, started map[string]int) (string, bool) {
	switch {
	case len(started) == 0:
		return "is new", true
	case len(now) == 0:
		return "is no longer planned", true
	case maps.Equal(now, started):
		return "", false
	default:
		return "now runs a different statement", true
	}
}

// ddlMatchesStoredPlan reports whether the re-plan describes the same set of
// table changes the operator reviewed in storedPlan. Comparison is
// order-independent (the flattening helpers may emit changes in different order)
// and keyed on the full change identity, not DDL text alone. Any mismatch means
// drift, and the caller downgrades an automatic apply to manual confirmation —
// so this errs toward requiring re-review, never toward silently applying a
// changed plan.
func ddlMatchesStoredPlan(planResp *apitypes.PlanResponse, storedPlan *storage.Plan) bool {
	newChanges := responsePlanIdentities(planResp)
	storedChanges := storedPlanIdentities(storedPlan)

	if len(newChanges) != len(storedChanges) {
		return false
	}

	for identity, count := range newChanges {
		if storedChanges[identity] != count {
			return false
		}
	}
	return true
}

// responsePlanIdentities builds the change-identity multiset from a re-plan
// response. Namespace comes from the SchemaChangeResponse grouping (the
// authoritative source; FlatTables() does not carry it onto each change) and is
// normalized the same way the stored plan is — an empty namespace becomes
// "default" — so the two multisets are keyed identically.
func responsePlanIdentities(planResp *apitypes.PlanResponse) map[planChangeIdentity]int {
	identities := make(map[planChangeIdentity]int)
	for _, sc := range planResp.Changes {
		namespace := normalizePlanNamespace(sc.Namespace)
		for _, tc := range sc.TableChanges {
			identities[planChangeIdentity{
				namespace: namespace,
				table:     tc.TableName,
				operation: strings.ToLower(tc.ChangeType),
				ddl:       tc.DDL,
			}]++
		}
	}
	return identities
}

// storedPlanIdentities builds the change-identity multiset from a stored plan.
// FlatDDLChanges backfills each change's namespace from its map key, which the
// store already normalized (empty → "default"), so it matches the response side.
func storedPlanIdentities(storedPlan *storage.Plan) map[planChangeIdentity]int {
	identities := make(map[planChangeIdentity]int)
	for _, tc := range storedPlan.FlatDDLChanges() {
		identities[planChangeIdentity{
			namespace: normalizePlanNamespace(tc.Namespace),
			table:     tc.Table,
			operation: strings.ToLower(tc.Operation),
			ddl:       tc.DDL,
		}]++
	}
	return identities
}

// normalizePlanNamespace mirrors the store's namespace handling so a plan whose
// proto namespace is empty (persisted as "default") compares equal to the
// re-plan response that still carries the empty grouping namespace.
func normalizePlanNamespace(namespace string) string {
	if namespace == "" {
		return "default"
	}
	return namespace
}
