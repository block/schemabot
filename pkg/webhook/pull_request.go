package webhook

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"slices"
	"sync"
	"time"

	ghclient "github.com/block/schemabot/pkg/github"
	"github.com/block/schemabot/pkg/metrics"
	"github.com/block/schemabot/pkg/state"
	"github.com/block/schemabot/pkg/storage"
	"github.com/block/schemabot/pkg/webhook/templates"
)

// pullRequestPayload represents the relevant fields from a GitHub pull_request webhook.
type pullRequestPayload struct {
	Action      string `json:"action"`
	Before      string `json:"before"`
	PullRequest struct {
		Number int  `json:"number"`
		Merged bool `json:"merged"`
		Head   struct {
			SHA string `json:"sha"`
			Ref string `json:"ref"`
		} `json:"head"`
		User struct {
			Login string `json:"login"`
		} `json:"user"`
	} `json:"pull_request"`
	Repository struct {
		FullName string `json:"full_name"`
	} `json:"repository"`
	Installation struct {
		ID int64 `json:"id"`
	} `json:"installation"`
	// Changes carries the previous values of fields an "edited" action
	// modified. GitHub populates changes.base.ref.from only when the PR was
	// retargeted, which is what separates a retarget from a title or body edit.
	Changes struct {
		Base struct {
			Ref struct {
				From string `json:"from"`
			} `json:"ref"`
		} `json:"base"`
	} `json:"changes"`
}

// isAutoPlannablePullRequestAction reports whether a pull_request action
// triggers auto-plan. The HTTP enqueue path, the durable dispatcher, and the
// reconciler's inbox coverage query (HasEventForHead) must share this
// predicate — it derives from storage.AutoPlanPullRequestActions so all three
// stay in lockstep: the dispatcher re-validates fail-closed, so an action
// added to only one side would either never enqueue or — worse — enqueue rows
// the dispatcher silently completes without planning, and a coverage mismatch
// would mask lost deliveries from the reconciler.
func isAutoPlannablePullRequestAction(action string) bool {
	return slices.Contains(storage.AutoPlanPullRequestActions, action)
}

// isBaseRetarget reports whether an "edited" delivery moved the PR's base
// branch. A retarget changes which commits the PR proposes, and the diff
// against the base is what decides which databases the PR touches, so it is a
// schema-relevant event by construction. A title or body edit arrives under the
// same action and must not re-plan.
func isBaseRetarget(p pullRequestPayload) bool {
	return p.Action == "edited" && p.Changes.Base.Ref.From != ""
}

// isAutoPlannablePullRequest reports whether a delivery triggers auto-plan.
// The HTTP enqueue path and the durable dispatcher share it so the dispatcher's
// fail-closed re-validation cannot disagree with what was enqueued.
//
// It deliberately covers more than isAutoPlannablePullRequestAction, and the
// inbox coverage query (HasEventForHead) keeps using the narrower action list.
// The two answer different questions. Coverage asks whether a delivery that
// plans a head reached the inbox, which a SQL "action IN (...)" test can only
// answer from the action alone — it cannot tell a retarget from a title edit,
// so admitting "edited" there would let a title edit mask a lost synchronize.
// Nothing is lost by leaving it out: a retarget does not move the head SHA, so
// the head it re-plans is already covered by the delivery that introduced it.
func isAutoPlannablePullRequest(p pullRequestPayload) bool {
	return isAutoPlannablePullRequestAction(p.Action) || isBaseRetarget(p)
}

// handlePullRequest processes GitHub pull_request webhook events.
// On PR open/synchronize/reopen, it auto-plans all databases with schema changes.
func (h *Handler) handlePullRequest(ctx context.Context, metricApp string, w http.ResponseWriter, body []byte, deliveryID string) {
	var payload pullRequestPayload
	if err := json.Unmarshal(body, &payload); err != nil {
		h.writeError(w, http.StatusBadRequest, "invalid pull_request payload")
		return
	}

	// Repo-level webhook deliveries carry no installation id in the payload; the
	// dispatcher resolves it and stashes it on the context.
	installationID := h.effectiveInstallationID(ctx, payload.Installation.ID)

	// Route PR actions
	switch {
	case isAutoPlannablePullRequest(payload):
		// proceed to auto-plan below
	case payload.Action == "closed":
		if h.durableWebhookDispatch {
			// Enqueue and ACK fast; a leased driver runs cleanup with retries so
			// a process restart mid-cleanup cannot drop the delivery and leave a
			// lock held or stale check state behind.
			inserted, err := h.enqueueDurablePullRequest(ctx, payload, body, deliveryID, installationID)
			if err != nil {
				h.logger.Error("failed to enqueue durable PR close cleanup",
					"repo", payload.Repository.FullName, "pr", payload.PullRequest.Number,
					"delivery_id", deliveryID, "error", err)
				metrics.RecordWebhookEvent(ctx, metricApp, "pull_request", payload.Action, payload.Repository.FullName, "durable_enqueue_failed")
				h.writeError(w, http.StatusInternalServerError, "failed to enqueue webhook delivery")
				return
			}
			if !inserted {
				h.logger.Info("durable PR close cleanup already queued",
					"repo", payload.Repository.FullName, "pr", payload.PullRequest.Number, "delivery_id", deliveryID)
				h.writeJSON(w, http.StatusOK, map[string]string{"message": "PR close cleanup already queued"})
				return
			}
			h.logger.Info("durable PR close cleanup queued",
				"repo", payload.Repository.FullName, "pr", payload.PullRequest.Number, "delivery_id", deliveryID)
			h.writeJSON(w, http.StatusOK, map[string]string{"message": "PR close cleanup queued"})
			return
		}
		h.goSafe(payload.Repository.FullName, payload.PullRequest.Number, installationID, func() {
			// One-shot: a failure here (including a lock lookup/release error,
			// which also skips the check-state delete) is logged and not
			// retried. That direction is fail-closed — retained locks and
			// check state block until an operator or a reopen reconciles;
			// nothing is wrongly unblocked. The durable path retries instead.
			if err := h.runPRCloseCleanup(context.Background(), payload.Repository.FullName, payload.PullRequest.Number, payload.PullRequest.Merged); err != nil {
				h.logger.Error("PR close cleanup failed",
					"repo", payload.Repository.FullName, "pr", payload.PullRequest.Number, "error", err)
			}
		})
		h.writeJSON(w, http.StatusOK, map[string]string{"message": "PR close cleanup started"})
		return
	default:
		h.writeJSON(w, http.StatusOK, map[string]string{
			"message": "pull_request action ignored",
		})
		return
	}

	if installationID == 0 {
		h.writeError(w, http.StatusBadRequest, "missing installation ID in webhook payload")
		return
	}

	repo := payload.Repository.FullName
	pr := payload.PullRequest.Number
	headSHA := payload.PullRequest.Head.SHA

	// Reject webhooks from repositories not in the configured allowlist
	if h.service != nil && !h.service.Config().IsRepoAllowed(repo) {
		h.logger.Warn("webhook from unregistered repository",
			"event", "pull_request",
			"action", payload.Action,
			"repo", repo,
			"pr", pr,
			"installation_id", installationID)
		metrics.RecordUnregisteredRepositoryWebhook(ctx, metricApp, "pull_request", payload.Action, repo)
		h.writeJSON(w, http.StatusOK, map[string]string{
			"message": "repository not registered",
		})
		return
	}

	h.logger.Info("auto-plan triggered",
		"action", payload.Action,
		"repo", repo,
		"pr", pr,
		"head_sha", headSHA,
		"delivery_id", deliveryID,
	)

	if h.durableWebhookDispatch {
		// Enqueue failure is a deliberate 500 with no in-process fallback: it
		// fails loudly (metric below + a red delivery in GitHub's webhook UI)
		// rather than silently, but GitHub never auto-retries, so the delivery
		// stays lost until an operator hits "Redeliver" (which re-opens a
		// terminal row — failed or completed) or a new push arrives.
		inserted, err := h.enqueueDurablePullRequest(ctx, payload, body, deliveryID, installationID)
		if err != nil {
			h.logger.Error("failed to enqueue durable pull_request auto-plan",
				"action", payload.Action,
				"repo", repo,
				"pr", pr,
				"head_sha", headSHA,
				"delivery_id", deliveryID,
				"error", err,
			)
			metrics.RecordWebhookEvent(ctx, metricApp, "pull_request", payload.Action, repo, "durable_enqueue_failed")
			h.writeError(w, http.StatusInternalServerError, "failed to enqueue webhook delivery")
			return
		}
		if !inserted {
			h.logger.Info("durable pull_request delivery already queued",
				"action", payload.Action,
				"repo", repo,
				"pr", pr,
				"head_sha", headSHA,
				"delivery_id", deliveryID,
			)
			h.writeJSON(w, http.StatusOK, map[string]string{"message": "auto-plan already queued"})
			return
		}
		h.logger.Info("durable pull_request auto-plan queued",
			"action", payload.Action,
			"repo", repo,
			"pr", pr,
			"head_sha", headSHA,
			"delivery_id", deliveryID,
		)
		h.writeJSON(w, http.StatusOK, map[string]string{"message": "auto-plan queued"})
		return
	}

	h.goSafe(repo, pr, installationID, func() {
		ctx, cancel, client, err := h.autoPlanBootstrap(context.Background(), repo, installationID)
		if err != nil {
			metrics.RecordWebhookEvent(context.Background(), metricApp, "pull_request", payload.Action, repo, "auto_plan_bootstrap_failed")
			h.logger.Error("failed to bootstrap auto-plan", "repo", repo, "pr", pr, "head_sha", headSHA, "delivery_id", deliveryID, "error", err)
			return
		}
		defer cancel()
		// The discovery error is logged and best-effort posted as a failing check
		// inside runAutoPlanForPR (the post itself re-verifies the head SHA and
		// can no-op during a GitHub outage); the fire-and-forget request path has
		// no durable row to retry, so the error is intentionally dropped here.
		message, _ := h.runAutoPlanForPR(ctx, client, repo, pr, headSHA, installationID, "pull_request", payload.Action, payload.Before, deliveryID)
		h.logger.Info("auto-plan dispatched",
			"action", payload.Action,
			"repo", repo,
			"pr", pr,
			"head_sha", headSHA,
			"delivery_id", deliveryID,
			"outcome", message,
		)
	})
	h.writeJSON(w, http.StatusOK, map[string]string{"message": "auto-plan started"})
}

// autoPlanBootstrap derives a bounded work context from parent and resolves the
// GitHub client for repo. Request-path callers pass context.Background() so the
// work is detached from the HTTP request lifetime; the durable webhook driver
// passes its run context so lease loss and shutdown cancel in-flight work.
func (h *Handler) autoPlanBootstrap(parent context.Context, repo string, installationID int64) (context.Context, context.CancelFunc, *ghclient.InstallationClient, error) {
	ctx, cancel := context.WithTimeout(parent, 30*time.Second)
	// Dedupe FetchPullRequest calls within this webhook delivery.
	ctx = ghclient.WithPRInfoCache(ctx)

	client, err := h.clientForRepo(repo, installationID)
	if err != nil {
		cancel()
		return nil, nil, nil, fmt.Errorf("create GitHub client for repo %s installation %d: %w", repo, installationID, err)
	}
	return ctx, cancel, client, nil
}

func (h *Handler) shouldPostAutoPlanComment(ctx context.Context, client *ghclient.InstallationClient, action, repo string, pr int, beforeSHA, headSHA string, configs []ghclient.DiscoveredConfig) bool {
	if action != "synchronize" {
		return true
	}
	comparedHeads := map[string]bool{}
	if beforeSHA == "" {
		// A synchronize with no before range (synthesized recovery deliveries)
		// cannot prove the push range schema-neutral, but the tracked plan
		// comments below can still prove the visible plan fresh — a head that
		// already carries a current plan must not get a duplicate comment just
		// because its organic delivery went missing.
		if len(configs) == 0 {
			h.logger.Info("auto-plan will post plan comment because synchronize has no previous HEAD SHA and no tracked plan slots to prove freshness",
				"repo", repo, "pr", pr, "head_sha", headSHA)
			return true
		}
		h.logger.Info("auto-plan synchronize has no previous HEAD SHA; deciding from tracked plan comment freshness",
			"repo", repo, "pr", pr, "head_sha", headSHA)
	} else {
		files, err := client.FetchChangedFilesBetween(ctx, repo, beforeSHA, headSHA)
		if err != nil {
			h.logger.Warn("auto-plan will post plan comment because changed files could not be compared",
				"repo", repo, "pr", pr, "before_sha", beforeSHA, "head_sha", headSHA, "error", err)
			return true
		}
		if ghclient.HasSchemaInputFiles(files) {
			return true
		}
		// The synchronize range was just proven schema-neutral. Reuse that
		// result when the visible plan was rendered at the immediately
		// preceding head.
		comparedHeads[beforeSHA] = true
	}
	for _, cfg := range configs {
		environments, err := h.allowedDatabaseEnvironments(cfg.Config.Database)
		if err != nil {
			h.logger.Warn("auto-plan will post plan comment because the expected environment scope could not be resolved",
				"repo", repo, "pr", pr, "database", cfg.Config.Database,
				"database_type", cfg.Config.GetType(), "head_sha", headSHA, "error", err)
			return true
		}
		expectedScope := (planCommentSlot{Environments: environments}).environmentScope()
		comments, err := h.service.Storage().PlanComments().ListUnminimizedForSlot(ctx,
			repo, pr, cfg.Config.Database, string(cfg.Config.GetType()))
		if err != nil {
			h.logger.Warn("auto-plan will post plan comment because prior plan comment state could not be read",
				"repo", repo, "pr", pr, "database", cfg.Config.Database,
				"database_type", cfg.Config.GetType(), "head_sha", headSHA, "error", err)
			return true
		}
		var prior *storage.PlanComment
		for _, v := range slices.Backward(comments) {
			if v.EnvironmentScope == expectedScope {
				prior = v
				break
			}
		}
		if prior == nil {
			h.logger.Info("auto-plan will post plan comment because no prior visible plan comment is tracked",
				"repo", repo, "pr", pr, "database", cfg.Config.Database,
				"database_type", cfg.Config.GetType(), "environment_scope", expectedScope,
				"head_sha", headSHA)
			return true
		}

		planHeadSHA := prior.HeadSHA
		if planHeadSHA == "" {
			// A tracked plan comment with no recorded head (a manual plan)
			// cannot prove freshness for the current head.
			h.logger.Info("auto-plan will post plan comment because the visible plan has no recorded head SHA",
				"repo", repo, "pr", pr, "database", cfg.Config.Database,
				"database_type", cfg.Config.GetType(), "head_sha", headSHA)
			return true
		}
		if planHeadSHA == headSHA || comparedHeads[planHeadSHA] {
			continue
		}
		filesSincePlan, err := client.FetchChangedFilesBetween(ctx, repo, planHeadSHA, headSHA)
		if err != nil {
			h.logger.Warn("auto-plan will post plan comment because plan freshness could not be compared",
				"repo", repo, "pr", pr, "database", cfg.Config.Database,
				"database_type", cfg.Config.GetType(), "plan_head_sha", planHeadSHA,
				"head_sha", headSHA, "error", err)
			return true
		}
		if ghclient.HasSchemaInputFiles(filesSincePlan) {
			h.logger.Info("auto-plan will post plan comment because the visible plan predates schema input changes",
				"repo", repo, "pr", pr, "database", cfg.Config.Database,
				"database_type", cfg.Config.GetType(), "plan_head_sha", planHeadSHA,
				"head_sha", headSHA)
			return true
		}
		comparedHeads[planHeadSHA] = true
	}
	h.logger.Info("auto-plan will refresh checks without posting a plan comment because the visible plan covers the current schema inputs",
		"repo", repo, "pr", pr, "before_sha", beforeSHA, "head_sha", headSHA)
	return false
}

// runAutoPlanForPR discovers schema configs for the PR and launches auto-plan
// work. The returned message describes the dispatch outcome. The returned error
// is non-nil only for discovery failures (typically transient GitHub API
// errors), so durable callers can retry the delivery; request-path callers may
// ignore it because the failure is logged and best-effort posted as a failing
// check here (the post re-verifies the head SHA and can no-op during a GitHub
// outage).
func (h *Handler) runAutoPlanForPR(ctx context.Context, client *ghclient.InstallationClient, repo string, pr int, headSHA string, installationID int64, source string, action string, beforeSHA string, deliveryID string) (string, error) {
	// Snapshot the base ref before anything is read, so the publish-time check
	// below covers the whole of discovery. A base that moves mid-discovery
	// changes which databases the PR touches, which is the case that check
	// exists to catch — so a base ref that cannot be established is a discovery
	// failure like any other, not a reason to publish under a weaker guarantee.
	prInfo, err := client.FetchPullRequest(ctx, repo, pr)
	if err != nil {
		h.logger.Error("failed to read the PR for auto-plan; its base branch cannot be re-verified before publishing",
			"repo", repo, "pr", pr, "head_sha", headSHA, "source", source, "delivery_id", deliveryID, "error", err)
		h.postConfigDiscoveryFailure(ctx, client, repo, pr, headSHA, err)
		h.minimizeStalePlanCommentsForPR(ctx, client, repo, pr, headSHA)
		return "config discovery failed", fmt.Errorf("read %s#%d for auto-plan: %w", repo, pr, err)
	}
	baseRef := prInfo.BaseRef
	if baseRef == "" {
		missingBase := fmt.Errorf("PR %s#%d reports no base branch", repo, pr)
		h.logger.Error("PR reports no base branch; a plan's base cannot be re-verified before publishing",
			"repo", repo, "pr", pr, "head_sha", headSHA, "source", source, "delivery_id", deliveryID)
		h.postConfigDiscoveryFailure(ctx, client, repo, pr, headSHA, missingBase)
		h.minimizeStalePlanCommentsForPR(ctx, client, repo, pr, headSHA)
		return "config discovery failed", missingBase
	}

	// Fetch the changed files once so the same list drives both config discovery
	// and the server-managed-directory safety check below.
	changedFiles, err := client.FetchPRFiles(ctx, repo, pr)
	if err != nil {
		h.logger.Error("failed to fetch PR files for auto-plan", "repo", repo, "pr", pr, "head_sha", headSHA, "source", source, "delivery_id", deliveryID, "error", err)
		h.postConfigDiscoveryFailure(ctx, client, repo, pr, headSHA, err)
		h.minimizeStalePlanCommentsForPR(ctx, client, repo, pr, headSHA)
		return "config discovery failed", fmt.Errorf("fetch PR files for %s#%d: %w", repo, pr, err)
	}

	// Narrow the list to what the PR proposes against the default branch before
	// anything reads it. Every guard below asks a question about the PR's own
	// schema changes — which databases it touches, whether it drops a config out
	// from under a managed directory, whether participants must report — and a
	// file the PR inherited from history it has not caught up with answers none
	// of them. Failing here is a discovery failure like any other: without the
	// comparison there is no way to tell an inherited file from a proposed one.
	files, defaultTipSHA, err := client.PRFilesProposedAgainstDefaultBranch(ctx, repo, headSHA, changedFiles)
	if err != nil {
		h.logger.Error("failed to scope PR files to what the PR proposes", "repo", repo, "pr", pr, "head_sha", headSHA, "source", source, "delivery_id", deliveryID, "error", err)
		h.postConfigDiscoveryFailure(ctx, client, repo, pr, headSHA, err)
		h.minimizeStalePlanCommentsForPR(ctx, client, repo, pr, headSHA)
		return "config discovery failed", fmt.Errorf("scope PR files for %s#%d to what it proposes: %w", repo, pr, err)
	}
	if len(files) != len(changedFiles) {
		h.logger.Info("auto-plan scoped to the files the PR proposes against the default branch",
			"repo", repo, "pr", pr, "head_sha", headSHA, "default_tip_sha", defaultTipSHA,
			"changed_files", len(changedFiles), "proposed_files", len(files),
			"source", source, "delivery_id", deliveryID)
	}

	// Discover all configs matching changed schema files in this PR
	configs, err := client.FindConfigsForPRFiles(ctx, repo, headSHA, files)
	if err != nil {
		h.logger.Error("failed to discover configs for PR", "repo", repo, "pr", pr, "head_sha", headSHA, "source", source, "delivery_id", deliveryID, "error", err)
		h.postConfigDiscoveryFailure(ctx, client, repo, pr, headSHA, err)
		h.minimizeStalePlanCommentsForPR(ctx, client, repo, pr, headSHA)
		return "config discovery failed", fmt.Errorf("discover configs for %s#%d: %w", repo, pr, err)
	}

	// Fail closed when the PR changes schema files under a directory the server
	// config manages but no schemabot.yaml resolves for them — e.g. the PR
	// removed the config while keeping schema changes. Dropping the config must
	// not silently unmanage a server-owned schema directory.
	if unmanaged := h.unmanagedServerManagedSchemaChanges(repo, files, configs); len(unmanaged) > 0 {
		h.failClosedOnUnmanagedSchemaDir(ctx, client, repo, pr, headSHA, source, unmanaged)
		// This exit posts a blocking aggregate and no plan comment, so an earlier
		// head's plan comment would otherwise stay expanded beside it, still
		// offering to apply DDL for a config this head no longer carries.
		h.minimizeStalePlanCommentsForPR(ctx, client, repo, pr, headSHA)
		return "schema change under managed directory has no config", nil
	}

	discovered := slices.Clone(configs)
	configs = h.filterManagedDiscoveredConfigs(ctx, repo, pr, headSHA, source, configs)
	// On synchronize the comment cadence compares changed files between SHAs —
	// a GitHub compare call. The unmanaged-config notice and the plan comments
	// follow the same cadence, so the decision is memoized and computed at most
	// once per delivery, only when a caller needs it.
	shouldPostComment := sync.OnceValue(func() bool {
		return h.shouldPostAutoPlanComment(ctx, client, action, repo, pr, beforeSHA, headSHA, configs)
	})
	h.notifyUnmanagedDiscoveredConfigs(repo, pr, installationID, source, headSHA, shouldPostComment, discovered, configs)

	// Config discovery and the managed-directory guard just re-verified this
	// commit, and the clear re-checks allowed-environment coverage for every
	// discovered database. Together those cover every condition that records
	// an aggregate blocking reason, so a stored block can now be released.
	h.clearAggregateBlocksForVerifiedPR(ctx, client, repo, pr, headSHA, configs)

	// Collect database names from discovered configs
	affectedDatabases := make(map[string]bool)
	for _, cfg := range configs {
		affectedDatabases[cfg.Config.Database] = true
	}

	// Clean up stale checks from databases no longer in the PR.
	// Pass the new HEAD SHA so cleanup can create new check runs on the correct commit.
	h.goSafe(repo, pr, installationID, func() {
		h.cleanupStaleChecks(repo, pr, headSHA, installationID, affectedDatabases)
	})

	if len(configs) == 0 {
		h.logger.Info("no schema files in PR, skipping auto-plan", "repo", repo, "pr", pr, "head_sha", headSHA, "source", source, "delivery_id", deliveryID)
		// No config resolved means no slot to sweep, so the plan comments an
		// earlier head left behind have nothing to retire them. Every exit below
		// moves the check on without posting a comment, which would otherwise
		// leave the PR showing a plan — and its apply prompt — for schema this
		// head no longer proposes.
		h.minimizeStalePlanCommentsForPR(ctx, client, repo, pr, headSHA)
		// An aggregate participant does not own the required check for the repo —
		// the leader does — so on a PR that touches none of this deployment's
		// schema it has nothing to report and stays silent, rather than posting a
		// passing check that only adds a per-tenant row near the merge button. The
		// leader publishes the required check and gates on participants' own
		// checks when they exist, so a silent participant cannot wedge branch
		// protection (its check is non-required by the aggregate contract).
		if h.isAggregateParticipant(repo) {
			h.logger.Info("aggregate participant staying silent on PR with no managed schema changes",
				"repo", repo, "pr", pr, "head_sha", headSHA, "source", source, "delivery_id", deliveryID)
			metrics.RecordStatusCheckOperation(ctx, metrics.StatusCheckOperation{
				Operation:  "aggregate_participant_skip",
				Repository: repo,
				Status:     "skipped",
			})
			return "no schema files in PR (aggregate participant, staying silent)", nil
		}
		// A PR with no leader-managed schema can still touch schema owned by
		// expected participant deployments. The leader's aggregate is the
		// required check, so it must gate on those participants' Check Runs —
		// route through the aggregate fold, which fails closed until every
		// expected participant reports terminal success, instead of posting an
		// unconditional passing aggregate. The fold re-runs as participants'
		// Check Run events arrive, converging to passing once they succeed.
		if h.leaderExpectsParticipantsForPR(repo, files) {
			h.logger.Info("no leader-managed schema in PR but expected participant paths are touched; aggregate gate will block until participants report",
				"repo", repo, "pr", pr, "head_sha", headSHA, "source", source, "delivery_id", deliveryID)
			h.goSafe(repo, pr, installationID, func() {
				ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
				defer cancel()
				c, err := h.clientForRepo(repo, installationID)
				if err != nil {
					h.logger.Error("failed to create GitHub client for participant-gated aggregate",
						"repo", repo, "pr", pr, "head_sha", headSHA, "delivery_id", deliveryID, "error", err)
					return
				}
				h.updateAggregateCheck(ctx, c, repo, pr, headSHA)
			})
			return "no schema files in PR (aggregate folds expected participants)", nil
		}
		// Post passing aggregates on the current HEAD SHA so branch protection
		// isn't blocked on PRs that don't touch schema files. Always post —
		// on synchronize events the HEAD SHA changes, so the aggregate must be
		// recreated on the new commit. If stale per-database check records exist,
		// cleanupStaleChecks (above) also updates the aggregate — both converge
		// to the same result (passing aggregate on new SHA) so the overlap is safe.
		h.goSafe(repo, pr, installationID, func() {
			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cancel()
			c, err := h.clientForRepo(repo, installationID)
			if err != nil {
				h.logger.Error("failed to create GitHub client for passing aggregate", "repo", repo, "pr", pr, "head_sha", headSHA, "delivery_id", deliveryID, "error", err)
				return
			}
			h.postPassingAggregates(ctx, c, repo, pr, headSHA)
		})
		return "no schema files in PR", nil
	}
	// Publishing a plan asserts that the inputs it was computed from still
	// describe the PR. Both can move while discovery runs: a push changes what
	// the DDL would be applied to, and a retarget changes which commits the PR
	// proposes and so which databases it touches. Re-read them and discard
	// rather than publish a plan and a merge-blocking check for a PR that has
	// moved on — whichever event moved it re-plans on its own.
	moved, err := h.autoPlanInputsMoved(ctx, client, repo, pr, headSHA, baseRef)
	if err != nil {
		h.logger.Error("failed to re-verify the PR before publishing its plan", "repo", repo, "pr", pr, "head_sha", headSHA, "source", source, "delivery_id", deliveryID, "error", err)
		h.postPlanPublishVerificationFailure(ctx, client, repo, pr, headSHA, err)
		return "plan publish verification failed", fmt.Errorf("re-verify %s#%d before publishing its plan: %w", repo, pr, err)
	}
	if moved {
		h.logger.Info("discarding auto-plan because the PR moved while it was being discovered; the event that moved it re-plans",
			"repo", repo, "pr", pr, "head_sha", headSHA, "base_ref", baseRef,
			"source", source, "delivery_id", deliveryID)
		return "auto-plan discarded because the PR moved during discovery", nil
	}

	postPlanComment := shouldPostComment()

	// Launch auto-plan for each discovered config
	tenant := ""
	if config := h.service.Config(); config != nil {
		tenant = config.Tenant
	}
	for _, cfg := range configs {
		database := cfg.Config.Database
		h.goSafe(repo, pr, installationID, func() {
			h.handleMultiEnvPlan(repo, pr, database, tenant, installationID, "", true, postPlanComment, 0)
		})
	}

	return "auto-plan started", nil
}

// autoPlanInputsMoved reports whether the PR changed underneath a plan that is
// about to be published. An error means the question could not be answered —
// the caller must not publish on an unverified PR, and durable callers retry
// the delivery.
func (h *Handler) autoPlanInputsMoved(ctx context.Context, client *ghclient.InstallationClient, repo string, pr int, headSHA, baseRef string) (bool, error) {
	fresh, err := client.FetchPullRequestNoCache(ctx, repo, pr)
	if err != nil {
		return false, fmt.Errorf("fetch PR %s#%d: %w", repo, pr, err)
	}
	if fresh.HeadSHA != headSHA {
		h.logger.Info("PR head moved while its plan was being discovered",
			"repo", repo, "pr", pr, "head_sha", headSHA, "current_head_sha", fresh.HeadSHA)
		return true, nil
	}
	if baseRef != "" && fresh.BaseRef != baseRef {
		h.logger.Info("PR base branch moved while its plan was being discovered",
			"repo", repo, "pr", pr, "head_sha", headSHA, "base_ref", baseRef, "current_base_ref", fresh.BaseRef)
		return true, nil
	}
	return false, nil
}

// notifyUnmanagedDiscoveredConfigs posts a PR-visible notice when auto-plan
// discovery dropped schema configs this deployment is not configured to
// manage. On a repo with no aggregate role this deployment is the only
// responder, so without the notice the drop is invisible on the PR — no plan
// comment and no check row cover the dropped config, and the author can merge
// a schema change nothing will ever apply. On an aggregate-role repo (leader
// or participant) a dropped config is routine cross-deployment fan-out — the
// owning deployment plans it and posts its own comment and check — so the
// notice stays a log line there.
func (h *Handler) notifyUnmanagedDiscoveredConfigs(repo string, pr int, installationID int64, source, headSHA string, shouldPostComment func() bool, discovered, managed []ghclient.DiscoveredConfig) {
	dropped := droppedDiscoveredConfigs(discovered, managed)
	if len(dropped) == 0 {
		return
	}
	if config, ok := h.serverConfig(); ok && config.AggregateRoleForRepo(repo) != "" {
		h.logger.Info("unmanaged schema configs in PR left to their owning deployments on aggregate repo",
			"repo", repo, "pr", pr, "head_sha", headSHA, "source", source,
			"unmanaged_configs", len(dropped))
		return
	}
	// Match the plan-comment cadence: a synchronize push that changed no
	// schema inputs re-verifies checks without re-posting comments, and the
	// notice follows suit so an active PR is not re-noticed on every push.
	if !shouldPostComment() {
		h.logger.Info("unmanaged schema config notice suppressed by comment cadence; PR was already noticed on an earlier commit",
			"repo", repo, "pr", pr, "head_sha", headSHA, "source", source,
			"unmanaged_configs", len(dropped))
		return
	}
	notice := make([]templates.UnmanagedSchemaConfigNoticeData, 0, len(dropped))
	for _, cfg := range dropped {
		notice = append(notice, templates.UnmanagedSchemaConfigNoticeData{
			Database:   cfg.Config.Database,
			SchemaPath: cfg.SchemaDir,
		})
	}
	h.postComment(repo, pr, installationID, templates.RenderUnmanagedSchemaConfigsNotice(notice))
}

// droppedDiscoveredConfigs returns the discovered configs the managed filter
// removed. Configs with no parsed config are excluded: the filter already
// warns about them, and they carry no database identity to report.
func droppedDiscoveredConfigs(discovered, managed []ghclient.DiscoveredConfig) []ghclient.DiscoveredConfig {
	managedDirs := make(map[string]bool, len(managed))
	for _, cfg := range managed {
		managedDirs[cfg.SchemaDir] = true
	}
	var dropped []ghclient.DiscoveredConfig
	for _, cfg := range discovered {
		if cfg.Config == nil || managedDirs[cfg.SchemaDir] {
			continue
		}
		dropped = append(dropped, cfg)
	}
	return dropped
}

func (h *Handler) filterManagedDiscoveredConfigs(ctx context.Context, repo string, pr int, headSHA, source string, configs []ghclient.DiscoveredConfig) []ghclient.DiscoveredConfig {
	managed := configs[:0]
	for _, cfg := range configs {
		if cfg.Config == nil {
			h.logger.Warn("discovered schema config is missing parsed config and will be ignored",
				"repo", repo, "pr", pr, "head_sha", headSHA,
				"schema_path", cfg.SchemaDir, "source", source)
			metrics.RecordStatusCheckOperation(ctx, metrics.StatusCheckOperation{
				Operation:  "schema_config_discovery",
				Repository: repo,
				Status:     "skipped",
			})
			continue
		}
		if h.shouldProcessSchemaConfig(ctx, repo, pr, headSHA, cfg.Config.Database, string(cfg.Config.GetType()), cfg.SchemaDir, source) {
			managed = append(managed, cfg)
		}
	}
	return managed
}

func (h *Handler) postConfigDiscoveryFailure(ctx context.Context, client *ghclient.InstallationClient, repo string, pr int, headSHA string, discoveryErr error) {
	metrics.RecordStatusCheckOperation(ctx, metrics.StatusCheckOperation{
		Operation:  "schema_config_discovery",
		Repository: repo,
		Status:     "error",
	})

	block := configDiscoveryFailedBlock
	if ghclient.IsUnavailableError(discoveryErr) {
		block = githubConfigDiscoveryUnavailableBlock
	}
	h.logger.Info("posting failing aggregate for config discovery failure",
		"repo", repo, "pr", pr, "head_sha", headSHA,
		"blocking_reason", block.blockingReason)
	h.postFailingAggregatesWithBlock(ctx, client, repo, pr, headSHA,
		h.aggregateMessagesForAllEnvironments(block.message), block)
}

// postPlanPublishVerificationFailure blocks the aggregate when the PR could not
// be re-read before its plan was published. The read is the last thing standing
// between a discovered plan and a merge-blocking check posted for a commit or
// base branch the PR may no longer have, so a failure to answer it fails closed
// on the PR rather than only in the logs — the request path has no durable row
// to retry, and the failures that reach here are the ones retrying does not fix.
func (h *Handler) postPlanPublishVerificationFailure(ctx context.Context, client *ghclient.InstallationClient, repo string, pr int, headSHA string, verifyErr error) {
	metrics.RecordStatusCheckOperation(ctx, metrics.StatusCheckOperation{
		Operation:  "plan_publish_verification",
		Repository: repo,
		Status:     "error",
	})

	block := planPublishVerificationFailedBlock
	if ghclient.IsUnavailableError(verifyErr) {
		block = githubConfigDiscoveryUnavailableBlock
	}
	h.logger.Info("posting failing aggregate for plan publish verification failure",
		"repo", repo, "pr", pr, "head_sha", headSHA,
		"blocking_reason", block.blockingReason)
	h.postFailingAggregatesWithBlock(ctx, client, repo, pr, headSHA,
		h.aggregateMessagesForAllEnvironments(block.message), block)
}

func (h *Handler) aggregateMessagesForAllEnvironments(message string) map[string]string {
	allowed := h.service.Config().AllowedEnvironments
	if len(allowed) == 0 {
		return map[string]string{aggregateSentinel: message}
	}

	messages := make(map[string]string, len(allowed))
	for _, env := range allowed {
		messages[env] = message
	}
	return messages
}

// runPRCloseCleanup cleans up resources when a PR is closed (merged or
// unmerged): it releases locks held by this PR and deletes its stored check
// state.
//
// Cleanup only covers finished work. While any apply for the PR is
// non-terminal, that apply's database lock is retained so another PR cannot
// acquire the database mid-apply, and the PR's stored check state is retained
// so a close-and-reopen cannot convert in-flight apply state into a passing
// check. Apply-owned check state that reached a terminal state without
// concluding successfully is also retained, so a close-and-reopen cannot
// bypass a block that requires operator reconciliation. On a close without
// merge, apply-owned check state that concluded successfully is retained too:
// the stored success may predate a commit that removed the applied change,
// and only reopen-time cleanup can re-verify it against the PR contents. If
// apply state cannot be read, cleanup fails closed and nothing is released
// or deleted.
//
// It takes a context so the durable driver can bound it to the delivery lease
// (shutdown or lease loss cancels it) while the legacy request path passes a
// detached context. Every failure is returned so a caller with a retry
// mechanism (the durable driver) can retry: each step — the applies read, the
// list-then-release lock pass, and the check delete — is safe to re-run, so
// re-running the whole cleanup after a partial failure reconciles rather than
// double-acts.
func (h *Handler) runPRCloseCleanup(ctx context.Context, repo string, pr int, merged bool) error {
	ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()

	// A closed PR gets no more scheduled re-folds; drop its budget entry so the
	// in-memory map does not accumulate one entry per closed PR. A reopen folds
	// fresh and re-arms with a full budget.
	h.clearParticipantRefoldBudget(repo, pr)

	applies, err := h.service.Storage().Applies().GetByPR(ctx, repo, pr)
	if err != nil {
		// Fail closed: with apply state unknown, releasing a lock or deleting
		// check state could unblock a database with an apply still in flight.
		metrics.RecordStatusCheckOperation(ctx, metrics.StatusCheckOperation{
			Operation:  "pr_close_cleanup",
			Repository: repo,
			Status:     "error",
		})
		h.logger.Error("PR close cleanup skipped: cannot verify apply state; all locks and check state are retained",
			"repo", repo, "pr", pr, "error", err)
		return fmt.Errorf("read applies for closed PR %s#%d: %w", repo, pr, err)
	}

	inFlight := h.inFlightAppliesForClosedPR(ctx, repo, pr, applies)

	if err := h.releaseLocksForClosedPR(ctx, repo, pr, inFlight); err != nil {
		return fmt.Errorf("release locks for closed PR %s#%d: %w", repo, pr, err)
	}

	if len(inFlight) > 0 {
		h.logger.Info("check state retained for closed PR until all applies reach a terminal state",
			"repo", repo, "pr", pr, "in_flight_databases", len(inFlight))
		return nil
	}

	// Delete stored check state for this PR. Apply-owned rows that still block
	// survive the delete at the storage layer: in-flight rows (even if the
	// applies table missed the in-flight work above) and terminal rows whose
	// conclusion is not success, such as a schema change removed from the PR
	// after its apply started. Those blocks require operator reconciliation
	// and must persist across a close and reopen. On an unmerged close,
	// apply-owned rows that concluded successfully survive too: the stored
	// success may predate a commit that removed the applied change, so
	// reopen-time cleanup must re-verify it before the row can stop blocking.
	if err := h.service.Storage().Checks().DeleteByPRRetainingBlockingApplyOwned(ctx, repo, pr, merged); err != nil {
		metrics.RecordStatusCheckOperation(ctx, metrics.StatusCheckOperation{
			Operation:  "pr_close_cleanup",
			Repository: repo,
			Status:     "error",
		})
		h.logger.Error("failed to delete checks for closed PR", "repo", repo, "pr", pr, "merged", merged, "error", err)
		return fmt.Errorf("delete checks for closed PR %s#%d (merged=%t): %w", repo, pr, merged, err)
	}
	metrics.RecordStatusCheckOperation(ctx, metrics.StatusCheckOperation{
		Operation:  "pr_close_cleanup",
		Repository: repo,
		Status:     "success",
	})
	if merged {
		h.logger.Info("deleted checks for merged PR; apply-owned rows that still block are retained",
			"repo", repo, "pr", pr)
	} else {
		h.logger.Info("deleted plan-only checks for unmerged closed PR; all apply-owned rows are retained",
			"repo", repo, "pr", pr)
	}
	return nil
}

// closedPRDatabase identifies the database lock an apply holds.
type closedPRDatabase struct {
	database     string
	databaseType string
}

// inFlightAppliesForClosedPR returns the databases for which the closed PR
// still has a non-terminal apply recorded. Each in-flight apply is logged and
// counted because it blocks close cleanup: the database stays locked and the
// PR's stored check state stays in place until the apply reaches a terminal
// state.
func (h *Handler) inFlightAppliesForClosedPR(ctx context.Context, repo string, pr int, applies []*storage.Apply) map[closedPRDatabase]bool {
	inFlight := make(map[closedPRDatabase]bool)
	for _, a := range applies {
		if state.IsTerminalApplyState(a.State) {
			// Terminal applies never block close cleanup.
			continue
		}
		inFlight[closedPRDatabase{database: a.Database, databaseType: a.DatabaseType}] = true
		metrics.RecordStatusCheckOperation(ctx, metrics.StatusCheckOperation{
			Operation:    "pr_close_cleanup",
			Repository:   repo,
			Database:     a.Database,
			DatabaseType: a.DatabaseType,
			Environment:  a.Environment,
			Status:       "blocked",
		})
		h.logger.Warn("retaining lock and check state for closed PR with in-flight apply; close cleanup skipped for this database",
			"repo", repo, "pr", pr,
			"database", a.Database, "database_type", a.DatabaseType,
			"environment", a.Environment,
			"apply_id", a.ID, "apply_identifier", a.ApplyIdentifier, "apply_state", a.State)
	}
	return inFlight
}

// releaseLocksForClosedPR releases the closed PR's locks, except locks on
// databases that still have an in-flight apply. Every release is attempted even
// if an earlier one fails, and the failures are joined and returned so a caller
// with a retry mechanism re-attempts the still-held locks. Release itself is
// not idempotent — a missing row returns storage.ErrLockNotFound and a
// re-acquired lock returns storage.ErrLockNotOwned — so re-running converges
// only because each attempt re-lists the PR's locks first and a released
// lock's row is deleted, never reappearing in a later attempt. A concurrent
// manual unlock between the list and the release surfaces as a spurious
// ErrLockNotFound that fails that attempt; the next retry no longer sees the
// row and converges.
func (h *Handler) releaseLocksForClosedPR(ctx context.Context, repo string, pr int, inFlight map[closedPRDatabase]bool) error {
	locks, err := h.service.Storage().Locks().GetByPR(ctx, repo, pr)
	if err != nil {
		h.logger.Error("failed to look up locks for closed PR; no locks released", "repo", repo, "pr", pr, "error", err)
		return fmt.Errorf("look up locks for closed PR %s#%d: %w", repo, pr, err)
	}
	var releaseErrs []error
	for _, lock := range locks {
		if inFlight[closedPRDatabase{database: lock.DatabaseName, databaseType: lock.DatabaseType}] {
			h.logger.Info("lock retained on PR close because an apply is in flight",
				"repo", repo, "pr", pr, "database", lock.DatabaseName, "database_type", lock.DatabaseType)
			continue
		}
		if err := h.service.Storage().Locks().Release(ctx, lock.DatabaseName, lock.DatabaseType, lock.Owner); err != nil {
			h.logger.Error("failed to release lock on PR close",
				"repo", repo, "pr", pr, "database", lock.DatabaseName, "database_type", lock.DatabaseType, "error", err)
			releaseErrs = append(releaseErrs, fmt.Errorf("release lock on %s (%s): %w", lock.DatabaseName, lock.DatabaseType, err))
			continue
		}
		h.logger.Info("released lock on PR close",
			"repo", repo, "pr", pr, "database", lock.DatabaseName)
	}
	return errors.Join(releaseErrs...)
}

// cleanupStaleChecks updates checks for databases no longer in the PR.
// Plan-only checks can be marked "success" because the current PR no longer asks
// SchemaBot to apply anything. Checks that represent a started apply remain
// blocking because the live database may already have changed or may still change.
//
// On synchronize events, headSHA is the new commit SHA. Stale checks must be created
// as new check runs on this SHA (not updated on the old SHA) so GitHub shows them
// on the current commit.
func (h *Handler) cleanupStaleChecks(repo string, pr int, headSHA string, installationID int64, affectedDatabases map[string]bool) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	if h.service == nil {
		metrics.RecordStatusCheckOperation(ctx, metrics.StatusCheckOperation{
			Operation:  "stale_check_cleanup",
			Repository: repo,
			Status:     "error",
		})
		h.logger.Error("cannot clean up stale checks without service", "repo", repo, "pr", pr, "head_sha", headSHA)
		return
	}
	if h.service.Storage() == nil {
		metrics.RecordStatusCheckOperation(ctx, metrics.StatusCheckOperation{
			Operation:  "stale_check_cleanup",
			Repository: repo,
			Status:     "error",
		})
		h.logger.Error("cannot clean up stale checks without storage", "repo", repo, "pr", pr, "head_sha", headSHA)
		return
	}

	client, clientErr := h.clientForRepo(repo, installationID)
	if clientErr != nil {
		metrics.RecordStatusCheckOperation(ctx, metrics.StatusCheckOperation{
			Operation:  "stale_check_cleanup",
			Repository: repo,
			Status:     "error",
		})
		h.logger.Error("failed to create GitHub client for stale cleanup", "repo", repo, "pr", pr, "head_sha", headSHA, "error", clientErr)
		return
	}
	if !h.verifyHeadSHAStillCurrentForPR(ctx, client, repo, pr, headSHA, "stale_check_cleanup") {
		return
	}

	checks, err := h.service.Storage().Checks().GetByPR(ctx, repo, pr)
	if err != nil {
		metrics.RecordStatusCheckOperation(ctx, metrics.StatusCheckOperation{
			Operation:  "stale_check_cleanup",
			Repository: repo,
			Status:     "error",
		})
		h.logger.Error("failed to get checks for stale cleanup", "repo", repo, "pr", pr, "error", err)
		return
	}

	cleaned := false

	for _, check := range checks {
		if isAggregateCheck(check) {
			h.logger.Debug("skipping aggregate check during stale cleanup",
				"repo", repo, "pr", pr, "head_sha", headSHA,
				"environment", check.Environment, "check_id", check.ID)
			continue
		}

		if affectedDatabases[check.DatabaseName] {
			h.logger.Debug("skipping check during stale cleanup because database is still affected",
				"repo", repo, "pr", pr, "head_sha", headSHA,
				"database", check.DatabaseName, "database_type", check.DatabaseType,
				"environment", check.Environment, "check_id", check.ID)
			continue
		}

		// This check's database is no longer in the PR.
		h.logger.Info("cleaning up stale check",
			"repo", repo, "pr", pr,
			"database", check.DatabaseName, "database_type", check.DatabaseType,
			"environment", check.Environment, "head_sha", headSHA,
			"previous_status", check.Status, "previous_conclusion", check.Conclusion,
			"previous_blocking_reason", check.BlockingReason, "apply_id", check.ApplyID)

		if checkHasStartedApply(check) {
			if h.blockStaleStartedApplyCheckState(ctx, repo, pr, headSHA, check) {
				cleaned = true
			}
			continue
		}

		if h.markStalePlanOnlyCheckStateSuccessful(ctx, repo, pr, headSHA, check) {
			cleaned = true
		}
	}

	// Recompute aggregate on the new HEAD SHA after cleaning up stale checks
	if cleaned {
		h.updateAggregateCheck(ctx, client, repo, pr, headSHA)
	} else {
		metrics.RecordStatusCheckOperation(ctx, metrics.StatusCheckOperation{
			Operation:  "stale_check_cleanup",
			Repository: repo,
			Status:     "noop",
		})
	}
}

func (h *Handler) blockStaleStartedApplyCheckState(ctx context.Context, repo string, pr int, headSHA string, check *storage.Check) bool {
	check.HeadSHA = headSHA
	check.HasChanges = true
	check.BlockingReason = schemaRemovedAfterApplyBlock.blockingReason
	check.ErrorMessage = schemaRemovedAfterApplyBlock.message
	if check.Status == checkStatusInProgress {
		check.Conclusion = ""
	} else {
		check.Status = checkStatusCompleted
		check.Conclusion = checkConclusionActionRequired
	}
	if err := h.service.Storage().Checks().Upsert(ctx, check); err != nil {
		metrics.RecordStatusCheckOperation(ctx, metrics.StatusCheckOperation{
			Operation:    "stale_check_cleanup",
			Repository:   repo,
			Database:     check.DatabaseName,
			DatabaseType: check.DatabaseType,
			Environment:  check.Environment,
			Status:       "error",
		})
		h.logger.Error("failed to block stale check with started apply",
			"repo", repo, "pr", pr, "head_sha", headSHA,
			"database", check.DatabaseName, "database_type", check.DatabaseType,
			"environment", check.Environment, "check_id", check.ID,
			"apply_id", check.ApplyID, "error", err)
		return false
	}
	metrics.RecordStatusCheckOperation(ctx, metrics.StatusCheckOperation{
		Operation:    "stale_check_cleanup",
		Repository:   repo,
		Database:     check.DatabaseName,
		DatabaseType: check.DatabaseType,
		Environment:  check.Environment,
		Status:       "blocked",
	})
	return true
}

func (h *Handler) markStalePlanOnlyCheckStateSuccessful(ctx context.Context, repo string, pr int, headSHA string, check *storage.Check) bool {
	priorApplyID := check.ApplyID
	check.HeadSHA = headSHA
	check.Conclusion = checkConclusionSuccess
	check.HasChanges = false
	check.Status = checkStatusCompleted
	check.ApplyID = 0
	check.BlockingReason = ""
	check.ErrorMessage = ""

	// The success write is guarded against in-flight apply-owned rows: an apply
	// that started after this cleanup read the row must keep blocking the PR.
	marked, err := h.service.Storage().Checks().MarkStalePlanSuccessful(ctx, check)
	if err != nil {
		metrics.RecordStatusCheckOperation(ctx, metrics.StatusCheckOperation{
			Operation:    "stale_check_cleanup",
			Repository:   repo,
			Database:     check.DatabaseName,
			DatabaseType: check.DatabaseType,
			Environment:  check.Environment,
			Status:       "error",
		})
		h.logger.Error("failed to mark stale plan check successful",
			"repo", repo, "pr", pr, "head_sha", headSHA,
			"database", check.DatabaseName, "database_type", check.DatabaseType,
			"environment", check.Environment, "check_id", check.ID,
			"prior_apply_id", priorApplyID, "error", err)
		return false
	}

	if !marked {
		// A concurrent apply claimed the row between the cleanup read and this
		// write. Leave it in_progress and apply-owned so it keeps blocking until
		// an operator reconciles the target environment.
		metrics.RecordStatusCheckOperation(ctx, metrics.StatusCheckOperation{
			Operation:    "stale_check_cleanup",
			Repository:   repo,
			Database:     check.DatabaseName,
			DatabaseType: check.DatabaseType,
			Environment:  check.Environment,
			Status:       "blocked",
		})
		h.logger.Warn("stale plan check left blocking because an apply started concurrently; the check gate will block PR applies until an operator reconciles the target",
			"repo", repo, "pr", pr, "head_sha", headSHA,
			"database", check.DatabaseName, "database_type", check.DatabaseType,
			"environment", check.Environment, "check_id", check.ID,
			"prior_apply_id", priorApplyID)
		return true
	}

	metrics.RecordStatusCheckOperation(ctx, metrics.StatusCheckOperation{
		Operation:    "stale_check_cleanup",
		Repository:   repo,
		Database:     check.DatabaseName,
		DatabaseType: check.DatabaseType,
		Environment:  check.Environment,
		Status:       "success",
	})
	return true
}
