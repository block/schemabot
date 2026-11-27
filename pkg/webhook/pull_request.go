package webhook

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"slices"
	"strings"
	"sync"
	"time"

	ghclient "github.com/block/schemabot/pkg/github"
	"github.com/block/schemabot/pkg/metrics"
	"github.com/block/schemabot/pkg/state"
	"github.com/block/schemabot/pkg/storage"
	"github.com/block/schemabot/pkg/webhook/action"
	"github.com/block/schemabot/pkg/webhook/templates"
)

// pullRequestPayload represents the relevant fields from a GitHub pull_request webhook.
type pullRequestPayload struct {
	Action string `json:"action"`
	Before string `json:"before"`
	Sender struct {
		Login string `json:"login"`
	} `json:"sender"`
	Label struct {
		Name string `json:"name"`
	} `json:"label"`
	PullRequest struct {
		Number    int       `json:"number"`
		Merged    bool      `json:"merged"`
		UpdatedAt time.Time `json:"updated_at"`
		Base      struct {
			SHA string `json:"sha"`
			Ref string `json:"ref"`
		} `json:"base"`
		Head struct {
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
		Stack json.RawMessage `json:"stack"`
		Base  struct {
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
// Recovery separately compares the observed target context, so an earlier
// delivery for the same head cannot conceal a lost retarget or stack edit.
func isAutoPlannablePullRequest(p pullRequestPayload) bool {
	return isAutoPlannablePullRequestAction(p.Action) || isBaseRetarget(p) || (p.Action == "edited" && len(p.Changes.Stack) > 0)
}

// handlePullRequest processes GitHub pull_request webhook events.
// On PR open/synchronize/reopen, it auto-plans all databases with schema changes.
func (h *Handler) handlePullRequest(ctx context.Context, metricApp string, w http.ResponseWriter, body []byte, deliveryID string) {
	var payload pullRequestPayload
	if err := json.Unmarshal(body, &payload); err != nil {
		h.writeError(w, http.StatusBadRequest, "invalid pull_request payload")
		return
	}
	payload.Repository.FullName = storage.CanonicalKey(payload.Repository.FullName)

	// Repo-level webhook deliveries carry no installation id in the payload; the
	// dispatcher resolves it and stashes it on the context.
	installationID := h.effectiveInstallationID(ctx, payload.Installation.ID)

	// Route PR actions
	switch {
	case h.isPRSchemaExceptionEvent(payload):
		h.handlePRSchemaExceptionEvent(ctx, w, body, deliveryID)
		return
	case isAutoPlannablePullRequest(payload):
		// proceed to auto-plan below
	case payload.Action == storage.PullRequestClosedAction:
		if h.durableWebhookDispatch {
			// Enqueue and ACK fast; a leased driver runs cleanup with retries so
			// a process restart mid-cleanup cannot drop the delivery and leave a
			// lock held or stale check state behind.
			inserted, _, err := h.enqueueDurablePullRequest(ctx, payload, body, deliveryID, installationID)
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
		h.goSafe(payload.Repository.FullName, payload.PullRequest.Number, installationID, deliveryID, func() {
			// One-shot: a failure here (including a lock lookup/release error,
			// which also skips the check-state delete) is logged and not
			// retried. That direction is fail-closed — retained locks and
			// check state block until an operator or a reopen reconciles;
			// nothing is wrongly unblocked. The durable path retries instead.
			if _, err := h.runPRCloseCleanup(context.Background(), payload.Repository.FullName, payload.PullRequest.Number, installationID, payload.PullRequest.Merged); err != nil {
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
		metrics.RecordUnregisteredRepositoryWebhook(ctx, metricApp, "pull_request", payload.Action)
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
		inserted, notBefore, err := h.enqueueDurablePullRequest(ctx, payload, body, deliveryID, installationID)
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
		if notBefore != nil {
			h.logger.Info("durable pull_request auto-plan queued; no driver will claim it until the synchronize quiet period has passed",
				"action", payload.Action,
				"repo", repo,
				"pr", pr,
				"head_sha", headSHA,
				"delivery_id", deliveryID,
				"not_before", notBefore.UTC(),
			)
		} else {
			h.logger.Info("durable pull_request auto-plan queued",
				"action", payload.Action,
				"repo", repo,
				"pr", pr,
				"head_sha", headSHA,
				"delivery_id", deliveryID,
			)
		}
		h.writeJSON(w, http.StatusOK, map[string]string{"message": "auto-plan queued"})
		return
	}

	h.goSafe(repo, pr, installationID, deliveryID, func() {
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
	ctx, cancel := autoPlanContext(parent)

	client, err := h.clientForRepo(repo, installationID)
	if err != nil {
		cancel()
		return nil, nil, nil, fmt.Errorf("create GitHub client for repo %s installation %d: %w", repo, installationID, err)
	}
	return ctx, cancel, client, nil
}

// autoPlanContext returns the bounded work context for one auto-plan run
// started from parent, with a PR-info cache that dedupes FetchPullRequest calls
// within the webhook delivery.
func autoPlanContext(parent context.Context) (context.Context, context.CancelFunc) {
	ctx, cancel := context.WithTimeout(withRunParent(parent), 30*time.Second)
	return ghclient.WithPRInfoCache(ctx), cancel
}

// runParentKey carries the context a bounded webhook run (an auto-plan run or
// a PR command) was started from, before the run's own work deadline was
// applied.
type runParentKey struct{}

func withRunParent(parent context.Context) context.Context {
	return context.WithValue(parent, runParentKey{}, parent)
}

// runParent returns the context the run in ctx was started from. It does not
// carry the run's work deadline, so it ends only when whatever started the run
// ends it, such as lease loss or shutdown for a durable delivery.
func runParent(ctx context.Context) (context.Context, bool) {
	parent, ok := ctx.Value(runParentKey{}).(context.Context)
	return parent, ok
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
		prior, err := h.visiblePlanComment(ctx, repo, pr, cfg.Config.Database, string(cfg.Config.GetType()))
		if err != nil {
			h.logger.Warn("auto-plan will post plan comment because the visible plan comment could not be resolved",
				"repo", repo, "pr", pr, "database", cfg.Config.Database,
				"database_type", cfg.Config.GetType(), "head_sha", headSHA, "error", err)
			return true
		}
		if prior == nil {
			h.logger.Info("auto-plan will post plan comment because no prior visible plan comment is tracked",
				"repo", repo, "pr", pr, "database", cfg.Config.Database,
				"database_type", cfg.Config.GetType(), "head_sha", headSHA)
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

// visiblePlanComment returns the newest unretired plan comment tracked for the
// database's slot under the environments this deployment plans, or nil when
// none is tracked. That comment is what the PR currently shows as the
// database's plan.
func (h *Handler) visiblePlanComment(ctx context.Context, repo string, pr int, database, databaseType string) (*storage.PlanComment, error) {
	environments, err := h.allowedDatabaseEnvironments(database)
	if err != nil {
		return nil, fmt.Errorf("resolve the plan comment environment scope for database %q: %w", database, err)
	}
	expectedScope := (planCommentSlot{Environments: environments}).environmentScope()
	comments, err := h.service.Storage().PlanComments().ListUnretiredForSlot(ctx, repo, pr, database, databaseType)
	if err != nil {
		return nil, fmt.Errorf("read the plan comments tracked for database %q on %s#%d: %w", database, repo, pr, err)
	}
	for _, v := range slices.Backward(comments) {
		if v.EnvironmentScope == expectedScope {
			return v, nil
		}
	}
	return nil, nil
}

// runAutoPlanForPR discovers schema configs for the PR and launches auto-plan
// work. The returned message describes the dispatch outcome. The returned error
// is non-nil only for discovery failures (typically transient GitHub API
// errors), so durable callers can retry the delivery; request-path callers may
// ignore it because the failure is logged and best-effort posted as a failing
// check here (the post re-verifies the head SHA and can no-op during a GitHub
// outage).
func (h *Handler) runAutoPlanForPR(ctx context.Context, client *ghclient.InstallationClient, repo string, pr int, headSHA string, installationID int64, source string, action string, beforeSHA string, deliveryID string) (string, error) {
	dispatch, err := h.dispatchAutoPlanForPR(ctx, client, repo, pr, headSHA, installationID, source, action, beforeSHA, deliveryID, autoPlanOptions{})
	return dispatch.message, err
}

// autoPlanOptions changes what one auto-plan dispatch is allowed to start.
type autoPlanOptions struct {
	// deferSchemaPlans stops before any Check Run or check state is written
	// when the PR changes schema this deployment manages, and reports the
	// databases it would have planned.
	// Planning is the expensive part of an auto-plan: every database reads its
	// schema files once per environment. An operator recovering checks across
	// many PRs sets this so the cheap PRs settle first and each schema PR is
	// planned only when asked for by number.
	deferSchemaPlans bool
	// Exception events reconcile only PRs that need an exception.
	schemaExceptionEvent bool
}

// autoPlanDispatch is what one auto-plan dispatch did.
type autoPlanDispatch struct {
	message string
	// deferredDatabases lists the managed databases a deferred dispatch would
	// have planned. It is set only when deferSchemaPlans stopped the dispatch.
	deferredDatabases []string
	// changedFiles counts the files the PR proposes against the default
	// branch, as a size hint for the plan a deferred dispatch skipped.
	changedFiles int
}

// dispatchAutoPlanForPR is runAutoPlanForPR with options; see its comment.
func (h *Handler) dispatchAutoPlanForPR(ctx context.Context, client *ghclient.InstallationClient, repo string, pr int, headSHA string, installationID int64, source string, action string, beforeSHA string, deliveryID string, opts autoPlanOptions) (autoPlanDispatch, error) {
	repo = storage.CanonicalKey(repo)
	// Every discovery failure below posts through this one call, so whether
	// the check says SchemaBot retries at the quota reset is decided in one
	// place. A schema exception delivery's retry stops before it clears the
	// aggregate block when the PR is within the database limit, so its retry
	// does not lift the block and the check asks for a re-run instead.
	retriedAtReset := durableRetryAtQuotaReset(ctx) && !opts.schemaExceptionEvent
	postDiscoveryFailure := func(err error) {
		h.postConfigDiscoveryFailure(ctx, client, repo, pr, headSHA, err, retriedAtReset)
	}
	// Snapshot the base ref before anything is read, so the publish-time check
	// below covers the whole of discovery. A base that moves mid-discovery
	// changes which databases the PR touches, which is the case that check
	// exists to catch — so a base ref that cannot be established is a discovery
	// failure like any other, not a reason to publish under a weaker guarantee.
	prInfo, err := client.FetchPullRequest(ctx, repo, pr)
	if err != nil {
		h.logger.Error("failed to read the PR for auto-plan; its base branch cannot be re-verified before publishing",
			"repo", repo, "pr", pr, "head_sha", headSHA, "source", source, "delivery_id", deliveryID, "error", err)
		postDiscoveryFailure(err)
		h.retireStalePlanCommentsForPR(ctx, client, repo, pr, headSHA)
		return autoPlanDispatch{message: "config discovery failed"}, fmt.Errorf("read %s#%d for auto-plan: %w", repo, pr, err)
	}
	decision, err := h.evaluatePRTarget(ctx, client, repo, prInfo)
	if err != nil {
		h.logger.Error("auto-plan could not verify PR target", "repo", repo, "pr", pr, "head_sha", headSHA, "source", source, "delivery_id", deliveryID, "error", err)
		return autoPlanDispatch{message: "target eligibility could not be verified"}, fmt.Errorf("evaluate target for %s#%d: %w", repo, pr, err)
	}
	if prInfo.HeadSHA != headSHA {
		return autoPlanDispatch{message: "auto-plan discarded because the PR moved during discovery"}, nil
	}
	if err := h.recordPRTargetObservation(ctx, repo, pr, prInfo, decision, deliveryID); err != nil {
		h.logger.Error("auto-plan could not record PR target observation", "repo", repo, "pr", pr, "head_sha", headSHA, "source", source, "delivery_id", deliveryID, "error", err)
		return autoPlanDispatch{message: "target observation failed"}, fmt.Errorf("record target for %s#%d: %w", repo, pr, err)
	}
	if !decision.Eligible {
		h.logger.Info("auto-plan skipped for excluded PR target", "repo", repo, "pr", pr, "head_sha", prInfo.HeadSHA, "effective_target", decision.Target, "source", source)
		return autoPlanDispatch{message: "skipped: PR target is excluded"}, nil
	}
	baseRef := prInfo.BaseRef

	policy := h.config().PRSchemaCapForRepo(repo)
	var discovery *prPlanDiscovery
	var cacheKey prPlanDiscoveryKey
	if policy.Limit > 0 {
		cacheKey = h.prPlanDiscoveryKey(repo, pr, prInfo)
		if !cacheKey.revision.valid() {
			err := fmt.Errorf("GitHub did not supply the PR head and base revision")
			postDiscoveryFailure(err)
			return autoPlanDispatch{message: "config discovery failed"}, err
		}
		discovery, err = h.cachedPRPlanDiscovery(ctx, client, cacheKey)
		if err != nil {
			postDiscoveryFailure(err)
			return autoPlanDispatch{message: "config discovery failed"}, err
		}
	}
	cacheMiss := discovery == nil
	if cacheMiss {
		discovery, err = h.readPRPlanDiscovery(ctx, client, repo, pr, prInfo, source, deliveryID)
		if err != nil {
			h.logConfigDiscoveryFailure("discover PR planning inputs", repo, pr, headSHA, source, deliveryID, err)
			postDiscoveryFailure(err)
			h.retireStalePlanCommentsForPR(ctx, client, repo, pr, headSHA)
			return autoPlanDispatch{message: "config discovery failed"}, err
		}
	}
	if len(discovery.unmanagedFiles) > 0 {
		h.failClosedOnUnmanagedSchemaDir(ctx, client, repo, pr, headSHA, source, discovery.unmanagedFiles)
		h.retireStalePlanCommentsForPR(ctx, client, repo, pr, headSHA)
		return autoPlanDispatch{message: "schema change under managed directory has no config"}, nil
	}

	// Publishing a plan asserts that the inputs it was computed from still
	// describe the PR. Both can move while discovery runs: a push changes what
	// the DDL would be applied to, and a retarget changes which commits the PR
	// proposes and so which databases it touches. Re-read them and discard
	// rather than publish a plan and a merge-blocking check for a PR that has
	// moved on — whichever event moved it re-plans on its own.
	fresh, moved, err := h.revalidateAutoPlanInputs(ctx, client, repo, pr, headSHA, prInfo)
	if err != nil {
		h.logger.Error("failed to re-verify the PR before publishing its plan", "repo", repo, "pr", pr, "head_sha", headSHA, "source", source, "delivery_id", deliveryID, "error", err)
		h.postPlanPublishVerificationFailure(ctx, client, repo, pr, headSHA, err)
		return autoPlanDispatch{message: "plan publish verification failed"}, fmt.Errorf("re-verify %s#%d before publishing its plan: %w", repo, pr, err)
	}
	if moved != autoPlanInputsUnchanged {
		h.logger.Info("discarding auto-plan because the PR moved while it was being discovered; the event that moved it re-plans",
			"repo", repo, "pr", pr, "head_sha", headSHA, "base_ref", baseRef,
			"source", source, "delivery_id", deliveryID)
		return autoPlanDispatch{message: "auto-plan discarded because the PR moved during discovery"}, nil
	}

	changedFiles, files := discovery.changedFiles, discovery.files
	discovered, configs := discovery.discovered, discovery.managed
	if policy.Limit > 0 {
		if cacheKey != h.prPlanDiscoveryKey(repo, pr, fresh) || !fresh.IsOpen() {
			return autoPlanDispatch{message: "auto-plan discarded because the PR or registrations moved during discovery"}, nil
		}
		if cacheMiss {
			h.prPlanDiscovery.put(cacheKey, discovery)
		}
	}
	if opts.schemaExceptionEvent && (policy.Limit == 0 || len(discovery.localDatabases) <= policy.Limit) {
		return autoPlanDispatch{message: "PR is within this control plane's database limit"}, nil
	}
	if opts.deferSchemaPlans && len(configs) > 0 {
		// No Check Run or stored check state has been written for this head yet,
		// so a missing required check keeps blocking the merge until the
		// operator asks for these plans (MG-1).
		databases := managedDatabaseNames(configs)
		h.logger.Info("auto-plan deferred schema plans; the PR's Check Runs are left as they are until its plans are requested",
			"repo", repo, "pr", pr, "head_sha", headSHA, "databases", databases,
			"proposed_files", len(files), "source", source, "delivery_id", deliveryID)
		return autoPlanDispatch{
			message:           "deferred: PR changes managed schema",
			deferredDatabases: databases,
			changedFiles:      len(files),
		}, nil
	}
	var admission prSchemaAdmission
	if policy.Limit > 0 {
		if policy.Observe {
			admission, err = h.evaluatePRSchemaCap(ctx, client, repo, pr, fresh, policy, discovery.localDatabases, schemaCapAutoPlan)
		} else {
			err = h.withPRSchemaPublication(ctx, repo, pr, func() error {
				current, err := h.verifyPRSchemaRevision(ctx, client, cacheKey, fresh.State)
				if err != nil {
					return err
				}
				admission, err = h.evaluatePRSchemaCap(ctx, client, repo, pr, current, policy, discovery.localDatabases, schemaCapAutoPlan)
				if err != nil {
					return err
				}
				ctx := withExpectedPRTarget(ctx, repo, pr, current)
				h.clearAggregateBlocksForVerifiedPR(ctx, client, repo, pr, headSHA, configs)
				return nil
			})
		}
		if errors.Is(err, errPRSchemaMoved) {
			return autoPlanDispatch{message: err.Error()}, nil
		}
		if errors.Is(err, errPRSchemaCapHeld) {
			h.retireStalePlanCommentsForPR(ctx, client, repo, pr, headSHA)
			return autoPlanDispatch{message: err.Error()}, nil
		}
		if err != nil {
			return autoPlanDispatch{message: "could not evaluate the PR schema cap"}, err
		}
		admission.key, admission.info, admission.discovery, admission.compareTip = cacheKey, fresh, discovery, discovery.defaultTip
	}
	// On synchronize the comment cadence compares changed files between SHAs —
	// a GitHub compare call. The unmanaged-config notice and the plan comments
	// follow the same cadence, so the decision is memoized and computed at most
	// once per delivery, only when a caller needs it.
	shouldPostComment := sync.OnceValue(func() bool {
		return h.shouldPostAutoPlanComment(ctx, client, action, repo, pr, beforeSHA, headSHA, configs)
	})
	unmanagedPlanNote := h.notifyUnmanagedDiscoveredConfigs(repo, pr, installationID, source, headSHA, shouldPostComment, discovered, configs)

	// Config discovery and the managed-directory guard just re-verified this
	// commit, and the clear re-checks allowed-environment coverage for every
	// discovered database. Together those cover every condition that records
	// a discovery block. Prior plan successes are invalidated before a stored
	// aggregate block is released, so only replacement results can pass.
	if policy.Limit == 0 || policy.Observe {
		h.clearAggregateBlocksForVerifiedPR(ctx, client, repo, pr, headSHA, configs)
	}

	affectedDatabases := checkDatabaseKeysForConfigs(configs)

	// Persist registration errors before cleanup or a successful sibling plan can
	// publish an aggregate. Keep all discovered keys in the stale-check sweep.
	planConfigs, err := h.registeredAutoPlanConfigs(ctx, client, repo, pr, headSHA, installationID, configs)
	if err != nil {
		return autoPlanDispatch{}, err
	}

	// Clean up stale checks from databases no longer in the PR, on the new HEAD
	// SHA. This runs to completion before any plan below launches, so every
	// plan's aggregate fold sees the rows cleanup settles.
	h.cleanupStaleChecks(repo, pr, headSHA, installationID, affectedDatabases)

	if len(configs) == 0 {
		h.logger.Info("no schema files in PR, skipping auto-plan", "repo", repo, "pr", pr, "head_sha", headSHA, "source", source, "delivery_id", deliveryID)
		// No config resolved means no slot to sweep, so the plan comments an
		// earlier head left behind have nothing to retire them. Every exit below
		// moves the check on without posting a comment, which would otherwise
		// leave the PR showing a plan — and its apply prompt — for schema this
		// head no longer proposes.
		h.retireStalePlanCommentsForPR(ctx, client, repo, pr, headSHA)
		// An aggregate participant does not own the required check for the repo —
		// the leader does — so on a PR that touches none of this deployment's
		// schema it has nothing to report and stays silent, rather than posting a
		// passing check that only adds a per-tenant row near the merge button. The
		// leader publishes the required check and gates on participants' own
		// checks when they exist, so a silent participant cannot wedge branch
		// protection (its check is non-required by the aggregate contract).
		if h.isAggregateParticipant(repo) && !h.participantPathsTouched(repo, files) {
			h.logger.Info("aggregate participant staying silent on PR with no managed schema changes",
				"repo", repo, "pr", pr, "head_sha", headSHA, "source", source, "delivery_id", deliveryID)
			metrics.RecordStatusCheckOperation(ctx, metrics.StatusCheckOperation{
				Operation:  "aggregate_participant_skip",
				Repository: repo,
				Status:     "skipped",
			})
			return autoPlanDispatch{message: "no schema files in PR (aggregate participant, staying silent)"}, nil
		}
		// A PR with no leader-managed schema can still touch schema owned by
		// expected participant deployments. The leader's aggregate is the
		// required check, so it must gate on those participants' Check Runs —
		// route through the aggregate fold, which fails closed until every
		// expected participant reports terminal success, instead of posting an
		// unconditional passing aggregate. The fold re-runs as participants'
		// Check Run events arrive, converging to passing once they succeed.
		expectedParticipants, err := h.leaderParticipantsForPR(ctx, client, repo, pr, headSHA, changedFiles, files)
		routeToFold := len(expectedParticipants) > 0
		switch {
		case err != nil:
			// The fold reads the same participants and fails closed on the same
			// error, arming its bounded re-fold, so route there instead of
			// posting a passing aggregate that could hide a participant's run.
			h.logger.Warn("cannot resolve participants for PR with no leader-managed schema; routing to the aggregate fold, which fails closed",
				"repo", repo, "pr", pr, "head_sha", headSHA, "source", source, "delivery_id", deliveryID, "error", err)
			routeToFold = true
		case routeToFold:
			h.logger.Info("no leader-managed schema in PR but expected participant paths are touched; aggregate gate will block until participants report",
				"repo", repo, "pr", pr, "head_sha", headSHA, "source", source, "delivery_id", deliveryID)
		}
		if routeToFold {
			h.goSafe(repo, pr, installationID, deliveryID, func() {
				ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
				defer cancel()
				ctx = withExpectedPRTarget(ctx, repo, pr, prInfo)
				c, err := h.clientForRepo(repo, installationID)
				if err != nil {
					h.logger.Error("failed to create GitHub client for participant-gated aggregate",
						"repo", repo, "pr", pr, "head_sha", headSHA, "delivery_id", deliveryID, "error", err)
					return
				}
				h.updateAggregateCheck(ctx, c, repo, pr, headSHA)
			})
			return autoPlanDispatch{message: "no schema files in PR (aggregate folds expected participants)"}, nil
		}
		// Post passing aggregates on the current HEAD SHA so branch protection
		// isn't blocked on PRs that don't touch schema files. Always post —
		// on synchronize events the HEAD SHA changes, so the aggregate must be
		// recreated on the new commit. If stale per-database check records exist,
		// cleanupStaleChecks (above) also updates the aggregate — both converge
		// to the same result (passing aggregate on new SHA) so the overlap is safe.
		// Schema the PR changes under configs this deployment does not manage
		// is named on the check, so its title does not claim no schema changed.
		unmanaged := unmanagedSchemaConfigData(droppedDiscoveredConfigs(discovered, configs))
		h.goSafe(repo, pr, installationID, deliveryID, func() {
			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cancel()
			ctx = withExpectedPRTarget(ctx, repo, pr, prInfo)
			c, err := h.clientForRepo(repo, installationID)
			if err != nil {
				h.logger.Error("failed to create GitHub client for passing aggregate", "repo", repo, "pr", pr, "head_sha", headSHA, "delivery_id", deliveryID, "error", err)
				return
			}
			h.postPassingAggregates(ctx, c, repo, pr, headSHA, unmanaged)
		})
		return autoPlanDispatch{message: "no schema files in PR"}, nil
	}

	// Discovery is complete. Publish empty environments and pending placeholders
	// before independent database planners can finish in a different order.
	if len(h.service.Config().AllowedEnvironments) > 0 {
		h.updateAggregateCheck(ctx, client, repo, pr, headSHA)
	}

	postPlanComment := shouldPostComment()

	// Launch auto-plan for each discovered config
	tenant := ""
	if config := h.service.Config(); config != nil {
		tenant = config.Tenant
	}
	commandScopeDatabases := h.autoPlanCommandScopeDatabases(ctx, client, repo, pr, headSHA, changedFiles, files, discovered)
	for _, cfg := range planConfigs {
		database := cfg.Config.Database
		h.goSafe(repo, pr, installationID, deliveryID, func() {
			h.handleMultiEnvPlan(repo, pr, database, tenant, installationID, "", true, commandScopeDatabases, postPlanComment, 0, unmanagedPlanNote, admission)
		})
	}

	return autoPlanDispatch{message: "auto-plan started"}, nil
}

// registeredAutoPlanConfigs records a durable aggregate failure before returning
// the databases that can proceed. A later successful plan folds that same block.
func (h *Handler) registeredAutoPlanConfigs(ctx context.Context, client *ghclient.InstallationClient, repo string, pr int, headSHA string, installationID int64, configs []ghclient.DiscoveredConfig) ([]ghclient.DiscoveredConfig, error) {
	if h.isRepoListenOnly(repo) || !h.config().AreChecksEnabled(repo) {
		return configs, nil
	}
	var messages []string
	var registered []ghclient.DiscoveredConfig
	for _, cfg := range configs {
		if _, err := h.configuredDatabaseEnvironments(cfg.Config.Database); err != nil {
			messages = append(messages, fmt.Sprintf("%s: %s", cfg.Path, userFacingError(err)))
			h.handleSchemaRequestError(repo, pr, installationID, "", cfg.Config.Database, "", action.Plan, err, false)
			continue
		}
		registered = append(registered, cfg)
	}
	if len(messages) > 0 {
		slices.Sort(messages)
		if !h.postFailingAggregatesWithBlock(ctx, client, repo, pr, headSHA,
			h.aggregateMessagesForAllEnvironments(strings.Join(messages, "\n")), configDiscoveryFailedBlock) {
			return nil, fmt.Errorf("could not record invalid schema configuration for %s#%d at %s", repo, pr, headSHA)
		}
	}
	return registered, nil
}

// severalDatabases is the count that makes an auto-plan comment name its
// database, used where discovery cannot answer how many a bare command reaches.
const severalDatabases = 2

// autoPlanCommandScopeDatabases counts the databases a bare command offered by
// an auto-plan comment would reach, which is not the set this deployment plans.
// The command path discovers over the PR's raw changed files, without the
// proposed-against-default-branch narrowing applied above, and an unscoped
// command reaches every deployment rather than this one's managed slice. Either
// gap lets a deployment hold one database where the command finds two, and the
// bare command it would then offer comes back rejected as ambiguous, or starts
// a database its comment is not about.
//
// Discovery resolves each file's config on its own, so the raw files reach what
// auto-plan discovered over the proposed files plus what the files narrowing
// set aside reach. Only those are walked again: the count shares the delivery's
// deadline with discovery, and a second walk over every file can find that
// deadline already spent on a large PR.
//
// A discovery failure counts as several: naming the database always yields a
// command that resolves, so an unanswerable question degrades to the scoped
// rendering rather than to a command that may not work.
func (h *Handler) autoPlanCommandScopeDatabases(ctx context.Context, client *ghclient.InstallationClient, repo string, pr int, headSHA string, changedFiles, proposedFiles []ghclient.PRFile, discovered []ghclient.DiscoveredConfig) int {
	configs := slices.Clone(discovered)
	if narrowed := filesNarrowedAway(changedFiles, proposedFiles); len(narrowed) > 0 {
		narrowedConfigs, err := client.FindConfigsForPRFiles(ctx, repo, headSHA, narrowed)
		if err != nil {
			h.logger.Warn("failed to resolve which databases a bare command would reach; auto-plan comments will name their database",
				"repo", repo, "pr", pr, "head_sha", headSHA, "narrowed_files", len(narrowed), "error", err)
			return severalDatabases
		}
		configs = append(configs, narrowedConfigs...)
	}
	databases := make(map[string]struct{}, len(configs))
	for _, cfg := range configs {
		databases[cfg.Config.Database] = struct{}{}
	}
	return len(databases)
}

// filesNarrowedAway returns the changed files the proposed list does not carry
// as they are: the files narrowing dropped as inherited, and the renames it kept
// with their vacated path cleared. Each is returned as it was changed, so
// discovery over it sees what a bare command sees.
func filesNarrowedAway(changed, proposed []ghclient.PRFile) []ghclient.PRFile {
	type fileKey struct{ filename, previous string }
	kept := make(map[fileKey]struct{}, len(proposed))
	for _, file := range proposed {
		kept[fileKey{file.Filename, file.PreviousFilename}] = struct{}{}
	}
	var narrowed []ghclient.PRFile
	for _, file := range changed {
		if _, ok := kept[fileKey{file.Filename, file.PreviousFilename}]; !ok {
			narrowed = append(narrowed, file)
		}
	}
	return narrowed
}

// autoPlanInputMove names which input of a plan moved while it was being
// discovered.
type autoPlanInputMove uint8

const (
	autoPlanInputsUnchanged autoPlanInputMove = iota
	// autoPlanHeadMoved means a push replaced the head commit. Checks on the
	// old head no longer gate the merge, and the push plans the new head.
	autoPlanHeadMoved
	// autoPlanBaseMoved means the PR target changed or lost eligibility while
	// the head stayed the same.
	autoPlanBaseMoved
)

// autoPlanInputsMoved reports which input, if any, of a plan that is about to
// be published changed underneath it. An error means the question could not
// be answered — the caller must not publish on an unverified PR, and durable
// callers retry the delivery.
func (h *Handler) autoPlanInputsMoved(ctx context.Context, client *ghclient.InstallationClient, repo string, pr int, headSHA string, previous *ghclient.PullRequestInfo) (autoPlanInputMove, error) {
	_, moved, err := h.revalidateAutoPlanInputs(ctx, client, repo, pr, headSHA, previous)
	return moved, err
}

func (h *Handler) revalidateAutoPlanInputs(ctx context.Context, client *ghclient.InstallationClient, repo string, pr int, headSHA string, previous *ghclient.PullRequestInfo) (*ghclient.PullRequestInfo, autoPlanInputMove, error) {
	fresh, err := client.FetchPullRequestNoCache(ctx, repo, pr)
	if err != nil {
		return fresh, autoPlanInputsUnchanged, fmt.Errorf("fetch PR %s#%d: %w", repo, pr, err)
	}
	if fresh.HeadSHA != headSHA {
		h.logger.Info("PR head moved while its plan was being discovered",
			"repo", repo, "pr", pr, "head_sha", headSHA, "current_head_sha", fresh.HeadSHA)
		return fresh, autoPlanHeadMoved, nil
	}
	if !samePRTargetContext(fresh, previous) {
		h.logger.Info("PR target metadata moved while its plan was being discovered",
			"repo", repo, "pr", pr, "head_sha", headSHA, "base_ref", previous.BaseRef, "current_base_ref", fresh.BaseRef)
		return fresh, autoPlanBaseMoved, nil
	}
	decision, err := h.evaluatePRTarget(ctx, client, repo, fresh)
	if err != nil {
		return fresh, autoPlanInputsUnchanged, err
	}
	if !decision.Eligible {
		return fresh, autoPlanBaseMoved, nil
	}
	return fresh, autoPlanInputsUnchanged, nil
}

// notifyUnmanagedDiscoveredConfigs posts a PR-visible notice when auto-plan
// discovery dropped schema configs this deployment is not configured to
// manage. A deployment serving every environment on a repo with no aggregate
// role is the only responder, so without the notice the drop is invisible on
// the PR — no plan comment and no check row cover the dropped config, and the
// author can merge a schema change nothing will ever apply. Two shapes leave
// the dropped config to a deployment this one cannot see, so the notice stays
// a log line there:
//
//   - an aggregate-role repo (leader or participant), where a dropped config is
//     routine cross-deployment fan-out and the owning deployment plans it and
//     posts its own comment and check
//   - a deployment scoped to some environments (allowed_environments), where a
//     sibling deployment serving another environment may register the
//     database; a notice from this one would tell the author a schema change
//     the sibling is planning will never be applied
//
// An environment-scoped deployment still shows the dropped configs on the PR,
// scoped to the environments it serves: it returns them, and each plan comment
// it posts for the PR names them. A PR that changes nothing this deployment
// manages posts no plan comment, so its passing check names them instead.
func (h *Handler) notifyUnmanagedDiscoveredConfigs(repo string, pr int, installationID int64, source, headSHA string, shouldPostComment func() bool, discovered, managed []ghclient.DiscoveredConfig) []templates.UnmanagedSchemaConfigNoticeData {
	dropped := droppedDiscoveredConfigs(discovered, managed)
	if len(dropped) == 0 {
		return nil
	}
	config, ok := h.serverConfig()
	if ok && config.AggregateRoleForRepo(repo) != "" {
		h.logger.Info("unmanaged schema configs in PR left to their owning deployments on aggregate repo",
			"repo", repo, "pr", pr, "head_sha", headSHA, "source", source,
			"unmanaged_configs", len(dropped))
		return nil
	}
	notice := unmanagedSchemaConfigData(dropped)
	if ok && len(config.AllowedEnvironments) > 0 {
		// A Warn rather than an Info: when no sibling registers the database
		// either, nothing will ever apply this schema change, and this line is
		// where an operator finds which directories were left out.
		h.logger.Warn("schema configs in PR are not managed by this environment-scoped deployment; no notice posted because a deployment serving another environment may manage them, and the plan comment or passing check names them",
			"repo", repo, "pr", pr, "head_sha", headSHA, "source", source,
			"databases", unmanagedSchemaDatabases(notice), "schema_paths", unmanagedSchemaPaths(notice),
			"allowed_environments", config.AllowedEnvironments)
		return notice
	}
	// Match the plan-comment cadence: a synchronize push that changed no
	// schema inputs re-verifies checks without re-posting comments, and the
	// notice follows suit so an active PR is not re-noticed on every push.
	if !shouldPostComment() {
		h.logger.Info("unmanaged schema config notice suppressed by comment cadence; PR was already noticed on an earlier commit",
			"repo", repo, "pr", pr, "head_sha", headSHA, "source", source,
			"unmanaged_configs", len(dropped))
		return nil
	}
	var environments []string
	if ok {
		environments = config.OrderedEnvironments(config.KnownEnvironments())
	}
	h.postComment(repo, pr, installationID, templates.RenderUnmanagedSchemaConfigsNotice(environments, notice))
	return nil
}

func unmanagedSchemaDatabases(configs []templates.UnmanagedSchemaConfigNoticeData) []string {
	databases := make([]string, 0, len(configs))
	for _, cfg := range configs {
		databases = append(databases, cfg.Database)
	}
	return databases
}

func unmanagedSchemaPaths(configs []templates.UnmanagedSchemaConfigNoticeData) []string {
	paths := make([]string, 0, len(configs))
	for _, cfg := range configs {
		paths = append(paths, cfg.SchemaPath)
	}
	return paths
}

// unmanagedSchemaConfigData identifies each dropped config by its database
// and schema directory, for the notice and the passing check that report it.
func unmanagedSchemaConfigData(dropped []ghclient.DiscoveredConfig) []templates.UnmanagedSchemaConfigNoticeData {
	data := make([]templates.UnmanagedSchemaConfigNoticeData, 0, len(dropped))
	for _, cfg := range dropped {
		data = append(data, templates.UnmanagedSchemaConfigNoticeData{
			Database:   cfg.Config.Database,
			SchemaPath: cfg.SchemaDir,
		})
	}
	return data
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

// logConfigDiscoveryFailure logs an auto-plan config-discovery failure at the
// severity its cause deserves, naming the discovery step that failed. A PR past
// GitHub's per-PR changed-file cap is deterministic — GitHub reports the same
// oversized diff the same truncated way on every attempt — so it is a warning
// about the PR, not a SchemaBot or GitHub error an operator can act on. Every
// other cause stays an error worth investigating.
func (h *Handler) logConfigDiscoveryFailure(step, repo string, pr int, headSHA, source, deliveryID string, err error) {
	attrs := []any{
		"step", step, "repo", repo, "pr", pr, "head_sha", headSHA,
		"source", source, "delivery_id", deliveryID, "error", err,
	}
	if errors.Is(err, ghclient.ErrPRFilesIncomplete) {
		h.logger.Warn("auto-plan will not plan this PR because it changes more files than GitHub will report for a single pull request; SchemaBot cannot see the full diff and fails the check closed", attrs...)
		return
	}
	h.logger.Error("auto-plan config discovery failed", attrs...)
}

// postConfigDiscoveryFailure fails the aggregate closed for a discovery that
// could not finish. retriedAtReset reports whether the caller's durable
// delivery re-runs this discovery when a quota reserve refusal lifts, which
// decides whether the check says SchemaBot retries or asks for a re-run.
func (h *Handler) postConfigDiscoveryFailure(ctx context.Context, client *ghclient.InstallationClient, repo string, pr int, headSHA string, discoveryErr error, retriedAtReset bool) {
	// The causes stay apart because operators triage them differently: GitHub
	// being unavailable is an outage to wait out, the quota reserve lifts at a
	// known reset time, a PR past GitHub's per-PR changed-file cap is a
	// property of the PR that no retry can change, and anything else is a
	// configuration problem to investigate.
	block := configDiscoveryFailedBlock
	discoveryStatus := "error"
	var refusal *ghclient.QuotaReservedError
	switch {
	case errors.Is(discoveryErr, ghclient.ErrPRFilesIncomplete):
		// A deterministic cap is a block, not an error: reporting it as one
		// keeps it out of the discovery-error signal.
		block = prFileCapExceededBlock
		discoveryStatus = "blocked"
	case ghclient.IsUnavailableError(discoveryErr):
		block = githubConfigDiscoveryUnavailableBlock
	case errors.As(discoveryErr, &refusal):
		// The refusal is a deliberate hold on SchemaBot's own reads, not a
		// discovery error; the quota reserve already counts and logs it.
		block = githubQuotaReservedBlockFor(refusal.ResetAt, retriedAtReset)
		discoveryStatus = "blocked"
	}
	metrics.RecordStatusCheckOperation(ctx, metrics.StatusCheckOperation{
		Operation:  "schema_config_discovery",
		Repository: repo,
		Status:     discoveryStatus,
	})

	logAttrs := []any{"repo", repo, "pr", pr, "head_sha", headSHA, "blocking_reason", block.blockingReason}
	if refusal != nil {
		// The check text promises either a retry at the reset or a re-run by
		// the author. Logging which one, and the reset it names, lets a PR
		// that says "It will retry" be matched to the durable retry, or to
		// its absence.
		logAttrs = append(logAttrs, "retried_at_reset", retriedAtReset, "reset_time", refusal.ResetAt.UTC())
	}
	h.logger.Info("posting failing aggregate for config discovery failure", logAttrs...)
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
//
// A close delivery can run long after the close: the durable driver retries a
// failed cleanup at one-minute intervals. Cleanup therefore acts on the PR's
// live state, not on the delivery's. A PR that is open again keeps its lock
// and stored check state, because the lock owner (repo#pr) is the same across
// a close and a reopen, so a stale close delivery would otherwise release a
// lock an operator took after the reopen. A merged PR cannot reopen, so a
// merged delivery skips the live read. When the live state cannot be read,
// cleanup fails closed and nothing is released or deleted.
//
// The live state is read once, before anything is released or deleted. A
// reopen that lands after that read, while cleanup is still running, is not
// seen by this pass, so the pass acts only on what it listed. The PR's
// removable stored check state is listed before any lock is released, and the
// delete removes a listed row only while it is still exactly as listed. A row
// an apply or plan wrote again after the listing, such as one a reopened PR's
// `apply` stored with its new lock, is kept, and the pass returns an error the
// same way it does for a lock held again, so a retry starts from a fresh live
// read.
//
// The returned bool reports whether cleanup ran. It is false, with a nil
// error, when the PR is open again and nothing was touched.
func (h *Handler) runPRCloseCleanup(ctx context.Context, repo string, pr int, installationID int64, merged bool) (bool, error) {
	ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()

	stillClosed, err := h.prStillClosed(ctx, repo, pr, installationID, merged)
	if err != nil {
		metrics.RecordStatusCheckOperation(ctx, metrics.StatusCheckOperation{
			Operation:  "pr_close_cleanup",
			Repository: repo,
			Status:     "error",
		})
		h.logger.Error("PR close cleanup skipped: cannot verify the PR is still closed; all locks and check state are retained",
			"repo", repo, "pr", pr, "installation_id", installationID, "merged", merged, "error", err)
		return false, fmt.Errorf("verify PR %s#%d is still closed before cleanup: %w", repo, pr, err)
	}
	if !stillClosed {
		return false, nil
	}

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
		return false, fmt.Errorf("read applies for closed PR %s#%d: %w", repo, pr, err)
	}

	inFlight := h.inFlightAppliesForClosedPR(ctx, repo, pr, applies)

	// List the stored check state this close removes before releasing any
	// lock: once a lock is released, a reopened PR can take it again and
	// store its new plan's check state under the same keys, and the delete
	// below must not remove that.
	removable, err := h.service.Storage().Checks().ListRemovableOnPRClose(ctx, repo, pr, merged)
	if err != nil {
		metrics.RecordStatusCheckOperation(ctx, metrics.StatusCheckOperation{
			Operation:  "pr_close_cleanup",
			Repository: repo,
			Status:     "error",
		})
		h.logger.Error("PR close cleanup skipped: cannot list the PR's stored check state; all locks and check state are retained",
			"repo", repo, "pr", pr, "merged", merged, "error", err)
		return false, fmt.Errorf("list removable checks for closed PR %s#%d (merged=%t): %w", repo, pr, merged, err)
	}

	keptForApply, err := h.releaseLocksForClosedPR(ctx, repo, pr, inFlight)
	if err != nil {
		return false, fmt.Errorf("release locks for closed PR %s#%d: %w", repo, pr, err)
	}

	if len(inFlight) > 0 || keptForApply > 0 {
		h.logger.Info("check state retained for closed PR until all of its applies settle",
			"repo", repo, "pr", pr, "in_flight_databases", len(inFlight), "locks_kept_for_applies", keptForApply)
		return true, nil
	}

	// Delete the stored check state listed above. Apply-owned rows that still
	// block were never listed: in-flight rows (even if the applies table missed
	// the in-flight work above) and terminal rows whose conclusion is not
	// success, such as a schema change removed from the PR after its apply
	// started. Those blocks require operator reconciliation and must persist
	// across a close and reopen. On an unmerged close, apply-owned rows that
	// concluded successfully are not listed either: the stored success may
	// predate a commit that removed the applied change, so reopen-time cleanup
	// must re-verify it before the row can stop blocking.
	kept, err := h.service.Storage().Checks().DeleteListedIfUnchanged(ctx, removable)
	if err != nil {
		metrics.RecordStatusCheckOperation(ctx, metrics.StatusCheckOperation{
			Operation:  "pr_close_cleanup",
			Repository: repo,
			Status:     "error",
		})
		h.logger.Error("failed to delete checks for closed PR", "repo", repo, "pr", pr, "merged", merged, "error", err)
		return false, fmt.Errorf("delete checks for closed PR %s#%d (merged=%t): %w", repo, pr, merged, err)
	}
	if len(kept) > 0 {
		metrics.RecordStatusCheckOperation(ctx, metrics.StatusCheckOperation{
			Operation:  "pr_close_cleanup",
			Repository: repo,
			Status:     "error",
		})
		for _, check := range kept {
			h.logger.Warn("stored check state written again since PR close cleanup listed it is kept; this cleanup pass returns an error so a retry re-reads the live PR",
				"repo", repo, "pr", pr, "merged", merged,
				"environment", check.Environment, "database_type", check.DatabaseType, "database", check.DatabaseName,
				"check_id", check.ID, "check_run_id", check.CheckRunID, "head_sha", check.HeadSHA,
				"apply_id", check.ApplyID, "check_status", check.Status, "conclusion", check.Conclusion)
		}
		return false, fmt.Errorf("%d stored check rows for closed PR %s#%d were written again after close cleanup listed them and are kept", len(kept), repo, pr)
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
	return true, nil
}

// prStillClosed reports whether close cleanup should proceed for the PR. It
// reads the PR live — a cached snapshot could predate a reopen — and answers
// false, with the skip logged, when the PR is open again. Any state other than
// open or closed is an error, so an unrecognized answer never authorizes
// cleanup. A merged PR cannot reopen, so merged skips the read and costs no
// GitHub request. Otherwise the cost is one request per close delivery
// attempt.
func (h *Handler) prStillClosed(ctx context.Context, repo string, pr int, installationID int64, merged bool) (bool, error) {
	if merged {
		h.logger.Debug("PR close cleanup proceeds without a live read: a merged PR cannot reopen",
			"repo", repo, "pr", pr)
		return true, nil
	}
	if installationID <= 0 {
		return false, fmt.Errorf("no GitHub installation ID to read the live PR (installation_id=%d)", installationID)
	}
	client, err := h.clientForRepo(repo, installationID)
	if err != nil {
		return false, fmt.Errorf("create GitHub client for repo %s installation %d: %w", repo, installationID, err)
	}
	// FetchPullRequestNoCache: only a live read can prove the PR did not reopen
	// after the close this delivery carries.
	prInfo, err := client.FetchPullRequestNoCache(ctx, repo, pr)
	if err != nil {
		return false, fmt.Errorf("fetch live PR %s#%d: %w", repo, pr, err)
	}
	switch {
	case prInfo.IsClosed():
		return true, nil
	case prInfo.IsOpen():
		h.logger.Info("PR close cleanup skipped because the PR was reopened; its locks and check state are retained",
			"repo", repo, "pr", pr, "head_sha", prInfo.HeadSHA)
		return false, nil
	default:
		return false, fmt.Errorf("live PR %s#%d reports unrecognized state %q", repo, pr, prInfo.State)
	}
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
// databases that still have an in-flight apply. Each release deletes only the
// lock row this pass listed, held for the pending plan it listed, so a lock
// taken after the listing is never released by it.
//
// A listed lock that is already gone, released by an unlock or an apply's own
// cleanup in between, is what cleanup wanted, so it is logged and not an
// error. A database locked again since the listing, by a new row or by the
// same row for a new plan, stays held and is returned as an error: the PR may
// have reopened and planned again after cleanup read its live state, so this
// pass must not go on to delete its check state. A new row can also belong to
// another owner, which locked the database once the listed lock was released;
// the release cannot tell the two apart, so it treats both the same way. A
// caller that retries re-reads the live PR first and lists the PR's locks
// again, so it releases a lock only if the PR is still closed and still holds
// it.
//
// The in-flight map comes from an applies read made before the release, so it
// cannot see an apply that records a listed lock and becomes durable after that
// read. Each release therefore re-checks under the lock row's lock and keeps
// the lock while an apply recording it has not settled, a stopped apply
// included. Such a lock is in flight exactly as one the read found, so it is
// counted in the returned keptForApply rather than reported as an error: the
// caller keeps the PR's check state and completes, and a retry would only find
// the same apply again.
//
// Every release is attempted even if an earlier one fails, and the failures
// are joined and returned so a caller with a retry mechanism re-attempts the
// still-held locks. Each attempt re-lists the PR's locks first, and a released
// lock's row is deleted, so a retry converges.
func (h *Handler) releaseLocksForClosedPR(ctx context.Context, repo string, pr int, inFlight map[closedPRDatabase]bool) (keptForApply int, err error) {
	locks, err := h.service.Storage().Locks().GetByPR(ctx, repo, pr)
	if err != nil {
		h.logger.Error("failed to look up locks for closed PR; no locks released", "repo", repo, "pr", pr, "error", err)
		return 0, fmt.Errorf("look up locks for closed PR %s#%d: %w", repo, pr, err)
	}
	var releaseErrs []error
	for _, lock := range locks {
		if inFlight[closedPRDatabase{database: lock.DatabaseName, databaseType: lock.DatabaseType}] {
			h.logger.Info("lock retained on PR close because an apply is in flight",
				"repo", repo, "pr", pr, "database", lock.DatabaseName, "database_type", lock.DatabaseType)
			continue
		}
		err := h.service.Storage().Locks().ReleaseByIDIfUnused(ctx, lock.ID, lock.DatabaseName, lock.DatabaseType, lock.Owner, lock.PendingPlanID)
		var inUse *storage.LockInUseError
		switch {
		case err == nil:
			h.logger.Info("released lock on PR close",
				"repo", repo, "pr", pr, "database", lock.DatabaseName)
		case errors.As(err, &inUse):
			// An apply recording the listed lock row became durable after
			// cleanup read the PR's applies, or is stopped and has not
			// settled. Either way it is in flight for this lock.
			keptForApply++
			h.logger.Warn("lock retained on PR close because an apply recording it has not settled; this cleanup pass deletes no check state",
				append(inUse.Apply.LogAttrs(),
					"closed_repo", repo, "closed_pr", pr, "lock_owner", lock.Owner,
					"listed_pending_plan_id", lock.PendingPlanID, "error", err)...)
		case isVettedLockGone(err):
			h.logger.Info("lock already released before PR close cleanup reached it",
				"repo", repo, "pr", pr, "database", lock.DatabaseName, "database_type", lock.DatabaseType,
				"owner", lock.Owner, "error", err)
		case errors.Is(err, storage.ErrLockReplaced):
			// The new row can belong to this PR or to another owner that
			// locked the database after the listed lock was released.
			h.logger.Warn("a different lock row holds the database since PR close cleanup listed its lock; that row is kept and this cleanup pass deletes no check state",
				"repo", repo, "pr", pr, "database", lock.DatabaseName, "database_type", lock.DatabaseType,
				"owner", lock.Owner, "listed_pending_plan_id", lock.PendingPlanID, "error", err)
			releaseErrs = append(releaseErrs, fmt.Errorf("lock on %s (%s) is held by a different lock row than the one close cleanup listed: %w", lock.DatabaseName, lock.DatabaseType, err))
		case errors.Is(err, storage.ErrLockIntentChanged):
			h.logger.Warn("lock acquired again for another pending plan after PR close cleanup listed it; it is kept and this cleanup pass deletes no check state",
				"repo", repo, "pr", pr, "database", lock.DatabaseName, "database_type", lock.DatabaseType,
				"owner", lock.Owner, "listed_pending_plan_id", lock.PendingPlanID, "error", err)
			releaseErrs = append(releaseErrs, fmt.Errorf("lock on %s (%s) was acquired again for another pending plan after close cleanup listed it: %w", lock.DatabaseName, lock.DatabaseType, err))
		default:
			h.logger.Error("failed to release lock on PR close",
				"repo", repo, "pr", pr, "database", lock.DatabaseName, "database_type", lock.DatabaseType, "error", err)
			releaseErrs = append(releaseErrs, fmt.Errorf("release lock on %s (%s): %w", lock.DatabaseName, lock.DatabaseType, err))
		}
	}
	return keptForApply, errors.Join(releaseErrs...)
}

// checkDatabaseKey identifies the database a per-database stored check row
// gates: its name and its type, the two database components of the stored
// check state key. A database whose type changes while a PR is open (for
// example a MySQL database converted to Strata) is planned under the new type,
// so its row under the old type no longer belongs to anything the PR plans.
type checkDatabaseKey struct {
	databaseName string
	databaseType string
}

// newCheckDatabaseKey folds both components the way stored check state folds
// them, so a row and the config that planned it always produce the same key.
func newCheckDatabaseKey(databaseName, databaseType string) checkDatabaseKey {
	return checkDatabaseKey{
		databaseName: storage.CanonicalKey(databaseName),
		databaseType: storage.CanonicalKey(databaseType),
	}
}

// checkDatabaseKeyForCheck returns the database a stored check row gates.
func checkDatabaseKeyForCheck(check *storage.Check) checkDatabaseKey {
	return newCheckDatabaseKey(check.DatabaseName, check.DatabaseType)
}

// checkDatabaseKeysForConfigs returns the databases the PR's discovered configs
// plan, keyed by name and type.
func checkDatabaseKeysForConfigs(configs []ghclient.DiscoveredConfig) map[checkDatabaseKey]bool {
	keys := make(map[checkDatabaseKey]bool, len(configs))
	for _, cfg := range configs {
		keys[newCheckDatabaseKey(cfg.Config.Database, string(cfg.Config.GetType()))] = true
	}
	return keys
}

// cleanupStaleChecks updates checks for databases no longer in the PR. A row is
// still affected only when the PR plans a database with the same name and type.
// A row under a database's old type, for a database the PR now plans under a
// new type, is left for that plan to settle (see settleLeftBehindChecks).
// When the PR plans databases, cleanup leaves the aggregate to their plans, so a
// caller passing affected databases must launch those plans only after this
// returns.
// Plan-only checks can be marked "success" because the current PR no longer asks
// SchemaBot to apply anything. Checks that represent a started apply remain
// blocking because the live database may already have changed or may still change.
//
// On synchronize events, headSHA is the new commit SHA. Stale checks must be created
// as new check runs on this SHA (not updated on the old SHA) so GitHub shows them
// on the current commit.
func (h *Handler) cleanupStaleChecks(repo string, pr int, headSHA string, installationID int64, affectedDatabases map[checkDatabaseKey]bool) {
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

	plannedDatabaseNames := make(map[string]bool, len(affectedDatabases))
	for key := range affectedDatabases {
		plannedDatabaseNames[key.databaseName] = true
	}
	cleaned := false

	for _, check := range checks {
		if isAggregateCheck(check) {
			h.logger.Debug("skipping aggregate check during stale cleanup",
				"repo", repo, "pr", pr, "head_sha", headSHA,
				"environment", check.Environment, "check_id", check.ID)
			continue
		}

		if affectedDatabases[checkDatabaseKeyForCheck(check)] {
			h.logger.Debug("skipping check during stale cleanup because database is still affected",
				"repo", repo, "pr", pr, "head_sha", headSHA,
				"database", check.DatabaseName, "database_type", check.DatabaseType,
				"environment", check.Environment, "check_id", check.ID)
			continue
		}

		if plannedDatabaseNames[storage.CanonicalKey(check.DatabaseName)] {
			// The PR still plans this database, under a new type. The row stays
			// on its earlier commit, where the aggregate reads it as blocking,
			// until the new type's plan stores its own result and settles it.
			h.logger.Info("stale cleanup leaves a check under a database's old type to the plan of its new type",
				"repo", repo, "pr", pr, "head_sha", headSHA,
				"database", check.DatabaseName, "database_type", check.DatabaseType,
				"environment", check.Environment, "check_id", check.ID)
			continue
		}

		// The PR no longer plans this check's database.
		h.logger.Info("cleaning up stale check",
			"repo", repo, "pr", pr,
			"database", check.DatabaseName, "database_type", check.DatabaseType,
			"environment", check.Environment, "head_sha", headSHA,
			"previous_status", check.Status, "previous_conclusion", check.Conclusion,
			"previous_blocking_reason", check.BlockingReason, "apply_id", check.ApplyID)

		if h.settleStaleCheckState(ctx, repo, pr, headSHA, check) {
			cleaned = true
		}
	}

	if !cleaned {
		metrics.RecordStatusCheckOperation(ctx, metrics.StatusCheckOperation{
			Operation:  "stale_check_cleanup",
			Repository: repo,
			Status:     "noop",
		})
		return
	}

	// Each plan the PR launches folds the aggregate after it stores its rows for
	// every environment, and runAutoPlanForPR launches them only after this
	// cleanup returns, so their folds see the rows settled here. Folding here
	// instead could publish the aggregate before those plans store anything,
	// with a plan-only row settled to success standing in for results that do
	// not exist yet.
	if len(affectedDatabases) > 0 {
		h.logger.Info("stale cleanup leaves the aggregate to the plans of the databases the PR plans",
			"repo", repo, "pr", pr, "head_sha", headSHA,
			"planned_databases", checkDatabaseKeyStrings(affectedDatabases))
		return
	}
	h.updateAggregateCheck(ctx, client, repo, pr, headSHA)
}

// checkDatabaseKeyStrings renders database keys as name/type, sorted for logs.
func checkDatabaseKeyStrings(keys map[checkDatabaseKey]bool) []string {
	out := make([]string, 0, len(keys))
	for key := range keys {
		out = append(out, key.databaseName+"/"+key.databaseType)
	}
	slices.Sort(out)
	return out
}

// settleLeftBehindChecks settles the rows a planned database left behind on
// an earlier commit that no plan on this commit will store again: rows under
// a type the database no longer has, and rows in an environment the database
// no longer has. A plan calls it after storing its rows and before folding the
// aggregate, so a left-behind row blocks the aggregate, from its earlier
// commit, for as long as it might still be replaced. That holds for every
// fold, not only the plan's own: a second database still planning keeps the
// aggregate open through its own old row.
//
// A row under an old type is settled once the plan of the new type has stored
// results. Whether a result replaces it is read from stored check state, not
// from what the calling plan stored, so a plan of one environment and a plan
// of every environment settle the same rows. It is settled in an environment
// where a row under the new type is stored on headSHA, and in an environment
// this deployment does not plan the database in, since nothing will replace it
// there. Anywhere else it keeps blocking until a later plan stores that
// environment.
//
// A row under the database's current type is settled only when its
// environment is no longer one this deployment plans the database in, for
// example after an operator removes the environment from the database's
// server config. No plan can store that environment again, so without this
// the row would hold the aggregate open for good. The environments come from
// the server config, never from the PR, so a PR cannot drop an environment to
// clear its own gate. A row in an environment the database still has is left
// for that environment's next plan. Such a row is settled only once a result
// of an environment the database is planned in is stored on headSHA, so a
// plan whose own write failed cannot leave the aggregate with no result of
// the database at all.
//
// Settling follows stale cleanup: a started apply keeps its row blocking. A
// plan-only row under an old type passes. A plan-only row in an environment
// the database no longer has is removed rather than passed, so the PR is left
// as if the environment had never been configured: if an operator lists the
// environment again, its missing result reads as it would for any newly
// configured environment, and never as a pass. A plan that finishes after the
// PR moved to a newer commit settles nothing, because the newer commit's
// cleanup and plans own those rows. The PR head is read only when there is a row to settle, so a
// plan with nothing left behind adds no GitHub call.
func (h *Handler) settleLeftBehindChecks(ctx context.Context, client *ghclient.InstallationClient, repo string, pr int, headSHA, databaseName, databaseType string) error {
	checks, err := h.service.Storage().Checks().GetByPR(ctx, repo, pr)
	if err != nil {
		return fmt.Errorf("load checks for %s#%d to settle rows %s left on earlier commits: %w", repo, pr, databaseName, err)
	}
	planned := newCheckDatabaseKey(databaseName, databaseType)
	var oldTypeChecks, earlierCommitChecks []*storage.Check
	replacedEnvironments := map[string]bool{}
	for _, check := range checks {
		if isAggregateCheck(check) {
			continue
		}
		key := checkDatabaseKeyForCheck(check)
		if key.databaseName != planned.databaseName {
			continue
		}
		if key.databaseType == planned.databaseType {
			if check.HeadSHA == headSHA {
				replacedEnvironments[storage.CanonicalKey(check.Environment)] = true
				continue
			}
			earlierCommitChecks = append(earlierCommitChecks, check)
			continue
		}
		if check.HeadSHA == headSHA {
			h.logger.Debug("check under a database's old type is already settled on this commit",
				"repo", repo, "pr", pr, "head_sha", headSHA,
				"database", check.DatabaseName, "database_type", check.DatabaseType, "planned_database_type", databaseType,
				"environment", check.Environment, "check_id", check.ID)
			continue
		}
		oldTypeChecks = append(oldTypeChecks, check)
	}
	if len(oldTypeChecks) == 0 && len(earlierCommitChecks) == 0 {
		h.logger.Debug("plan of a database leaves no rows on earlier commits to settle",
			"repo", repo, "pr", pr, "head_sha", headSHA, "database", databaseName, "database_type", databaseType)
		return nil
	}

	environments, err := h.allowedDatabaseEnvironments(databaseName)
	if err != nil {
		return fmt.Errorf("resolve environments of %s to settle the rows it left on earlier commits of %s#%d: %w", databaseName, repo, pr, err)
	}
	plannedEnvironments := make(map[string]bool, len(environments))
	for _, environment := range environments {
		plannedEnvironments[storage.CanonicalKey(environment)] = true
	}
	removedEnvironmentChecks := checksOutsideEnvironments(earlierCommitChecks, plannedEnvironments)
	if len(removedEnvironmentChecks) > 0 && !storesResultInEnvironments(replacedEnvironments, plannedEnvironments) {
		// The plan's own result was not stored, so settling these rows could
		// leave the aggregate passing with no result of this database on this
		// commit. They keep blocking until a plan whose result is stored.
		h.logger.Warn("checks in environments the database is no longer planned in keep blocking because no result of an environment it is planned in is stored on this commit",
			"repo", repo, "pr", pr, "head_sha", headSHA, "database", databaseName, "database_type", databaseType,
			"planned_environments", environments, "rows_in_removed_environments", len(removedEnvironmentChecks))
		removedEnvironmentChecks = nil
	}
	if len(oldTypeChecks) == 0 && len(removedEnvironmentChecks) == 0 {
		h.logger.Debug("plan of a database leaves its rows on earlier commits to the plans of their environments",
			"repo", repo, "pr", pr, "head_sha", headSHA, "database", databaseName, "database_type", databaseType,
			"rows_on_earlier_commits", len(earlierCommitChecks))
		return nil
	}

	prInfo, err := client.FetchPullRequestNoCache(ctx, repo, pr)
	if err != nil {
		return fmt.Errorf("verify head of %s#%d before settling rows %s left on earlier commits, on %s: %w", repo, pr, databaseName, headSHA, err)
	}
	if prInfo.HeadSHA != headSHA {
		h.logger.Info("plan of a database settles no rows it left on earlier commits because the PR moved to a newer commit",
			"repo", repo, "pr", pr, "head_sha", headSHA, "current_head_sha", prInfo.HeadSHA,
			"database", databaseName, "database_type", databaseType)
		return nil
	}
	for _, check := range oldTypeChecks {
		environment := storage.CanonicalKey(check.Environment)
		if plannedEnvironments[environment] && !replacedEnvironments[environment] {
			h.logger.Warn("check under a database's old type keeps blocking because no result under its new type is stored for this environment on this commit",
				"repo", repo, "pr", pr, "head_sha", headSHA,
				"database", check.DatabaseName, "database_type", check.DatabaseType, "planned_database_type", databaseType,
				"environment", check.Environment, "check_id", check.ID)
			continue
		}
		h.logger.Info("settling check under a database's old type after the plan of its new type",
			"repo", repo, "pr", pr, "head_sha", headSHA,
			"database", check.DatabaseName, "database_type", check.DatabaseType, "planned_database_type", databaseType,
			"environment", check.Environment, "check_id", check.ID,
			"previous_status", check.Status, "previous_conclusion", check.Conclusion, "apply_id", check.ApplyID)
		h.settleStaleCheckState(ctx, repo, pr, headSHA, check)
	}
	for _, check := range removedEnvironmentChecks {
		h.logger.Info("settling check in an environment the database is no longer planned in",
			"repo", repo, "pr", pr, "head_sha", headSHA,
			"database", check.DatabaseName, "database_type", check.DatabaseType,
			"environment", check.Environment, "check_id", check.ID,
			"planned_environments", environments,
			"previous_head_sha", check.HeadSHA,
			"previous_status", check.Status, "previous_conclusion", check.Conclusion, "apply_id", check.ApplyID)
		if checkHasStartedApply(check) {
			h.blockStaleStartedApplyCheckState(ctx, repo, pr, headSHA, check)
			continue
		}
		h.removeStalePlanOnlyCheckState(ctx, repo, pr, headSHA, check)
	}
	return nil
}

// removeStalePlanOnlyCheckState removes a plan-only row in an environment the
// database no longer has. The delete is guarded in storage: a row an apply
// claimed, or one a newer write moved, after this plan read it is kept.
func (h *Handler) removeStalePlanOnlyCheckState(ctx context.Context, repo string, pr int, headSHA string, check *storage.Check) {
	readHeadSHA := check.HeadSHA
	removed, err := h.service.Storage().Checks().DeleteStalePlanOnly(ctx, check, readHeadSHA)
	status := "success"
	switch {
	case err != nil:
		status = "error"
		h.logger.Error("failed to remove check in an environment the database is no longer planned in; the row keeps blocking the aggregate",
			"repo", repo, "pr", pr, "head_sha", headSHA,
			"database", check.DatabaseName, "database_type", check.DatabaseType,
			"environment", check.Environment, "check_id", check.ID,
			"read_head_sha", readHeadSHA, "error", err)
	case !removed:
		status = "noop"
		h.logger.Info("check in an environment the database is no longer planned in left to the apply or newer write that changed it after this plan read it",
			"repo", repo, "pr", pr, "head_sha", headSHA,
			"database", check.DatabaseName, "database_type", check.DatabaseType,
			"environment", check.Environment, "check_id", check.ID,
			"read_head_sha", readHeadSHA)
	}
	metrics.RecordStatusCheckOperation(ctx, metrics.StatusCheckOperation{
		Operation:    "stale_check_cleanup",
		Repository:   repo,
		Database:     check.DatabaseName,
		DatabaseType: check.DatabaseType,
		Environment:  check.Environment,
		Status:       status,
	})
}

// checksOutsideEnvironments returns the rows whose environment is not one of
// environments, keyed by storage.CanonicalKey.
func checksOutsideEnvironments(checks []*storage.Check, environments map[string]bool) []*storage.Check {
	var outside []*storage.Check
	for _, check := range checks {
		if !environments[storage.CanonicalKey(check.Environment)] {
			outside = append(outside, check)
		}
	}
	return outside
}

// storesResultInEnvironments reports whether any environment in stored is one
// of environments. Both are keyed by storage.CanonicalKey.
func storesResultInEnvironments(stored, environments map[string]bool) bool {
	for environment := range stored {
		if environments[environment] {
			return true
		}
	}
	return false
}

// settleStaleCheckState moves a row nothing will replace onto headSHA. A row a
// started apply owns keeps blocking, because the live database may already have
// changed; a plan-only row passes. It reports whether the row was written.
func (h *Handler) settleStaleCheckState(ctx context.Context, repo string, pr int, headSHA string, check *storage.Check) bool {
	if checkHasStartedApply(check) {
		return h.blockStaleStartedApplyCheckState(ctx, repo, pr, headSHA, check)
	}
	return h.markStalePlanOnlyCheckStateSuccessful(ctx, repo, pr, headSHA, check)
}

// blockStaleStartedApplyCheckState blocks a row a started apply owns on headSHA.
// Whether the row stays in_progress or settles as action_required is decided by
// the store from the row as it is at write time, so an apply that completes
// after this cleanup read the row is never shown as running again.
func (h *Handler) blockStaleStartedApplyCheckState(ctx context.Context, repo string, pr int, headSHA string, check *storage.Check) bool {
	readStatus := check.Status
	readHeadSHA := check.HeadSHA
	check.BlockingReason = schemaRemovedAfterApplyBlock.blockingReason
	check.ErrorMessage = schemaRemovedAfterApplyBlock.message
	blocked, err := h.service.Storage().Checks().BlockStaleStartedApply(ctx, check, headSHA)
	if err != nil {
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
			"read_head_sha", readHeadSHA, "error", err)
		return false
	}
	if !blocked {
		// Between the cleanup read and this write, the apply released the row,
		// another apply claimed it, or a newer commit's cleanup moved it on.
		// That later write is the stored state and stays in place.
		metrics.RecordStatusCheckOperation(ctx, metrics.StatusCheckOperation{
			Operation:    "stale_check_cleanup",
			Repository:   repo,
			Database:     check.DatabaseName,
			DatabaseType: check.DatabaseType,
			Environment:  check.Environment,
			Status:       "noop",
		})
		h.logger.Info("stale check left to the write that released, claimed, or moved it after cleanup read it",
			"repo", repo, "pr", pr, "head_sha", headSHA,
			"database", check.DatabaseName, "database_type", check.DatabaseType,
			"environment", check.Environment, "check_id", check.ID,
			"read_head_sha", readHeadSHA, "read_status", readStatus)
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
	if check.Status != readStatus {
		h.logger.Info("stale check blocked against the status its apply recorded after cleanup read it",
			"repo", repo, "pr", pr, "head_sha", headSHA,
			"database", check.DatabaseName, "database_type", check.DatabaseType,
			"environment", check.Environment, "check_id", check.ID,
			"read_status", readStatus,
			"stored_status", check.Status, "stored_conclusion", check.Conclusion)
	}
	return true
}

func (h *Handler) markStalePlanOnlyCheckStateSuccessful(ctx context.Context, repo string, pr int, headSHA string, check *storage.Check) bool {
	priorApplyID := check.ApplyID
	readHeadSHA := check.HeadSHA
	check.HeadSHA = headSHA
	check.Conclusion = checkConclusionSuccess
	check.HasChanges = false
	check.Status = checkStatusCompleted
	check.ApplyID = 0
	check.BlockingReason = ""
	check.ErrorMessage = ""

	// The success write is guarded against in-flight apply-owned rows: an apply
	// that started after this cleanup read the row must keep blocking the PR.
	// It is also pinned to the head the row was read at, so a newer commit's
	// write that moved the row first is not pulled back to this older commit.
	marked, err := h.service.Storage().Checks().MarkStalePlanSuccessful(ctx, check, readHeadSHA)
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

	if !marked && h.stalePlanCheckMovedByNewerWrite(ctx, repo, pr, headSHA, readHeadSHA, check) {
		metrics.RecordStatusCheckOperation(ctx, metrics.StatusCheckOperation{
			Operation:    "stale_check_cleanup",
			Repository:   repo,
			Database:     check.DatabaseName,
			DatabaseType: check.DatabaseType,
			Environment:  check.Environment,
			Status:       "noop",
		})
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

// stalePlanCheckMovedByNewerWrite reports whether a plan-only row the stale
// success write skipped was moved to another commit, by a write newer than
// this cleanup's read, rather than claimed by an apply. Such a row belongs to
// that newer write. An apply-owned row, or one that cannot be re-read, reports
// false, so the caller keeps treating it as blocking.
func (h *Handler) stalePlanCheckMovedByNewerWrite(ctx context.Context, repo string, pr int, headSHA, readHeadSHA string, check *storage.Check) bool {
	stored, err := h.service.Storage().Checks().Get(ctx, check.Repository, check.PullRequest, check.Environment, check.DatabaseType, check.DatabaseName)
	if err != nil {
		h.logger.Error("failed to re-read stale plan check after its success write was skipped; the row is treated as claimed by an apply and keeps blocking",
			"repo", repo, "pr", pr, "head_sha", headSHA,
			"database", check.DatabaseName, "database_type", check.DatabaseType,
			"environment", check.Environment, "check_id", check.ID,
			"read_head_sha", readHeadSHA, "error", err)
		return false
	}
	if stored == nil {
		h.logger.Warn("stale plan check was deleted after cleanup read it; nothing is left to settle",
			"repo", repo, "pr", pr, "head_sha", headSHA,
			"database", check.DatabaseName, "database_type", check.DatabaseType,
			"environment", check.Environment, "check_id", check.ID,
			"read_head_sha", readHeadSHA)
		return true
	}
	if checkHasStartedApply(stored) || stored.HeadSHA == readHeadSHA || stored.HeadSHA == headSHA {
		return false
	}
	h.logger.Info("stale plan check left to the newer write that moved it to another commit after cleanup read it",
		"repo", repo, "pr", pr, "head_sha", headSHA,
		"database", check.DatabaseName, "database_type", check.DatabaseType,
		"environment", check.Environment, "check_id", check.ID,
		"read_head_sha", readHeadSHA, "stored_head_sha", stored.HeadSHA,
		"stored_status", stored.Status, "stored_conclusion", stored.Conclusion)
	return true
}

// managedDatabaseNames returns the sorted, de-duplicated database names of
// configs.
func managedDatabaseNames(configs []ghclient.DiscoveredConfig) []string {
	names := make([]string, 0, len(configs))
	for _, cfg := range configs {
		names = append(names, cfg.Config.Database)
	}
	slices.Sort(names)
	return slices.Compact(names)
}
