package webhook

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
	"time"

	"github.com/block/schemabot/pkg/api"
	ghclient "github.com/block/schemabot/pkg/github"
	"github.com/block/schemabot/pkg/metrics"
	"github.com/block/schemabot/pkg/storage"
)

// mergeGroupPayload is the subset of the GitHub merge_group webhook payload
// SchemaBot needs. GitHub fires this event when a pull request enters or leaves
// a repository's merge queue.
type mergeGroupPayload struct {
	Action     string `json:"action"`
	MergeGroup struct {
		HeadSHA string `json:"head_sha"`
	} `json:"merge_group"`
	Repository struct {
		FullName string `json:"full_name"`
	} `json:"repository"`
	Installation struct {
		ID int64 `json:"id"`
	} `json:"installation"`
}

// handleMergeGroup responds to merge_group webhook events so SchemaBot's
// required Check Runs do not wedge a repository's merge queue.
//
// A merge queue tests queued pull requests combined, on a synthetic head
// commit, before they land on the base branch. Branch protection re-evaluates
// the same required checks against that merge-group head SHA — but SchemaBot
// only ever publishes its checks on PR head SHAs, so without this handler the
// required SchemaBot check would never appear on the merge-group commit and the
// queue entry would block indefinitely.
//
// Posting a passing check is correct: SchemaBot applies schema changes before a
// PR merges, and branch protection already required the PR-head check to pass
// before the PR could enter the queue. The merge group sits strictly downstream
// of an already-completed, already-gated apply, so there is nothing left to
// verify on the combined commit.
func (h *Handler) handleMergeGroup(ctx context.Context, metricApp string, w http.ResponseWriter, body []byte, deliveryID string) {
	var payload mergeGroupPayload
	if err := json.Unmarshal(body, &payload); err != nil {
		h.writeError(w, http.StatusBadRequest, "invalid merge_group payload")
		return
	}
	payload.Repository.FullName = storage.CanonicalKey(payload.Repository.FullName)

	// GitHub sends "checks_requested" when a PR joins the queue and "destroyed"
	// when it leaves. Only checks_requested needs a check run on the new SHA.
	if payload.Action != "checks_requested" {
		h.logger.Debug("merge_group action ignored",
			"action", payload.Action, "repo", payload.Repository.FullName)
		h.writeJSON(w, http.StatusOK, map[string]string{"message": "merge_group action ignored"})
		return
	}

	repo := payload.Repository.FullName
	headSHA := payload.MergeGroup.HeadSHA
	installationID := h.effectiveInstallationID(ctx, payload.Installation.ID)
	if installationID == 0 {
		h.writeError(w, http.StatusBadRequest, "missing installation ID in merge_group payload")
		return
	}
	if headSHA == "" {
		h.writeError(w, http.StatusBadRequest, "missing merge_group head_sha in merge_group payload")
		return
	}

	// A repo SchemaBot does not manage gets no check — its check is not required
	// on that repo, so there is nothing to unblock.
	if h.service != nil && !h.service.Config().IsRepoAllowed(repo) {
		h.logger.Warn("merge_group webhook from unregistered repository",
			"repo", repo, "head_sha", headSHA, "installation_id", installationID)
		metrics.RecordUnregisteredRepositoryWebhook(ctx, metricApp, "merge_group", payload.Action)
		h.writeJSON(w, http.StatusOK, map[string]string{"message": "repository not registered"})
		return
	}

	// An aggregate participant's checks are never required — the leader owns
	// the required aggregate and posts it on the merge-group commit — so a
	// participant posting here would only re-add the per-tenant rows the
	// aggregate removes. A silent participant cannot wedge the queue: only
	// required checks gate a merge-group entry.
	if h.isAggregateParticipant(repo) {
		h.logger.Info("aggregate participant staying silent on merge_group; the leader posts the required checks",
			"repo", repo, "head_sha", headSHA, "installation_id", installationID)
		metrics.RecordStatusCheckOperation(ctx, metrics.StatusCheckOperation{
			Operation:  "merge_group_check",
			Repository: repo,
			Status:     "skipped",
		})
		h.writeJSON(w, http.StatusOK, map[string]string{"message": "merge_group ignored (aggregate participant, staying silent)"})
		return
	}

	// When check publishing is disabled for this repo, SchemaBot's check is not
	// required either, so skipping the merge-group check is correct.
	if !h.shouldRunPassingAggregateChecks(ctx, repo, "merge_group_check") {
		h.writeJSON(w, http.StatusOK, map[string]string{"message": "check publishing disabled"})
		return
	}

	if h.durableWebhookDispatch {
		// Enqueue and ACK fast; a leased driver posts the check with retries so
		// a process restart mid-post cannot drop the delivery. Enqueue failure
		// is a deliberate 500 with no in-process fallback — it fails loudly (a
		// red delivery in GitHub's webhook UI) so an operator can Redeliver.
		inserted, err := h.enqueueDurableMergeGroup(ctx, payload, body, deliveryID, installationID)
		if err != nil {
			h.logger.Error("failed to enqueue durable merge_group check",
				"repo", repo, "head_sha", headSHA, "installation_id", installationID,
				"delivery_id", deliveryID, "error", err)
			metrics.RecordWebhookEvent(ctx, metricApp, "merge_group", payload.Action, repo, "durable_enqueue_failed")
			h.writeError(w, http.StatusInternalServerError, "failed to enqueue webhook delivery")
			return
		}
		if !inserted {
			h.logger.Info("durable merge_group delivery already queued",
				"repo", repo, "head_sha", headSHA, "delivery_id", deliveryID)
			h.writeJSON(w, http.StatusOK, map[string]string{"message": "merge_group check already queued"})
			return
		}
		h.logger.Info("durable merge_group check queued",
			"repo", repo, "head_sha", headSHA, "delivery_id", deliveryID)
		h.writeJSON(w, http.StatusOK, map[string]string{"message": "merge_group check queued"})
		return
	}

	postCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 30*time.Second)
	defer cancel()

	client, err := h.clientForRepo(repo, installationID)
	if err != nil {
		h.logger.Error("failed to create GitHub client for merge_group check",
			"repo", repo, "head_sha", headSHA, "installation_id", installationID, "error", err)
		h.writeError(w, http.StatusInternalServerError, "failed to initialize GitHub client")
		return
	}

	if err := h.postPassingAggregateChecks(postCtx, client, repo, headSHA, mergeGroupCheckContent()); err != nil {
		// Return 500 so the delivery is recorded as failed and shows up in the
		// App's delivery log for redelivery. The merge queue blocks until the
		// check is posted, so the failure must be visible for retry, not
		// silently dropped.
		metrics.RecordStatusCheckOperation(ctx, metrics.StatusCheckOperation{
			Operation:  "merge_group_check",
			Repository: repo,
			Status:     "error",
		})
		h.logger.Error("failed to post merge_group checks",
			"repo", repo, "head_sha", headSHA, "error", err)
		h.writeError(w, http.StatusInternalServerError, "failed to post merge_group checks")
		return
	}

	if h.isRepoListenOnly(repo) {
		h.writeJSON(w, http.StatusOK, map[string]string{"message": "merge_group checks skipped (listen-only repository)"})
		return
	}
	h.writeJSON(w, http.StatusOK, map[string]string{"message": "merge_group checks posted"})
}

// mergeGroupCheckContent is the passing Check Run content published on a
// merge-queue head. Both the request path and the durable driver use it so a
// redelivery updates the same check with identical output.
func mergeGroupCheckContent() passingAggregateCheckContent {
	return passingAggregateCheckContent{
		operation: "merge_group_check",
		title:     "Schema changes verified before merge queue",
		summary: "Schema changes in queued pull requests are applied and verified by SchemaBot before " +
			"they enter the merge queue, so no additional verification is required for this merge group.",
	}
}

// enqueueDurableMergeGroup persists a merge_group delivery in the inbox. The
// stored TenantID is the resolved installation ID so the driver, which runs
// outside any HTTP request, does not have to re-resolve a repo-level install.
func (h *Handler) enqueueDurableMergeGroup(ctx context.Context, payload mergeGroupPayload, body []byte, deliveryID string, installationID int64) (bool, error) {
	return h.enqueueDurableWebhookEvent(ctx, &storage.WebhookEvent{
		Provider:   storage.WebhookProviderGitHub,
		DeliveryID: deliveryID,
		Event:      "merge_group",
		Action:     payload.Action,
		Repository: storage.CanonicalKey(payload.Repository.FullName),
		HeadSHA:    payload.MergeGroup.HeadSHA,
		TenantID:   strconv.FormatInt(installationID, 10),
		Payload:    body,
	})
}

// processDurableMergeGroup posts the passing merge-queue check for a claimed
// merge_group delivery. It re-validates every enqueue-time guard fail-closed —
// config can change between enqueue and drive, and rows can arrive via replay —
// so a now-ignored delivery completes as a no-op rather than posting a check
// the current config would not. GitHub client and post failures are retryable;
// posting is idempotent (the check is looked up by name and updated in place),
// so a retry after a partial post reconciles rather than duplicates.
func (h *Handler) processDurableMergeGroup(ctx context.Context, event *storage.WebhookEvent) (retry bool, err error) {
	var payload mergeGroupPayload
	if err := decodeDurablePayload(event, &payload); err != nil {
		return false, err
	}

	if payload.Action != "checks_requested" {
		h.logger.Info("durable merge_group delivery ignored because action needs no check",
			"delivery_id", event.DeliveryID, "action", payload.Action, "repo", event.Repository)
		return false, nil
	}

	repo := storage.CanonicalKey(payload.Repository.FullName)
	headSHA := payload.MergeGroup.HeadSHA
	if repo == "" || headSHA == "" {
		return false, fmt.Errorf("durable merge_group delivery %s missing repo or head SHA", event.DeliveryID)
	}

	installationID, err := durableInstallationID(event)
	if err != nil {
		return false, err
	}

	if skip := h.mergeGroupCheckSkipReason(ctx, repo, "delivery_id", event.DeliveryID, "head_sha", headSHA, "installation_id", installationID); skip != "" {
		if skip == mergeGroupSkipUnregistered {
			metrics.RecordUnregisteredRepositoryWebhook(ctx, h.metricAppForRepo(repo), "merge_group", payload.Action)
		}
		return false, nil
	}

	// Bound the GitHub work so a stalled API call cannot be kept alive
	// indefinitely by the lease heartbeat and monopolize a driver. Matches the
	// request path's post budget and the durable auto-plan bootstrap. The driver
	// context stays the parent, so shutdown or lease loss still cancels.
	ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()

	client, err := h.clientForRepo(repo, installationID)
	if err != nil {
		return true, fmt.Errorf("create GitHub client for durable merge_group %s@%s: %w", repo, headSHA, err)
	}

	if err := h.postPassingAggregateChecks(ctx, client, repo, headSHA, mergeGroupCheckContent()); err != nil {
		metrics.RecordStatusCheckOperation(ctx, metrics.StatusCheckOperation{
			Operation:  "merge_group_check",
			Repository: repo,
			Status:     "error",
		})
		return true, fmt.Errorf("post durable merge_group checks for %s@%s: %w", repo, headSHA, err)
	}

	if h.isRepoListenOnly(repo) {
		h.logger.Info("durable merge_group delivery completed without posting checks because the repository is listen-only",
			"delivery_id", event.DeliveryID, "repo", repo, "head_sha", headSHA)
		return false, nil
	}
	h.logger.Info("durable merge_group checks posted",
		"delivery_id", event.DeliveryID, "repo", repo, "head_sha", headSHA)
	return false, nil
}

// Reasons this instance posts no merge-queue check for a repository.
const (
	mergeGroupSkipUnregistered = "skipped: repository is not registered"
	mergeGroupSkipParticipant  = "skipped: aggregate participant; the leader posts the required checks"
	mergeGroupSkipDisabled     = "skipped: Check Run publishing is disabled"
)

// mergeGroupCheckSkipReason reports why this instance posts no merge-queue
// check for repo, or "" when it posts one. Each reason means SchemaBot's check
// is not required on the merge group, so posting nothing cannot wedge the
// queue. attrs identify the caller's work in the logs.
func (h *Handler) mergeGroupCheckSkipReason(ctx context.Context, repo string, attrs ...any) string {
	logAttrs := append([]any{"repo", repo}, attrs...)
	if h.service != nil && !h.service.Config().IsRepoAllowed(repo) {
		h.logger.Warn("merge_group check skipped: repository is not registered", logAttrs...)
		return mergeGroupSkipUnregistered
	}
	if h.isAggregateParticipant(repo) {
		h.logger.Info("merge_group check skipped: aggregate participant stays silent; the leader posts the required checks", logAttrs...)
		metrics.RecordStatusCheckOperation(ctx, metrics.StatusCheckOperation{
			Operation:  "merge_group_check",
			Repository: repo,
			Status:     "skipped",
		})
		return mergeGroupSkipParticipant
	}
	// shouldRunPassingAggregateChecks logs and records its own skip.
	if !h.shouldRunPassingAggregateChecks(ctx, repo, "merge_group_check") {
		return mergeGroupSkipDisabled
	}
	return ""
}

// BackfillMergeQueueCheck posts the passing merge-queue check on headSHA for
// each target in scope that the head is still missing, as the merge_group
// delivery for that merge group would have. A check already on the head is
// left as it is, so the backfill only fills gaps. The caller must have
// verified that headSHA is a merge group the queue is still testing: the
// check passes unconditionally, which is right only for a commit the queue
// built from PRs whose own checks already passed.
func (h *Handler) BackfillMergeQueueCheck(ctx context.Context, repo string, installationID int64, headSHA string, scope api.MergeQueueCheckScope) (string, error) {
	repo = storage.CanonicalKey(repo)
	if skip := h.mergeGroupCheckSkipReason(ctx, repo, "head_sha", headSHA, "installation_id", installationID, "source", "checks.backfill"); skip != "" {
		return skip, nil
	}
	ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	client, err := h.clientForRepo(repo, installationID)
	if err != nil {
		return "", fmt.Errorf("create GitHub client for merge queue check %s@%s: %w", repo, headSHA, err)
	}
	posted, err := h.postMissingMergeQueueChecks(ctx, client, repo, headSHA, scope)
	if err != nil {
		metrics.RecordStatusCheckOperation(ctx, metrics.StatusCheckOperation{
			Operation:  "merge_group_check",
			Repository: repo,
			Status:     "error",
		})
		return "", fmt.Errorf("post merge queue checks for %s@%s: %w", repo, headSHA, err)
	}
	if h.isRepoListenOnly(repo) {
		h.logger.Info("merge queue check backfill posted nothing because the repository is listen-only",
			"repo", repo, "head_sha", headSHA)
		return "skipped: listen-only repository", nil
	}
	if posted == 0 {
		h.logger.Info("merge queue check backfill posted nothing: every check in scope is already on the merge group",
			"repo", repo, "head_sha", headSHA, "environment", scope.Environment, "check_name", scope.CheckName)
		return mergeQueueChecksAlreadyPresent, nil
	}
	h.logger.Info("merge queue check backfilled", "repo", repo, "head_sha", headSHA, "installation_id", installationID, "posted", posted)
	return "posted passing merge queue check", nil
}

// mergeQueueChecksAlreadyPresent is the outcome for a merge group whose checks
// in scope appeared between the scan and the post, for example from a late
// merge_group delivery.
const mergeQueueChecksAlreadyPresent = "skipped: checks already present"

// postMissingMergeQueueChecks creates the passing merge queue check for each
// target in scope that headSHA does not already carry, and returns how many it
// created. A scope that keeps no target is an error: the operator asked for a
// check this server does not post on the merge group, so the group stays
// reported as missing it. A failed lookup is an error too, rather than a
// create, because a backfill that cannot tell whether a check exists must not
// add a second one beside it.
func (h *Handler) postMissingMergeQueueChecks(ctx context.Context, client *ghclient.InstallationClient, repo, headSHA string, scope api.MergeQueueCheckScope) (int, error) {
	content := mergeGroupCheckContent()
	targets, err := h.passingAggregateCheckTargets(ctx, client, repo, headSHA, content.operation)
	if err != nil {
		return 0, err
	}
	var inScope []aggregateCheckTarget
	for _, target := range targets {
		if scope.Environment != "" && target.environment != scope.Environment {
			continue
		}
		if scope.CheckName != "" && target.name != scope.CheckName {
			continue
		}
		inScope = append(inScope, target)
	}
	if len(inScope) == 0 {
		return 0, fmt.Errorf("this server posts no merge queue check matching environment %q and check name %q on %s@%s", scope.Environment, scope.CheckName, repo, headSHA)
	}
	listenOnly := h.isRepoListenOnly(repo)
	posted := 0
	for _, target := range inScope {
		existing, _, err := client.FindCheckRunByName(ctx, repo, headSHA, target.name)
		if err != nil {
			return posted, fmt.Errorf("look up %s check %q on %s@%s: %w", content.operation, target.name, repo, headSHA, err)
		}
		if existing != nil {
			h.logger.Info("merge queue check backfill leaving an existing check as it is",
				"repo", repo, "head_sha", headSHA, "check_name", target.name, "environment", target.environment, "check_run_id", existing.ID)
			continue
		}
		if listenOnly {
			h.logger.Info("skipping passing aggregate Check Run on listen-only repository",
				"repo", repo, "head_sha", headSHA, "check_name", target.name,
				"environment", target.environment, "operation", content.operation)
			continue
		}
		if _, err := client.CreateCheckRun(ctx, repo, headSHA, passingAggregateCheckRunOptions(target, content)); err != nil {
			return posted, fmt.Errorf("create %s check %q on %s@%s: %w", content.operation, target.name, repo, headSHA, err)
		}
		posted++
		metrics.RecordStatusCheckOperation(ctx, metrics.StatusCheckOperation{
			Operation:   content.operation,
			Repository:  repo,
			Environment: target.environment,
			Status:      "success",
		})
		h.logger.Info("posted passing aggregate",
			"repo", repo, "head_sha", headSHA, "check_name", target.name,
			"environment", target.environment, "action", "created",
			"operation", content.operation)
	}
	return posted, nil
}

// passingAggregateCheckContent carries the operation name and Check Run output
// for a passing aggregate published outside the PR-head path (merge-queue
// heads, default-branch pushes).
type passingAggregateCheckContent struct {
	operation string
	title     string
	summary   string
}

// postPassingAggregateChecks publishes a passing aggregate Check Run on
// headSHA for each environment this instance gates, reusing the same check
// names as the PR-head aggregates so branch protection's required checks
// always match.
func (h *Handler) postPassingAggregateChecks(ctx context.Context, client *ghclient.InstallationClient, repo, headSHA string, content passingAggregateCheckContent) error {
	targets, err := h.passingAggregateCheckTargets(ctx, client, repo, headSHA, content.operation)
	if err != nil {
		return err
	}
	listenOnly := h.isRepoListenOnly(repo)
	for _, target := range targets {
		opts := passingAggregateCheckRunOptions(target, content)
		// Reuse an existing run for this name on the SHA so a webhook
		// redelivery updates it rather than creating a duplicate Check Run.
		// The lookup errors when the App slug is unknown; on any lookup error
		// fall back to creating a new run — a duplicate Check Run is the safe
		// outcome, a missing one is not.
		existing, _, findErr := client.FindCheckRunByName(ctx, repo, headSHA, target.name)
		if findErr != nil {
			h.logger.Warn("could not look up existing check; creating a new one",
				"repo", repo, "head_sha", headSHA, "check_name", target.name,
				"operation", content.operation, "error", findErr)
		}
		// The coverage and lookup reads above run as for any repository, so a
		// listen-only repository still shows what this path costs; only the
		// write is skipped.
		if listenOnly {
			h.logger.Info("skipping passing aggregate Check Run on listen-only repository",
				"repo", repo, "head_sha", headSHA, "check_name", target.name,
				"environment", target.environment, "operation", content.operation)
			continue
		}
		action := "created"
		switch {
		case findErr == nil && existing != nil:
			if err := client.UpdateCheckRun(ctx, repo, existing.ID, opts); err != nil {
				return fmt.Errorf("update %s check %q on %s@%s: %w", content.operation, target.name, repo, headSHA, err)
			}
			action = "updated"
		default:
			if _, err := client.CreateCheckRun(ctx, repo, headSHA, opts); err != nil {
				return fmt.Errorf("create %s check %q on %s@%s: %w", content.operation, target.name, repo, headSHA, err)
			}
		}
		metrics.RecordStatusCheckOperation(ctx, metrics.StatusCheckOperation{
			Operation:   content.operation,
			Repository:  repo,
			Environment: target.environment,
			Status:      "success",
		})
		h.logger.Info("posted passing aggregate",
			"repo", repo, "head_sha", headSHA, "check_name", target.name,
			"environment", target.environment, "action", action,
			"operation", content.operation)
	}
	return nil
}

// passingAggregateCheckTargets lists the aggregate checks a passing post on
// headSHA covers: one per environment the head's changes reach when this
// instance gates environments, otherwise the single global aggregate.
func (h *Handler) passingAggregateCheckTargets(ctx context.Context, client *ghclient.InstallationClient, repo, headSHA, operation string) ([]aggregateCheckTarget, error) {
	config, ok := h.serverConfig()
	if !ok || len(config.AllowedEnvironments) == 0 {
		return h.aggregateCheckTargetsForRepo(repo), nil
	}
	// Resolving the environments reads config files at headSHA, which decide
	// which checks to post, so the quota reserve sends those reads.
	ctx = ghclient.WithCheckPublicationReads(ctx)
	environments, err := config.CheckEnvironmentsForRepo(ctx, client, repo, headSHA)
	if err != nil {
		return nil, fmt.Errorf("resolve %s coverage: %w", operation, err)
	}
	targets := make([]aggregateCheckTarget, 0, len(environments))
	for _, env := range environments {
		targets = append(targets, aggregateCheckTarget{name: aggregateCheckNameForEnv(h.aggregateCheckNameForRepo(repo), env), environment: env})
	}
	return targets, nil
}

// passingAggregateCheckRunOptions is the completed, successful Check Run a
// passing post writes for target.
func passingAggregateCheckRunOptions(target aggregateCheckTarget, content passingAggregateCheckContent) ghclient.CheckRunOptions {
	return ghclient.CheckRunOptions{
		Name:       target.name,
		Status:     checkStatusCompleted,
		Conclusion: checkConclusionSuccess,
		Output: &ghclient.CheckRunOutput{
			Title:   content.title,
			Summary: content.summary,
		},
	}
}
