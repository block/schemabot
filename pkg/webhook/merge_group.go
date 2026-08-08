package webhook

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"regexp"
	"strconv"
	"strings"
	"time"

	ghclient "github.com/block/schemabot/pkg/github"
	"github.com/block/schemabot/pkg/metrics"
	"github.com/block/schemabot/pkg/storage"
	"github.com/block/schemabot/pkg/webhook/templates"
)

// mergeGroupPayload is the subset of the GitHub merge_group webhook payload
// SchemaBot needs. GitHub fires this event when a pull request enters or leaves
// a repository's merge queue.
type mergeGroupPayload struct {
	Action     string `json:"action"`
	MergeGroup struct {
		HeadSHA string `json:"head_sha"`
		HeadRef string `json:"head_ref"`
	} `json:"merge_group"`
	Repository struct {
		FullName string `json:"full_name"`
	} `json:"repository"`
	Installation struct {
		ID int64 `json:"id"`
	} `json:"installation"`
}

// handleMergeGroup responds to merge_group webhook events so SchemaBot's
// required Check Runs gate a repository's merge queue without wedging it.
//
// A merge queue tests queued pull requests combined, on a synthetic head
// commit, before they land on the base branch. Branch protection re-evaluates
// the same required checks against that merge-group head SHA — but SchemaBot
// only ever publishes its checks on PR head SHAs, so without this handler the
// required SchemaBot check would never appear on the merge-group commit and the
// queue entry would block indefinitely.
//
// The check posted here is a merge-time revalidation, not an unconditional
// pass. The PR-head check gated queue entry, but stored check state can turn
// blocking after entry — most importantly when a sibling PR's apply starts
// changing a target this PR's verdict was computed against and the preflight
// holds this PR's stored checks. The queue no longer looks at the PR head, so
// this admission check is the only surface where that hold can stop the
// queued merge: it re-folds the PR's stored check state and passes only when
// nothing blocks.
func (h *Handler) handleMergeGroup(ctx context.Context, metricApp string, w http.ResponseWriter, body []byte, deliveryID string) {
	var payload mergeGroupPayload
	if err := json.Unmarshal(body, &payload); err != nil {
		h.writeError(w, http.StatusBadRequest, "invalid merge_group payload")
		return
	}

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
		metrics.RecordUnregisteredRepositoryWebhook(ctx, metricApp, "merge_group", payload.Action, repo)
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
	if !h.shouldPublishChecks(ctx, repo, "merge_group_check") {
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

	if err := h.postMergeGroupAdmissionChecks(postCtx, client, repo, headSHA, payload.MergeGroup.HeadRef); err != nil {
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

	h.writeJSON(w, http.StatusOK, map[string]string{"message": "merge_group checks posted"})
}

// mergeGroupHeadRefPattern matches the tail segment of a merge-queue branch
// ref, refs/heads/gh-readonly-queue/<base branch>/pr-<number>-<sha>. Only the
// final path segment is matched so a base branch containing slashes cannot
// shift the pull request number.
var mergeGroupHeadRefPattern = regexp.MustCompile(`^pr-(\d+)-`)

// mergeGroupPRNumber extracts the queued pull request's number from the
// merge_group head ref. Each queue entry gets its own merge_group event whose
// head ref names that entry's PR, so the ref identifies which PR's stored
// check state this admission check must re-fold.
func mergeGroupPRNumber(headRef string) (int, error) {
	tail := headRef
	if idx := strings.LastIndex(headRef, "/"); idx >= 0 {
		tail = headRef[idx+1:]
	}
	m := mergeGroupHeadRefPattern.FindStringSubmatch(tail)
	if m == nil {
		return 0, fmt.Errorf("merge group head ref %q does not name a pull request", headRef)
	}
	pr, err := strconv.Atoi(m[1])
	if err != nil {
		return 0, fmt.Errorf("parse pull request number from merge group head ref %q: %w", headRef, err)
	}
	return pr, nil
}

// mergeGroupPassContent is the passing Check Run content published on a
// merge-queue head when the queued PR's stored check state blocks nothing.
// Both the request path and the durable driver use it so a redelivery updates
// the same check with identical output.
func mergeGroupPassContent() passingAggregateCheckContent {
	return passingAggregateCheckContent{
		operation: "merge_group_check",
		title:     "Schema changes verified before merge queue",
		summary: "Schema changes in queued pull requests are applied and verified by SchemaBot before " +
			"they enter the merge queue, and nothing is blocking this pull request's schema checks now, " +
			"so no additional verification is required for this merge group.",
	}
}

// mergeGroupBlockedSummary is the Check Run output published on a merge-queue
// head when the queued PR's stored check state blocks admission. The blocked
// PR leaves the queue; the summary tells its author where to look and when to
// re-queue.
const (
	mergeGroupBlockedTitle   = "Schema checks are blocking this pull request"
	mergeGroupBlockedSummary = "This pull request's SchemaBot check state turned blocking after it entered the " +
		"merge queue — most often because another change's apply is in flight on a database this pull request " +
		"also changes, which invalidates the verdict it queued with. Check this pull request's SchemaBot check " +
		"for the reason, and re-queue once it is green again (held checks re-plan automatically when the " +
		"in-flight apply settles)."
)

// mergeGroupUnidentifiedSummary is the fail-closed Check Run output published
// when the queued PR cannot be identified from the merge group ref. Stored
// check state cannot be consulted for a PR that cannot be named, and admission
// must never pass on uncertainty.
const (
	mergeGroupUnidentifiedTitle   = "SchemaBot could not identify the queued pull request"
	mergeGroupUnidentifiedSummary = "SchemaBot could not determine which pull request this merge group tests, " +
		"so it cannot verify the pull request's schema check state and fails closed. See the server logs for " +
		"the merge group ref that could not be parsed."
)

// postMergeGroupAdmissionChecks publishes the admission Check Runs on a
// merge-queue head: a re-fold of the queued PR's stored check state at
// admission time. Every aggregate target this instance gates gets a run —
// passing when nothing in the PR's stored state blocks, action_required when
// something does (a preflight hold, a failed apply, a fail-closed re-plan) or
// when the queued PR cannot be identified. A storage read failure returns an
// error so the delivery retries; uncertainty never admits a merge.
func (h *Handler) postMergeGroupAdmissionChecks(ctx context.Context, client *ghclient.InstallationClient, repo, headSHA, headRef string) error {
	pr, prErr := mergeGroupPRNumber(headRef)
	if prErr != nil {
		h.logger.Error("merge group admission failing closed: queued pull request could not be identified; the admission check will block this merge group",
			"repo", repo, "head_sha", headSHA, "head_ref", headRef, "error", prErr)
		return h.postAggregateChecks(ctx, client, repo, headSHA, passingAggregateCheckContent{
			operation: "merge_group_check",
			title:     mergeGroupUnidentifiedTitle,
			summary:   mergeGroupUnidentifiedSummary,
		}, checkConclusionActionRequired, func(environment string) {
			metrics.RecordMergeGroupAdmissionBlocked(ctx, repo, environment)
		})
	}

	if h.service == nil || h.service.Storage() == nil {
		return fmt.Errorf("storage unavailable for merge group admission of %s#%d@%s", repo, pr, headSHA)
	}
	checks, err := h.service.Storage().Checks().GetByPR(ctx, repo, pr)
	if err != nil {
		return fmt.Errorf("load stored check state for merge group admission of %s#%d@%s: %w", repo, pr, headSHA, err)
	}

	var blockingRows []*storage.Check
	for _, target := range h.aggregateCheckTargetsForRepo(repo) {
		if !hasBlockingCheckForEnvironment(checks, target.environment) {
			if err := h.postAggregateCheck(ctx, client, repo, headSHA, target, mergeGroupPassContent(), checkConclusionSuccess); err != nil {
				return err
			}
			continue
		}
		blockingRows = append(blockingRows, blockingChecksForEnvironment(checks, target.environment)...)
		h.logger.Warn("merge group admission blocked: the queued pull request's stored check state turned blocking after queue entry; the admission check will remove it from the queue",
			"repo", repo, "pr", pr, "head_sha", headSHA,
			"environment", target.environment, "check_name", target.name)
		metrics.RecordMergeGroupAdmissionBlocked(ctx, repo, target.environment)
		if err := h.postAggregateCheck(ctx, client, repo, headSHA, target, passingAggregateCheckContent{
			operation: "merge_group_check",
			title:     mergeGroupBlockedTitle,
			summary:   mergeGroupBlockedSummary,
		}, checkConclusionActionRequired); err != nil {
			return err
		}
	}
	if len(blockingRows) == 0 {
		h.logger.Info("merge group admission passed: nothing in the queued pull request's stored check state blocks",
			"repo", repo, "pr", pr, "head_sha", headSHA)
		return nil
	}
	return h.ensureMergeQueueEjectedComment(ctx, client, repo, pr, headSHA, blockingRows)
}

// mergeQueueEjectedCommentMarker makes the ejection comment idempotent per
// queue attempt: the merge-group head SHA identifies one queue entry, so a
// webhook redelivery finds the marker and skips the re-post, while a later
// re-queue produces a new merge-group commit and gets a fresh comment at the
// bottom of the PR timeline. The SHA comes from GitHub's payload, not user
// input, so the marker needs no sanitization.
func mergeQueueEjectedCommentMarker(mergeGroupHeadSHA string) string {
	return fmt.Sprintf("<!-- schemabot:merge-queue-ejected:%s -->", mergeGroupHeadSHA)
}

// ensureMergeQueueEjectedComment posts the guidance comment on a pull request
// the admission check just removed from the merge queue. The blocking Check
// Run lives on the synthetic merge-group commit — invisible from the PR page —
// and the queue never re-adds a pull request on its own, so without this
// comment the author sees their merge silently vanish with no next step. The
// comment is part of the admission contract: a posting failure returns an
// error so the delivery retries (the already-posted checks reconcile
// idempotently on the retry).
func (h *Handler) ensureMergeQueueEjectedComment(ctx context.Context, client *ghclient.InstallationClient, repo string, pr int, mergeGroupHeadSHA string, blocking []*storage.Check) error {
	marker := mergeQueueEjectedCommentMarker(mergeGroupHeadSHA)
	exists, err := client.HasIssueCommentWithMarker(ctx, repo, pr, marker)
	if err != nil {
		return fmt.Errorf("search for existing merge-queue ejection comment on %s#%d (merge group %s): %w",
			repo, pr, mergeGroupHeadSHA, err)
	}
	if exists {
		h.logger.Debug("merge-queue ejection comment already posted",
			"repo", repo, "pr", pr, "merge_group_head_sha", mergeGroupHeadSHA)
		return nil
	}

	data := templates.MergeQueueEjectedData{}
	seen := make(map[string]bool, len(blocking))
	for _, c := range blocking {
		key := c.DatabaseName + "\x00" + c.Environment
		if c.DatabaseName == "" || seen[key] {
			continue
		}
		seen[key] = true
		data.Blocking = append(data.Blocking, templates.MergeQueueBlockedTarget{
			Database:    c.DatabaseName,
			Environment: c.Environment,
		})
	}
	body := h.renderPRComment(templates.RenderMergeQueueEjected(data)) + "\n" + marker
	if _, _, err := client.CreateIssueComment(ctx, repo, pr, body); err != nil {
		return fmt.Errorf("post merge-queue ejection comment on %s#%d (merge group %s): %w",
			repo, pr, mergeGroupHeadSHA, err)
	}
	h.logger.Info("merge-queue ejection comment posted",
		"repo", repo, "pr", pr, "merge_group_head_sha", mergeGroupHeadSHA,
		"blocking_targets", len(data.Blocking))
	return nil
}

// enqueueDurableMergeGroup persists a merge_group delivery in the inbox. The
// stored TenantID is the resolved installation ID so the driver, which runs
// outside any HTTP request, does not have to re-resolve a repo-level install.
func (h *Handler) enqueueDurableMergeGroup(ctx context.Context, payload mergeGroupPayload, body []byte, deliveryID string, installationID int64) (bool, error) {
	return h.enqueueDurableWebhookEvent(ctx, &storage.WebhookEvent{
		Provider:   storage.ProviderGitHub,
		DeliveryID: deliveryID,
		Event:      "merge_group",
		Action:     payload.Action,
		Repository: payload.Repository.FullName,
		HeadSHA:    payload.MergeGroup.HeadSHA,
		TenantID:   strconv.FormatInt(installationID, 10),
		Payload:    body,
	})
}

// processDurableMergeGroup posts the merge-queue admission check for a claimed
// merge_group delivery. It re-validates every enqueue-time guard fail-closed —
// config can change between enqueue and drive, and rows can arrive via replay —
// so a now-ignored delivery completes as a no-op rather than posting a check
// the current config would not. GitHub client, storage, and post failures are
// retryable; posting is idempotent (the check is looked up by name and updated
// in place), so a retry after a partial post reconciles rather than duplicates.
func (h *Handler) processDurableMergeGroup(ctx context.Context, event *storage.WebhookEvent) (retry bool, err error) {
	var payload mergeGroupPayload
	if err := json.Unmarshal(event.Payload, &payload); err != nil {
		return false, fmt.Errorf("decode durable merge_group delivery %s: %w", event.DeliveryID, err)
	}

	if payload.Action != "checks_requested" {
		h.logger.Info("durable merge_group delivery ignored because action needs no check",
			"delivery_id", event.DeliveryID, "action", payload.Action, "repo", event.Repository)
		return false, nil
	}

	repo := payload.Repository.FullName
	headSHA := payload.MergeGroup.HeadSHA
	if repo == "" || headSHA == "" {
		return false, fmt.Errorf("durable merge_group delivery %s missing repo or head SHA", event.DeliveryID)
	}

	installationID := h.durableInstallationID(ctx, event, payload.Installation.ID)
	if installationID == 0 {
		return false, fmt.Errorf("durable merge_group delivery %s missing installation ID", event.DeliveryID)
	}

	if h.service != nil && !h.service.Config().IsRepoAllowed(repo) {
		h.logger.Warn("durable merge_group delivery from unregistered repository",
			"delivery_id", event.DeliveryID, "repo", repo, "head_sha", headSHA, "installation_id", installationID)
		metrics.RecordUnregisteredRepositoryWebhook(ctx, h.metricAppForRepo(repo), "merge_group", payload.Action, repo)
		return false, nil
	}

	if h.isAggregateParticipant(repo) {
		h.logger.Info("durable merge_group delivery skipped: aggregate participant stays silent; the leader posts the required checks",
			"delivery_id", event.DeliveryID, "repo", repo, "head_sha", headSHA, "installation_id", installationID)
		metrics.RecordStatusCheckOperation(ctx, metrics.StatusCheckOperation{
			Operation:  "merge_group_check",
			Repository: repo,
			Status:     "skipped",
		})
		return false, nil
	}

	if !h.shouldPublishChecks(ctx, repo, "merge_group_check") {
		// shouldPublishChecks logs and records the skip.
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

	if err := h.postMergeGroupAdmissionChecks(ctx, client, repo, headSHA, payload.MergeGroup.HeadRef); err != nil {
		metrics.RecordStatusCheckOperation(ctx, metrics.StatusCheckOperation{
			Operation:  "merge_group_check",
			Repository: repo,
			Status:     "error",
		})
		return true, fmt.Errorf("post durable merge_group checks for %s@%s: %w", repo, headSHA, err)
	}

	h.logger.Info("durable merge_group checks posted",
		"delivery_id", event.DeliveryID, "repo", repo, "head_sha", headSHA)
	return false, nil
}

// passingAggregateCheckContent carries the operation name and Check Run output
// for an aggregate published outside the PR-head path (merge-queue heads,
// default-branch pushes).
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
	return h.postAggregateChecks(ctx, client, repo, headSHA, content, checkConclusionSuccess, nil)
}

// postAggregateChecks publishes one aggregate Check Run per gated environment
// on headSHA with the given conclusion. onEach, when non-nil, runs once per
// environment before its check posts (for per-environment metrics).
func (h *Handler) postAggregateChecks(ctx context.Context, client *ghclient.InstallationClient, repo, headSHA string, content passingAggregateCheckContent, conclusion string, onEach func(environment string)) error {
	for _, target := range h.aggregateCheckTargetsForRepo(repo) {
		if onEach != nil {
			onEach(target.environment)
		}
		if err := h.postAggregateCheck(ctx, client, repo, headSHA, target, content, conclusion); err != nil {
			return err
		}
	}
	return nil
}

// postAggregateCheck publishes a single aggregate Check Run on headSHA for one
// aggregate target with the given conclusion, reusing the PR-head aggregate's
// check name so branch protection's required-check names always match.
func (h *Handler) postAggregateCheck(ctx context.Context, client *ghclient.InstallationClient, repo, headSHA string, target aggregateCheckTarget, content passingAggregateCheckContent, conclusion string) error {
	opts := ghclient.CheckRunOptions{
		Name:       target.name,
		Status:     checkStatusCompleted,
		Conclusion: conclusion,
		Output: &ghclient.CheckRunOutput{
			Title:   content.title,
			Summary: content.summary,
		},
	}
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
	switch {
	case findErr == nil && existing != nil:
		if err := client.UpdateCheckRun(ctx, repo, existing.ID, opts); err != nil {
			return fmt.Errorf("update %s check %q on %s@%s: %w", content.operation, target.name, repo, headSHA, err)
		}
	default:
		if _, err := client.CreateCheckRun(ctx, repo, headSHA, opts); err != nil {
			return fmt.Errorf("create %s check %q on %s@%s: %w", content.operation, target.name, repo, headSHA, err)
		}
	}
	metrics.RecordStatusCheckOperation(ctx, metrics.StatusCheckOperation{
		Operation:   content.operation,
		Repository:  repo,
		Environment: target.environment,
		Status:      conclusion,
	})
	h.logger.Info("aggregate check posted",
		"repo", repo, "head_sha", headSHA, "check_name", target.name,
		"environment", target.environment, "operation", content.operation,
		"conclusion", conclusion)
	return nil
}
