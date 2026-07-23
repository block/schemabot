package webhook

import (
	"context"
	"encoding/json"
	"net/http"

	"github.com/block/schemabot/pkg/metrics"
)

type checkSuitePayload struct {
	Action     string `json:"action"`
	CheckSuite struct {
		ID           int64  `json:"id"`
		HeadSHA      string `json:"head_sha"`
		PullRequests []struct {
			Number int `json:"number"`
		} `json:"pull_requests"`
	} `json:"check_suite"`
	Repository struct {
		FullName string `json:"full_name"`
	} `json:"repository"`
	Installation struct {
		ID int64 `json:"id"`
	} `json:"installation"`
}

// handleCheckSuite routes check_suite deliveries. A rerequested action is the
// GitHub UI's "Re-run all checks" — the only re-run affordance on a merged
// PR — and re-runs the SchemaBot evaluation for the suite's commit. Other
// actions (created, completed) fire on every commit as part of the normal
// check lifecycle and carry no operator intent, so they are acknowledged
// without work.
func (h *Handler) handleCheckSuite(ctx context.Context, metricApp string, w http.ResponseWriter, body []byte, deliveryID string) {
	var payload checkSuitePayload
	if err := json.Unmarshal(body, &payload); err != nil {
		h.writeError(w, http.StatusBadRequest, "invalid check_suite payload")
		return
	}

	switch payload.Action {
	case "rerequested":
		h.handleCheckSuiteRerequest(ctx, metricApp, w, payload, body, deliveryID)
	default:
		h.logger.Debug("check_suite action ignored",
			"action", payload.Action,
			"repo", payload.Repository.FullName,
			"check_suite_id", payload.CheckSuite.ID)
		h.writeJSON(w, http.StatusOK, map[string]string{"message": "check_suite action ignored"})
	}
}

// handleCheckSuiteRerequest accepts a "Re-run all checks" request for this
// App's suite. A suite rerequest covers every check the App owns on the
// commit, so unlike a check_run rerequest there is no per-check name filter:
// the re-plan republishes the full aggregate.
func (h *Handler) handleCheckSuiteRerequest(ctx context.Context, metricApp string, w http.ResponseWriter, payload checkSuitePayload, body []byte, deliveryID string) {
	installationID := h.effectiveInstallationID(ctx, payload.Installation.ID)
	if installationID == 0 {
		h.writeError(w, http.StatusBadRequest, "missing installation ID in webhook payload")
		return
	}

	repo := payload.Repository.FullName
	pr, ok := checkSuitePullRequestNumber(payload)
	if !ok {
		h.logger.Info("check_suite rerequest ignored without pull request",
			"repo", repo,
			"check_suite_id", payload.CheckSuite.ID,
			"head_sha", payload.CheckSuite.HeadSHA,
			"delivery_id", deliveryID)
		h.writeJSON(w, http.StatusOK, map[string]string{"message": "check_suite rerequest ignored without pull request"})
		return
	}

	if h.service != nil && !h.service.Config().IsRepoAllowed(repo) {
		h.logger.Warn("webhook from unregistered repository",
			"event", "check_suite",
			"action", payload.Action,
			"repo", repo,
			"pr", pr,
			"installation_id", installationID,
			"check_suite_id", payload.CheckSuite.ID,
			"delivery_id", deliveryID)
		h.writeJSON(w, http.StatusOK, map[string]string{"message": "repository not registered"})
		return
	}

	if payload.CheckSuite.HeadSHA == "" {
		h.writeError(w, http.StatusBadRequest, "missing check_suite head SHA")
		return
	}

	if h.durableWebhookDispatch {
		// Enqueue failure is a deliberate 500 with no in-process fallback: it
		// fails loudly (metric below + a red delivery in GitHub's webhook UI)
		// rather than silently, but GitHub never auto-retries a check_suite
		// rerequest, so the delivery stays lost until an operator hits
		// "Redeliver" (which re-creates the row, or reopens it if a terminal
		// row exists) or re-triggers the suite.
		inserted, err := h.enqueueDurableCheckSuite(ctx, payload, body, deliveryID, pr, installationID)
		if err != nil {
			h.logger.Error("failed to enqueue durable check_suite rerequest",
				"repo", repo,
				"pr", pr,
				"head_sha", payload.CheckSuite.HeadSHA,
				"installation_id", installationID,
				"check_suite_id", payload.CheckSuite.ID,
				"delivery_id", deliveryID,
				"error", err)
			metrics.RecordWebhookEvent(ctx, metricApp, "check_suite", payload.Action, repo, "durable_enqueue_failed")
			h.writeError(w, http.StatusInternalServerError, "failed to enqueue webhook delivery")
			return
		}
		if !inserted {
			h.logger.Info("durable check_suite rerequest already queued",
				"repo", repo,
				"pr", pr,
				"head_sha", payload.CheckSuite.HeadSHA,
				"installation_id", installationID,
				"check_suite_id", payload.CheckSuite.ID,
				"delivery_id", deliveryID)
			h.writeJSON(w, http.StatusOK, map[string]string{"message": "check_suite rerequest already queued"})
			return
		}
		h.logger.Info("durable check_suite rerequest queued",
			"repo", repo,
			"pr", pr,
			"head_sha", payload.CheckSuite.HeadSHA,
			"installation_id", installationID,
			"check_suite_id", payload.CheckSuite.ID,
			"delivery_id", deliveryID)
		h.writeJSON(w, http.StatusOK, map[string]string{"message": "check_suite rerequest queued"})
		return
	}

	ctx, cancel, client, err := h.autoPlanBootstrap(context.Background(), repo, installationID)
	if err != nil {
		h.logger.Error("failed to bootstrap check_suite rerequest",
			"repo", repo,
			"pr", pr,
			"head_sha", payload.CheckSuite.HeadSHA,
			"installation_id", installationID,
			"check_suite_id", payload.CheckSuite.ID,
			"error", err)
		h.writeError(w, http.StatusInternalServerError, "failed to initialize GitHub client")
		return
	}
	defer cancel()

	if !h.verifyHeadSHAStillCurrentForPR(ctx, client, repo, pr, payload.CheckSuite.HeadSHA, "check_suite_rerequest") {
		h.writeJSON(w, http.StatusOK, map[string]string{"message": "check_suite rerequest ignored for stale head SHA"})
		return
	}

	h.logger.Info("check_suite rerequest triggered auto-plan",
		"repo", repo,
		"pr", pr,
		"head_sha", payload.CheckSuite.HeadSHA,
		"installation_id", installationID,
		"check_suite_id", payload.CheckSuite.ID)

	// Discovery failures are already logged and posted as a failing check
	// inside runAutoPlanForPR; the rerequest path has no retry mechanism.
	message, _ := h.runAutoPlanForPR(ctx, client, repo, pr, payload.CheckSuite.HeadSHA, installationID, "check_suite.rerequested", "check_suite.rerequested", "", "")

	h.writeJSON(w, http.StatusOK, map[string]string{"message": message})
}

func checkSuitePullRequestNumber(payload checkSuitePayload) (int, bool) {
	if len(payload.CheckSuite.PullRequests) == 0 {
		return 0, false
	}
	pr := payload.CheckSuite.PullRequests[0].Number
	return pr, pr != 0
}
