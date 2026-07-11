package webhook

import (
	"context"
	"encoding/json"
	"net/http"
)

type checkRunPayload struct {
	Action   string `json:"action"`
	CheckRun struct {
		ID           int64  `json:"id"`
		Name         string `json:"name"`
		Status       string `json:"status"`
		Conclusion   string `json:"conclusion"`
		HeadSHA      string `json:"head_sha"`
		PullRequests []struct {
			Number int `json:"number"`
		} `json:"pull_requests"`
	} `json:"check_run"`
	Repository struct {
		FullName string `json:"full_name"`
	} `json:"repository"`
	Installation struct {
		ID int64 `json:"id"`
	} `json:"installation"`
}

func (h *Handler) handleCheckRun(ctx context.Context, w http.ResponseWriter, body []byte) {
	var payload checkRunPayload
	if err := json.Unmarshal(body, &payload); err != nil {
		h.writeError(w, http.StatusBadRequest, "invalid check_run payload")
		return
	}

	switch payload.Action {
	case "rerequested":
		h.handleCheckRunRerequest(ctx, w, payload)
	case "completed":
		h.handleParticipantCheckCompleted(ctx, w, payload)
	default:
		h.logger.Debug("check_run action ignored",
			"action", payload.Action,
			"repo", payload.Repository.FullName,
			"check_run_id", payload.CheckRun.ID,
			"check_name", payload.CheckRun.Name)
		h.writeJSON(w, http.StatusOK, map[string]string{"message": "check_run action ignored"})
	}
}

// handleParticipantCheckCompleted re-folds the aggregate leader's check when a
// participant deployment's Check Run completes on a repo this deployment leads.
// The completed payload only triggers the re-fold; updateAggregateCheck re-reads
// every participant's state via the trusted FindCheckRunByName path, so reacting
// to any completed check on a led repo is safe. The leader's own aggregate check
// is skipped so its own writes do not trigger a self-loop.
func (h *Handler) handleParticipantCheckCompleted(ctx context.Context, w http.ResponseWriter, payload checkRunPayload) {
	repo := payload.Repository.FullName

	if h.service == nil || !h.service.Config().IsAggregateLeaderForRepo(repo) {
		h.logger.Debug("check_run completion ignored because deployment is not the aggregate leader",
			"repo", repo, "check_run_id", payload.CheckRun.ID, "check_name", payload.CheckRun.Name)
		h.writeJSON(w, http.StatusOK, map[string]string{"message": "check_run completion ignored for non-leader repo"})
		return
	}

	if h.isSchemaBotAggregateCheckName(repo, payload.CheckRun.Name) {
		h.logger.Debug("check_run completion ignored because it is the leader's own aggregate check",
			"repo", repo, "check_run_id", payload.CheckRun.ID, "check_name", payload.CheckRun.Name)
		h.writeJSON(w, http.StatusOK, map[string]string{"message": "check_run completion ignored for own aggregate check"})
		return
	}

	if !h.service.Config().IsRepoAllowed(repo) {
		h.logger.Warn("webhook from unregistered repository",
			"event", "check_run", "action", payload.Action, "repo", repo,
			"check_run_id", payload.CheckRun.ID, "check_name", payload.CheckRun.Name)
		h.writeJSON(w, http.StatusOK, map[string]string{"message": "repository not registered"})
		return
	}

	installationID := h.effectiveInstallationID(ctx, payload.Installation.ID)
	if installationID == 0 {
		h.writeError(w, http.StatusBadRequest, "missing installation ID in webhook payload")
		return
	}

	pr, ok := checkRunPullRequestNumber(payload)
	if !ok {
		h.logger.Info("participant check completion ignored without pull request",
			"repo", repo, "check_run_id", payload.CheckRun.ID,
			"check_name", payload.CheckRun.Name, "head_sha", payload.CheckRun.HeadSHA)
		h.writeJSON(w, http.StatusOK, map[string]string{"message": "check_run completion ignored without pull request"})
		return
	}

	if payload.CheckRun.HeadSHA == "" {
		h.writeError(w, http.StatusBadRequest, "missing check_run head SHA")
		return
	}

	ctx, cancel, client, err := h.autoPlanBootstrap(repo, installationID)
	if err != nil {
		h.logger.Error("failed to bootstrap participant check completion re-fold",
			"repo", repo, "pr", pr, "head_sha", payload.CheckRun.HeadSHA,
			"installation_id", installationID, "check_run_id", payload.CheckRun.ID,
			"check_name", payload.CheckRun.Name, "error", err)
		h.writeError(w, http.StatusInternalServerError, "failed to initialize GitHub client")
		return
	}
	defer cancel()

	h.logger.Info("participant check completion triggered aggregate re-fold",
		"repo", repo, "pr", pr, "head_sha", payload.CheckRun.HeadSHA,
		"installation_id", installationID, "check_run_id", payload.CheckRun.ID,
		"check_name", payload.CheckRun.Name,
		"check_status", payload.CheckRun.Status, "check_conclusion", payload.CheckRun.Conclusion)

	h.updateAggregateCheck(ctx, client, repo, pr, payload.CheckRun.HeadSHA)

	h.writeJSON(w, http.StatusOK, map[string]string{"message": "aggregate re-folded on participant check completion"})
}

func (h *Handler) handleCheckRunRerequest(ctx context.Context, w http.ResponseWriter, payload checkRunPayload) {
	installationID := h.effectiveInstallationID(ctx, payload.Installation.ID)
	if installationID == 0 {
		h.writeError(w, http.StatusBadRequest, "missing installation ID in webhook payload")
		return
	}

	repo := payload.Repository.FullName
	pr, ok := checkRunPullRequestNumber(payload)
	if !ok {
		h.logger.Info("check_run rerequest ignored without pull request",
			"repo", repo,
			"check_run_id", payload.CheckRun.ID,
			"check_name", payload.CheckRun.Name,
			"head_sha", payload.CheckRun.HeadSHA)
		h.writeJSON(w, http.StatusOK, map[string]string{"message": "check_run rerequest ignored without pull request"})
		return
	}

	if h.service != nil && !h.service.Config().IsRepoAllowed(repo) {
		h.logger.Warn("webhook from unregistered repository",
			"event", "check_run",
			"action", payload.Action,
			"repo", repo,
			"pr", pr,
			"installation_id", installationID,
			"check_run_id", payload.CheckRun.ID,
			"check_name", payload.CheckRun.Name)
		h.writeJSON(w, http.StatusOK, map[string]string{"message": "repository not registered"})
		return
	}

	if !h.isSchemaBotAggregateCheckName(repo, payload.CheckRun.Name) {
		h.logger.Info("check_run rerequest ignored for non-SchemaBot check",
			"repo", repo,
			"pr", pr,
			"check_run_id", payload.CheckRun.ID,
			"check_name", payload.CheckRun.Name,
			"head_sha", payload.CheckRun.HeadSHA)
		h.writeJSON(w, http.StatusOK, map[string]string{"message": "check_run rerequest ignored for non-SchemaBot check"})
		return
	}

	if payload.CheckRun.HeadSHA == "" {
		h.writeError(w, http.StatusBadRequest, "missing check_run head SHA")
		return
	}

	ctx, cancel, client, err := h.autoPlanBootstrap(repo, installationID)
	if err != nil {
		h.logger.Error("failed to bootstrap check_run rerequest",
			"repo", repo,
			"pr", pr,
			"head_sha", payload.CheckRun.HeadSHA,
			"installation_id", installationID,
			"check_run_id", payload.CheckRun.ID,
			"check_name", payload.CheckRun.Name,
			"error", err)
		h.writeError(w, http.StatusInternalServerError, "failed to initialize GitHub client")
		return
	}
	defer cancel()

	if !h.verifyHeadSHAStillCurrentForPR(ctx, client, repo, pr, payload.CheckRun.HeadSHA, "check_run_rerequest") {
		h.writeJSON(w, http.StatusOK, map[string]string{"message": "check_run rerequest ignored for stale head SHA"})
		return
	}

	h.logger.Info("check_run rerequest triggered auto-plan",
		"repo", repo,
		"pr", pr,
		"head_sha", payload.CheckRun.HeadSHA,
		"installation_id", installationID,
		"check_run_id", payload.CheckRun.ID,
		"check_name", payload.CheckRun.Name)

	message := h.runAutoPlanForPR(ctx, client, repo, pr, payload.CheckRun.HeadSHA, installationID, "check_run.rerequested", "check_run.rerequested", "", "")

	h.writeJSON(w, http.StatusOK, map[string]string{"message": message})
}

func checkRunPullRequestNumber(payload checkRunPayload) (int, bool) {
	if len(payload.CheckRun.PullRequests) == 0 {
		return 0, false
	}
	pr := payload.CheckRun.PullRequests[0].Number
	return pr, pr != 0
}

func (h *Handler) isSchemaBotAggregateCheckName(repo string, checkName string) bool {
	baseName := h.aggregateCheckNameForRepo(repo)
	if checkName == baseName {
		return true
	}
	config, ok := h.serverConfig()
	if !ok {
		return false
	}
	for _, env := range config.AllowedEnvironments {
		if checkName == aggregateCheckNameForEnv(baseName, env) {
			return true
		}
	}
	return false
}
