package webhook

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/block/schemabot/pkg/api"
	ghclient "github.com/block/schemabot/pkg/github"
	"github.com/block/schemabot/pkg/storage"
	gh "github.com/google/go-github/v86/github"
)

type prSchemaLabelAck struct {
	Revision        prSchemaRevision
	Label           string
	EventID         int64
	RevisionEventID int64
}

func (h *Handler) evaluatePRSchemaLabel(ctx context.Context, client *ghclient.InstallationClient, repo string, pr int, info *ghclient.PullRequestInfo, label string) (string, error) {
	timeline, err := client.FetchPRSchemaEvents(ctx, repo, pr, label)
	if err != nil {
		return "", err
	}
	_, ack, err := h.readPRSchemaLabel(ctx, repo, pr)
	if err != nil {
		return "", err
	}
	if !timeline.LabelPresent || timeline.LabelEventID == 0 {
		return "", nil
	}
	sameLabelEvent := ack.Label == label && ack.EventID == timeline.LabelEventID
	if sameLabelEvent && ack.Revision == schemaRevision(info) && ack.RevisionEventID == timeline.RevisionEventID && timeline.LabelEventID > timeline.RevisionEventID {
		return "label:" + strconv.FormatInt(ack.EventID, 10), nil
	}
	if sameLabelEvent || timeline.LabelEventID < timeline.RevisionEventID {
		h.removeStalePRSchemaLabel(ctx, client, repo, pr, info, label, timeline)
	}
	return "", nil
}

func (h *Handler) readPRSchemaLabel(ctx context.Context, repo string, pr int) (*storage.Setting, prSchemaLabelAck, error) {
	row, err := h.service.Storage().Settings().Get(ctx, schemaCapKey("label", repo, pr))
	var ack prSchemaLabelAck
	if err == nil && row != nil {
		err = json.Unmarshal([]byte(row.Value), &ack)
	}
	return row, ack, err
}

func (h *Handler) recordPRSchemaLabel(ctx context.Context, client *ghclient.InstallationClient, repo string, pr int, info *ghclient.PullRequestInfo, label string, payload pullRequestPayload, received time.Time) (bool, error) {
	revision := schemaRevision(info)
	if !info.HasLabel(label) || payload.PullRequest.Head.SHA != revision.Head ||
		payload.PullRequest.Base.SHA != revision.Base || payload.PullRequest.Base.Ref != revision.BaseRef {
		return false, nil
	}
	timeline, err := client.FetchPRSchemaEvents(ctx, repo, pr, label)
	if err != nil {
		return false, err
	}
	// GitHub does not include the timeline event ID in a label webhook.
	// Match its signed actor and revision and bound the action time by the
	// webhook snapshot and its original receipt, including on a replay.
	if !timeline.LabelPresent || timeline.LabelEventID <= timeline.RevisionEventID ||
		timeline.LabelActor == "" || !strings.EqualFold(payload.Sender.Login, timeline.LabelActor) ||
		payload.PullRequest.UpdatedAt.IsZero() || timeline.LabelChangedAt.IsZero() ||
		timeline.LabelChangedAt.Before(payload.PullRequest.UpdatedAt) || timeline.LabelChangedAt.After(received) {
		return false, nil
	}
	ack := prSchemaLabelAck{revision, label, timeline.LabelEventID, timeline.RevisionEventID}
	encoded, err := json.Marshal(ack)
	if err != nil {
		return false, err
	}
	settings := h.service.Storage().Settings()
	for {
		row, previous, err := h.readPRSchemaLabel(ctx, repo, pr)
		if err != nil {
			return false, err
		}
		if previous.EventID >= ack.EventID {
			return previous == ack, nil
		}
		saved, err := settings.CompareAndSet(ctx, schemaCapKey("label", repo, pr), row, string(encoded))
		if err != nil || saved {
			return saved, err
		}
	}
}

func (h *Handler) removeStalePRSchemaLabel(ctx context.Context, client *ghclient.InstallationClient, repo string, pr int, info *ghclient.PullRequestInfo, label string, timeline ghclient.PRSchemaEvents) {
	policy := h.config().PRSchemaCapForRepo(repo)
	manage := !h.isAggregateParticipant(repo)
	if policy.ManageLabel != nil {
		manage = *policy.ManageLabel
	}
	if !manage {
		return
	}

	// One writer per old action, even across replicas of the designated
	// control plane. A second DELETE could remove a label just re-added.
	key := schemaCapKey("stale-label", repo, pr, label, strconv.FormatInt(timeline.LabelEventID, 10))
	settings := h.service.Storage().Settings()
	claimed, err := settings.CompareAndSet(ctx, key, nil, "claimed")
	if err != nil || !claimed {
		return
	}
	fresh, err := client.FetchPullRequestNoCache(ctx, repo, pr)
	if err != nil || !fresh.IsOpen() || !fresh.HasLabel(label) || schemaRevision(fresh) != schemaRevision(info) {
		return
	}
	latest, err := client.FetchPRSchemaEvents(ctx, repo, pr, label)
	if err != nil || latest != timeline {
		return
	}
	if err := client.RemovePRLabel(ctx, repo, pr, label); err != nil {
		h.logger.Warn("could not remove stale PR schema limit label", "repo", repo, "pr", pr, "label", label, "error", err)
	}
}

func (h *Handler) evaluatePRSchemaReview(ctx context.Context, client *ghclient.InstallationClient, repo string, pr int, info *ghclient.PullRequestInfo, policy api.PRSchemaCapConfig, request bool) (string, string, error) {
	org, slug, _ := strings.Cut(policy.ReviewTeam, "/")
	revision, err := json.Marshal(schemaRevision(info))
	if err != nil {
		return "", "", err
	}
	var timeline ghclient.PRSchemaEvents
	var approved bool
	reviewClient, verifyErr := h.prSchemaReviewClient(ctx, client, repo, policy.ReviewApp)
	if verifyErr == nil {
		timeline, approved, verifyErr = effectivePRSchemaReview(ctx, reviewClient, repo, pr, info, org, slug)
	}
	cycleKey := schemaCapKey("review-cycle", repo, pr, policy.ReviewTeam, string(revision))
	cycle, err := h.updatePRSchemaReviewCycle(ctx, cycleKey, approved)
	if err := errors.Join(err, verifyErr); err != nil {
		return "", "", err
	}
	if approved {
		return fmt.Sprintf("review:%d:%d", timeline.RevisionEventID, cycle), "", nil
	}
	if !request || !policy.RequestTeamReview || info.Draft || info.HasRequestedTeam(slug) {
		return "", "", nil
	}
	key := schemaCapKey("review-request", repo, pr, policy.ReviewTeam, string(revision),
		strconv.FormatInt(timeline.RevisionEventID, 10), strconv.FormatUint(cycle, 10))
	notice, err := h.requestPRSchemaReview(ctx, reviewClient, repo, pr, slug, key, schemaRevision(info))
	return "", notice, err
}

func (h *Handler) prSchemaReviewClient(ctx context.Context, client *ghclient.InstallationClient, repo, app string) (*ghclient.InstallationClient, error) {
	if app == "" {
		return client, nil
	}
	factory, err := h.ghClients.For(app)
	if err != nil {
		return nil, err
	}
	installation, err := factory.InstallationIDForRepo(ctx, repo)
	if err != nil {
		return nil, err
	}
	return factory.ForInstallation(installation)
}

func effectivePRSchemaReview(ctx context.Context, client *ghclient.InstallationClient, repo string, pr int, info *ghclient.PullRequestInfo, org, slug string) (ghclient.PRSchemaEvents, bool, error) {
	if info.User == "" || info.HeadSHA == "" {
		return ghclient.PRSchemaEvents{}, false, fmt.Errorf("GitHub did not supply the PR author or head")
	}
	reviews, err := client.ListReviews(ctx, repo, pr)
	if err != nil {
		return ghclient.PRSchemaEvents{}, false, err
	}
	timeline, err := client.FetchPRSchemaEvents(ctx, repo, pr, "")
	if err != nil {
		return ghclient.PRSchemaEvents{}, false, err
	}
	var membershipErr error
	for _, review := range ghclient.GetApprovedReviews(reviews) {
		if review.CommitID != info.HeadSHA || strings.EqualFold(info.User, review.User) ||
			!review.SubmittedAt.After(timeline.RetargetedAt) {
			continue
		}
		member, err := client.IsTeamMember(ctx, org, slug, review.User)
		if err != nil {
			membershipErr = err
			continue
		}
		if member {
			return timeline, true, nil
		}
	}
	return timeline, false, membershipErr
}

type prSchemaReviewCycle struct {
	Granted bool
	Number  uint64
}

// The grant stays stable while any qualifying approval remains. A fresh
// approval after revocation starts a new planning and review-request cycle.
func (h *Handler) updatePRSchemaReviewCycle(ctx context.Context, key string, granted bool) (uint64, error) {
	settings := h.service.Storage().Settings()
	for {
		row, err := settings.Get(ctx, key)
		if err != nil {
			return 0, err
		}
		cycle := prSchemaReviewCycle{Number: 1}
		if row != nil {
			if err := json.Unmarshal([]byte(row.Value), &cycle); err != nil {
				return 0, err
			}
			if cycle.Granted == granted {
				return cycle.Number, nil
			}
			if !granted {
				cycle.Number++
			}
		}
		cycle.Granted = granted
		encoded, err := json.Marshal(cycle)
		if err != nil {
			return 0, err
		}
		saved, err := settings.CompareAndSet(ctx, key, row, string(encoded))
		if err != nil || saved {
			return cycle.Number, err
		}
	}
}

func (h *Handler) requestPRSchemaReview(ctx context.Context, client *ghclient.InstallationClient, repo string, pr int, slug, key string, revision prSchemaRevision) (string, error) {
	settings := h.service.Storage().Settings()
	row, err := settings.Get(ctx, key)
	if err != nil {
		return "", err
	}
	const pendingNotice = "SchemaBot has queued a team review request. An eligible team member can also review this PR directly."
	if row != nil && row.Value != "" {
		switch {
		case row.Value == "requested":
			return "", nil
		case strings.HasPrefix(row.Value, "pending:"):
			deadline, err := time.Parse(time.RFC3339Nano, strings.TrimPrefix(row.Value, "pending:"))
			if err != nil {
				return "", err
			}
			if time.Now().Before(deadline) {
				return pendingNotice, nil
			}
		default:
			return row.Value, nil
		}
	}

	pending := "pending:" + time.Now().UTC().Add(3*time.Minute).Format(time.RFC3339Nano)
	claimed, err := settings.CompareAndSet(ctx, key, row, pending)
	if err != nil || !claimed {
		return "", err
	}
	fresh, err := client.FetchPullRequestNoCache(ctx, repo, pr)
	if err != nil || !fresh.IsOpen() || fresh.Draft || schemaRevision(fresh) != revision {
		_, storeErr := settings.CompareAndSet(ctx, key, &storage.Setting{Key: key, Value: pending}, "")
		return "", errors.Join(err, storeErr)
	}
	outcome := "requested"
	if !fresh.HasRequestedTeam(slug) {
		requestErr := client.RequestReviewers(ctx, repo, pr, nil, []string{slug})
		if requestErr != nil {
			h.logger.Warn("could not request PR schema limit review", "repo", repo, "pr", pr, "team", slug, "error", requestErr)
			outcome = "GitHub refused the team review request. Ask a team member to review this PR directly."
			var response *gh.ErrorResponse
			if errors.As(requestErr, &response) && response.Response != nil {
				outcome = fmt.Sprintf("GitHub refused the team review request (HTTP %d). Ask a team member to review this PR directly.", response.Response.StatusCode)
			}
		}
	}
	_, err = settings.CompareAndSet(ctx, key, &storage.Setting{Key: key, Value: pending}, outcome)
	if outcome == "requested" {
		outcome = ""
	}
	return outcome, err
}

func (h *Handler) isPRSchemaExceptionEvent(event string, payload pullRequestPayload) bool {
	policy := h.config().PRSchemaCapForRepo(payload.Repository.FullName)
	if policy.Limit == 0 || policy.Observe {
		return false
	}
	if event == "pull_request_review" {
		return policy.ReviewTeam != "" && (payload.Action == "submitted" || payload.Action == "dismissed")
	}
	return (policy.Label != "" && strings.EqualFold(payload.Label.Name, policy.Label) &&
		(payload.Action == "labeled" || payload.Action == "unlabeled")) ||
		(policy.ReviewTeam != "" && payload.Action == "ready_for_review" && !isAutoPlannablePullRequest(payload))
}

func (h *Handler) handlePRSchemaExceptionEvent(ctx context.Context, w http.ResponseWriter, event string, body []byte, deliveryID string) {
	var payload pullRequestPayload
	if err := json.Unmarshal(body, &payload); err != nil {
		h.writeError(w, http.StatusBadRequest, "invalid PR schema exception payload")
		return
	}
	repo, pr := storage.CanonicalKey(payload.Repository.FullName), payload.PullRequest.Number
	if !h.isRepoAllowed(repo) || !h.isPRSchemaExceptionEvent(event, payload) {
		h.writeJSON(w, http.StatusOK, map[string]string{"message": "PR schema exception event ignored"})
		return
	}
	installation := h.effectiveInstallationID(ctx, payload.Installation.ID)
	if pr <= 0 || installation == 0 {
		h.writeError(w, http.StatusBadRequest, "PR schema exception event missing PR or installation")
		return
	}
	if h.durableWebhookDispatch {
		_, err := h.enqueueDurableWebhookEvent(ctx, &storage.WebhookEvent{
			Provider: storage.WebhookProviderGitHub, DeliveryID: deliveryID, Event: event, Action: payload.Action,
			Repository: repo, PullRequest: pr, HeadSHA: payload.PullRequest.Head.SHA,
			TenantID: strconv.FormatInt(installation, 10), Payload: body,
		})
		if err != nil {
			h.logger.Error("could not enqueue PR schema exception", "repo", repo, "pr", pr, "error", err)
			h.writeError(w, http.StatusInternalServerError, "failed to enqueue PR schema exception")
			return
		}
	} else {
		received := time.Now()
		if payload.Action == "labeled" {
			var err error
			received, err = h.originalSchemaLabelReceipt(ctx, repo, pr, deliveryID, received)
			if err != nil {
				h.logger.Error("could not record label event receipt", "repo", repo, "pr", pr, "error", err)
				h.writeError(w, http.StatusInternalServerError, "failed to record label event")
				return
			}
		}
		h.goSafe(repo, pr, installation, deliveryID, func() {
			if err := h.runPRSchemaExceptionEvent(context.Background(), event, payload, installation, deliveryID, received); err != nil {
				h.logger.Error("could not reconcile PR schema exception", "repo", repo, "pr", pr, "error", err)
			}
		})
	}
	h.writeJSON(w, http.StatusOK, map[string]string{"message": "PR schema exception reconciliation queued"})
}

func (h *Handler) originalSchemaLabelReceipt(ctx context.Context, repo string, pr int, deliveryID string, received time.Time) (time.Time, error) {
	if deliveryID == "" {
		return time.Time{}, fmt.Errorf("missing label delivery ID")
	}
	settings := h.service.Storage().Settings()
	key := schemaCapKey("receipt", repo, pr, deliveryID)
	saved, err := settings.CompareAndSet(ctx, key, nil, received.UTC().Format(time.RFC3339Nano))
	if err != nil || saved {
		return received, err
	}
	row, err := settings.Get(ctx, key)
	if err != nil {
		return time.Time{}, err
	}
	if row == nil {
		return time.Time{}, fmt.Errorf("missing original label delivery receipt")
	}
	return time.Parse(time.RFC3339Nano, row.Value)
}

func (h *Handler) runPRSchemaExceptionEvent(parent context.Context, event string, payload pullRequestPayload, installation int64, deliveryID string, received time.Time) error {
	repo, pr := storage.CanonicalKey(payload.Repository.FullName), payload.PullRequest.Number
	if !h.isRepoAllowed(repo) || !h.isPRSchemaExceptionEvent(event, payload) {
		return nil
	}
	ctx, cancel, client, err := h.autoPlanBootstrap(parent, repo, installation)
	if err != nil {
		return err
	}
	defer cancel()
	info, err := client.FetchPullRequestNoCache(ctx, repo, pr)
	if err != nil || !info.IsOpen() {
		return err
	}
	if payload.Action == "labeled" {
		label := h.config().PRSchemaCapForRepo(repo).Label
		adopted, err := h.recordPRSchemaLabel(ctx, client, repo, pr, info, label, payload, received)
		if err != nil || !adopted {
			return err
		}
	}
	ctx = ghclient.WithPRInfo(ctx, repo, pr, info)
	_, err = h.dispatchAutoPlanForPR(ctx, client, repo, pr, info.HeadSHA, installation, event, payload.Action, "", deliveryID, autoPlanOptions{schemaExceptionEvent: true})
	return err
}

func (h *Handler) processDurablePRSchemaException(ctx context.Context, event *storage.WebhookEvent, payload pullRequestPayload) (bool, error) {
	id, err := durableInstallationID(event)
	if err != nil {
		return false, err
	}
	err = h.runPRSchemaExceptionEvent(ctx, event.Event, payload, id, event.DeliveryID, event.CreatedAt)
	return err != nil && !autoPlanFailureIsPermanent(err), err
}
