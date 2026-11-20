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

	ghclient "github.com/block/schemabot/pkg/github"
	"github.com/block/schemabot/pkg/storage"
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

func (h *Handler) isPRSchemaExceptionEvent(payload pullRequestPayload) bool {
	policy := h.config().PRSchemaCapForRepo(payload.Repository.FullName)
	if policy.Limit == 0 || policy.Observe {
		return false
	}
	return strings.EqualFold(payload.Label.Name, policy.Label) && (payload.Action == "labeled" || payload.Action == "unlabeled")
}

func (h *Handler) handlePRSchemaExceptionEvent(ctx context.Context, w http.ResponseWriter, body []byte, deliveryID string) {
	var payload pullRequestPayload
	if err := json.Unmarshal(body, &payload); err != nil {
		h.writeError(w, http.StatusBadRequest, "invalid PR schema exception payload")
		return
	}
	repo, pr := storage.CanonicalKey(payload.Repository.FullName), payload.PullRequest.Number
	if !h.isRepoAllowed(repo) || !h.isPRSchemaExceptionEvent(payload) {
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
			Provider: storage.WebhookProviderGitHub, DeliveryID: deliveryID, Event: "pull_request", Action: payload.Action,
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
			if err := h.runPRSchemaExceptionEvent(context.Background(), payload, installation, deliveryID, received); err != nil {
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

func (h *Handler) runPRSchemaExceptionEvent(parent context.Context, payload pullRequestPayload, installation int64, deliveryID string, received time.Time) error {
	repo, pr := storage.CanonicalKey(payload.Repository.FullName), payload.PullRequest.Number
	if !h.isRepoAllowed(repo) || !h.isPRSchemaExceptionEvent(payload) {
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
		// An incomplete timeline cannot authorize the label. Continue to
		// admission so an over-limit PR gets the hold and recovery instructions.
		if !errors.Is(err, ghclient.ErrPRSchemaTimelineIncomplete) && (err != nil || !adopted) {
			return err
		}
	}
	ctx = ghclient.WithPRInfo(ctx, repo, pr, info)
	_, err = h.dispatchAutoPlanForPR(ctx, client, repo, pr, info.HeadSHA, installation, "pull_request", payload.Action, "", deliveryID, autoPlanOptions{schemaExceptionEvent: true})
	return err
}

func (h *Handler) processDurablePRSchemaException(ctx context.Context, event *storage.WebhookEvent, payload pullRequestPayload) (bool, error) {
	id, err := durableInstallationID(event)
	if err != nil {
		return false, err
	}
	err = h.runPRSchemaExceptionEvent(ctx, payload, id, event.DeliveryID, event.CreatedAt)
	return err != nil && !autoPlanFailureIsPermanent(err), err
}
