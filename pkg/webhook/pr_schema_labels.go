package webhook

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"slices"
	"strings"
	"time"

	"github.com/block/schemabot/pkg/api"
	ghclient "github.com/block/schemabot/pkg/github"
	"github.com/block/schemabot/pkg/storage"
)

type prSchemaAcknowledgment struct {
	Revision   prSchemaRevision
	Label      string
	EventID    int64
	RevisionID int64
}

func (a prSchemaAcknowledgment) authorizes(revision prSchemaRevision, state ghclient.PRLabelState, label string) bool {
	return state.Present && a.EventID != 0 && state.EventID == a.EventID &&
		state.RevisionID == a.RevisionID && a.Revision == revision && a.Label == label
}

func prSchemaLabelKey(repo string, pr int) string {
	return fmt.Sprintf("pr_schema_cap/labels/%s/%d", repo, pr)
}

func readPRSchemaAcknowledgment(ctx context.Context, settings storage.SettingsStore, repo string, pr int) (*storage.Setting, prSchemaAcknowledgment, error) {
	key := prSchemaLabelKey(repo, pr)
	current, err := settings.Get(ctx, key)
	if err != nil {
		return nil, prSchemaAcknowledgment{}, fmt.Errorf("read schema cap acknowledgment: %w", err)
	}
	var acknowledgment prSchemaAcknowledgment
	if current != nil {
		if err := json.Unmarshal([]byte(current.Value), &acknowledgment); err != nil {
			return nil, prSchemaAcknowledgment{}, fmt.Errorf("decode schema cap acknowledgment: %w", err)
		}
	}
	return current, acknowledgment, nil
}

func savePRSchemaAcknowledgment(ctx context.Context, settings storage.SettingsStore, repo string, pr int, previous *storage.Setting, acknowledgment prSchemaAcknowledgment) (bool, error) {
	encoded, err := json.Marshal(acknowledgment)
	if err != nil {
		return false, err
	}
	return settings.CompareAndSet(ctx, prSchemaLabelKey(repo, pr), previous, string(encoded))
}

func (h *Handler) isPRSchemaLabelEvent(payload pullRequestPayload) bool {
	policy := h.prSchemaCap(payload.Repository.FullName)
	return policy.Limit > 0 && !policy.Observe &&
		(payload.Action == "labeled" || payload.Action == "unlabeled") &&
		strings.EqualFold(payload.Label.Name, policy.Label)
}

// Non-durable handlers still pin the first receipt for a delivery GUID.
func (h *Handler) originalPRSchemaLabelReceipt(ctx context.Context, repo string, pr int, deliveryID string, receivedAt time.Time) (time.Time, error) {
	if deliveryID == "" {
		return time.Time{}, fmt.Errorf("schema cap label event has no GitHub delivery ID")
	}
	key := fmt.Sprintf("pr_schema_cap/receipt/%s/%d/%x", repo, pr, sha256.Sum256([]byte(deliveryID)))
	settings := h.service.Storage().Settings()
	if saved, err := settings.CompareAndSet(ctx, key, nil, receivedAt.UTC().Format(time.RFC3339Nano)); err != nil || saved {
		return receivedAt, err
	}
	row, err := settings.Get(ctx, key)
	if err != nil {
		return time.Time{}, err
	}
	if row == nil {
		return time.Time{}, fmt.Errorf("original schema cap label delivery receipt is missing")
	}
	return time.Parse(time.RFC3339Nano, row.Value)
}

func (h *Handler) runPRSchemaLabelEvent(ctx context.Context, client *ghclient.InstallationClient, payload pullRequestPayload, installationID int64, deliveryID string, receivedAt time.Time) (string, error) {
	repo, pr := storage.CanonicalKey(payload.Repository.FullName), payload.PullRequest.Number
	policy := h.prSchemaCap(repo)
	if payload.Action == "unlabeled" {
		// Reconcile current GitHub state. A delayed removal from an older
		// revision cannot revoke a newer label or start planning.
		_, err := h.admitPRSchemaWork(ctx, client, repo, pr, prSchemaRevision{})
		if isPRSchemaCapStop(err) {
			return "schema cap hold reconciled", nil
		}
		return "schema cap label removal reconciled", err
	}

	fresh, err := client.FetchPullRequestNoCache(ctx, repo, pr)
	if err != nil {
		return "", err
	}
	revision := schemaRevision(fresh)
	if !fresh.IsOpen() || !fresh.HasLabel(policy.Label) || !revision.valid() ||
		revision != payload.schemaRevision() {
		return "schema cap label delivery superseded", nil
	}
	state, err := client.FetchPRLabelState(ctx, repo, pr, policy.Label)
	if err != nil {
		return "", err
	}
	if !matchesPRSchemaLabelAddition(payload, state, receivedAt) {
		return "schema cap label event not current", nil
	}
	discovery, err := h.discoverPRSchemaCap(ctx, client, repo, pr, revision)
	if err != nil {
		if isPRSchemaCapStop(err) {
			return err.Error(), nil
		}
		h.postConfigDiscoveryFailure(ctx, client, repo, pr, revision.Head, err)
		return "", err
	}
	if len(discovery.schemas) <= policy.Limit {
		return "PR is within the schema limit", nil
	}
	if err := h.recordPRSchemaLabel(ctx, repo, pr, policy, revision, state); err != nil {
		return "", err
	}
	return h.runAutoPlanForPR(ctx, client, repo, pr, revision.Head, installationID, "pull_request", payload.Action, "", deliveryID)
}

func matchesPRSchemaLabelAddition(payload pullRequestPayload, state ghclient.PRLabelState, receivedAt time.Time) bool {
	// A label change has no event timestamp in the webhook. GitHub's PR
	// updated_at can precede the label action. The signed snapshot gives
	// the lower bound and the original receipt the upper bound. The live
	// revision and the timeline handle intervening changes.
	return state.Present && state.EventID > state.RevisionID && state.Actor != "" &&
		strings.EqualFold(payload.Sender.Login, state.Actor) &&
		!payload.PullRequest.UpdatedAt.IsZero() &&
		!state.ChangedAt.Before(payload.PullRequest.UpdatedAt) &&
		!state.ChangedAt.After(receivedAt)
}

func (h *Handler) recordPRSchemaLabel(ctx context.Context, repo string, pr int, policy api.PRSchemaCapConfig, revision prSchemaRevision, state ghclient.PRLabelState) error {
	settings := h.service.Storage().Settings()
	for {
		row, existing, err := readPRSchemaAcknowledgment(ctx, settings, repo, pr)
		if err != nil {
			return err
		}
		if existing.EventID >= state.EventID {
			return nil
		}
		saved, err := savePRSchemaAcknowledgment(ctx, settings, repo, pr, row, prSchemaAcknowledgment{
			Revision: revision, Label: policy.Label, EventID: state.EventID, RevisionID: state.RevisionID,
		})
		if err != nil || saved {
			return err
		}
	}
}

type prSchemaPlanClaim struct {
	EventID int64
	Until   time.Time
}

const prSchemaPlanClaimDuration = commandTimeout + 30*time.Second

// Each database gets its own claim so a restarted worker can recover a partial
// fan-out without repeating the databases that already returned.
func (h *Handler) claimPRSchemaAutoPlan(ctx context.Context, repo string, pr int, database string, allowed prSchemaAcknowledgment) (*storage.Setting, error) {
	settings := h.service.Storage().Settings()
	_, current, err := readPRSchemaAcknowledgment(ctx, settings, repo, pr)
	if err != nil || current != allowed {
		return nil, err
	}
	config := h.service.Config()
	scope := sha256.Sum256(fmt.Appendf(nil, "%q/%q", config.Tenant, slices.Sorted(slices.Values(config.AllowedEnvironments))))
	key := fmt.Sprintf("pr_schema_cap/dispatch/%s/%d/%x/%s", repo, pr, scope[:8], database)
	row, err := settings.Get(ctx, key)
	if err != nil {
		return nil, err
	}
	if row != nil {
		var previous prSchemaPlanClaim
		if err := json.Unmarshal([]byte(row.Value), &previous); err != nil {
			return nil, fmt.Errorf("decode schema cap plan claim: %w", err)
		}
		if previous.EventID > allowed.EventID || (previous.EventID == allowed.EventID && previous.Until.IsZero()) {
			return nil, nil
		}
		if previous.EventID == allowed.EventID && time.Now().Before(previous.Until) {
			return nil, fmt.Errorf("schema cap auto-plan for %s already has an active claim", database)
		}
	}
	claim := prSchemaPlanClaim{EventID: allowed.EventID, Until: time.Now().UTC().Add(prSchemaPlanClaimDuration)}
	encoded, err := json.Marshal(claim)
	if err != nil {
		return nil, err
	}
	ok, err := settings.CompareAndSet(ctx, key, row, string(encoded))
	if err != nil {
		return nil, err
	}
	if !ok {
		return nil, fmt.Errorf("schema cap auto-plan claim for %s changed concurrently", database)
	}
	return &storage.Setting{Key: key, Value: string(encoded)}, nil
}

func (h *Handler) finishPRSchemaAutoPlan(ctx context.Context, claim *storage.Setting, eventID int64, planFailed bool) error {
	result := prSchemaPlanClaim{EventID: eventID}
	if planFailed {
		result.Until = time.Now().UTC()
	}
	encoded, err := json.Marshal(result)
	if err != nil {
		return err
	}
	_, err = h.service.Storage().Settings().CompareAndSet(ctx, claim.Key, claim, string(encoded))
	return err
}

func (h *Handler) removeStalePRSchemaLabel(ctx context.Context, client *ghclient.InstallationClient, repo string, pr int, policy api.PRSchemaCapConfig, revision prSchemaRevision, state ghclient.PRLabelState) {
	if !h.publishesPRSchemaCapComment(repo, policy) {
		return
	}
	fresh, err := client.FetchPullRequestNoCache(ctx, repo, pr)
	if err != nil || !fresh.HasLabel(policy.Label) || schemaRevision(fresh) != revision {
		return
	}
	latest, err := client.FetchPRLabelState(ctx, repo, pr, policy.Label)
	if err != nil || latest != state {
		return
	}
	if err := client.RemovePRLabel(ctx, repo, pr, policy.Label); err != nil {
		h.logger.Warn("could not remove stale schema cap label", "repo", repo, "pr", pr, "label", policy.Label, "error", err)
	}
}
