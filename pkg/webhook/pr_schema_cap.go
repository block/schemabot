package webhook

import (
	"context"
	"errors"
	"fmt"

	"github.com/block/schemabot/pkg/api"
	ghclient "github.com/block/schemabot/pkg/github"
	"github.com/block/schemabot/pkg/metrics"
)

var (
	errPRSchemaCapHeld       = errors.New("SchemaBot planning is paused by the PR schema limit")
	errPRSchemaRevisionMoved = errors.New("the PR revision changed during schema cap evaluation")
)

func isPRSchemaCapStop(err error) bool {
	return errors.Is(err, errPRSchemaCapHeld) || errors.Is(err, errPRSchemaRevisionMoved)
}

func (h *Handler) prSchemaCap(repo string) api.PRSchemaCapConfig {
	return h.config().PRSchemaCapForRepo(repo)
}

func (h *Handler) admitPRSchemaWork(ctx context.Context, client *ghclient.InstallationClient, repo string, pr int, expected prSchemaRevision) (prSchemaRevision, error) {
	policy := h.prSchemaCap(repo)
	if policy.Limit == 0 || policy.Observe {
		return prSchemaRevision{}, nil
	}
	info, err := client.FetchPullRequestNoCache(ctx, repo, pr)
	if err != nil {
		return prSchemaRevision{}, err
	}
	revision := schemaRevision(info)
	if !info.IsOpen() || (expected.valid() && revision != expected) {
		return prSchemaRevision{}, errPRSchemaRevisionMoved
	}
	discovery, err := h.discoverPRSchemaCap(ctx, client, repo, pr, revision)
	if err != nil {
		if !isPRSchemaCapStop(err) {
			h.postConfigDiscoveryFailure(ctx, client, repo, pr, revision.Head, err)
		}
		return prSchemaRevision{}, err
	}
	_, err = h.admitDiscoveredPRSchemaWork(ctx, client, repo, pr, policy, revision, discovery)
	return revision, err
}

func (h *Handler) admitDiscoveredPRSchemaWork(ctx context.Context, client *ghclient.InstallationClient, repo string, pr int, policy api.PRSchemaCapConfig, revision prSchemaRevision, discovery *prSchemaDiscovery) (prSchemaAcknowledgment, error) {
	aboveLimit := len(discovery.schemas) > policy.Limit
	if policy.Observe {
		if !aboveLimit {
			return prSchemaAcknowledgment{}, nil
		}
		h.logger.Info("PR exceeds the observed schema limit", "repo", repo, "pr", pr, "head_sha", revision.Head,
			"schemas", len(discovery.schemas), "limit", policy.Limit, "discovered_configs", len(discovery.configs))
		metrics.RecordPRSchemaCap(ctx, repo, "would_block", len(discovery.configs))
		return prSchemaAcknowledgment{}, nil
	}

	fresh, err := client.FetchPullRequestNoCache(ctx, repo, pr)
	if err != nil {
		return prSchemaAcknowledgment{}, err
	}
	if !fresh.IsOpen() || schemaRevision(fresh) != revision {
		return prSchemaAcknowledgment{}, errPRSchemaRevisionMoved
	}

	if fresh.HasLabel(policy.Label) {
		state, err := client.FetchPRLabelState(ctx, repo, pr, policy.Label)
		if err != nil {
			return prSchemaAcknowledgment{}, err
		}
		_, acknowledgment, err := readPRSchemaAcknowledgment(ctx, h.service.Storage().Settings(), repo, pr)
		if err != nil {
			return prSchemaAcknowledgment{}, err
		}
		if acknowledgment.authorizes(revision, state, policy.Label) {
			if aboveLimit {
				return acknowledgment, nil
			}
			return prSchemaAcknowledgment{}, nil
		}
		// Absence of an acknowledgment may mean its delivery is still in
		// flight. Remove only an event with positive evidence of staleness.
		stale := acknowledgment.EventID != 0 && acknowledgment.EventID == state.EventID
		if state.Present && (stale || (state.EventID > 0 && state.EventID < state.RevisionID)) {
			h.removeStalePRSchemaLabel(ctx, client, repo, pr, policy, revision, state)
		}
	}

	if !aboveLimit {
		return prSchemaAcknowledgment{}, nil
	}
	metrics.RecordPRSchemaCap(ctx, repo, "blocked", len(discovery.configs))
	if err := h.publishPRSchemaHold(ctx, client, repo, pr, policy, revision, discovery.schemas); err != nil {
		return prSchemaAcknowledgment{}, fmt.Errorf("publish PR schema cap hold: %w", err)
	}
	return prSchemaAcknowledgment{}, errPRSchemaCapHeld
}

type prSchemaAdmissionContextKey struct{}

type prSchemaAdmission struct {
	client   *ghclient.InstallationClient
	revision prSchemaRevision
}

func withPRSchemaAdmission(ctx context.Context, client *ghclient.InstallationClient, revision prSchemaRevision) context.Context {
	if !revision.valid() {
		return ctx
	}
	return context.WithValue(ctx, prSchemaAdmissionContextKey{}, prSchemaAdmission{client: client, revision: revision})
}

func (h *Handler) admitPRSchemaBeforeQueue(parent context.Context, repo string, pr int, installationID int64, expected prSchemaRevision) (prSchemaRevision, error) {
	policy := h.prSchemaCap(repo)
	if policy.Limit == 0 || policy.Observe {
		return prSchemaRevision{}, nil
	}
	ctx, cancel, client, err := h.commandBootstrap(parent, repo, installationID)
	if err != nil {
		return prSchemaRevision{}, err
	}
	defer cancel()
	return h.admitPRSchemaWork(ctx, client, repo, pr, expected)
}

// admitDispatchPRSchemaWork is invoked immediately before a PR-associated
// planner or apply dispatch. Webhook jobs carry the revision that was admitted
// before reading schema files, including the base commit and branch.
func (h *Handler) admitDispatchPRSchemaWork(ctx context.Context, repo string, pr int, head string) error {
	if head == "" {
		return fmt.Errorf("a PR-associated request needs its planned head commit")
	}
	if admission, ok := ctx.Value(prSchemaAdmissionContextKey{}).(prSchemaAdmission); ok {
		if admission.revision.Head != head {
			return errPRSchemaRevisionMoved
		}
		_, err := h.admitPRSchemaWork(ctx, admission.client, repo, pr, admission.revision)
		return err
	}
	installationID, err := h.resolveRepoWebhookInstallation(ctx, repo)
	if err != nil {
		return err
	}
	client, err := h.clientForRepo(repo, installationID)
	if err != nil {
		return err
	}
	info, err := client.FetchPullRequestNoCache(ctx, repo, pr)
	if err != nil {
		return err
	}
	if info.HeadSHA != head {
		return errPRSchemaRevisionMoved
	}
	_, err = h.admitPRSchemaWork(ctx, client, repo, pr, schemaRevision(info))
	return err
}
