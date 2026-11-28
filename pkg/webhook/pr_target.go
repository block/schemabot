package webhook

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	ghclient "github.com/block/schemabot/pkg/github"
	"github.com/block/schemabot/pkg/storage"
	"github.com/google/uuid"
)

var errPRTargetExcluded = errors.New("pull request target is excluded from SchemaBot")

// errPRClosed refuses a plan's stored check state write on a closed PR. Close
// cleanup removes a closed PR's stored check state and does not run again, so
// a row written afterwards would be orphaned.
var errPRClosed = errors.New("pull request is closed")

func (h *Handler) evaluatePRTarget(ctx context.Context, client *ghclient.InstallationClient, repo string, info *ghclient.PullRequestInfo) (ghclient.PRTargetEligibility, error) {
	if !h.skipsUnrequiredBranches() {
		return servedPRTarget(), nil
	}
	return client.EvaluatePRTarget(ctx, repo, info, h.prTargetCheckBases(repo))
}

// skipsUnrequiredBranches reports whether skip_unrequired_branches is on. With
// it off, every PR target is served, so no branch policy is read.
func (h *Handler) skipsUnrequiredBranches() bool {
	config, ok := h.serverConfig()
	return ok && config.SkipsUnrequiredBranches()
}

// servedPRTarget is the decision for every PR while skip_unrequired_branches
// is off: the target is served. No target is resolved, so Target stays empty.
func servedPRTarget() ghclient.PRTargetEligibility {
	return ghclient.PRTargetEligibility{Eligible: true}
}

func (h *Handler) prTargetCheckBases(repo string) []string {
	bases := []string{h.aggregateCheckNameForRepo(repo)}
	if config, ok := h.serverConfig(); ok {
		// Participants must honor the leader's required promotion check too.
		bases = append(bases, config.PromotionCheckNameBaseForRepo(repo))
	}
	return bases
}

// enforcePRTarget runs before command discovery. It reads fresh PR metadata
// and can reuse the installation's recent policy for a non-default target.
func (h *Handler) enforcePRTarget(ctx context.Context, client *ghclient.InstallationClient, repo string, pr int, installationID int64, command, environment, requestedBy string, silent, suppressRetryComments bool) (bool, error) {
	return h.enforcePRTargetWithPolicy(ctx, client, repo, pr, installationID, command, environment, requestedBy, silent, suppressRetryComments, client.EvaluatePRTarget)
}

type prTargetEvaluator func(context.Context, string, *ghclient.PullRequestInfo, []string) (ghclient.PRTargetEligibility, error)

func (h *Handler) enforcePRTargetWithPolicy(ctx context.Context, client *ghclient.InstallationClient, repo string, pr int, installationID int64, command, environment, requestedBy string, silent, suppressRetryComments bool, evaluate prTargetEvaluator) (bool, error) {
	if !h.skipsUnrequiredBranches() {
		h.logger.Debug("PR target not checked because skip_unrequired_branches is off", "repo", repo, "pr", pr, "command", command)
		return false, nil
	}
	info, err := client.FetchPullRequestNoCache(ctx, repo, pr)
	if err != nil {
		return h.prTargetVerificationFailure(repo, pr, installationID, command, environment, requestedBy, silent, suppressRetryComments, err)
	}
	decision, err := evaluate(ctx, repo, info, h.prTargetCheckBases(repo))
	return h.enforcePRTargetDecision(repo, pr, installationID, info, command, environment, requestedBy, silent, suppressRetryComments, decision, err)
}

// enforcePRTargetSnapshot checks an already-fresh PR read before a command can
// release or replace pending confirmation intent. The acceptance gate still
// fetches its own snapshot immediately before queuing an apply.
func (h *Handler) enforcePRTargetSnapshot(ctx context.Context, client *ghclient.InstallationClient, repo string, pr int, installationID int64, info *ghclient.PullRequestInfo, command, environment, requestedBy string, silent, suppressRetryComments bool) (bool, error) {
	decision, err := h.evaluatePRTarget(ctx, client, repo, info)
	return h.enforcePRTargetDecision(repo, pr, installationID, info, command, environment, requestedBy, silent, suppressRetryComments, decision, err)
}

func (h *Handler) enforcePRTargetDecision(repo string, pr int, installationID int64, info *ghclient.PullRequestInfo, command, environment, requestedBy string, silent, suppressRetryComments bool, decision ghclient.PRTargetEligibility, err error) (bool, error) {
	if err != nil {
		return h.prTargetVerificationFailure(repo, pr, installationID, command, environment, requestedBy, silent, suppressRetryComments, err)
	}
	if decision.Eligible {
		return false, nil
	}
	h.logger.Info("command rejected because PR target is excluded", "repo", repo, "pr", pr, "head_sha", info.HeadSHA, "effective_target", decision.Target, "command", command)
	if !silent {
		h.postCommandError(repo, pr, installationID, command, environment, requestedBy,
			fmt.Sprintf("This PR targets `%s`, where SchemaBot plans and applies are disabled. Retarget it to `%s` or a branch requiring SchemaBot checks, then retry.", sanitizeCheckRunErrorSummary(decision.Target), sanitizeCheckRunErrorSummary(info.DefaultBranch)))
	}
	return true, nil
}

func (h *Handler) prTargetVerificationFailure(repo string, pr int, installationID int64, command, environment, requestedBy string, silent, suppressRetryComments bool, err error) (bool, error) {
	h.logger.Error("could not verify PR target eligibility", "repo", repo, "pr", pr, "command", command, "error", err)
	if !silent && !suppressRetryComments {
		h.postCommandError(repo, pr, installationID, command, environment, requestedBy, "Could not verify the PR target, so the command was blocked. Retry, and see server logs if it persists.")
	}
	return false, fmt.Errorf("verify target eligibility for %s#%d: %w", repo, pr, err)
}

// Recovery tracks the latest observed target separately from inbox head coverage.
// Each observation identifies an evaluation context for recovery. It never
// authorizes an apply or revokes prior results or confirmations.
func prTargetObservationKey(repo string, pr int) string {
	digest := sha256.Sum256([]byte(storage.CanonicalKey(repo)))
	return fmt.Sprintf("webhook-pr-target:%x:%d", digest[:12], pr)
}

func prTargetObservation(info *ghclient.PullRequestInfo, decision ghclient.PRTargetEligibility) string {
	data := fmt.Appendf(nil, "%s\x00%s\x00%s\x00%t\x00%s\x00%t", info.HeadSHA, info.BaseRef, info.DefaultBranch, info.NativeStack, info.StackBaseRef, decision.Eligible)
	digest := sha256.Sum256(data)
	return fmt.Sprintf("%x", digest)
}

func (h *Handler) recordPRTargetObservation(ctx context.Context, repo string, pr int, info *ghclient.PullRequestInfo, decision ghclient.PRTargetEligibility, deliveryID ...string) error {
	if !h.webhookReconciler && !h.checkSuiteRecovery {
		return nil
	}
	store := h.service.Storage().Settings()
	if store == nil {
		return fmt.Errorf("PR target observation storage is unavailable")
	}
	observation := recordedPRTarget{Identity: prTargetObservation(info, decision), EvaluationID: uuid.NewString()}
	if len(deliveryID) > 0 {
		observation.DeliveryID = deliveryID[0]
	}
	data, err := json.Marshal(observation)
	if err != nil {
		return fmt.Errorf("encode PR target observation: %w", err)
	}
	return store.Set(ctx, prTargetObservationKey(repo, pr), string(data))

}

type recordedPRTarget struct {
	Identity     string `json:"identity"`
	DeliveryID   string `json:"delivery_id"`
	EvaluationID string `json:"evaluation_id"`
}

func (h *Handler) loadPRTargetObservation(ctx context.Context, repo string, pr int) (*recordedPRTarget, error) {
	settings := h.service.Storage().Settings()
	if settings == nil {
		return nil, fmt.Errorf("PR target observation storage is unavailable")
	}
	setting, err := settings.Get(ctx, prTargetObservationKey(repo, pr))
	if err != nil {
		return nil, fmt.Errorf("read PR target observation: %w", err)
	}
	if setting == nil {
		return nil, nil
	}
	var observation recordedPRTarget
	if err := json.Unmarshal([]byte(setting.Value), &observation); err != nil {
		return nil, fmt.Errorf("decode PR target observation: %w", err)
	}
	if observation.Identity == "" || observation.EvaluationID == "" {
		return nil, fmt.Errorf("PR target observation is incomplete")
	}
	return &observation, nil
}

// A new visit after another target was observed needs a fresh recovery identity,
// even at the same SHA. Prior completed and dead-lettered rows remain unchanged.
func recoveryGUIDForObservation(repo string, pr int, headSHA, identity string, observed *recordedPRTarget) string {
	if observed == nil {
		return synthesizedDeliveryGUID(repo, pr, headSHA, identity)
	}
	if observed.Identity == identity {
		// Retry this context's own failed synthesized row, preserving its
		// existing retry/dead-letter semantics rather than minting a new budget.
		if strings.HasPrefix(observed.DeliveryID, storage.SynthesizedWebhookDeliveryIDPrefix) {
			return observed.DeliveryID
		}
		return synthesizedDeliveryGUID(repo, pr, headSHA, identity)
	}
	return synthesizedDeliveryGUID(repo, pr, headSHA, identity+"\x00"+observed.EvaluationID)
}

func (h *Handler) recoveryDeliveryGUID(ctx context.Context, repo string, pr int, headSHA, identity string) (string, error) {
	observed, err := h.loadPRTargetObservation(ctx, repo, pr)
	if err != nil {
		return "", err
	}
	return recoveryGUIDForObservation(repo, pr, headSHA, identity, observed), nil
}

func samePRTargetContext(a, b *ghclient.PullRequestInfo) bool {
	return a.BaseRef == b.BaseRef && a.DefaultBranch == b.DefaultBranch && a.NativeStack == b.NativeStack && a.StackBaseRef == b.StackBaseRef
}

// closedSinceSnapshot reports whether a PR that was open when the work
// started is closed now. Close cleanup removes a closed PR's stored check
// state, so a publication from work that started before the close would
// recreate it. Work that started on an already closed PR, such as an apply's
// terminal outcome after a merge, is not affected.
func closedSinceSnapshot(snapshot, live *ghclient.PullRequestInfo) bool {
	return !snapshot.IsClosed() && live.IsClosed()
}

type publicationTargetKey struct{}
type publicationTarget struct {
	repository string
	pr         int
	info       ghclient.PullRequestInfo
}

func withExpectedPRTarget(ctx context.Context, repo string, pr int, info *ghclient.PullRequestInfo) context.Context {
	return context.WithValue(ctx, publicationTargetKey{}, publicationTarget{repository: repo, pr: pr, info: *info})
}

func snapshotPublicationTarget(ctx context.Context, client *ghclient.InstallationClient, repo string, pr int) (context.Context, error) {
	if snapshot, ok := ctx.Value(publicationTargetKey{}).(publicationTarget); ok && snapshot.repository == repo && snapshot.pr == pr {
		return ctx, nil
	}
	info, err := client.FetchPullRequest(ctx, repo, pr)
	if err != nil {
		return ctx, fmt.Errorf("snapshot PR target for %s#%d: %w", repo, pr, err)
	}
	return withExpectedPRTarget(ctx, repo, pr, info), nil
}

// Head coverage alone cannot prove the current target was evaluated. The scan
// and check-suite recovery share both the target observation and the identity
// of an already queued recovery, so concurrent signals converge on one delivery.
func (h *Handler) recoveryTargetCovered(ctx context.Context, store storage.WebhookEventStore, repo string, pr int, headSHA, observation string) (bool, error) {
	found, err := store.HasEventForHead(ctx, storage.WebhookProviderGitHub, repo, pr, headSHA)
	if err != nil {
		return false, fmt.Errorf("read head coverage: %w", err)
	}
	if !found {
		return false, nil
	}
	observed, err := h.loadPRTargetObservation(ctx, repo, pr)
	if err != nil {
		return false, err
	}
	if observed != nil && observed.Identity == observation {
		if observed.DeliveryID == "" {
			return true, nil
		}
		delivery, err := store.GetByDeliveryID(ctx, storage.WebhookProviderGitHub, observed.DeliveryID)
		if err != nil {
			return false, fmt.Errorf("read observed target delivery: %w", err)
		}
		if delivery != nil && delivery.State != storage.WebhookEventSuperseded {
			if delivery.State != storage.WebhookEventFailed {
				return true, nil
			}
			if !strings.HasPrefix(delivery.DeliveryID, storage.SynthesizedWebhookDeliveryIDPrefix) {
				failedAt := delivery.UpdatedAt
				if delivery.CompletedAt != nil {
					failedAt = *delivery.CompletedAt
				}
				if time.Since(failedAt) < storage.WebhookEventFailedCoverageCoolOff {
					return true, nil
				}
			}
		}
	}
	queued, err := store.GetByDeliveryID(ctx, storage.WebhookProviderGitHub, recoveryGUIDForObservation(repo, pr, headSHA, observation, observed))
	if err != nil {
		return false, fmt.Errorf("read target recovery delivery: %w", err)
	}
	if queued == nil {
		// Keep legacy head coverage until we have an observation or a
		// recovery delivery for the current target. The next evaluation
		// records the actual target.
		return observed == nil, nil
	}
	// Only work still queued or processing covers a context not yet observed.
	// An older completed recovery cannot cover a later same-SHA return.
	return queued.State == storage.WebhookEventPending || queued.State == storage.WebhookEventProcessing, nil
}
