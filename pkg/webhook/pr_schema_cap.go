package webhook

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"net/http"
	"slices"
	"strings"
	"time"

	"github.com/block/schemabot/pkg/api"
	ghclient "github.com/block/schemabot/pkg/github"
	"github.com/block/schemabot/pkg/metrics"
	"github.com/block/schemabot/pkg/storage"
	"github.com/block/schemabot/pkg/webhook/action"
)

var (
	errPRSchemaCapHeld = errors.New("SchemaBot planning is paused by the PR database limit")
	errPRSchemaMoved   = errors.New("the PR or database registrations changed during planning")
	// errPRSchemaHeadMoved is the errPRSchemaMoved a push causes. An auto-plan
	// that meets it planned nothing for its head, so its delivery ends
	// superseded rather than completed; a base, state, or registration change
	// leaves the head the delivery carried still current.
	errPRSchemaHeadMoved = fmt.Errorf("%w: the head commit moved", errPRSchemaMoved)
)

type prSchemaAdmission struct {
	key        prPlanDiscoveryKey
	client     *ghclient.InstallationClient
	info       *ghclient.PullRequestInfo
	discovery  *prPlanDiscovery
	compareTip string
	exception  string
}

type prSchemaAdmissionKey struct{}

func withPRSchemaAdmission(ctx context.Context, admission prSchemaAdmission) context.Context {
	if admission.client == nil {
		return ctx
	}
	var configs []ghclient.DiscoveredConfig
	if slices.Equal(admission.discovery.changedFiles, admission.discovery.files) {
		configs = admission.discovery.discovered
	}
	ctx = ghclient.WithPRDiscovery(ctx, admission.key.repo, admission.key.pr, admission.info, admission.discovery.changedFiles, configs)
	return context.WithValue(ctx, prSchemaAdmissionKey{}, admission)
}

func (h *Handler) admitPRSchemaCommand(ctx context.Context, client *ghclient.InstallationClient, repo string, pr int, expected prPlanDiscoveryKey) (prSchemaAdmission, error) {
	repo = storage.CanonicalKey(repo)
	policy := h.config().PRSchemaCapForRepo(repo)
	if policy.Limit == 0 || policy.Observe {
		return prSchemaAdmission{}, nil
	}
	info, err := client.FetchPullRequestNoCache(ctx, repo, pr)
	if err != nil {
		return prSchemaAdmission{}, fmt.Errorf("verify PR revision: %w", err)
	}
	key := h.prPlanDiscoveryKey(repo, pr, info)
	if !key.revision.valid() {
		return prSchemaAdmission{}, fmt.Errorf("GitHub did not supply the PR head and base revision")
	}
	if expected.pr != 0 && key != expected {
		return prSchemaAdmission{}, errPRSchemaMoved
	}

	// A durable apply command that fails here is retried, but its retry
	// re-runs admission, not the auto-plan that releases a stored aggregate
	// block, so a quota reserve refusal here asks the author to re-run the
	// check rather than saying SchemaBot retries it.
	discovery, err := h.cachedPRPlanDiscovery(ctx, client, key)
	if err != nil {
		h.postConfigDiscoveryFailure(ctx, client, repo, pr, info.HeadSHA, err, false)
		return prSchemaAdmission{}, err
	}
	if discovery == nil {
		discovery, err = h.readPRPlanDiscovery(ctx, client, repo, pr, info, "schema cap", "")
		if err != nil {
			h.postConfigDiscoveryFailure(ctx, client, repo, pr, info.HeadSHA, err, false)
			return prSchemaAdmission{}, err
		}
		if len(discovery.unmanagedFiles) > 0 {
			h.failClosedOnUnmanagedSchemaDir(ctx, client, repo, pr, info.HeadSHA, "schema cap", discovery.unmanagedFiles)
			return prSchemaAdmission{}, fmt.Errorf("schema changes under a managed directory have no configuration")
		}
		fresh, err := client.FetchPullRequestNoCache(ctx, repo, pr)
		if err != nil {
			return prSchemaAdmission{}, err
		}
		if h.prPlanDiscoveryKey(repo, pr, fresh) != key || fresh.State != info.State {
			return prSchemaAdmission{}, errPRSchemaMoved
		}
		info = fresh
		h.prPlanDiscovery.put(key, discovery)
	}
	admission, err := h.evaluatePRSchemaCap(ctx, client, repo, pr, info, policy, discovery.localDatabases, schemaCapVerify)
	if errors.Is(err, errPRSchemaCapHeld) {
		err = h.withPRSchemaPublication(ctx, repo, pr, func() error {
			var verifyErr error
			info, verifyErr = h.verifyPRSchemaRevision(ctx, client, key, info.State)
			if verifyErr != nil {
				return verifyErr
			}
			admission, verifyErr = h.evaluatePRSchemaCap(ctx, client, repo, pr, info, policy, discovery.localDatabases, schemaCapReport)
			return verifyErr
		})
	}
	if err == nil && admission.exception != "" {
		info, err = h.verifyPRSchemaRevision(ctx, client, key, info.State)
		if err == nil && !info.HasLabel(policy.Label) {
			err = errPRSchemaCapHeld
		}
	}
	admission.key, admission.info, admission.discovery, admission.compareTip = key, info, discovery, discovery.defaultTip
	return admission, err
}

// Enforce immediately before the data-plane Plan RPC or accepting a new
// apply. Queued webhook work also carries the base and registration revision
// it was admitted against, even when its head commit has stayed the same.
func (h *Handler) admitPRSchemaDispatch(ctx context.Context, repo string, pr int, head string) error {
	admission, _ := ctx.Value(prSchemaAdmissionKey{}).(prSchemaAdmission)
	ctx = ghclient.WithPRInfoCache(ctx)
	repo = storage.CanonicalKey(repo)
	if admission.client != nil && admission.key.repo == repo && admission.key.pr == pr {
		if head != "" && admission.key.revision.Head != head {
			return errPRSchemaMoved
		}
		current, err := h.admitPRSchemaCommand(ctx, admission.client, repo, pr, admission.key)
		if err == nil && (admission.compareTip != current.compareTip || (admission.exception != "" && current.exception != admission.exception)) {
			return errPRSchemaMoved
		}
		return err
	}
	installation, err := h.resolveRepoWebhookInstallation(ctx, repo)
	if err != nil {
		return err
	}
	client, err := h.clientForRepo(repo, installation)
	if err != nil {
		return err
	}
	info, err := client.FetchPullRequestNoCache(ctx, repo, pr)
	if err != nil {
		return err
	}
	if head != "" && info.HeadSHA != head {
		return errPRSchemaMoved
	}
	_, err = h.admitPRSchemaCommand(ctx, client, repo, pr, h.prPlanDiscoveryKey(repo, pr, info))
	return err
}

type schemaCapEvaluation uint8

const (
	schemaCapVerify schemaCapEvaluation = iota
	schemaCapReport
	schemaCapAutoPlan
)

func (h *Handler) evaluatePRSchemaCap(ctx context.Context, client *ghclient.InstallationClient, repo string, pr int, info *ghclient.PullRequestInfo, policy api.PRSchemaCapConfig, databases []string, mode schemaCapEvaluation) (prSchemaAdmission, error) {
	if policy.Observe {
		if len(databases) > policy.Limit {
			h.recordPRSchemaCap(ctx, repo, pr, "would_block", len(databases))
		}
		return prSchemaAdmission{}, nil
	}
	admission := prSchemaAdmission{client: client}
	if len(databases) <= policy.Limit {
		if mode == schemaCapAutoPlan {
			h.resolvePRSchemaHold(ctx, client, repo, pr, policy)
		}
		return admission, nil
	}
	var notice string
	var verificationErr error
	if info.IsOpen() && info.HasLabel(policy.Label) {
		admission.exception, verificationErr = h.evaluatePRSchemaLabel(ctx, client, repo, pr, info, policy.Label)
		if errors.Is(verificationErr, ghclient.ErrPRSchemaTimelineIncomplete) {
			notice = "The timeline exceeds GitHub’s 100-event verification page. Split the changes into smaller PRs to proceed."
			verificationErr = nil
		} else if verificationErr != nil {
			h.logger.Warn("could not verify the PR schema limit exception", "repo", repo, "pr", pr, "error", verificationErr)
			notice = "SchemaBot could not verify the exception with GitHub. Retry the check."
		}
	}
	if admission.exception != "" {
		if mode == schemaCapAutoPlan {
			h.resolvePRSchemaHold(ctx, client, repo, pr, policy)
		}
		return admission, nil
	}
	if mode != schemaCapVerify {
		h.recordPRSchemaCap(ctx, repo, pr, "blocked", len(databases))
	}
	if mode != schemaCapVerify && info.IsOpen() {
		if err := h.publishPRSchemaHold(ctx, client, repo, pr, info, policy, databases, notice); err != nil {
			return prSchemaAdmission{}, fmt.Errorf("publish database limit hold: %w", err)
		}
	}
	if verificationErr != nil {
		return prSchemaAdmission{}, fmt.Errorf("verify database limit exception for %s#%d: %w", repo, pr, verificationErr)
	}
	return prSchemaAdmission{}, errPRSchemaCapHeld
}

func (h *Handler) verifyPRSchemaRevision(ctx context.Context, client *ghclient.InstallationClient, key prPlanDiscoveryKey, state string) (*ghclient.PullRequestInfo, error) {
	fresh, err := client.FetchPullRequestNoCache(ctx, key.repo, key.pr)
	if err != nil {
		return nil, err
	}
	if fresh.HeadSHA != key.revision.Head {
		return nil, errPRSchemaHeadMoved
	}
	if h.prPlanDiscoveryKey(key.repo, key.pr, fresh) != key || fresh.State != state {
		return nil, errPRSchemaMoved
	}
	return fresh, nil
}

// Hold publication and release must be ordered across workers and replicas.
// A valid-at-read exception must never clear a hold already published by a
// concurrent label removal on the same head.
func (h *Handler) withPRSchemaPublication(ctx context.Context, repo string, pr int, publish func() error) error {
	policy := h.config().PRSchemaCapForRepo(repo)
	settings := h.service.Storage().Settings()
	key := schemaCapKey("publication", repo, pr, policy.ControlPlane)
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()
	for {
		row, err := settings.Get(ctx, key)
		if err != nil {
			return err
		}
		available := row == nil || row.Value == ""
		if !available {
			deadline, err := time.Parse(time.RFC3339Nano, row.Value)
			if err != nil {
				return err
			}
			available = !time.Now().Before(deadline)
		}
		if available {
			value := time.Now().UTC().Add(5 * time.Minute).Format(time.RFC3339Nano)
			claimed, err := settings.CompareAndSet(ctx, key, row, value)
			if err != nil {
				return err
			}
			if claimed {
				defer func() {
					releaseCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
					defer cancel()
					if _, err := settings.CompareAndSet(releaseCtx, key, &storage.Setting{Key: key, Value: value}, ""); err != nil {
						h.logger.Warn("could not release PR database limit publication", "repo", repo, "pr", pr, "error", err)
					}
				}()
				return publish()
			}
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
		}
	}
}

func (h *Handler) recordPRSchemaCap(ctx context.Context, repo string, pr int, result string, databases int) {
	h.logger.Info("PR database limit evaluated", "repo", repo, "pr", pr, "result", result, "databases", databases)
	metrics.RecordPRSchemaCap(ctx, repo, result, databases)
}

func schemaCapKey(kind, repo string, pr int, scope ...string) string {
	key := sha256.Sum256([]byte(strings.Join(append([]string{storage.CanonicalKey(repo), fmt.Sprint(pr)}, scope...), "\x00")))
	return fmt.Sprintf("pr-schema-cap:%s:%x", kind, key)
}

func isPRSchemaRefusal(err error) bool {
	return errors.Is(err, errPRSchemaCapHeld) || errors.Is(err, errPRSchemaMoved)
}

func (h *Handler) admitPRSchemaBeforeQueue(parent context.Context, repo string, pr int, installation int64, expected prPlanDiscoveryKey) (prSchemaAdmission, error) {
	policy := h.config().PRSchemaCapForRepo(repo)
	if policy.Limit == 0 || policy.Observe {
		return prSchemaAdmission{}, nil
	}
	ctx, cancel, client, err := h.commandBootstrap(parent, repo, installation)
	if err != nil {
		return prSchemaAdmission{}, err
	}
	defer cancel()
	return h.admitPRSchemaCommand(ctx, client, repo, pr, expected)
}

func (h *Handler) answerPRSchemaPlanRefusal(w http.ResponseWriter, repo string, pr int, installation int64, environment, requestedBy string, err error) {
	message := "Could not verify the PR database limit. Retry the plan."
	if isPRSchemaRefusal(err) {
		message = err.Error()
	}
	if !errors.Is(err, errPRSchemaCapHeld) {
		h.logger.Warn("plan stopped before dispatch by the PR database limit", "repo", repo, "pr", pr, "error", err)
		if requestedBy != "" {
			h.postCommandError(repo, pr, installation, action.Plan, environment, requestedBy, message)
		}
	}
	if w != nil {
		h.writeJSON(w, http.StatusOK, map[string]string{"message": message})
	}
}
