// Package webhook handles GitHub webhook events for SchemaBot.
// It processes PR comments, check run actions, and pull request lifecycle events,
// routing them to the appropriate command handlers.
package webhook

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"maps"
	"net/http"
	"runtime/debug"
	"strconv"
	"strings"
	"sync"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"

	"github.com/block/schemabot/pkg/api"
	"github.com/block/schemabot/pkg/github"
	"github.com/block/schemabot/pkg/metrics"
	"github.com/block/schemabot/pkg/state"
	"github.com/block/schemabot/pkg/storage"
)

// defaultAppName is the synthetic App name used when SchemaBot runs against
// the legacy single-App ServerConfig.GitHub field. It matches the name
// ServerConfig.ResolveGitHubAppForRepo returns for legacy configs, so the
// per-repo client resolution path works uniformly across single-App and
// multi-App deployments.
const defaultAppName = "default"

// Webhook headers GitHub sends on every App-originated delivery. Used by
// multi-App dispatch to identify which configured App signed the request
// before HMAC verification.
//
// For GitHub App webhook deliveries — including repository-scoped events
// like pull_request, issue_comment, and check_run — GitHub sets:
//
//	X-GitHub-Hook-Installation-Target-Type: integration   (legacy "GitHub
//	                                                       Integrations"
//	                                                       naming for Apps)
//	X-GitHub-Hook-Installation-Target-ID:   <app_id>
//
// The Target-Type value "repository" is sent for repository-level webhooks
// (registered directly on a repo, distinct from App-installed webhooks).
// SchemaBot handles those only in the opt-in repo-webhook dispatch mode
// (when repoWebhookSecret is set); otherwise they are rejected. The single
// example in https://docs.github.com/en/webhooks/webhook-events-and-payloads
// happens to be a repository webhook, which is a known source of confusion.
// Verified against a live SchemaBot App delivery: for App-installed
// deliveries the header values were Target-Type "integration" and Target-ID
// equal to the SchemaBot App ID — i.e., never the repository ID, regardless
// of the underlying event type.
const (
	headerHookTargetID   = "X-GitHub-Hook-Installation-Target-ID"
	headerHookTargetType = "X-GitHub-Hook-Installation-Target-Type"
	headerDeliveryID     = "X-GitHub-Delivery"
	headerSignature256   = "X-Hub-Signature-256"
	hookTargetTypeApp    = "integration"
	hookTargetTypeRepo   = "repository"
	maxWebhookBodyBytes  = 10 << 20

	defaultDurableWebhookPollInterval  = 1 * time.Second
	defaultDurableWebhookLeaseDuration = 1 * time.Minute
)

// Handler processes GitHub webhook events.
type Handler struct {
	service   *api.Service
	ghClients github.ClientSet

	// transientPlanRetryDelay overrides the pause before retrying a plan
	// request that failed with transient remote unavailability. Zero means
	// the package default.
	transientPlanRetryDelay time.Duration

	// participantNudgeRefoldDelay overrides the pause before the second
	// aggregate re-fold after a participant-comment nudge. Zero means the
	// package default.
	participantNudgeRefoldDelay time.Duration

	// participantRefoldDelayOverride overrides the backoff before each
	// self-scheduled aggregate re-fold armed while expected participants
	// remain unresolved. Test-only: when set it applies to every attempt.
	// Zero means the package backoff schedule.
	participantRefoldDelayOverride time.Duration

	// participantRefoldAttempts tracks, per repo#pr, how many self-scheduled
	// aggregate re-folds have been armed while expected participants remain
	// unresolved. Bounded by maxParticipantRefoldAttempts and cleared when a
	// fold resolves every participant. Guarded by participantRefoldMu.
	participantRefoldMu       sync.Mutex
	participantRefoldAttempts map[string]int

	// webhookSecretsByApp maps each configured App's logical name to its
	// HMAC webhook secret. In legacy single-App mode there is exactly one
	// entry under defaultAppName. In multi-App mode there is one entry per
	// configured App.
	webhookSecretsByApp map[string][]byte

	// webhookAppByID maps the App ID GitHub sends in the
	// X-GitHub-Hook-Installation-Target-ID header to the configured App's
	// logical name. Non-nil enables multi-App dispatch: the handler
	// requires the header, looks up the App by ID, and verifies HMAC
	// against that App's secret only. Nil/empty preserves legacy
	// single-secret behavior.
	webhookAppByID map[int64]string

	// repoWebhookSecret, when non-empty, enables repository-level webhook
	// dispatch: deliveries with target type "repository" are HMAC-verified
	// against this secret and their App installation is resolved per repo
	// (such deliveries carry no installation id in the payload). Empty
	// disables repo-webhook dispatch and leaves the App-webhook paths
	// (single- and multi-App) unchanged.
	repoWebhookSecret []byte

	durableWebhookDispatch      bool
	durableWebhookDriverCount   int
	durableWebhookPollInterval  time.Duration
	durableWebhookLeaseDuration time.Duration
	durableWebhookMu            sync.Mutex
	durableWebhookStop          chan struct{}
	durableWebhookCancel        context.CancelFunc
	durableWebhookWake          chan struct{}
	durableWebhookWg            sync.WaitGroup
	// durableWebhookProcessOverride is a test seam that replaces
	// processDurableWebhookEvent so driver finish-path behavior (for example
	// refusing to complete a delivery after lease loss) can be exercised
	// deterministically. Production code never sets it.
	durableWebhookProcessOverride func(ctx context.Context, event *storage.WebhookEvent) (retry bool, err error)

	// inProcessWebhookCount tracks the detached goSafe goroutines that
	// non-durable event paths (issue_comment, participant_nudge, check_run,
	// push, and the legacy pull_request branches) launch after acking 200. The
	// request handler returns immediately, so the HTTP server's own graceful
	// shutdown does not wait for this work; DrainInProcessWebhookWork does, so a
	// deploy drains already-acked work instead of dropping it.
	//
	// The drain is transitive over work chains: tracked work often spawns more
	// goSafe work while it runs (for example handleMultiEnvPlan ->
	// acknowledgeCommand -> goSafe(add eyes reaction)). Registration is allowed
	// to continue during the drain as long as the count is provably nonzero — a
	// child of tracked work always registers while its parent is still counted,
	// so the drain waits for the whole chain. Only truly-fresh work that arrives
	// when the count is zero (the delayed time.AfterFunc timer case, which can
	// fire after the drain has already reached empty) runs untracked, which
	// bounds the drain so late timers cannot keep it alive forever.
	//
	// inProcessWebhookMu guards the count, the draining flag, and the
	// inProcessWebhookDrained signal so registration and the drain's
	// zero-count check are mutually consistent.
	inProcessWebhookMu       sync.Mutex
	inProcessWebhookDraining bool
	inProcessWebhookCount    int
	// inProcessWebhookDrained is created by DrainInProcessWebhookWork when it
	// begins waiting on in-flight work and closed by the goroutine that drops
	// the count to zero. Nil when no drain is waiting.
	inProcessWebhookDrained chan struct{}

	webhookReconciler        bool
	webhookReconcileInterval time.Duration
	webhookReconcileLookback time.Duration
	webhookReconcileGrace    time.Duration
	webhookReconcileMaxPages int

	logger                     *slog.Logger
	priorEnvCheckMaxAttempts   int
	priorEnvCheckRetryInterval time.Duration
}

// HandlerOption configures optional Handler behavior at construction. Options
// are variadic and backward-compatible: existing callers that pass none are
// unaffected.
type HandlerOption func(*Handler)

// WithRepoWebhookSecret enables repository-level webhook dispatch by setting the
// HMAC secret used to verify repository-targeted deliveries. Empty is a no-op.
func WithRepoWebhookSecret(secret []byte) HandlerOption {
	return func(h *Handler) {
		if len(secret) > 0 {
			h.repoWebhookSecret = secret
		}
	}
}

// WithDurableWebhookDispatch enables the durable webhook inbox path for events
// that have been moved out of the request path. Unsupported events keep using
// their existing synchronous or fire-and-forget handlers.
func WithDurableWebhookDispatch() HandlerOption {
	return func(h *Handler) {
		h.durableWebhookDispatch = true
	}
}

// WithWebhookReconciler enables the webhook reconciliation loop. It does two
// things per pass: (1) an active stuck-processing sweep that terminalizes inbox
// rows wedged past the attempt cap so they emit failures and become
// redeliverable, and (2) a report-only scan of recently updated open PRs in
// registered repositories that reports PR heads with no corresponding inbox
// delivery (no rows are synthesized). Only the missing-delivery scan is
// report-only. It takes effect alongside WithDurableWebhookDispatch, whose
// lifecycle it shares.
func WithWebhookReconciler() HandlerOption {
	return func(h *Handler) {
		h.webhookReconciler = true
	}
}

// NewHandler creates a new webhook handler for the legacy single-App
// configuration. The provided factory is registered in the internal
// ClientSet under the "default" App name so per-repo client resolution
// works uniformly with the multi-App path used by NewHandlerWithDispatch.
func NewHandler(service *api.Service, ghClient github.GitHubClientFactory, webhookSecret []byte, logger *slog.Logger, opts ...HandlerOption) *Handler {
	return NewHandlerWithClientSet(service, github.NewSingleClientSet(defaultAppName, ghClient), webhookSecret, logger, opts...)
}

// NewHandlerWithClientSet creates a new webhook handler from an
// already-built single-App ClientSet. The provided webhook secret is
// associated with the defaultAppName entry and verified directly on every
// request (legacy single-secret mode).
func NewHandlerWithClientSet(service *api.Service, ghClients github.ClientSet, webhookSecret []byte, logger *slog.Logger, opts ...HandlerOption) *Handler {
	secrets := map[string][]byte{}
	if len(webhookSecret) > 0 {
		secrets[defaultAppName] = webhookSecret
	}
	return NewHandlerWithDispatch(service, ghClients, secrets, nil, logger, opts...)
}

// NewHandlerWithDispatch creates a new webhook handler with header-keyed
// multi-App dispatch. webhookSecretsByApp must contain an entry per
// configured App keyed by logical name; webhookAppByID maps the App ID
// carried in the X-GitHub-Hook-Installation-Target-ID header to that name.
//
// Pass a non-empty webhookAppByID to enable multi-App dispatch: the
// handler will require the header, reject unknown App IDs, and HMAC-verify
// against the resolved App's secret only. Pass an empty/nil webhookAppByID
// for legacy single-secret behavior (used internally by NewHandler).
func NewHandlerWithDispatch(service *api.Service, ghClients github.ClientSet, webhookSecretsByApp map[string][]byte, webhookAppByID map[int64]string, logger *slog.Logger, opts ...HandlerOption) *Handler {
	h := &Handler{
		service:                     service,
		ghClients:                   ghClients,
		webhookSecretsByApp:         maps.Clone(webhookSecretsByApp),
		webhookAppByID:              maps.Clone(webhookAppByID),
		logger:                      logger,
		durableWebhookPollInterval:  defaultDurableWebhookPollInterval,
		durableWebhookLeaseDuration: defaultDurableWebhookLeaseDuration,
		webhookReconcileInterval:    defaultWebhookReconcileInterval,
		webhookReconcileLookback:    defaultWebhookReconcileLookback,
		webhookReconcileGrace:       defaultWebhookReconcileGrace,
		webhookReconcileMaxPages:    defaultWebhookReconcileMaxPages,
		priorEnvCheckMaxAttempts:    defaultPriorEnvCheckMaxAttempts,
		priorEnvCheckRetryInterval:  defaultPriorEnvCheckRetryInterval,
	}
	for _, opt := range opts {
		opt(h)
	}

	// Register recovery callback so the operator can attach comment observers
	// before resuming recovered applies.
	if service != nil {
		service.OnApplyRecovered = func(apply *storage.Apply) {
			if apply.Repository == "" || apply.PullRequest == 0 || apply.InstallationID == 0 {
				return
			}
			factory, err := h.factoryForRepo(apply.Repository)
			if err != nil {
				logger.Error("recovered apply skipped: cannot resolve GitHub App client",
					"apply_id", apply.ApplyIdentifier,
					"repo", apply.Repository,
					"pr", apply.PullRequest,
					"error", err)
				return
			}
			logger.Info("setting comment observer for recovered apply",
				"apply_id", apply.ApplyIdentifier,
				"repo", apply.Repository,
				"pr", apply.PullRequest)
			service.SetApplyObserver(apply.Database, apply.Deployment, apply.Environment, apply.ID,
				NewCommentObserver(CommentObserverConfig{
					GHClient:       factory,
					Storage:        service.Storage(),
					Repo:           apply.Repository,
					PR:             apply.PullRequest,
					InstallationID: apply.InstallationID,
					ApplyID:        apply.ID,
					ApplyLease:     apply.Lease(),
					SupportChannel: h.supportChannel(),
					Tenant:         h.deploymentTenant(),
					Logger:         logger,
					OnTerminalHook: func(a *storage.Apply) {
						h.refreshChecksForTerminalApply(context.Background(), a, "recovered apply")
					},
				}))
		}

		// Register the aggregate terminal-summary callback, invoked by the
		// operator that won the aggregate projection CAS. A multi-operation apply
		// suppresses the per-driver observer, so this is its only summary
		// publisher; a single-operation apply reaches it from paths with no live
		// observer (stop reconciliation), where the summary-marker claim keeps
		// the publish exactly-once against any observer still alive.
		service.OnApplyTerminalSummary = func(ctx context.Context, apply *storage.Apply, tasks []*storage.Task) error {
			if apply.Repository == "" || apply.PullRequest == 0 || apply.InstallationID == 0 {
				logger.Debug("aggregate terminal summary skipped: apply has no GitHub destination",
					"apply_id", apply.ApplyIdentifier,
					"database", apply.Database,
					"environment", apply.Environment)
				return nil
			}
			factory, err := h.factoryForRepo(apply.Repository)
			if err != nil {
				return fmt.Errorf("resolve GitHub App client for aggregate terminal summary of apply %s repo %s pr %d: %w",
					apply.ApplyIdentifier, apply.Repository, apply.PullRequest, err)
			}
			logger.Info("publishing aggregate terminal summary",
				"apply_id", apply.ApplyIdentifier,
				"repo", apply.Repository,
				"pr", apply.PullRequest,
				"state", apply.State)
			obs := NewAggregateTerminalCommentObserver(CommentObserverConfig{
				GHClient:       factory,
				Storage:        service.Storage(),
				Repo:           apply.Repository,
				PR:             apply.PullRequest,
				InstallationID: apply.InstallationID,
				ApplyID:        apply.ID,
				SupportChannel: h.supportChannel(),
				Tenant:         h.deploymentTenant(),
				Logger:         logger,
				OnTerminalHook: func(a *storage.Apply) {
					h.refreshChecksForTerminalApply(context.Background(), a, "aggregate terminal apply")
				},
			})
			obs.OnTerminal(apply, tasks)
			return nil
		}
	}

	return h
}

// refreshChecksForTerminalApply updates the stored check record for a terminal
// apply result and refreshes the GitHub aggregate Check Run. It drives every
// identifier and side effect from the apply it is handed so logs and check
// updates never mix fields from two apply instances. logCtx names the caller
// (e.g. "recovered apply") so operators can tell which path refreshed the check.
func (h *Handler) refreshChecksForTerminalApply(ctx context.Context, a *storage.Apply, logCtx string) {
	checkFields := func() []any {
		return []any{
			"apply_id", a.ApplyIdentifier,
			"repo", a.Repository,
			"pr", a.PullRequest,
			"database", a.Database,
			"environment", a.Environment,
			"context", logCtx,
		}
	}
	updated, err := h.updateCheckRecordForApplyResult(ctx, a.Repository, a.PullRequest, a)
	if err != nil {
		h.logger.Error("observer: failed to update check record for terminal apply",
			append(checkFields(), "error", err)...)
		return
	}
	if !updated {
		h.logger.Debug("observer: skipping aggregate check update for terminal apply, apply no longer owns check state",
			checkFields()...)
		return
	}
	ghInstClient, err := h.clientForRepo(a.Repository, a.InstallationID)
	if err != nil {
		h.logger.Warn("observer: aggregate check not refreshed for terminal apply; cannot resolve GitHub App client",
			append(checkFields(), "error", err)...)
		return
	}
	checkRecord, err := h.service.Storage().Checks().Get(ctx, a.Repository, a.PullRequest, a.Environment, a.DatabaseType, a.Database)
	if err != nil {
		h.logger.Warn("observer: aggregate check not refreshed for terminal apply; failed to load stored check state",
			append(checkFields(), "error", err)...)
		return
	}
	if checkRecord == nil {
		h.logger.Debug("observer: no stored check state for terminal apply; nothing to refresh",
			checkFields()...)
		return
	}
	h.updateAggregateCheck(ctx, ghInstClient, a.Repository, a.PullRequest, checkRecord.HeadSHA)
}

// factoryForRepo returns the GitHub App client factory that owns the given
// repository. In multi-App mode (ServerConfig.Apps is non-empty) the
// resolution goes through ServerConfig.ResolveGitHubAppForRepo so unknown
// repositories fail closed. In legacy single-App mode the ClientSet has
// exactly one entry under defaultAppName and is used uniformly for every
// repo.
func (h *Handler) factoryForRepo(repo string) (github.GitHubClientFactory, error) {
	if h.ghClients.Len() == 0 {
		return nil, fmt.Errorf("no GitHub App clients configured")
	}
	appName := defaultAppName
	if cfg := h.config(); cfg != nil && len(cfg.Apps) > 0 {
		resolved, err := cfg.ResolveGitHubAppForRepo(repo)
		if err != nil {
			return nil, fmt.Errorf("resolve GitHub App for repo %q: %w", repo, err)
		}
		appName = resolved.Name
	}
	factory, err := h.ghClients.For(appName)
	if err != nil {
		return nil, fmt.Errorf("lookup GitHub App client %q for repo %q: %w", appName, repo, err)
	}
	return factory, nil
}

// config returns the active ServerConfig if reachable, or nil. Centralized
// so callers can short-circuit safely when the service is not wired (e.g.
// some tests).
func (h *Handler) config() *api.ServerConfig {
	if h.service == nil {
		return nil
	}
	return h.service.Config()
}

// deploymentTenant returns this deployment's own tenant, or "" when the
// deployment is untenanted or the service is not wired. Command hints posted
// back to the PR carry it so pasted commands address this deployment: in
// tenant mode, commands without an explicit tenant target are ignored.
func (h *Handler) deploymentTenant() string {
	cfg := h.config()
	if cfg == nil {
		return ""
	}
	return cfg.Tenant
}

// clientForRepo returns an installation-scoped GitHub client for the App
// that owns the given repository. Callers that already have a factory in
// scope should use it directly; this is the convenience for the common
// "I have a repo + installation_id" path.
func (h *Handler) clientForRepo(repo string, installationID int64) (*github.InstallationClient, error) {
	factory, err := h.factoryForRepo(repo)
	if err != nil {
		return nil, err
	}
	return factory.ForInstallation(installationID)
}

// ReconcileMissingSummaryComments repairs the apply_comments outbox on startup.
// It finds recently terminal applies (including stopped) with a progress
// comment but no summary comment, then posts the missing summary so the PR
// shows the final result after a restart. Each repair goes through the atomic
// summary-marker claim, so a publisher racing this reconciler — or another
// pod's reconciler — still yields exactly one summary; a claim sentinel left
// by a publisher that crashed before posting is taken over once stale.
func (h *Handler) ReconcileMissingSummaryComments(ctx context.Context) {
	if h.service == nil {
		h.logger.Debug("skipping missing summary reconciliation without service")
		return
	}

	applies, err := h.service.Storage().Applies().FindMissingSummaryComment(ctx)
	if err != nil {
		h.logger.Error("summary comment reconciliation skipped this startup; terminal applies (including stopped) missing a summary comment stay unrepaired until the next restart",
			"error", err)
		return
	}

	if len(applies) == 0 {
		h.logger.Debug("no missing summary comments found")
		return
	}

	h.logger.Info("found applies missing summary comments", "count", len(applies))

	for _, apply := range applies {
		if !h.claimSummaryForReconciliation(ctx, apply) {
			continue
		}

		tasks, err := h.service.Storage().Tasks().GetByApplyID(ctx, apply.ID)
		if err != nil {
			h.logger.Error("failed to load tasks for missing summary reconciliation; releasing summary claim", append(apply.LogAttrs(), "error", err)...)
			h.releaseSummaryClaim(ctx, apply)
			continue
		}

		h.logger.Info("posting missing summary comment",
			"apply_id", apply.ApplyIdentifier,
			"repo", apply.Repository,
			"pr", apply.PullRequest,
			"state", apply.State)

		// Choose the single- or multi-deployment summary layout by the apply's
		// operation-row count. A load failure leaves ops nil, which renders the
		// single-deployment summary so a transient storage error never blocks
		// the reconciled comment.
		ops, err := h.service.Storage().ApplyOperations().ListByApply(ctx, apply.ID)
		if err != nil {
			h.logger.Error("failed to load apply operations for summary reconciliation; rendering single-deployment layout",
				"apply_id", apply.ApplyIdentifier, "error", err)
			ops = nil
		}
		released := releasedForApply(ctx, h.service.Storage(), apply, ops, h.logger)
		summaryBase := formatApplySummaryComment(apply, ops, released, tasks, resolveDisplayByOperation(ctx, h.service.Storage(), apply, ops), nil, h.deploymentTenant())
		summaryBody := summaryBase + failureLogsSection(ctx, h.service.Storage(), h.logger, apply, summaryBase)
		h.postClaimedSummaryComment(ctx, apply, summaryBody)
	}
}

// claimSummaryForReconciliation acquires the atomic summary-marker claim for a
// reconciliation repair. A fresh claim wins when no marker exists; when the
// marker is a claim sentinel left behind by a crashed publisher, the stale
// takeover path transfers it instead. Losing both means another writer posted
// or is actively posting the summary, so the repair is skipped.
func (h *Handler) claimSummaryForReconciliation(ctx context.Context, apply *storage.Apply) bool {
	won, err := h.service.Storage().ApplyComments().ClaimSummaryComment(ctx, apply.ID)
	if err != nil {
		h.logger.Error("failed to claim summary comment for reconciliation; apply skipped until next startup",
			append(apply.LogAttrs(), "error", err)...)
		return false
	}
	if won {
		return true
	}
	reclaimed, err := h.service.Storage().ApplyComments().ReclaimStaleSummaryClaim(ctx, apply.ID)
	if err != nil {
		h.logger.Error("failed to reclaim stale summary claim for reconciliation; apply skipped until next startup",
			append(apply.LogAttrs(), "error", err)...)
		return false
	}
	if !reclaimed {
		h.logger.Info("summary comment already claimed or posted by another writer; skipping reconciliation for apply",
			apply.LogAttrs()...)
		return false
	}
	return true
}

// postClaimedSummaryComment posts a reconciled summary comment under a claim
// this reconciler holds and records the posted comment ID. A failed post
// releases the claim so the next startup can retry immediately; a post that
// lands but fails to record leaves the sentinel to be reclaimed once stale
// (bounding the duplicate to one).
func (h *Handler) postClaimedSummaryComment(ctx context.Context, apply *storage.Apply, body string) {
	client, err := h.clientForRepo(apply.Repository, apply.InstallationID)
	if err != nil {
		h.logger.Error("failed to create GitHub client for reconciled summary comment; releasing summary claim",
			append(apply.LogAttrs(), "error", err)...)
		h.releaseSummaryClaim(ctx, apply)
		return
	}

	commentID, _, err := client.CreateIssueComment(ctx, apply.Repository, apply.PullRequest, h.renderPRComment(body))
	if err != nil {
		h.logger.Error("failed to post reconciled summary comment; releasing summary claim",
			append(apply.LogAttrs(), "error", err)...)
		h.releaseSummaryClaim(ctx, apply)
		return
	}

	metrics.RecordSummaryCommentRepaired(ctx, apply.Repository, apply.State)

	marker := &storage.ApplyComment{
		ApplyID:         apply.ID,
		CommentState:    state.Comment.Summary,
		GitHubCommentID: commentID,
	}
	if err := h.service.Storage().ApplyComments().Upsert(ctx, marker); err != nil {
		h.logger.Error("posted reconciled summary comment but failed to record its comment ID",
			append(apply.LogAttrs(), "error", err, "comment_id", commentID)...)
	}
}

// releaseSummaryClaim returns a won-but-unused summary claim so a later
// publisher or the next startup's reconciliation can retry immediately instead
// of waiting out the stale-claim window.
func (h *Handler) releaseSummaryClaim(ctx context.Context, apply *storage.Apply) {
	if err := h.service.Storage().ApplyComments().ReleaseSummaryClaim(ctx, apply.ID); err != nil {
		h.logger.Error("failed to release summary claim; reconciliation will reclaim it once stale",
			append(apply.LogAttrs(), "error", err)...)
	}
}

// ServeHTTP handles incoming webhook requests.
func (h *Handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	// Read body for signature validation
	r.Body = http.MaxBytesReader(w, r.Body, maxWebhookBodyBytes)
	body, err := io.ReadAll(r.Body)
	if err != nil {
		var maxBytesErr *http.MaxBytesError
		if errors.As(err, &maxBytesErr) {
			h.logger.Warn("webhook rejected: request body too large",
				"delivery_id", r.Header.Get(headerDeliveryID),
				"event", r.Header.Get("X-GitHub-Event"),
				"limit_bytes", maxBytesErr.Limit)
			h.writeError(w, http.StatusRequestEntityTooLarge, "request body too large")
			return
		}
		h.logger.Warn("webhook rejected: failed to read request body",
			"delivery_id", r.Header.Get(headerDeliveryID),
			"event", r.Header.Get("X-GitHub-Event"),
			"error", err)
		h.writeError(w, http.StatusBadRequest, "failed to read request body")
		return
	}

	// Resolve the signing App and verify HMAC. Failures here are recorded
	// against an unattributed event so the rejection still shows up in the
	// metrics counter with a clear status.
	eventType := r.Header.Get("X-GitHub-Event")
	appName, appID, authStatus, ok := h.authenticateWebhook(r, body)
	if !ok {
		h.logger.Warn("webhook rejected",
			"status", authStatus,
			"app_name", appName,
			"app_id", appID,
			"delivery_id", r.Header.Get(headerDeliveryID),
			"event", eventType)
		metrics.RecordWebhookEvent(r.Context(), metricAppName(appName), eventType, "", "", authStatus)
		h.writeError(w, http.StatusUnauthorized, "invalid webhook dispatch")
		return
	}

	action, repo := webhookMetadata(body)

	// Cross-check that the App that signed the webhook is the App that
	// owns this repository in config. A signed delivery for a repo owned
	// by a different App is a config drift or hostile install — fail
	// closed before any handler runs. Repo-level webhook deliveries are
	// attributed to the single shared App rather than a per-repo App, so this
	// per-repo-App ownership check does not apply to them; the repo allowlist
	// (enforced per handler) is the authorization boundary in that mode.
	if !h.repoWebhookTargeted(r) {
		if err := h.verifySignedAppOwnsRepo(repo, appName); err != nil {
			h.logger.Warn("webhook rejected: signing App does not own repo",
				"app_name", appName,
				"app_id", appID,
				"delivery_id", r.Header.Get(headerDeliveryID),
				"event", eventType,
				"action", action,
				"repo", repo,
				"error", err)
			metrics.RecordWebhookEvent(r.Context(), metricAppName(appName), eventType, action, repo, "app_repo_mismatch")
			h.writeError(w, http.StatusUnauthorized, "invalid webhook dispatch")
			return
		}
	}

	ctx, span := otel.Tracer("schemabot").Start(r.Context(), "webhook",
		trace.WithAttributes(
			attribute.String("app_name", appName),
			attribute.String("event_type", eventType),
			attribute.String("action", action),
			attribute.String("repository", repo),
		),
	)
	defer span.End()

	h.logger.Debug("webhook received",
		"app_name", appName,
		"delivery_id", r.Header.Get(headerDeliveryID),
		"event", eventType,
		"action", action,
		"repo", repo)

	metricApp := metricAppName(appName)

	// Repo-level webhook deliveries carry no installation id in the payload, so
	// resolve the shared App's installation for the repo and stash it on the
	// context for downstream handlers. Only for repos this deployment manages;
	// others are rejected by the per-handler allowlist anyway. Fail closed (500)
	// so GitHub redelivers if resolution fails transiently.
	if h.repoWebhookTargeted(r) && repo != "" && h.isRepoAllowed(repo) {
		installationID, err := h.resolveRepoWebhookInstallation(ctx, repo)
		if err != nil {
			h.logger.Error("failed to resolve installation for repo-level webhook",
				"repo", repo, "event", eventType, "action", action,
				"delivery_id", r.Header.Get(headerDeliveryID), "error", err)
			metrics.RecordWebhookEvent(ctx, metricApp, eventType, action, repo, "installation_resolution_failed")
			h.writeError(w, http.StatusInternalServerError, "failed to resolve installation for repository webhook")
			return
		}
		ctx = withResolvedInstallationID(ctx, installationID)
	}

	// Capture the handler's HTTP status so "processed" is only recorded when the
	// delivery actually succeeded. A handler that fails closed with a 5xx has
	// already emitted a specific failure status; recording "processed" too would
	// double-count the same delivery under contradictory labels.
	sw := &statusCapturingResponseWriter{ResponseWriter: w}
	recordProcessed := func() {
		if sw.statusCode() >= http.StatusInternalServerError {
			return
		}
		metrics.RecordWebhookEvent(ctx, metricApp, eventType, action, repo, "processed")
	}
	switch eventType {
	case "issue_comment":
		h.handleIssueComment(ctx, metricApp, sw, body)
		recordProcessed()
	case "check_run":
		h.handleCheckRun(ctx, sw, body)
		recordProcessed()
	case "pull_request":
		h.handlePullRequest(ctx, metricApp, sw, body, r.Header.Get(headerDeliveryID))
		recordProcessed()
	case "merge_group":
		h.handleMergeGroup(ctx, metricApp, sw, body)
		recordProcessed()
	case "push":
		h.handlePush(ctx, metricApp, sw, body)
		recordProcessed()
	default:
		h.logger.Info("webhook ignored",
			"reason", "unsupported_event_type",
			"app_name", appName,
			"delivery_id", r.Header.Get(headerDeliveryID),
			"event", eventType,
			"action", action,
			"repo", repo)
		h.writeJSON(w, http.StatusOK, map[string]string{
			"message": fmt.Sprintf("event type '%s' ignored", eventType),
		})
		metrics.RecordWebhookEvent(ctx, metricApp, eventType, action, repo, "ignored")
	}
}

// authenticateWebhook identifies which configured App a request was signed
// by and verifies the HMAC against that App's secret. Returns the resolved
// App name plus the parsed App ID (when present), a structured status for
// metrics, and ok=true when the request is authentic.
//
// Multi-App mode (webhookAppByID non-empty) requires the GitHub-supplied
// X-GitHub-Hook-Installation-Target-{ID,Type} headers and rejects unknown
// or non-integration target IDs before HMAC verification.
//
// Legacy single-secret mode verifies the request against the single
// configured secret (when set) and reports the App name as defaultAppName.
func (h *Handler) authenticateWebhook(r *http.Request, body []byte) (appName string, appID int64, status string, ok bool) {
	signature := r.Header.Get(headerSignature256)

	// Repository-level webhook dispatch: when configured, a delivery whose
	// target type is "repository" is verified against the repo-webhook secret.
	// The shared App provides API identity (its installation is resolved per
	// repo downstream); events are delivered by repo webhooks, not the App
	// webhook. Falls through to App-webhook handling for "integration"
	// deliveries so a deployment can run both during the cutover.
	if h.repoWebhookTargeted(r) {
		if !verifyHMAC(signature, body, h.repoWebhookSecret) {
			return defaultAppName, 0, "invalid_signature", false
		}
		return defaultAppName, 0, "", true
	}

	if len(h.webhookAppByID) > 0 {
		targetType := r.Header.Get(headerHookTargetType)
		if !strings.EqualFold(targetType, hookTargetTypeApp) {
			return "", 0, "invalid_target_type", false
		}
		rawID := r.Header.Get(headerHookTargetID)
		if rawID == "" {
			return "", 0, "missing_app_id", false
		}
		parsedID, err := strconv.ParseInt(rawID, 10, 64)
		if err != nil || parsedID == 0 {
			return "", 0, "invalid_app_id", false
		}
		name, found := h.webhookAppByID[parsedID]
		if !found {
			// Do NOT include the raw header value in the returned name —
			// callers feed it into a bounded metric label.
			return "", parsedID, "unknown_app_id", false
		}
		secret := h.webhookSecretsByApp[name]
		if len(secret) == 0 {
			return name, parsedID, "missing_webhook_secret", false
		}
		if !verifyHMAC(signature, body, secret) {
			return name, parsedID, "invalid_signature", false
		}
		return name, parsedID, "", true
	}

	// Legacy single-App mode.
	secret := h.webhookSecretsByApp[defaultAppName]
	if len(secret) == 0 {
		// No secret configured — preserve historical behaviour and skip
		// signature verification entirely. Operators are nudged toward
		// configuring a secret by serve.go's startup validation.
		return defaultAppName, 0, "", true
	}
	if !verifyHMAC(signature, body, secret) {
		return defaultAppName, 0, "invalid_signature", false
	}
	return defaultAppName, 0, "", true
}

// verifySignedAppOwnsRepo returns an error when the App that signed the
// webhook is not the App configured to own the given repository. Returns
// nil in legacy single-App mode (no per-repo App mapping exists) and when
// no repo could be extracted from the payload.
func (h *Handler) verifySignedAppOwnsRepo(repo, signedAppName string) error {
	if repo == "" {
		return nil
	}
	cfg := h.config()
	if cfg == nil || len(cfg.Apps) == 0 {
		return nil
	}
	expected, err := cfg.ResolveGitHubAppForRepo(repo)
	if err != nil {
		return err
	}
	if expected.Name != signedAppName {
		return fmt.Errorf("repo %q is configured to be owned by app %q but webhook was signed by app %q", repo, expected.Name, signedAppName)
	}
	return nil
}

// repoWebhookTargeted reports whether this delivery is a repository-level
// webhook that repo-webhook dispatch should handle: repo-webhook mode is
// configured and the delivery carries the "repository" hook target type.
func (h *Handler) repoWebhookTargeted(r *http.Request) bool {
	return len(h.repoWebhookSecret) > 0 &&
		strings.EqualFold(r.Header.Get(headerHookTargetType), hookTargetTypeRepo)
}

// installationIDContextKey carries an installation id resolved out of band
// (repo-webhook dispatch, whose deliveries omit the payload installation id).
type installationIDContextKey struct{}

// withResolvedInstallationID returns a context carrying an out-of-band-resolved
// installation id for downstream handlers.
func withResolvedInstallationID(ctx context.Context, id int64) context.Context {
	return context.WithValue(ctx, installationIDContextKey{}, id)
}

// effectiveInstallationID returns the payload-supplied installation id when
// present, otherwise the id resolved out of band and stashed on the context by
// repo-webhook dispatch. Returns 0 when neither is available, which callers
// already treat as a missing-installation error.
func (h *Handler) effectiveInstallationID(ctx context.Context, payloadID int64) int64 {
	if payloadID != 0 {
		return payloadID
	}
	if id, ok := ctx.Value(installationIDContextKey{}).(int64); ok {
		return id
	}
	return 0
}

// isRepoAllowed reports whether the repository is permitted by config. Mirrors
// the per-handler allowlist check so repo-webhook dispatch can skip installation
// resolution for repos this deployment does not manage.
func (h *Handler) isRepoAllowed(repo string) bool {
	return h.service == nil || h.service.Config().IsRepoAllowed(repo)
}

// resolveRepoWebhookInstallation resolves the shared App's installation id for
// repo so a repo-level webhook delivery (which carries no installation id) can
// mint an installation client downstream.
func (h *Handler) resolveRepoWebhookInstallation(ctx context.Context, repo string) (int64, error) {
	factory, err := h.factoryForRepo(repo)
	if err != nil {
		return 0, fmt.Errorf("resolve client factory for repo %s: %w", repo, err)
	}
	id, err := factory.InstallationIDForRepo(ctx, repo)
	if err != nil {
		return 0, fmt.Errorf("resolve installation for repo %s: %w", repo, err)
	}
	if id == 0 {
		return 0, fmt.Errorf("resolved installation id 0 for repo %s", repo)
	}
	return id, nil
}

// metricAppName normalizes the App name for the webhook events counter so
// rejected/unattributed deliveries surface under a bounded label rather
// than the empty string.
func metricAppName(appName string) string {
	if appName == "" {
		return "unknown"
	}
	return appName
}

// webhookMetadata extracts the "action" and repository name from a GitHub webhook payload.
func webhookMetadata(body []byte) (action, repo string) {
	var payload struct {
		Action     string `json:"action"`
		Repository struct {
			FullName string `json:"full_name"`
		} `json:"repository"`
	}
	if err := json.Unmarshal(body, &payload); err != nil {
		return "", ""
	}
	return payload.Action, payload.Repository.FullName
}

// verifyHMAC validates a GitHub-style "sha256=<hex>" signature against the
// given body and shared secret. Constant-time comparison; returns false for
// any malformed input.
func verifyHMAC(signature string, body, secret []byte) bool {
	if signature == "" {
		return false
	}
	const prefix = "sha256="
	if !strings.HasPrefix(signature, prefix) {
		return false
	}
	sigBytes, err := hex.DecodeString(signature[len(prefix):])
	if err != nil {
		return false
	}
	mac := hmac.New(sha256.New, secret)
	mac.Write(body)
	return hmac.Equal(sigBytes, mac.Sum(nil))
}

// recoverPanic recovers from panics in async goroutines, logs the stack trace,
// and posts an error comment on the PR so the user gets feedback instead of silence.
// Usage: defer h.recoverPanic(repo, pr, installationID)
func (h *Handler) recoverPanic(repo string, pr int, installationID int64) {
	if r := recover(); r != nil {
		stack := debug.Stack()
		h.logger.Error("goroutine panic", "repo", repo, "pr", pr, "installation_id", installationID, "error", r, "stack", string(stack))
		h.postComment(repo, pr, installationID,
			fmt.Sprintf("**Internal error: goroutine panic. This is a bug — please report it.**\n```\n%v\n```", r))
	}
}

// goSafe launches fn in a goroutine with panic recovery that posts an error
// comment on the PR so the user gets feedback instead of silence. The goroutine
// is counted on inProcessWebhookCount so DrainInProcessWebhookWork can wait for
// already-acked work to finish on shutdown.
//
// Registration continues during the drain as long as the count is nonzero, so
// work spawned by an in-flight tracked goroutine (a child of tracked work
// always registers while its parent is still counted) is drained too. Only
// truly-fresh work that arrives when the count is zero after the drain began
// (the delayed time.AfterFunc timer case) runs untracked: the drain has
// committed to a bounded wait and reached empty, so it will not wait for late
// timers.
func (h *Handler) goSafe(repo string, pr int, installationID int64, fn func()) {
	run := func() {
		defer h.recoverPanic(repo, pr, installationID)
		fn()
	}

	h.inProcessWebhookMu.Lock()
	if h.inProcessWebhookDraining && h.inProcessWebhookCount == 0 {
		h.inProcessWebhookMu.Unlock()
		h.logger.Warn("webhook work started after shutdown drain reached empty; running untracked and will be dropped if shutdown completes before it finishes",
			"repo", repo, "pr", pr, "installation_id", installationID)
		go run()
		return
	}
	h.inProcessWebhookCount++
	h.inProcessWebhookMu.Unlock()

	go func() {
		defer h.finishInProcessWebhookWork()
		run()
	}()
}

// finishInProcessWebhookWork decrements the in-flight count and, when it reaches
// zero while a drain is waiting, closes the drain's signal channel.
func (h *Handler) finishInProcessWebhookWork() {
	h.inProcessWebhookMu.Lock()
	h.inProcessWebhookCount--
	if h.inProcessWebhookCount == 0 && h.inProcessWebhookDrained != nil {
		close(h.inProcessWebhookDrained)
		h.inProcessWebhookDrained = nil
	}
	h.inProcessWebhookMu.Unlock()
}

// DrainInProcessWebhookWork waits for the detached goSafe goroutines to finish,
// bounded by ctx. It flips the draining flag so fresh work arriving once the
// count reaches zero runs untracked, then waits for the work in flight (and its
// transitively-spawned children) to drop the count to zero. On timeout it
// returns without waiting further; the already-acked work still running may be
// dropped when the process exits.
func (h *Handler) DrainInProcessWebhookWork(ctx context.Context) {
	h.inProcessWebhookMu.Lock()
	h.inProcessWebhookDraining = true
	if h.inProcessWebhookCount == 0 {
		h.inProcessWebhookMu.Unlock()
		h.logger.Info("in-process webhook goroutines drained")
		return
	}
	done := make(chan struct{})
	h.inProcessWebhookDrained = done
	h.inProcessWebhookMu.Unlock()

	select {
	case <-done:
		h.logger.Info("in-process webhook goroutines drained")
	case <-ctx.Done():
		h.logger.Warn("timed out draining in-process webhook goroutines; already-acked webhook work still running will be dropped on exit",
			"error", ctx.Err())
	}
}

// statusCapturingResponseWriter records the HTTP status a handler wrote so the
// dispatcher can tell a successful delivery from one that failed closed with a
// 5xx. Without it the dispatcher records "processed" unconditionally, which
// double-counts a failed delivery alongside the specific failure status the
// handler already emitted (e.g. durable_enqueue_failed).
type statusCapturingResponseWriter struct {
	http.ResponseWriter
	status int
}

func (w *statusCapturingResponseWriter) WriteHeader(status int) {
	w.status = status
	w.ResponseWriter.WriteHeader(status)
}

// statusCode returns the status the handler wrote, defaulting to 200 for
// handlers that write a body without an explicit WriteHeader.
func (w *statusCapturingResponseWriter) statusCode() int {
	if w.status == 0 {
		return http.StatusOK
	}
	return w.status
}

func (h *Handler) writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

func (h *Handler) writeError(w http.ResponseWriter, status int, message string) {
	h.writeJSON(w, status, map[string]string{"error": message})
}
