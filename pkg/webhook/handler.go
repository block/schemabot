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
// The Target-Type value "repository" is only sent for repository-level
// webhooks (registered directly on a repo, distinct from App-installed
// webhooks); SchemaBot does not handle those. The single example in
// https://docs.github.com/en/webhooks/webhook-events-and-payloads
// happens to be a repository webhook, which is a known source of
// confusion. Verified against a live SchemaBot App delivery: header
// values were Target-Type "integration" and Target-ID equal to the
// SchemaBot App ID — i.e., never the repository ID, regardless of the
// underlying event type.
const (
	headerHookTargetID   = "X-GitHub-Hook-Installation-Target-ID"
	headerHookTargetType = "X-GitHub-Hook-Installation-Target-Type"
	headerDeliveryID     = "X-GitHub-Delivery"
	headerSignature256   = "X-Hub-Signature-256"
	hookTargetTypeApp    = "integration"
	maxWebhookBodyBytes  = 10 << 20
)

// Handler processes GitHub webhook events.
type Handler struct {
	service   *api.Service
	ghClients github.ClientSet

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

	logger                     *slog.Logger
	priorEnvCheckMaxAttempts   int
	priorEnvCheckRetryInterval time.Duration
}

// NewHandler creates a new webhook handler for the legacy single-App
// configuration. The provided factory is registered in the internal
// ClientSet under the "default" App name so per-repo client resolution
// works uniformly with the multi-App path used by NewHandlerWithDispatch.
func NewHandler(service *api.Service, ghClient github.GitHubClientFactory, webhookSecret []byte, logger *slog.Logger) *Handler {
	return NewHandlerWithClientSet(service, github.NewSingleClientSet(defaultAppName, ghClient), webhookSecret, logger)
}

// NewHandlerWithClientSet creates a new webhook handler from an
// already-built single-App ClientSet. The provided webhook secret is
// associated with the defaultAppName entry and verified directly on every
// request (legacy single-secret mode).
func NewHandlerWithClientSet(service *api.Service, ghClients github.ClientSet, webhookSecret []byte, logger *slog.Logger) *Handler {
	secrets := map[string][]byte{}
	if len(webhookSecret) > 0 {
		secrets[defaultAppName] = webhookSecret
	}
	return NewHandlerWithDispatch(service, ghClients, secrets, nil, logger)
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
func NewHandlerWithDispatch(service *api.Service, ghClients github.ClientSet, webhookSecretsByApp map[string][]byte, webhookAppByID map[int64]string, logger *slog.Logger) *Handler {
	h := &Handler{
		service:                    service,
		ghClients:                  ghClients,
		webhookSecretsByApp:        maps.Clone(webhookSecretsByApp),
		webhookAppByID:             maps.Clone(webhookAppByID),
		logger:                     logger,
		priorEnvCheckMaxAttempts:   defaultPriorEnvCheckMaxAttempts,
		priorEnvCheckRetryInterval: defaultPriorEnvCheckRetryInterval,
	}

	// Register recovery callback so the scheduler can attach comment observers
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
					Logger:         logger,
					OnTerminalHook: func(a *storage.Apply) {
						updated, err := h.updateCheckRecordForApplyResult(context.Background(), apply.Repository, apply.PullRequest, a)
						if err != nil {
							logger.Error("observer: failed to update check record for recovered apply", "error", err)
							return
						}
						if !updated {
							logger.Debug("observer: skipping aggregate check update for recovered apply, apply no longer owns check state",
								"apply_id", a.ID, "apply_identifier", a.ApplyIdentifier)
							return
						}
						if ghInstClient, err := h.clientForRepo(apply.Repository, apply.InstallationID); err == nil {
							if checkRecord, err := service.Storage().Checks().Get(context.Background(), apply.Repository, apply.PullRequest, a.Environment, a.DatabaseType, a.Database); err == nil && checkRecord != nil {
								h.updateAggregateCheck(context.Background(), ghInstClient, apply.Repository, apply.PullRequest, checkRecord.HeadSHA)
							}
						}
					},
				}))
		}

	}

	return h
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
// It finds applies with a progress comment but no summary comment, then posts
// the missing summary so the PR shows the final result after a restart.
func (h *Handler) ReconcileMissingSummaryComments(ctx context.Context) {
	if h.service == nil {
		h.logger.Debug("skipping missing summary reconciliation without service")
		return
	}

	applies, err := h.service.Storage().Applies().FindMissingSummaryComment(ctx)
	if err != nil {
		h.logger.Error("failed to find applies missing summary comments", "error", err)
		return
	}

	if len(applies) == 0 {
		h.logger.Debug("no missing summary comments found")
		return
	}

	h.logger.Info("found applies missing summary comments", "count", len(applies))

	for _, apply := range applies {
		tasks, err := h.service.Storage().Tasks().GetByApplyID(ctx, apply.ID)
		if err != nil {
			h.logger.Error("failed to load tasks for missing summary reconciliation", "apply_id", apply.ApplyIdentifier, "error", err)
			continue
		}

		h.logger.Info("posting missing summary comment",
			"apply_id", apply.ApplyIdentifier,
			"repo", apply.Repository,
			"pr", apply.PullRequest,
			"state", apply.State)

		summaryBody := formatSummaryComment(apply, tasks)
		h.postAndTrackComment(ctx, apply.Repository, apply.PullRequest, apply.InstallationID, apply.ID, state.Comment.Summary, summaryBody)
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
	// closed before any handler runs.
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
		"event", eventType,
		"action", action,
		"repo", repo)

	metricApp := metricAppName(appName)

	switch eventType {
	case "issue_comment":
		h.handleIssueComment(w, body)
		metrics.RecordWebhookEvent(ctx, metricApp, eventType, action, repo, "processed")
	case "check_run":
		// Phase 2: h.handleCheckRun(w, body)
		h.writeJSON(w, http.StatusOK, map[string]string{"message": "check_run events not yet implemented"})
		metrics.RecordWebhookEvent(ctx, metricApp, eventType, action, repo, "ignored")
	case "pull_request":
		h.handlePullRequest(w, body)
		metrics.RecordWebhookEvent(ctx, metricApp, eventType, action, repo, "processed")
	default:
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
		h.logger.Error("goroutine panic", "error", r, "stack", string(stack))
		h.postComment(repo, pr, installationID,
			fmt.Sprintf("**Internal error: goroutine panic. This is a bug — please report it.**\n```\n%v\n```", r))
	}
}

// goSafe launches fn in a goroutine with panic recovery that posts an error
// comment on the PR so the user gets feedback instead of silence.
func (h *Handler) goSafe(repo string, pr int, installationID int64, fn func()) {
	go func() {
		defer h.recoverPanic(repo, pr, installationID)
		fn()
	}()
}

func (h *Handler) writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

func (h *Handler) writeError(w http.ResponseWriter, status int, message string) {
	h.writeJSON(w, status, map[string]string{"error": message})
}
