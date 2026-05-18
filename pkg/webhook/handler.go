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
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"runtime/debug"
	"strings"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"

	"github.com/block/schemabot/pkg/api"
	"github.com/block/schemabot/pkg/github"
	"github.com/block/schemabot/pkg/metrics"
	"github.com/block/schemabot/pkg/state"
	"github.com/block/schemabot/pkg/storage"
)

// Handler processes GitHub webhook events.
type Handler struct {
	service       *api.Service
	ghClient      github.GitHubClientFactory
	webhookSecret []byte
	logger        *slog.Logger
}

// NewHandler creates a new webhook handler.
func NewHandler(service *api.Service, ghClient github.GitHubClientFactory, webhookSecret []byte, logger *slog.Logger) *Handler {
	h := &Handler{
		service:       service,
		ghClient:      ghClient,
		webhookSecret: webhookSecret,
		logger:        logger,
	}

	// Register recovery callback so the recovery worker can set up comment
	// observers for recovered applies. The observer is set on the LocalClient
	// so the progress poller (same goroutine as Spirit) posts comments.
	if service != nil {
		service.OnApplyRecovered = func(apply *storage.Apply) {
			if apply.Repository == "" || apply.PullRequest == 0 || apply.InstallationID == 0 {
				return
			}
			logger.Info("setting comment observer for recovered apply",
				"apply_id", apply.ApplyIdentifier,
				"repo", apply.Repository,
				"pr", apply.PullRequest)
			service.SetApplyObserver(apply.Database, apply.Deployment, apply.Environment, apply.ID,
				NewCommentObserver(CommentObserverConfig{
					GHClient:       h.ghClient,
					Storage:        service.Storage(),
					Repo:           apply.Repository,
					PR:             apply.PullRequest,
					InstallationID: apply.InstallationID,
					ApplyID:        apply.ID,
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
						if ghInstClient, err := h.ghClient.ForInstallation(apply.InstallationID); err == nil {
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
	body, err := io.ReadAll(r.Body)
	if err != nil {
		h.writeError(w, http.StatusBadRequest, "failed to read request body")
		return
	}

	// Validate webhook signature
	if len(h.webhookSecret) > 0 {
		signature := r.Header.Get("X-Hub-Signature-256")
		if !h.verifySignature(signature, body) {
			eventType := r.Header.Get("X-GitHub-Event")
			metrics.RecordWebhookEvent(r.Context(), eventType, "", "", "invalid_signature")
			h.writeError(w, http.StatusUnauthorized, "invalid webhook signature")
			return
		}
	}

	eventType := r.Header.Get("X-GitHub-Event")
	action, repo := webhookMetadata(body)

	ctx, span := otel.Tracer("schemabot").Start(r.Context(), "webhook",
		trace.WithAttributes(
			attribute.String("event_type", eventType),
			attribute.String("action", action),
			attribute.String("repository", repo),
		),
	)
	defer span.End()

	h.logger.Debug("webhook received", "event", eventType, "action", action, "repo", repo)

	switch eventType {
	case "issue_comment":
		h.handleIssueComment(w, body)
		metrics.RecordWebhookEvent(ctx, eventType, action, repo, "processed")
	case "check_run":
		// Phase 2: h.handleCheckRun(w, body)
		h.writeJSON(w, http.StatusOK, map[string]string{"message": "check_run events not yet implemented"})
		metrics.RecordWebhookEvent(ctx, eventType, action, repo, "ignored")
	case "pull_request":
		h.handlePullRequest(w, body)
		metrics.RecordWebhookEvent(ctx, eventType, action, repo, "processed")
	default:
		h.writeJSON(w, http.StatusOK, map[string]string{
			"message": fmt.Sprintf("event type '%s' ignored", eventType),
		})
		metrics.RecordWebhookEvent(ctx, eventType, action, repo, "ignored")
	}
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

// verifySignature validates the HMAC-SHA256 webhook signature.
func (h *Handler) verifySignature(signature string, body []byte) bool {
	if signature == "" {
		return false
	}

	// Signature format: "sha256=<hex>"
	prefix := "sha256="
	if !strings.HasPrefix(signature, prefix) {
		return false
	}

	sigHex := signature[len(prefix):]
	sigBytes, err := hex.DecodeString(sigHex)
	if err != nil {
		return false
	}

	mac := hmac.New(sha256.New, h.webhookSecret)
	mac.Write(body)
	expectedMAC := mac.Sum(nil)

	return hmac.Equal(sigBytes, expectedMAC)
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
