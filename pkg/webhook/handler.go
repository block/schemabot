// Package webhook handles GitHub webhook events for SchemaBot.
// It processes PR comments, check run actions, and pull request lifecycle events,
// routing them to the appropriate command handlers.
package webhook

import (
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

	"github.com/block/schemabot/pkg/api"
	"github.com/block/schemabot/pkg/github"
	"github.com/block/schemabot/pkg/metrics"
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
	return &Handler{
		service:       service,
		ghClient:      ghClient,
		webhookSecret: webhookSecret,
		logger:        logger,
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
	h.logger.Debug("webhook received", "event", eventType, "action", action, "repo", repo)

	switch eventType {
	case "issue_comment":
		h.handleIssueComment(w, body)
		metrics.RecordWebhookEvent(r.Context(), eventType, action, repo, "processed")
	case "check_run":
		// Phase 2: h.handleCheckRun(w, body)
		h.writeJSON(w, http.StatusOK, map[string]string{"message": "check_run events not yet implemented"})
		metrics.RecordWebhookEvent(r.Context(), eventType, action, repo, "ignored")
	case "pull_request":
		h.handlePullRequest(w, body)
		metrics.RecordWebhookEvent(r.Context(), eventType, action, repo, "processed")
	default:
		h.writeJSON(w, http.StatusOK, map[string]string{
			"message": fmt.Sprintf("event type '%s' ignored", eventType),
		})
		metrics.RecordWebhookEvent(r.Context(), eventType, action, repo, "ignored")
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
