package webhook

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"strconv"
	"time"

	"github.com/block/schemabot/pkg/api"
	"github.com/block/schemabot/pkg/metrics"
	"github.com/block/schemabot/pkg/storage"
)

const (
	durableWebhookRetryDelay = time.Minute

	// maxDurableWebhookAttempts caps how many times a delivery is claimed
	// before a retryable failure is recorded as terminal. Attempts increment on
	// claim, so an unclassified permanent error (e.g. a misconfigured GitHub
	// App) cannot retry forever.
	maxDurableWebhookAttempts = 5
)

// StartDurableWebhookDispatch starts the durable webhook driver pool. The pool
// is opt-in so direct handler tests and embedders keep the legacy request-path
// behavior unless they explicitly enable durable dispatch.
func (h *Handler) StartDurableWebhookDispatch(ctx context.Context) {
	if !h.durableWebhookDispatch {
		h.logger.Debug("durable webhook dispatch disabled")
		return
	}
	store := h.webhookEventStore()
	if store == nil {
		h.logger.Warn("durable webhook dispatch disabled because webhook event storage is unavailable")
		return
	}

	h.durableWebhookMu.Lock()
	if h.durableWebhookStop != nil {
		h.durableWebhookMu.Unlock()
		h.logger.Info("durable webhook dispatch already running")
		return
	}

	driverCount := h.durableWebhookDriverCount
	if driverCount <= 0 {
		driverCount = api.DefaultDrivers
	}
	stop := make(chan struct{})
	wake := make(chan struct{}, driverCount)
	driverCtx, cancel := context.WithCancel(ctx)
	h.durableWebhookStop = stop
	h.durableWebhookCancel = cancel
	h.durableWebhookWake = wake
	h.durableWebhookMu.Unlock()

	for i := range driverCount {
		driverID := i
		h.durableWebhookWg.Go(func() {
			h.durableWebhookDriver(driverCtx, driverID, stop, wake)
		})
	}

	h.logger.Info("durable webhook dispatch started", "drivers", driverCount, "interval", h.durableWebhookPollInterval)
}

// StopDurableWebhookDispatch stops the durable webhook driver pool and waits for
// in-flight claimed deliveries to finish their current drive.
func (h *Handler) StopDurableWebhookDispatch() {
	h.durableWebhookMu.Lock()
	if h.durableWebhookStop == nil {
		h.durableWebhookMu.Unlock()
		return
	}
	stop := h.durableWebhookStop
	cancel := h.durableWebhookCancel
	h.durableWebhookStop = nil
	h.durableWebhookCancel = nil
	h.durableWebhookWake = nil
	h.durableWebhookMu.Unlock()

	close(stop)
	if cancel != nil {
		cancel()
	}
	h.durableWebhookWg.Wait()
	h.logger.Info("durable webhook dispatch stopped")
}

func (h *Handler) wakeDurableWebhookDispatch() {
	h.durableWebhookMu.Lock()
	wake := h.durableWebhookWake
	h.durableWebhookMu.Unlock()
	if wake == nil {
		return
	}
	select {
	case wake <- struct{}{}:
	default:
	}
}

func (h *Handler) durableWebhookDriver(ctx context.Context, driverID int, stop <-chan struct{}, wake <-chan struct{}) {
	ticker := time.NewTicker(h.durableWebhookPollInterval)
	defer ticker.Stop()

	h.logger.Debug("durable webhook driver started", "driver", driverID)
	h.driveNextDurableWebhook(ctx, driverID)

	for {
		select {
		case <-stop:
			h.logger.Debug("durable webhook driver stopping", "driver", driverID)
			return
		case <-ctx.Done():
			h.logger.Debug("durable webhook driver context cancelled", "driver", driverID)
			return
		case <-wake:
			h.logger.Debug("durable webhook driver woke for queued delivery", "driver", driverID)
			h.driveNextDurableWebhook(ctx, driverID)
		case <-ticker.C:
			h.driveNextDurableWebhook(ctx, driverID)
		}
	}
}

func (h *Handler) driveNextDurableWebhook(ctx context.Context, driverID int) {
	store := h.webhookEventStore()
	if store == nil {
		h.logger.Warn("durable webhook driver cannot claim because webhook event storage is unavailable", "driver", driverID)
		return
	}
	owner := durableWebhookLeaseOwner(driverID)
	event, err := store.FindNext(ctx, owner, h.durableWebhookLeaseDuration)
	if err != nil {
		h.logger.Error("durable webhook driver failed to claim delivery", "driver", driverID, "lease_owner", owner, "error", err)
		return
	}
	if event == nil {
		h.logger.Debug("durable webhook driver found no delivery to claim", "driver", driverID)
		return
	}

	h.logger.Info("durable webhook driver claimed delivery",
		"driver", driverID,
		"lease_owner", owner,
		"provider", event.Provider,
		"delivery_id", event.DeliveryID,
		"event", event.Event,
		"action", event.Action,
		"repo", event.Repository,
		"pr", event.PullRequest,
		"head_sha", event.HeadSHA,
		"attempts", event.Attempts)

	runCtx, cancelRun := context.WithCancel(ctx)
	stopHeartbeat := h.startDurableWebhookHeartbeat(runCtx, driverID, event, cancelRun)
	process := h.processDurableWebhookEvent
	if h.durableWebhookProcessOverride != nil {
		process = h.durableWebhookProcessOverride
	}
	retry, processErr := process(runCtx, event)
	heartbeatErr := stopHeartbeat()
	cancelRun()

	finishCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
	defer cancel()
	if processErr != nil {
		retryAfter := (*time.Time)(nil)
		if retry && event.Attempts < maxDurableWebhookAttempts {
			due := time.Now().Add(durableWebhookRetryDelay)
			retryAfter = &due
		} else if retry {
			h.logger.Error("durable webhook delivery exhausted its retry budget and is now terminal",
				"driver", driverID, "delivery_id", event.DeliveryID, "event", event.Event,
				"action", event.Action, "repo", event.Repository, "pr", event.PullRequest,
				"attempts", event.Attempts, "error", processErr)
		}
		if err := store.MarkFailed(finishCtx, event.ID, event.LeaseToken, processErr.Error(), retryAfter); err != nil {
			if errors.Is(err, storage.ErrWebhookEventLeaseLost) {
				h.logger.Warn("durable webhook driver lost the delivery lease before recording failure; another driver owns the delivery",
					"driver", driverID, "delivery_id", event.DeliveryID, "event", event.Event,
					"repo", event.Repository, "pr", event.PullRequest)
				return
			}
			h.logger.Error("durable webhook driver failed to record delivery failure",
				"driver", driverID, "delivery_id", event.DeliveryID, "event", event.Event,
				"repo", event.Repository, "pr", event.PullRequest, "error", err)
			return
		}
		h.logger.Warn("durable webhook driver recorded delivery failure",
			"driver", driverID, "delivery_id", event.DeliveryID, "event", event.Event,
			"action", event.Action, "repo", event.Repository, "pr", event.PullRequest,
			"retry", retryAfter != nil, "error", processErr)
		return
	}
	if heartbeatErr != nil {
		// Processing reported success but the lease heartbeat failed, so
		// ownership is uncertain: the row may already be re-claimed, or the
		// heartbeat cancellation may have interrupted the work mid-flight. Do
		// not mark it completed — leave the row processing so lease expiry
		// hands it to another driver. Re-processing an already-planned delivery
		// re-runs auto-plan on the same head SHA, which is safe.
		h.logger.Warn("durable webhook driver skipped completion because the delivery lease heartbeat failed; leaving delivery for reclaim",
			"driver", driverID, "delivery_id", event.DeliveryID, "event", event.Event,
			"repo", event.Repository, "pr", event.PullRequest, "error", heartbeatErr)
		return
	}
	if err := store.MarkCompleted(finishCtx, event.ID, event.LeaseToken); err != nil {
		if errors.Is(err, storage.ErrWebhookEventLeaseLost) {
			h.logger.Warn("durable webhook driver lost the delivery lease before recording completion; another driver owns the delivery",
				"driver", driverID, "delivery_id", event.DeliveryID, "event", event.Event,
				"repo", event.Repository, "pr", event.PullRequest)
			return
		}
		h.logger.Error("durable webhook driver failed to mark delivery completed",
			"driver", driverID, "delivery_id", event.DeliveryID, "event", event.Event,
			"repo", event.Repository, "pr", event.PullRequest, "error", err)
		return
	}
	h.logger.Info("durable webhook driver completed delivery",
		"driver", driverID, "delivery_id", event.DeliveryID, "event", event.Event,
		"action", event.Action, "repo", event.Repository, "pr", event.PullRequest)
}

// startDurableWebhookHeartbeat extends the delivery lease on a fixed cadence
// while the event is processed. On a heartbeat failure it cancels the run
// context so in-flight work stops. The returned join function stops the
// heartbeat and reports the heartbeat failure (nil when the lease was held for
// the whole run), so the driver can refuse to complete a delivery it may no
// longer own.
func (h *Handler) startDurableWebhookHeartbeat(ctx context.Context, driverID int, event *storage.WebhookEvent, cancelRun context.CancelFunc) func() error {
	hbCtx, stop := context.WithCancel(ctx)
	done := make(chan struct{})
	var heartbeatErr error // written once before close(done)
	interval := h.durableWebhookLeaseDuration / 3
	if interval <= 0 {
		interval = 10 * time.Second
	}
	go func() {
		defer close(done)
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		for {
			select {
			case <-hbCtx.Done():
				return
			case <-ticker.C:
				if err := h.webhookEventStore().Heartbeat(hbCtx, event.ID, event.LeaseToken, h.durableWebhookLeaseDuration); err != nil {
					if hbCtx.Err() != nil {
						// The heartbeat was stopped intentionally mid-call; the
						// interrupted call is not a lease failure.
						return
					}
					h.logger.Warn("durable webhook heartbeat lost delivery lease; driver will stop",
						"driver", driverID, "delivery_id", event.DeliveryID, "event", event.Event,
						"repo", event.Repository, "pr", event.PullRequest, "error", err)
					heartbeatErr = err
					cancelRun()
					return
				}
			}
		}
	}()
	return func() error {
		stop()
		<-done
		return heartbeatErr
	}
}

func (h *Handler) processDurableWebhookEvent(ctx context.Context, event *storage.WebhookEvent) (retry bool, err error) {
	if event.Provider != storage.WebhookProviderGitHub {
		h.logger.Info("durable webhook delivery ignored because provider is unsupported",
			"delivery_id", event.DeliveryID, "provider", event.Provider, "event", event.Event,
			"action", event.Action, "repo", event.Repository, "pr", event.PullRequest)
		return false, nil
	}
	switch event.Event {
	case "pull_request":
		return h.processDurablePullRequest(ctx, event)
	default:
		h.logger.Info("durable webhook delivery ignored because event type is unsupported",
			"delivery_id", event.DeliveryID, "event", event.Event, "action", event.Action,
			"repo", event.Repository, "pr", event.PullRequest)
		return false, nil
	}
}

// processDurablePullRequest drives the auto-plan flow for a claimed
// pull_request delivery. The durability contract of this slice covers config
// discovery and plan dispatch: per-database plan execution still runs in
// detached goroutines (handleMultiEnvPlan via goSafe), exactly as on the
// request path, and its durability is owned by the applies/tasks layer once
// plans are created.
func (h *Handler) processDurablePullRequest(ctx context.Context, event *storage.WebhookEvent) (retry bool, err error) {
	var payload pullRequestPayload
	if err := json.Unmarshal(event.Payload, &payload); err != nil {
		return false, fmt.Errorf("decode durable pull_request delivery %s: %w", event.DeliveryID, err)
	}

	// The HTTP handler only enqueues auto-plannable actions, but rows can also
	// come from replay or future producers; re-validate so a replayed "closed"
	// action can never trigger an auto-plan.
	switch payload.Action {
	case "opened", "synchronize", "reopened":
	default:
		h.logger.Info("durable pull_request delivery ignored because action is not auto-plannable",
			"delivery_id", event.DeliveryID, "action", payload.Action,
			"repo", event.Repository, "pr", event.PullRequest)
		return false, nil
	}

	installationID, err := strconv.ParseInt(event.TenantID, 10, 64)
	if err != nil || installationID == 0 {
		installationID = h.effectiveInstallationID(ctx, payload.Installation.ID)
	}
	if installationID == 0 {
		return false, fmt.Errorf("durable pull_request delivery %s missing installation ID", event.DeliveryID)
	}

	repo := payload.Repository.FullName
	pr := payload.PullRequest.Number
	headSHA := payload.PullRequest.Head.SHA
	if repo == "" || pr == 0 || headSHA == "" {
		return false, fmt.Errorf("durable pull_request delivery %s missing repo, PR, or head SHA", event.DeliveryID)
	}
	if h.service != nil && !h.service.Config().IsRepoAllowed(repo) {
		h.logger.Warn("durable pull_request delivery from unregistered repository",
			"delivery_id", event.DeliveryID, "action", payload.Action, "repo", repo, "pr", pr,
			"installation_id", installationID)
		metrics.RecordUnregisteredRepositoryWebhook(ctx, "unknown", "pull_request", payload.Action, repo)
		return false, nil
	}

	ctx, cancel, client, err := h.autoPlanBootstrap(ctx, repo, installationID)
	if err != nil {
		return true, fmt.Errorf("bootstrap durable auto-plan delivery %s for %s#%d: %w", event.DeliveryID, repo, pr, err)
	}
	defer cancel()

	message, planErr := h.runAutoPlanForPR(ctx, client, repo, pr, headSHA, installationID, "pull_request", payload.Action, payload.Before, event.DeliveryID)
	if planErr != nil {
		return true, fmt.Errorf("auto-plan for durable delivery %s (%s#%d): %w", event.DeliveryID, repo, pr, planErr)
	}
	if ctxErr := ctx.Err(); ctxErr != nil {
		// The run was cancelled (shutdown or lease loss) — the dispatch may be
		// partial, so keep the delivery retryable instead of completing it.
		return true, fmt.Errorf("durable delivery %s run cancelled during auto-plan dispatch: %w", event.DeliveryID, ctxErr)
	}
	h.logger.Info("durable pull_request auto-plan dispatched",
		"action", payload.Action, "repo", repo, "pr", pr, "head_sha", headSHA,
		"delivery_id", event.DeliveryID, "message", message)
	return false, nil
}

func (h *Handler) enqueueDurablePullRequest(ctx context.Context, payload pullRequestPayload, body []byte, deliveryID string, installationID int64) (bool, error) {
	store := h.webhookEventStore()
	if store == nil {
		return false, fmt.Errorf("webhook event storage is unavailable")
	}
	if deliveryID == "" {
		return false, fmt.Errorf("missing GitHub delivery ID")
	}
	inserted, err := store.Create(ctx, &storage.WebhookEvent{
		Provider:    storage.WebhookProviderGitHub,
		DeliveryID:  deliveryID,
		Event:       "pull_request",
		Action:      payload.Action,
		Repository:  payload.Repository.FullName,
		PullRequest: payload.PullRequest.Number,
		HeadSHA:     payload.PullRequest.Head.SHA,
		TenantID:    strconv.FormatInt(installationID, 10),
		Payload:     body,
	})
	if err != nil {
		return false, fmt.Errorf("store durable pull_request delivery %s: %w", deliveryID, err)
	}
	if inserted {
		h.wakeDurableWebhookDispatch()
	}
	return inserted, nil
}

func (h *Handler) webhookEventStore() storage.WebhookEventStore {
	if h.service == nil || h.service.Storage() == nil {
		return nil
	}
	return h.service.Storage().WebhookEvents()
}

func durableWebhookLeaseOwner(driverID int) string {
	hostname, err := os.Hostname()
	if err != nil || hostname == "" {
		hostname = "unknown-host"
	}
	return fmt.Sprintf("%s/%d/webhook-driver-%d", hostname, os.Getpid(), driverID)
}
