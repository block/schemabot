package webhook

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"runtime/debug"
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
	// App) cannot retry forever. It aliases the store's claim ceiling so the
	// driver records a retryable failure as terminal on the same attempt at
	// which FindNext would stop handing the delivery out.
	maxDurableWebhookAttempts = storage.MaxWebhookEventAttempts
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
		// Only the driver pool is skipped here — durable dispatch stays enabled,
		// so the ingest path still routes deliveries through the durable branch
		// and rejects them with 500 until storage recovers. This is not a
		// fallback to in-process handling.
		h.logger.Warn("durable webhook driver pool not started: webhook event storage is unavailable; incoming deliveries will be rejected with 500 until storage recovers")
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
	// Register every driver goroutine on the WaitGroup while the mutex is still
	// held. Adding to the WaitGroup concurrently with a StopDurableWebhookDispatch
	// Wait is the documented reuse misuse; holding the lock across the spawn keeps
	// Start and Stop from racing.
	for i := range driverCount {
		driverID := i
		h.durableWebhookWg.Go(func() {
			h.durableWebhookDriver(driverCtx, driverID, stop, wake)
		})
	}
	h.durableWebhookMu.Unlock()

	h.logger.Info("durable webhook dispatch started", "drivers", driverCount, "interval", h.durableWebhookPollInterval)
}

// StopDurableWebhookDispatch stops the durable webhook driver pool and waits for
// in-flight claimed deliveries to finish their current drive.
func (h *Handler) StopDurableWebhookDispatch() {
	h.durableWebhookMu.Lock()
	if h.durableWebhookStop == nil {
		h.durableWebhookMu.Unlock()
		h.logger.Debug("durable webhook dispatch stop requested but pool is not running")
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
		// The pool isn't running (never started, or stopped): drivers reclaim on
		// the poll ticker, so a missed wake only delays pickup, it doesn't drop
		// the durable row.
		h.logger.Debug("durable webhook wake skipped because the driver pool is not running")
		return
	}
	select {
	case wake <- struct{}{}:
	default:
	}
}

func (h *Handler) durableWebhookDriver(ctx context.Context, driverID int, stop <-chan struct{}, wake <-chan struct{}) {
	// The lease owner is stable for the driver's lifetime; compute it once rather
	// than re-deriving hostname/pid on every claim.
	owner := durableWebhookLeaseOwner(driverID)
	ticker := time.NewTicker(h.durableWebhookPollInterval)
	defer ticker.Stop()

	h.logger.Debug("durable webhook driver started", "driver", driverID, "lease_owner", owner)
	h.drainDurableWebhooks(ctx, driverID, owner)

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
			h.drainDurableWebhooks(ctx, driverID, owner)
		case <-ticker.C:
			h.drainDurableWebhooks(ctx, driverID, owner)
		}
	}
}

// drainDurableWebhooks claims and drives deliveries until none remain claimable,
// so a backlog is worked down within a single wake/tick instead of one delivery
// per signal. It stops on the first empty claim or claim error — a storage error
// must not spin a tight loop — and on context cancellation.
func (h *Handler) drainDurableWebhooks(ctx context.Context, driverID int, owner string) {
	for {
		if ctx.Err() != nil {
			return
		}
		if !h.driveNextDurableWebhook(ctx, driverID, owner) {
			return
		}
	}
}

// driveNextDurableWebhook claims and drives at most one delivery. It reports
// whether a delivery was claimed, so the drain loop knows whether to continue.
func (h *Handler) driveNextDurableWebhook(ctx context.Context, driverID int, owner string) (claimed bool) {
	store := h.webhookEventStore()
	if store == nil {
		h.logger.Warn("durable webhook driver cannot claim because webhook event storage is unavailable", "driver", driverID)
		return false
	}
	event, err := store.FindNext(ctx, owner, h.durableWebhookLeaseDuration)
	if err != nil {
		h.logger.Error("durable webhook driver failed to claim delivery", "driver", driverID, "lease_owner", owner, "error", err)
		return false
	}
	if event == nil {
		h.logger.Debug("durable webhook driver found no delivery to claim", "driver", driverID)
		return false
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

	h.driveClaimedDurableWebhook(ctx, driverID, store, event)
	return true
}

// driveClaimedDurableWebhook runs the process → heartbeat → finish lifecycle for
// a freshly claimed delivery.
func (h *Handler) driveClaimedDurableWebhook(ctx context.Context, driverID int, store storage.WebhookEventStore, event *storage.WebhookEvent) {
	runCtx, cancelRun := context.WithCancel(ctx)
	stopHeartbeat := h.startDurableWebhookHeartbeat(runCtx, driverID, event, cancelRun)
	process := h.processDurableWebhookEvent
	if h.durableWebhookProcessOverride != nil {
		process = h.durableWebhookProcessOverride
	}
	retry, processErr := h.safeProcessDurableWebhookEvent(runCtx, event, process)
	heartbeatErr := stopHeartbeat()
	cancelRun()

	finishCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
	defer cancel()
	if processErr != nil {
		if heartbeatErr == nil && ctx.Err() != nil && errors.Is(processErr, context.Canceled) {
			// Shutdown (not lease loss) cancelled the run mid-flight. The claim
			// consumed an attempt when FindNext incremented the counter, but no
			// real processing happened — refund it, or deploy-churn restarts
			// that each claim-and-cancel the same delivery would terminally
			// fail it without a single genuine attempt.
			if err := store.Release(finishCtx, event.ID, event.LeaseToken); err != nil {
				h.logger.Warn("durable webhook driver could not release delivery claim on shutdown; lease expiry will hand it to another driver",
					"driver", driverID, "delivery_id", event.DeliveryID, "event", event.Event,
					"repo", event.Repository, "pr", event.PullRequest, "error", err)
				return
			}
			h.logger.Info("durable webhook driver released delivery claim on shutdown",
				"driver", driverID, "delivery_id", event.DeliveryID, "event", event.Event,
				"repo", event.Repository, "pr", event.PullRequest)
			return
		}
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
			if errors.Is(err, storage.ErrWebhookEventLeaseLost) || errors.Is(err, storage.ErrWebhookEventNotFound) {
				h.logger.Warn("durable webhook driver lost the delivery lease before recording failure; another driver owns the delivery or the row is gone",
					"driver", driverID, "delivery_id", event.DeliveryID, "event", event.Event,
					"repo", event.Repository, "pr", event.PullRequest)
				return
			}
			h.logger.Error("durable webhook driver failed to record delivery failure",
				"driver", driverID, "delivery_id", event.DeliveryID, "event", event.Event,
				"repo", event.Repository, "pr", event.PullRequest, "error", err)
			return
		}
		status := "durable_dispatch_failed"
		if retryAfter != nil {
			status = "durable_dispatch_retrying"
		}
		metrics.RecordWebhookEvent(finishCtx, h.metricAppForRepo(event.Repository), event.Event, event.Action, event.Repository, status)
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
		if errors.Is(err, storage.ErrWebhookEventLeaseLost) || errors.Is(err, storage.ErrWebhookEventNotFound) {
			h.logger.Warn("durable webhook driver lost the delivery lease before recording completion; another driver owns the delivery or the row is gone",
				"driver", driverID, "delivery_id", event.DeliveryID, "event", event.Event,
				"repo", event.Repository, "pr", event.PullRequest)
			return
		}
		h.logger.Error("durable webhook driver failed to mark delivery completed",
			"driver", driverID, "delivery_id", event.DeliveryID, "event", event.Event,
			"repo", event.Repository, "pr", event.PullRequest, "error", err)
		return
	}
	metrics.RecordWebhookEvent(finishCtx, h.metricAppForRepo(event.Repository), event.Event, event.Action, event.Repository, "durable_dispatch_completed")
	h.logger.Info("durable webhook driver completed delivery",
		"driver", driverID, "delivery_id", event.DeliveryID, "event", event.Event,
		"action", event.Action, "repo", event.Repository, "pr", event.PullRequest)
}

// safeProcessDurableWebhookEvent runs process with panic recovery. The legacy
// request path ran auto-plan under goSafe/recoverPanic; a driver panic here
// would kill the whole process, and — because the row stays processing until
// its lease expires and is then claimable again — every restarted replica
// would reclaim the same poison delivery and crash-loop the fleet. A recovered
// panic is reported as a retryable failure, so the normal attempt cap makes a
// deterministic panic terminal instead.
func (h *Handler) safeProcessDurableWebhookEvent(ctx context.Context, event *storage.WebhookEvent, process func(context.Context, *storage.WebhookEvent) (bool, error)) (retry bool, err error) {
	defer func() {
		if r := recover(); r != nil {
			h.logger.Error("durable webhook driver recovered from panic while processing delivery",
				"delivery_id", event.DeliveryID, "event", event.Event, "action", event.Action,
				"repo", event.Repository, "pr", event.PullRequest,
				"panic", fmt.Sprintf("%v", r), "stack", string(debug.Stack()))
			retry = true
			err = fmt.Errorf("panic processing durable webhook delivery %s: %v", event.DeliveryID, r)
		}
	}()
	return process(ctx, event)
}

// metricAppForRepo resolves the metrics app_name label for repo. Driver work
// runs outside any HTTP request, so the signed-App identity from webhook
// verification is gone; derive the owning App from config the same way
// signature/ownership verification does. Single-App deployments have no
// per-repo App mapping and always report the default App name.
func (h *Handler) metricAppForRepo(repo string) string {
	cfg := h.config()
	if cfg == nil || len(cfg.Apps) == 0 {
		return defaultAppName
	}
	app, err := cfg.ResolveGitHubAppForRepo(repo)
	if err != nil {
		// A multi-App deployment that cannot map the repo to an App still needs a
		// metric label; fall back to the default rather than dropping the metric,
		// but log so a misconfigured mapping does not silently mislabel telemetry.
		h.logger.Warn("could not resolve owning GitHub App for durable webhook metrics; using default app label",
			"repo", repo, "error", err)
		return metricAppName("")
	}
	return app.Name
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
						h.logger.Debug("durable webhook heartbeat interrupted by intentional stop; not a lease failure",
							"driver", driverID, "delivery_id", event.DeliveryID)
						return
					}
					if errors.Is(err, storage.ErrWebhookEventLeaseLost) || errors.Is(err, storage.ErrWebhookEventNotFound) {
						// Lease loss and a deleted row are both terminal for
						// this run: the result has nowhere to land, so stop
						// instead of finishing work that cannot be recorded.
						h.logger.Warn("durable webhook heartbeat lost delivery lease or the delivery row is gone; driver will stop",
							"driver", driverID, "delivery_id", event.DeliveryID, "event", event.Event,
							"repo", event.Repository, "pr", event.PullRequest, "error", err)
						heartbeatErr = err
						cancelRun()
						return
					}
					// A transient store error is not lease loss: the lease is
					// still ours until it expires, so keep working and retry on
					// the next tick. If the blip outlasts the lease and a peer
					// claims the row, the next heartbeat returns
					// ErrWebhookEventLeaseLost and stops the run.
					h.logger.Warn("durable webhook heartbeat failed; will retry",
						"driver", driverID, "delivery_id", event.DeliveryID, "event", event.Event,
						"repo", event.Repository, "pr", event.PullRequest, "error", err)
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
	if !isAutoPlannablePullRequestAction(payload.Action) {
		h.logger.Info("durable pull_request delivery ignored because action is not auto-plannable",
			"delivery_id", event.DeliveryID, "action", payload.Action,
			"repo", event.Repository, "pr", event.PullRequest)
		return false, nil
	}

	installationID, parseErr := strconv.ParseInt(event.TenantID, 10, 64)
	if parseErr != nil {
		// A non-numeric TenantID is a corrupted/enqueue-side bug, distinct from a
		// legitimately empty tenant; surface it rather than silently folding it
		// into the payload fallback below.
		h.logger.Warn("durable pull_request delivery has an unparseable tenant ID; falling back to the payload installation",
			"delivery_id", event.DeliveryID, "tenant_id", event.TenantID,
			"repo", event.Repository, "pr", event.PullRequest, "error", parseErr)
	}
	if parseErr != nil || installationID == 0 {
		// Fallback for rows without a stored tenant. Today the only producer
		// (enqueueDurablePullRequest) always stores a resolved non-zero
		// installation ID, so this only matters for replay or future producers.
		// The payload carries an installation ID only for org/user App installs;
		// repo-level deliveries have none, so a future producer that synthesizes
		// such rows (WH-6b) must persist a resolved installation ID in TenantID
		// rather than relying on this fallback, or repo-level deliveries will
		// terminally fail here.
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
		metrics.RecordUnregisteredRepositoryWebhook(ctx, h.metricAppForRepo(repo), "pull_request", payload.Action, repo)
		return false, nil
	}

	// Keep the driver's run context: autoPlanBootstrap rebinds ctx to a
	// timeout-bounded work context, and the post-run cancellation check below
	// must distinguish "shutdown or lease loss" (runCtx) from "the work outran
	// its own timeout after already succeeding" (ctx).
	runCtx := ctx
	ctx, cancel, client, err := h.autoPlanBootstrap(ctx, repo, installationID)
	if err != nil {
		metrics.RecordWebhookEvent(runCtx, h.metricAppForRepo(repo), "pull_request", payload.Action, repo, "auto_plan_bootstrap_failed")
		return true, fmt.Errorf("bootstrap durable auto-plan delivery %s for %s#%d: %w", event.DeliveryID, repo, pr, err)
	}
	defer cancel()

	message, planErr := h.runAutoPlanForPR(ctx, client, repo, pr, headSHA, installationID, "pull_request", payload.Action, payload.Before, event.DeliveryID)
	if planErr != nil {
		return true, fmt.Errorf("auto-plan for durable delivery %s (%s#%d): %w", event.DeliveryID, repo, pr, planErr)
	}
	if ctxErr := runCtx.Err(); ctxErr != nil {
		// The run was cancelled (shutdown or lease loss) — the dispatch may be
		// partial, so keep the delivery retryable instead of completing it.
		// Checking the bootstrap-bounded ctx here would misclassify a slow but
		// fully successful run as a failure and re-plan it on retry.
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
