package api

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"slices"
	"sort"
	"strings"
	"time"

	"github.com/bradleyfalzon/ghinstallation/v2"
	gh "github.com/google/go-github/v86/github"

	"github.com/block/schemabot/pkg/apitypes"
	ghclient "github.com/block/schemabot/pkg/github"
)

const (
	webhookRedrivePageSize = 100
	webhookRedriveDelay    = 150 * time.Millisecond

	// webhookOpsRequestTimeout bounds the webhook operator endpoints, which
	// crawl GitHub delivery history or every open PR and routinely need far
	// longer than the server-wide write timeout allows.
	webhookOpsRequestTimeout = 15 * time.Minute

	// webhookRedriveMaxPagesPerRequest bounds one redrive request's listing so
	// each HTTP request finishes in seconds; callers continue via the cursor.
	webhookRedriveMaxPagesPerRequest = 25
)

// webhookOpsRequestError marks a failure caused by the request itself
// (validation, unknown app name), as opposed to a server-side or GitHub-side
// failure while executing it — the two need different HTTP status codes so
// operators know whether to fix the request or investigate the server.
type webhookOpsRequestError struct{ err error }

func (e *webhookOpsRequestError) Error() string { return e.err.Error() }
func (e *webhookOpsRequestError) Unwrap() error { return e.err }

func webhookOpsRequestErrorf(format string, args ...any) error {
	return &webhookOpsRequestError{err: fmt.Errorf(format, args...)}
}

// extendWebhookOpsDeadline lifts the server-wide write timeout for a webhook
// operator request and returns a context bounded to the same budget, so the
// crawl can outlive the default timeout without running unbounded.
func (s *Service) extendWebhookOpsDeadline(w http.ResponseWriter, r *http.Request) (context.Context, context.CancelFunc) {
	if err := http.NewResponseController(w).SetWriteDeadline(time.Now().Add(webhookOpsRequestTimeout)); err != nil {
		s.logger.Warn("failed to extend webhook ops write deadline; the server-wide write timeout still applies",
			"path", r.URL.Path, "error", err)
	}
	return context.WithTimeout(r.Context(), webhookOpsRequestTimeout)
}

func (s *Service) writeWebhookOpsError(w http.ResponseWriter, err error) {
	var reqErr *webhookOpsRequestError
	if errors.As(err, &reqErr) {
		s.writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	s.writeError(w, http.StatusInternalServerError, err.Error())
}

type WebhookRedriveRequest = apitypes.WebhookRedriveRequest
type WebhookRedriveResponse = apitypes.WebhookRedriveResponse
type WebhookRedriveResult = apitypes.WebhookRedriveResult
type WebhookRedriveSelection = apitypes.WebhookRedriveSelection

type webhookRedriveDeliveryClient interface {
	ListHookDeliveries(ctx context.Context, opts *gh.ListCursorOptions) ([]*gh.HookDelivery, *gh.Response, error)
	GetHookDelivery(ctx context.Context, deliveryID int64) (*gh.HookDelivery, *gh.Response, error)
	RedeliverHookDelivery(ctx context.Context, deliveryID int64) (*gh.HookDelivery, *gh.Response, error)
}

type webhookRedriveApp struct {
	name   string
	id     int64
	config GitHubAppConfig
}

func (s *Service) handleWebhookRedrive(w http.ResponseWriter, r *http.Request) {
	var req WebhookRedriveRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		s.writeBodyDecodeError(w, err)
		return
	}
	ctx, cancel := s.extendWebhookOpsDeadline(w, r)
	defer cancel()
	response, err := executeWebhookRedrive(ctx, s.config, req, s.logger)
	if err != nil {
		s.writeWebhookOpsError(w, err)
		return
	}
	s.writeJSON(w, http.StatusOK, response)
}

func executeWebhookRedrive(ctx context.Context, cfg *ServerConfig, req WebhookRedriveRequest, logger *slog.Logger) (*WebhookRedriveResponse, error) {
	if req.MaxPages <= 0 {
		return nil, webhookOpsRequestErrorf("max_pages must be positive")
	}
	windowStart, err := time.Parse(time.RFC3339, req.WindowStart)
	if err != nil {
		return nil, webhookOpsRequestErrorf("parse window_start %q as RFC3339: %v", req.WindowStart, err)
	}
	windowEnd, err := time.Parse(time.RFC3339, req.WindowEnd)
	if err != nil {
		return nil, webhookOpsRequestErrorf("parse window_end %q as RFC3339: %v", req.WindowEnd, err)
	}
	if windowEnd.Before(windowStart) {
		return nil, webhookOpsRequestErrorf("window_end must be at or after window_start")
	}
	if req.PR < 0 {
		return nil, webhookOpsRequestErrorf("pr must be non-negative; 0 disables PR filtering")
	}
	if req.Cursor != "" && req.App == "" {
		return nil, webhookOpsRequestErrorf("cursor continuation requires app to be set")
	}
	apps, err := webhookRedriveApps(cfg, req.App)
	if err != nil {
		return nil, err
	}
	maxPages := min(req.MaxPages, webhookRedriveMaxPagesPerRequest)
	if logger == nil {
		logger = slog.New(slog.NewJSONHandler(os.Stderr, nil))
	}
	response := &WebhookRedriveResponse{}
	for _, app := range apps {
		client, err := newWebhookRedriveGitHubClient(app, logger)
		if err != nil {
			return nil, err
		}
		result, err := redriveWebhookAppDeliveries(ctx, client.Apps, app.name, req.Cursor, windowStart, windowEnd, maxPages, req.DryRun, req.Repo, req.PR, logger)
		if err != nil {
			return nil, err
		}
		response.Results = append(response.Results, result)
	}
	return response, nil
}

func webhookRedriveApps(cfg *ServerConfig, onlyApp string) ([]webhookRedriveApp, error) {
	if cfg == nil {
		return nil, fmt.Errorf("server config is nil")
	}
	configured := cfg.Apps
	if len(configured) == 0 {
		if !cfg.GitHub.Configured() {
			return nil, fmt.Errorf("no GitHub App is configured")
		}
		configured = map[string]GitHubAppConfig{"default": cfg.GitHub}
	}
	names := make([]string, 0, len(configured))
	for name := range configured {
		names = append(names, name)
	}
	sort.Strings(names)
	apps := make([]webhookRedriveApp, 0, len(names))
	for _, name := range names {
		if onlyApp != "" && name != onlyApp {
			continue
		}
		appConfig := configured[name]
		appID := appConfig.ResolveAppID()
		if appID == 0 {
			return nil, fmt.Errorf("app %q has empty or unparseable app-id", name)
		}
		apps = append(apps, webhookRedriveApp{name: name, id: appID, config: appConfig})
	}
	if len(apps) == 0 {
		return nil, webhookOpsRequestErrorf("no configured GitHub App named %q", onlyApp)
	}
	return apps, nil
}

func newWebhookRedriveGitHubClient(app webhookRedriveApp, logger *slog.Logger) (*gh.Client, error) {
	privateKey, err := app.config.ResolvePrivateKey()
	if err != nil {
		return nil, fmt.Errorf("resolve private key for app %q: %w", app.name, err)
	}
	if privateKey == "" {
		return nil, fmt.Errorf("app %q private key resolved to empty value", app.name)
	}
	appsTransport, err := ghinstallation.NewAppsTransport(http.DefaultTransport, app.id, []byte(privateKey))
	if err != nil {
		return nil, fmt.Errorf("create GitHub App transport for app %q: %w", app.name, err)
	}
	// A crawl issues many list/detail/redeliver calls per run, so wrap the
	// transport with the shared secondary-rate-limit handling (bounded sleep
	// on a 403 secondary limit) that every other GitHub client here uses,
	// rather than issuing bare requests that would trip the limit mid-crawl.
	client := gh.NewClient(&http.Client{Transport: ghclient.NewRateLimitedTransport(appsTransport), Timeout: 30 * time.Second})
	logger.Info("created GitHub App webhook redrive client", "app_name", app.name, "app_id", app.id)
	return client, nil
}

func redriveWebhookAppDeliveries(ctx context.Context, client webhookRedriveDeliveryClient, appName, startCursor string, windowStart, windowEnd time.Time, maxPages int, dryRun bool, repo string, pr int, logger *slog.Logger) (WebhookRedriveResult, error) {
	result := WebhookRedriveResult{AppName: appName, DryRun: dryRun}
	cursor := startCursor
	historyExhausted := false
	// A GitHub delivery record is immutable: a failed delivery stays "failed"
	// forever, and a redelivery is a new record sharing the original's GUID.
	// Track GUIDs whose attempt succeeded so a failed original is not
	// re-redelivered when a newer redelivery of it already succeeded. The
	// crawl walks newest→oldest, so the success is seen before its failed
	// original — keeping repeated redrives of a stable window convergent.
	succeededGUIDs := make(map[string]struct{})
	for result.Pages < maxPages {
		result.Pages++
		deliveries, resp, err := client.ListHookDeliveries(ctx, &gh.ListCursorOptions{PerPage: webhookRedrivePageSize, Cursor: cursor})
		if err != nil {
			return result, fmt.Errorf("list GitHub App webhook deliveries for app %q: %w", appName, err)
		}
		if len(deliveries) == 0 {
			historyExhausted = true
			break
		}
		for _, delivery := range deliveries {
			result.Fetched++
			delivered := delivery.GetDeliveredAt().Time
			if delivered.IsZero() {
				if logger != nil {
					logger.Warn("skipping webhook delivery with missing delivered_at", "app_name", appName, "delivery_id", delivery.GetID(), "event", delivery.GetEvent(), "action", delivery.GetAction())
				}
				continue
			}
			result.OldestFetched = delivered.Format(time.RFC3339)

			guid := delivery.GetGUID()
			if webhookDeliverySucceeded(delivery) {
				if guid != "" {
					succeededGUIDs[guid] = struct{}{}
				}
				continue
			}
			if delivered.Before(windowStart) || delivered.After(windowEnd) || !webhookRedriveEventEligible(delivery) {
				continue
			}
			if _, ok := succeededGUIDs[guid]; ok && guid != "" {
				// A newer redelivery of this delivery already succeeded during
				// this crawl; skip it so a re-run over a stable window does not
				// re-fire downstream events.
				continue
			}
			selection, ok, err := webhookRedriveSelectionFor(ctx, client, delivery, repo, pr)
			if err != nil {
				// A per-delivery detail-fetch or payload-parse failure must not
				// abort the whole crawl; skip this one and keep going so the
				// operator still gets (and can act on) the rest.
				result.Skipped++
				if logger != nil {
					logger.Warn("skipping webhook delivery whose detail could not be resolved for repo/PR filtering",
						"app_name", appName, "delivery_id", delivery.GetID(), "event", delivery.GetEvent(), "action", delivery.GetAction(), "error", err)
				}
				continue
			}
			if ok {
				result.Selected = append(result.Selected, selection)
			}
		}
		oldestInPage := deliveries[len(deliveries)-1].GetDeliveredAt().Time
		if !oldestInPage.IsZero() && oldestInPage.Before(windowStart) {
			result.ReachedWindowStart = true
			break
		}
		if resp == nil || resp.Cursor == "" {
			historyExhausted = true
			break
		}
		cursor = resp.Cursor
	}
	// Coverage is reported as facts, not errors: a follow-up request with
	// next_cursor continues the listing, and history_exhausted tells the
	// caller that older deliveries no longer exist on GitHub. The caller
	// decides whether incomplete coverage fails its run.
	if !result.ReachedWindowStart {
		result.HistoryExhausted = historyExhausted
		if !historyExhausted {
			result.NextCursor = cursor
		}
	}
	if dryRun {
		return result, nil
	}
	for i, selected := range result.Selected {
		if _, _, err := client.RedeliverHookDelivery(ctx, selected.ID); err != nil {
			result.Failed++
			if logger != nil {
				logger.Warn("GitHub webhook redelivery failed", "app_name", appName, "delivery_id", selected.ID, "repo", selected.Repo, "pr", selected.PR, "event", selected.Event, "action", selected.Action, "error", err)
			}
			continue
		}
		result.Redelivered++
		if i < len(result.Selected)-1 {
			if err := waitWebhookRedriveDelay(ctx); err != nil {
				return result, fmt.Errorf("wait between GitHub webhook redeliveries for app %q: %w", appName, err)
			}
		}
	}
	return result, nil
}

func waitWebhookRedriveDelay(ctx context.Context) error {
	timer := time.NewTimer(webhookRedriveDelay)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

// webhookDeliverySucceeded reports whether a delivery's own attempt succeeded.
// GitHub's status string mirrors the HTTP response text ("OK", "Accepted",
// …), so any 2xx status code is the robust success predicate — matching on
// the literal "OK" would misclassify a 202 as a failure and redrive it.
func webhookDeliverySucceeded(delivery *gh.HookDelivery) bool {
	return delivery.GetStatusCode()/100 == 2
}

// webhookRedriveEventEligible reports whether a delivery is one of the
// check-creating events a redrive targets. It does not consider success or
// failure — the caller redrives only the failed ones.
func webhookRedriveEventEligible(delivery *gh.HookDelivery) bool {
	if delivery == nil || delivery.GetID() == 0 {
		return false
	}
	switch delivery.GetEvent() {
	case "pull_request":
		switch delivery.GetAction() {
		case "opened", "synchronize", "reopened", "ready_for_review":
			return true
		}
	case "check_suite":
		return delivery.GetAction() == "requested"
	case "merge_group":
		return delivery.GetAction() == "checks_requested"
	}
	return false
}

func webhookRedriveSelectionFor(ctx context.Context, client webhookRedriveDeliveryClient, delivery *gh.HookDelivery, repo string, pr int) (WebhookRedriveSelection, bool, error) {
	selection := WebhookRedriveSelection{
		ID:          delivery.GetID(),
		DeliveredAt: delivery.GetDeliveredAt().Format(time.RFC3339),
		Event:       delivery.GetEvent(),
		Action:      delivery.GetAction(),
		Status:      delivery.GetStatus(),
		StatusCode:  delivery.GetStatusCode(),
	}
	if repo == "" && pr == 0 {
		return selection, true, nil
	}
	detail, _, err := client.GetHookDelivery(ctx, delivery.GetID())
	if err != nil {
		return selection, false, fmt.Errorf("get GitHub App webhook delivery %d details for repo/PR filtering: %w", delivery.GetID(), err)
	}
	payload, err := webhookRedrivePayloadMetadata(detail)
	if err != nil {
		return selection, false, fmt.Errorf("parse GitHub App webhook delivery %d payload for repo/PR filtering: %w", delivery.GetID(), err)
	}
	selection.Repo = payload.repo
	if len(payload.prs) > 0 {
		selection.PR = payload.prs[0]
	}
	if repo != "" && !strings.EqualFold(payload.repo, repo) {
		return selection, false, nil
	}
	// check_suite and merge_group payloads can carry several pull requests;
	// the delivery is selected when any of them matches the filter. A
	// pr-filtered redrive intentionally skips deliveries whose payload names
	// no pull request — merge_group deliveries and check_suite deliveries for
	// fork PRs — since the filter cannot confirm they belong to the requested
	// PR. Run without --pr (repo-scoped) to include those.
	if pr != 0 {
		if !slices.Contains(payload.prs, pr) {
			return selection, false, nil
		}
		selection.PR = pr
	}
	return selection, true, nil
}

type webhookRedrivePayload struct {
	repo string
	prs  []int
}

func webhookRedrivePayloadMetadata(delivery *gh.HookDelivery) (webhookRedrivePayload, error) {
	if delivery == nil || delivery.Request == nil || delivery.Request.RawPayload == nil {
		return webhookRedrivePayload{}, fmt.Errorf("delivery payload is missing")
	}
	var payload struct {
		Number     int `json:"number"`
		Repository struct {
			FullName string `json:"full_name"`
		} `json:"repository"`
		PullRequest struct {
			Number int `json:"number"`
		} `json:"pull_request"`
		CheckSuite struct {
			PullRequests []struct {
				Number int `json:"number"`
			} `json:"pull_requests"`
		} `json:"check_suite"`
		MergeGroup struct {
			PullRequests []struct {
				Number int `json:"number"`
			} `json:"pull_requests"`
		} `json:"merge_group"`
	}
	if err := json.Unmarshal(*delivery.Request.RawPayload, &payload); err != nil {
		return webhookRedrivePayload{}, err
	}
	var prs []int
	appendPR := func(n int) {
		if n == 0 {
			return
		}
		if slices.Contains(prs, n) {
			return
		}
		prs = append(prs, n)
	}
	appendPR(payload.PullRequest.Number)
	appendPR(payload.Number)
	for _, pr := range payload.CheckSuite.PullRequests {
		appendPR(pr.Number)
	}
	for _, pr := range payload.MergeGroup.PullRequests {
		appendPR(pr.Number)
	}
	return webhookRedrivePayload{repo: payload.Repository.FullName, prs: prs}, nil
}
