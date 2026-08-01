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

	// webhookScanPRPageSize bounds one scan request to a single page of open
	// PRs; callers continue via next_page.
	webhookScanPRPageSize = 30
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
type ChecksScanRequest = apitypes.ChecksScanRequest
type ChecksScanResponse = apitypes.ChecksScanResponse
type ChecksReposResponse = apitypes.ChecksReposResponse
type MissingCheckPR = apitypes.MissingCheckPR
type StuckCheckPR = apitypes.StuckCheckPR
type IncompleteCheckRun = apitypes.IncompleteCheckRun

// CheckRunBackfiller replays the auto-plan flow for a PR to recreate missing
// Check Runs, exactly as the check-creating webhook delivery would have. The
// webhook handler implements it; the API service holds it so the checks
// operator endpoints can reach the webhook pipeline.
type CheckRunBackfiller interface {
	BackfillPRCheckRuns(ctx context.Context, repo string, pr int, installationID int64) (string, error)
}

type ChecksSynthesizeRequest = apitypes.ChecksSynthesizeRequest
type ChecksSynthesizeResponse = apitypes.ChecksSynthesizeResponse
type ChecksSynthesizeResult = apitypes.ChecksSynthesizeResult

// checksSynthesizeMaxPRsPerRequest bounds one synthesize request; each PR's
// heavy plan work runs asynchronously, so a chunk only pays discovery.
const checksSynthesizeMaxPRsPerRequest = 10

// checksDisabledSkipOutcome is the per-PR outcome when a synthesize request
// names a repository whose Check Run publishing is turned off. A skip, not an
// error: a fleet sweep that includes a disabled repo should keep going.
const checksDisabledSkipOutcome = "skipped: Check Runs are disabled for this repository (enable_checks: false)"

func (s *Service) handleChecksSynthesize(w http.ResponseWriter, r *http.Request) {
	var req ChecksSynthesizeRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		s.writeBodyDecodeError(w, err)
		return
	}
	ctx, cancel := s.extendWebhookOpsDeadline(w, r)
	defer cancel()
	response, err := executeChecksSynthesize(ctx, s.config, s.checkRunBackfiller, req, s.logger)
	if err != nil {
		s.writeWebhookOpsError(w, err)
		return
	}
	s.writeJSON(w, http.StatusOK, response)
}

func executeChecksSynthesize(ctx context.Context, cfg *ServerConfig, backfiller CheckRunBackfiller, req ChecksSynthesizeRequest, logger *slog.Logger) (*ChecksSynthesizeResponse, error) {
	if cfg == nil {
		return nil, fmt.Errorf("server config is nil")
	}
	if req.Repo == "" {
		return nil, webhookOpsRequestErrorf("repo is required")
	}
	if len(req.PRs) == 0 {
		return nil, webhookOpsRequestErrorf("prs is required")
	}
	if len(req.PRs) > checksSynthesizeMaxPRsPerRequest {
		return nil, webhookOpsRequestErrorf("at most %d prs per request; send the rest in follow-up requests", checksSynthesizeMaxPRsPerRequest)
	}
	for _, pr := range req.PRs {
		if pr <= 0 {
			return nil, webhookOpsRequestErrorf("pr numbers must be positive; got %d", pr)
		}
	}
	if logger == nil {
		logger = slog.New(slog.NewJSONHandler(os.Stderr, nil))
	}
	// Backfill exists to create Check Runs; on a repo with publishing turned
	// off it could only replay auto-plans whose checks the publisher refuses,
	// leaving plan comments as the sole side effect. Skip per PR rather than
	// erroring so a fleet sweep that includes a disabled repo keeps going.
	if !cfg.AreChecksEnabled(req.Repo) {
		logger.Info("Check Run backfill skipping repository with checks disabled", "repo", req.Repo, "prs", len(req.PRs))
		response := &ChecksSynthesizeResponse{Repo: req.Repo}
		for _, pr := range req.PRs {
			response.Results = append(response.Results, ChecksSynthesizeResult{PR: pr, Outcome: checksDisabledSkipOutcome})
		}
		return response, nil
	}
	if backfiller == nil {
		return nil, fmt.Errorf("check run backfill is unavailable: no GitHub webhook runtime is configured")
	}
	installationClient, installationID, err := resolveRepoInstallationClient(ctx, cfg, req.Repo, logger)
	if err != nil {
		return nil, err
	}
	response := &ChecksSynthesizeResponse{Repo: req.Repo}
	for _, pr := range req.PRs {
		outcome, err := backfiller.BackfillPRCheckRuns(ctx, req.Repo, pr, installationID)
		result := ChecksSynthesizeResult{PR: pr, Outcome: outcome}
		if err != nil {
			// Each PR's backfill is independent; report the failure and keep
			// going so one bad PR does not strand the rest of the batch.
			logger.Warn("check run backfill failed for PR", "repo", req.Repo, "pr", pr, "error", err)
			result.Error = err.Error()
		}
		response.Results = append(response.Results, result)
	}
	response.RateLimit = gitHubRateLimitSnapshot(ctx, installationClient, logger, "checks synthesize batch")
	return response, nil
}

// gitHubRateLimitSnapshot reads the installation's remaining core budget so
// the caller can pace a large backfill instead of starving the live webhook
// path that shares the same budget. The snapshot is advisory: the operation
// that produced the response already succeeded, so a read failure is logged
// and the pacing info omitted rather than failing the request.
func gitHubRateLimitSnapshot(ctx context.Context, client interface {
	CoreRateLimit(ctx context.Context) (remaining, limit int, resetAt time.Time, err error)
}, logger *slog.Logger, operation string) *apitypes.GitHubRateLimit {
	remaining, limit, resetAt, err := client.CoreRateLimit(ctx)
	if err != nil {
		logger.Warn("failed to read GitHub rate limit; response omits pacing info and the caller will not pause",
			"operation", operation, "error", err)
		return nil
	}
	return &apitypes.GitHubRateLimit{
		Remaining: remaining,
		Limit:     limit,
		ResetAt:   resetAt.UTC().Format(time.RFC3339),
	}
}

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

func (s *Service) handleChecksScan(w http.ResponseWriter, r *http.Request) {
	var req ChecksScanRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		s.writeBodyDecodeError(w, err)
		return
	}
	ctx, cancel := s.extendWebhookOpsDeadline(w, r)
	defer cancel()
	response, err := executeChecksScan(ctx, s.config, req, s.logger)
	if err != nil {
		s.writeWebhookOpsError(w, err)
		return
	}
	s.writeJSON(w, http.StatusOK, response)
}

func executeWebhookRedrive(ctx context.Context, cfg *ServerConfig, req WebhookRedriveRequest, logger *slog.Logger) (*WebhookRedriveResponse, error) {
	if len(req.DeliveryIDs) > 0 {
		return executeWebhookRedriveByIDs(ctx, cfg, req, logger)
	}
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

func executeChecksScan(ctx context.Context, cfg *ServerConfig, req ChecksScanRequest, logger *slog.Logger) (*ChecksScanResponse, error) {
	if cfg == nil {
		return nil, fmt.Errorf("server config is nil")
	}
	if req.Repo == "" {
		return nil, webhookOpsRequestErrorf("repo is required")
	}
	if req.Page < 0 {
		return nil, webhookOpsRequestErrorf("page must be non-negative")
	}
	// A stale or mistyped environment would otherwise scan for a check name
	// that can never exist and report every PR as missing it; reject it as a
	// request error instead.
	if req.Environment != "" && !cfg.IsEnvironmentAllowed(req.Environment) {
		return nil, webhookOpsRequestErrorf("environment %q is not one this instance handles", req.Environment)
	}
	var updatedSince time.Time
	if req.UpdatedSince != "" {
		var err error
		updatedSince, err = time.Parse(time.RFC3339, req.UpdatedSince)
		if err != nil {
			return nil, webhookOpsRequestErrorf("parse updated_since %q as RFC3339: %v", req.UpdatedSince, err)
		}
	}
	if logger == nil {
		logger = slog.New(slog.NewJSONHandler(os.Stderr, nil))
	}
	// With Check Run publishing turned off, every open PR would trivially be
	// missing a check the server refuses to create — the scan result would be
	// all noise. Report the repo as disabled instead of scanning, as a normal
	// response rather than an error so a fleet sweep keeps going.
	if !cfg.AreChecksEnabled(req.Repo) {
		logger.Info("checks scan skipping repository with checks disabled", "repo", req.Repo)
		return &ChecksScanResponse{Repo: req.Repo, ChecksDisabled: true}, nil
	}
	installationClient, _, err := resolveRepoInstallationClient(ctx, cfg, req.Repo, logger)
	if err != nil {
		return nil, err
	}
	response, err := scanWebhookMissingChecks(ctx, installationClient, req.Repo, webhookMissingCheckNames(cfg, req.Repo, req.Environment, req.CheckName), req.Page, updatedSince)
	if err != nil {
		return nil, err
	}
	response.RateLimit = gitHubRateLimitSnapshot(ctx, installationClient, logger, "checks scan page")
	return response, nil
}

func (s *Service) handleChecksRepos(w http.ResponseWriter, r *http.Request) {
	response, err := executeChecksRepos(s.config)
	if err != nil {
		s.writeWebhookOpsError(w, err)
		return
	}
	s.writeJSON(w, http.StatusOK, response)
}

// executeChecksRepos lists the repositories declared in the server's repos
// config — the inventory a fleet-wide checks scan iterates. Repositories with
// Check Run publishing turned off are listed separately as disabled: a scan
// of one would report every open PR as missing a check the server refuses to
// create, so callers skip them and tell the operator. A legacy single-App
// config declares no repos, so a fleet sweep cannot know what to scan; the
// operator must name repositories explicitly there.
func executeChecksRepos(cfg *ServerConfig) (*ChecksReposResponse, error) {
	if cfg == nil {
		return nil, fmt.Errorf("server config is nil")
	}
	if len(cfg.Repos) == 0 {
		return nil, webhookOpsRequestErrorf("this server declares no repos config; name a repository explicitly instead of scanning all repos")
	}
	response := &ChecksReposResponse{Repos: make([]string, 0, len(cfg.Repos))}
	for repo := range cfg.Repos {
		if cfg.AreChecksEnabled(repo) {
			response.Repos = append(response.Repos, repo)
		} else {
			response.Disabled = append(response.Disabled, repo)
		}
	}
	sort.Strings(response.Repos)
	sort.Strings(response.Disabled)
	return response, nil
}

// resolveRepoInstallationClient builds an installation-scoped GitHub client
// for the App that serves repo, from the server config's App credentials.
func resolveRepoInstallationClient(ctx context.Context, cfg *ServerConfig, repo string, logger *slog.Logger) (*ghclient.InstallationClient, int64, error) {
	app, err := cfg.ResolveGitHubAppForRepo(repo)
	if err != nil {
		return nil, 0, err
	}
	appID := app.Config.ResolveAppID()
	if appID == 0 {
		return nil, 0, fmt.Errorf("app %q has empty or unparseable app-id", app.Name)
	}
	privateKey, err := app.Config.ResolvePrivateKey()
	if err != nil {
		return nil, 0, fmt.Errorf("resolve private key for app %q: %w", app.Name, err)
	}
	if privateKey == "" {
		return nil, 0, fmt.Errorf("app %q private key resolved to empty value", app.Name)
	}
	client := ghclient.NewClient(appID, []byte(privateKey), logger,
		ghclient.WithTrustedCheckAppSlugs(app.Config.TrustedCheckAppSlugs))
	installationID, err := client.InstallationIDForRepo(ctx, repo)
	if err != nil {
		return nil, 0, fmt.Errorf("resolve GitHub App installation for %s: %w", repo, err)
	}
	installationClient, err := client.ForInstallation(installationID)
	if err != nil {
		return nil, 0, fmt.Errorf("create installation client for %s: %w", repo, err)
	}
	return installationClient, installationID, nil
}

// executeWebhookRedriveByIDs redelivers exactly the requested deliveries for
// one App, skipping the window listing — the caller already identified the
// failed deliveries (for example a checks backfill classification pass).
func executeWebhookRedriveByIDs(ctx context.Context, cfg *ServerConfig, req WebhookRedriveRequest, logger *slog.Logger) (*WebhookRedriveResponse, error) {
	if req.App == "" {
		return nil, webhookOpsRequestErrorf("delivery_ids redelivery requires app to be set")
	}
	if req.Cursor != "" {
		return nil, webhookOpsRequestErrorf("delivery_ids and cursor are mutually exclusive")
	}
	if req.DryRun {
		return nil, webhookOpsRequestErrorf("delivery_ids redelivery has no dry run; the listing pass already reported the selection")
	}
	apps, err := webhookRedriveApps(cfg, req.App)
	if err != nil {
		return nil, err
	}
	if logger == nil {
		logger = slog.New(slog.NewJSONHandler(os.Stderr, nil))
	}
	app := apps[0]
	client, err := newWebhookRedriveGitHubClient(app, logger)
	if err != nil {
		return nil, err
	}
	result := WebhookRedriveResult{AppName: app.name, ReachedWindowStart: true}
	for i, id := range req.DeliveryIDs {
		result.Selected = append(result.Selected, WebhookRedriveSelection{ID: id})
		if _, _, err := client.Apps.RedeliverHookDelivery(ctx, id); err != nil {
			result.Failed++
			logger.Warn("GitHub webhook redelivery failed", "app_name", app.name, "delivery_id", id, "error", err)
			continue
		}
		result.Redelivered++
		if i < len(req.DeliveryIDs)-1 {
			if err := waitWebhookRedriveDelay(ctx); err != nil {
				return nil, fmt.Errorf("wait between GitHub webhook redeliveries for app %q: %w", app.name, err)
			}
		}
	}
	return &WebhookRedriveResponse{Results: []WebhookRedriveResult{result}}, nil
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

func webhookMissingCheckNames(cfg *ServerConfig, repo, environment, override string) []string {
	if override != "" {
		return []string{override}
	}
	base := cfg.GitHubCheckNameBaseForRepo(repo)
	if environment != "" {
		return []string{fmt.Sprintf("%s (%s)", base, environment)}
	}
	if len(cfg.AllowedEnvironments) > 0 {
		out := make([]string, 0, len(cfg.AllowedEnvironments))
		for _, env := range cfg.AllowedEnvironments {
			out = append(out, fmt.Sprintf("%s (%s)", base, env))
		}
		return out
	}
	return []string{base}
}

type webhookMissingCheckScanClient interface {
	ListOpenPullRequestsPage(ctx context.Context, repo string, page, perPage int) (prs []ghclient.OpenPullRequest, nextPage, lastPage int, err error)
	FindCheckRunByName(ctx context.Context, repo, headSHA, checkName string) (*ghclient.CheckRunResult, []string, error)
}

func scanWebhookMissingChecks(ctx context.Context, client webhookMissingCheckScanClient, repo string, checkNames []string, page int, updatedSince time.Time) (*ChecksScanResponse, error) {
	prs, nextPage, lastPage, err := client.ListOpenPullRequestsPage(ctx, repo, page, webhookScanPRPageSize)
	if err != nil {
		return nil, err
	}
	result := &ChecksScanResponse{
		Repo:             repo,
		CheckNames:       checkNames,
		NextPage:         nextPage,
		EstimatedOpenPRs: estimateOpenPRCount(page, lastPage, len(prs)),
	}
	for _, pr := range prs {
		if !updatedSince.IsZero() && pr.UpdatedAt.Before(updatedSince) {
			// The listing is ordered newest-updated first, so every PR from
			// here on is older than the requested window: the sweep is done.
			result.NextPage = 0
			break
		}
		result.Scanned++
		var missing []string
		var untrustedConflicts []string
		var incomplete []IncompleteCheckRun
		for _, checkName := range checkNames {
			run, untrustedApps, err := client.FindCheckRunByName(ctx, repo, pr.HeadSHA, checkName)
			if err != nil {
				return nil, fmt.Errorf("scan %s#%d for check %q at %s: %w", repo, pr.Number, checkName, pr.HeadSHA, err)
			}
			if run != nil {
				if !checkRunCompleted(run) {
					incomplete = append(incomplete, IncompleteCheckRun{
						Name:       run.Name,
						CheckRunID: run.ID,
						Status:     run.Status,
						StartedAt:  formatCheckRunStartedAt(run.StartedAt),
					})
				}
				continue
			}
			missing = append(missing, checkName)
			// No trusted check exists, but a same-named one from an untrusted
			// app does: backfill will still create the trusted check, yet the
			// operator likely also needs to resolve the conflicting check.
			if len(untrustedApps) > 0 {
				untrustedConflicts = append(untrustedConflicts, checkName)
			}
		}
		if len(incomplete) > 0 {
			result.Stuck = append(result.Stuck, StuckCheckPR{
				Number:  pr.Number,
				URL:     fmt.Sprintf("https://github.com/%s/pull/%d", repo, pr.Number),
				Title:   pr.Title,
				HeadSHA: pr.HeadSHA,
				HeadRef: pr.HeadRef,
				Checks:  incomplete,
			})
		}
		if len(missing) == 0 {
			continue
		}
		result.Missing = append(result.Missing, MissingCheckPR{
			Number:                 pr.Number,
			URL:                    fmt.Sprintf("https://github.com/%s/pull/%d", repo, pr.Number),
			Title:                  pr.Title,
			HeadSHA:                pr.HeadSHA,
			HeadRef:                pr.HeadRef,
			MissingNames:           missing,
			UntrustedConflictNames: untrustedConflicts,
		})
	}
	return result, nil
}

// estimateOpenPRCount derives the repository's total open-PR count from one
// listing page, so a long scan can report progress against a denominator that
// is recomputed every page instead of feeling unbounded. GitHub reports the
// listing's last page while more pages remain — an upper bound of
// lastPage×pageSize — and on the final page (lastPage 0) the count is exact:
// the PRs before this page plus the PRs on it.
func estimateOpenPRCount(page, lastPage, pageLen int) int {
	if lastPage > 0 {
		return lastPage * webhookScanPRPageSize
	}
	if page <= 0 {
		page = 1
	}
	return (page-1)*webhookScanPRPageSize + pageLen
}

// checkRunCompleted reports whether the Check Run's status is "completed".
// Any other status (queued, in_progress) is still holding its slot without a
// conclusion, which the scan surfaces so an operator can judge whether the
// run is legitimately in flight or wedged.
func checkRunCompleted(run *ghclient.CheckRunResult) bool {
	return run.Status == "completed"
}

// formatCheckRunStartedAt renders the start time for the API response; GitHub
// can omit it, and a zero time would otherwise serialize as a misleading
// year-one timestamp.
func formatCheckRunStartedAt(startedAt time.Time) string {
	if startedAt.IsZero() {
		return ""
	}
	return startedAt.UTC().Format(time.RFC3339)
}
