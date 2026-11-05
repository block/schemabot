package github

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"time"

	"github.com/block/schemabot/pkg/metrics"
	"github.com/gofri/go-github-ratelimit/v2/github_ratelimit"
	"github.com/gofri/go-github-ratelimit/v2/github_ratelimit/github_primary_ratelimit"
	"github.com/gofri/go-github-ratelimit/v2/github_ratelimit/github_secondary_ratelimit"
	gh "github.com/google/go-github/v86/github"
)

type primaryRejectionObservationKey struct{}

type primaryRejectionObservation struct {
	preventedResponse *http.Response
}

type githubQuotaWaitTransport struct {
	base           http.RoundTripper
	installationID int64
	appSlug        func() string
}

func newGitHubRateLimitTransport(base http.RoundTripper) http.RoundTripper {
	return newGitHubRateLimitTransportWithTotalAllowance(base, nil)
}

// No total allowance is configured in production. Keeping configuration and its
// callback together lets a future explicit allowance retain report coverage.
func newGitHubRateLimitTransportWithTotalAllowance(base http.RoundTripper, totalAllowance *time.Duration) http.RoundTripper {
	t := &githubQuotaWaitTransport{}
	if mt, ok := base.(*githubMetricsTransport); ok {
		t.installationID = mt.installationID
		t.appSlug = mt.appSlug
	}
	options := []any{
		github_secondary_ratelimit.WithSingleSleepLimit(githubSecondaryRateLimitMaxSleep, t.cooldownCallback(metrics.GitHubCooldownSingleSleepLimitExceeded)),
		github_secondary_ratelimit.WithLimitDetectedCallback(t.cooldownCallback(metrics.GitHubCooldownAccepted)),
	}
	if totalAllowance != nil {
		options = append(options, github_secondary_ratelimit.WithTotalSleepLimit(*totalAllowance, t.cooldownCallback(metrics.GitHubCooldownTotalSleepLimitExceeded)))
	}
	options = append(options,
		github_primary_ratelimit.WithLimitDetectedCallback(func(c *github_primary_ratelimit.CallbackContext) {
			t.recordPrimaryQuotaResponse(c, string(c.Category))
		}),
		github_primary_ratelimit.WithUnknownCategoryCallback(func(c *github_primary_ratelimit.CallbackContext) {
			// This branch has verified quota exhaustion but cannot track its bucket.
			// Keep the rejection visible without exporting an unbounded header value.
			t.recordPrimaryQuotaResponse(c, "unknown")
		}),
		github_primary_ratelimit.WithRequestPreventedCallback(func(c *github_primary_ratelimit.CallbackContext) {
			if observation, ok := c.Request.Context().Value(primaryRejectionObservationKey{}).(*primaryRejectionObservation); ok {
				observation.preventedResponse = c.Response
			}
		}),
	)
	t.base = github_ratelimit.New(base, options...)
	return t
}

// Log only HTTP detections, not each subsequent local refusal. The reporting
// repository encountered the exhausted shared budget; it need not have spent it.
func (t *githubQuotaWaitTransport) recordPrimaryQuotaResponse(c *github_primary_ratelimit.CallbackContext, resource string) {
	sample := t.sample(c.Request, resource)
	metrics.RecordGitHubPrimaryRateLimitRejection(c.Request.Context(), sample, metrics.GitHubRejectionOriginHTTP)
	repository := githubRateLimitContextFromRequest(c.Request).repository
	if metricCtx, ok := githubRateLimitContextFrom(c.Request.Context()); ok && metricCtx.repository != "" {
		repository = metricCtx.repository
	}
	if repository == "" {
		repository = "unknown"
	}
	app := sample.GitHubApp
	if app == "" {
		app = "unknown"
	}
	reset := "unknown"
	if c.ResetTime != nil {
		reset = c.ResetTime.UTC().Format(time.RFC3339)
	}
	slog.WarnContext(c.Request.Context(), "GitHub primary quota exhausted",
		"repo", repository, "operation", sample.Operation,
		"github_app", app, "installation_id", sample.InstallationID,
		"resource", resource, "reset_time", reset)
}

func (t *githubQuotaWaitTransport) cooldownCallback(disposition string) func(*github_secondary_ratelimit.CallbackContext) {
	return func(c *github_secondary_ratelimit.CallbackContext) {
		// The dependency holds its lock here. Record in-process without retaining
		// callback pointers or re-entering the limiter.
		if c.Request == nil {
			slog.Error("GitHub cooldown report missing request", "disposition", disposition)
			return
		}
		resource := githubRateLimitResourceFromRequest(c.Request)
		if c.Response != nil {
			resource = githubRateLimitResourceFromHeaders(c.Request, c.Response)
		}
		metrics.RecordGitHubSecondaryCooldownReport(c.Request.Context(), t.sample(c.Request, resource), disposition)
	}
}

func (t *githubQuotaWaitTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	observation := &primaryRejectionObservation{}
	req = req.WithContext(context.WithValue(req.Context(), primaryRejectionObservationKey{}, observation))
	resp, err := t.base.RoundTrip(req)
	var quotaErr *github_primary_ratelimit.RateLimitReachedError
	// The prevention callback also runs in bypass mode. Count only a returned
	// refusal carrying that callback's synthetic response, not an advisory or a
	// later real HTTP quota response from the same invocation.
	if errors.As(err, &quotaErr) && observation.preventedResponse != nil && quotaErr.Response == observation.preventedResponse {
		metrics.RecordGitHubPrimaryRateLimitRejection(req.Context(), t.sample(req, string(quotaErr.Category)), metrics.GitHubRejectionOriginLocal)
	}
	return resp, err
}

func (t *githubQuotaWaitTransport) sample(req *http.Request, resource string) metrics.GitHubClientMetricContext {
	metricCtx, ok := githubRateLimitContextFrom(req.Context())
	if !ok {
		metricCtx = githubRateLimitContextFromRequest(req)
	}
	app := ""
	if t.appSlug != nil {
		app = t.appSlug()
	}
	return metrics.GitHubClientMetricContext{
		Operation: metricCtx.operation, Resource: resource,
		GitHubApp: app, InstallationID: t.installationID,
	}
}

func (ic *InstallationClient) readWaitMetricContext(operation string) metrics.GitHubClientMetricContext {
	return metrics.GitHubClientMetricContext{
		Operation: operation, Resource: metrics.GitHubRateLimitResourceCore,
		GitHubApp: ic.loadAppSlug(), InstallationID: ic.installationID,
	}
}

func waitToRetryGitHubRead(ctx context.Context, sample metrics.GitHubClientMetricContext, delay time.Duration, readErr error) error {
	reason := metrics.GitHubWaitReasonTransientError
	var secondaryErr *gh.AbuseRateLimitError
	if errors.As(readErr, &secondaryErr) {
		reason = metrics.GitHubWaitReasonSecondaryLimit
	}
	start := time.Now()
	timer := time.NewTimer(delay)
	defer timer.Stop()
	defer func() {
		if delay > 0 {
			metrics.RecordGitHubWait(ctx, sample, metrics.GitHubWaitLayerReadRetry, reason, time.Since(start))
		}
	}()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}
