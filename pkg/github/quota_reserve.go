package github

import (
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"sync"
	"time"

	"github.com/block/schemabot/pkg/metrics"
)

// ErrGitHubQuotaReserved reports that a request was refused before it was sent
// because the installation's core REST quota is down to its configured reserve.
// The reserve is kept for check runs, pull request reads, and comments, so a
// burst of plan work delays plans instead of leaving required checks unable to
// post. The refusal lasts until the quota resets; QuotaReservedError carries
// the reset time.
var ErrGitHubQuotaReserved = errors.New("GitHub core quota is held in reserve for check runs")

// QuotaReservedError is the refusal returned for a deferrable request while the
// installation's core quota is at or below its reserve. It matches
// ErrGitHubQuotaReserved with errors.Is.
type QuotaReservedError struct {
	// Operation names the refused request, such as fetch_file_content.
	Operation string
	// Remaining is the core quota GitHub last reported for the installation.
	Remaining int64
	// Reserve is the configured number of requests held back.
	Reserve int64
	// ResetAt is when GitHub resets the installation's core quota. Deferrable
	// requests are sent again from then on.
	ResetAt time.Time
}

func (e *QuotaReservedError) Error() string {
	return fmt.Sprintf("%s: %s was not sent because %d requests remain against a reserve of %d; the quota resets at %s",
		ErrGitHubQuotaReserved, e.Operation, e.Remaining, e.Reserve, e.ResetAt.UTC().Format(time.RFC3339))
}

func (e *QuotaReservedError) Unwrap() error {
	return ErrGitHubQuotaReserved
}

// isDeferrablePlanInputRead reports whether a request only reads schema or
// plan inputs, so it can wait for the quota to reset without blocking a merge.
// Directory listings use the same Contents API route as file reads and so
// share fetch_file_content. Everything else — check runs, pull request reads,
// changed-file lists, comments, token exchanges, and unattributed requests —
// is never refused, because check publication depends on it.
func isDeferrablePlanInputRead(operation string) bool {
	switch operation {
	case metrics.GitHubOperationFetchFileContent,
		metrics.GitHubOperationFetchBlob,
		metrics.GitHubOperationFetchGitTree:
		return true
	default:
		return false
	}
}

// quotaReserveTransport refuses deferrable requests for one installation once
// GitHub reports its core quota at or below the reserve, until the quota
// resets. It wraps the installation token transport, so it observes only
// installation-authenticated API responses: token exchanges are App-JWT
// requests counted against the App's own quota, and a refused request never
// mints a token or reaches the rate-limit and metrics layers below it.
type quotaReserveTransport struct {
	base           http.RoundTripper
	reserve        int64
	installationID int64
	appSlug        func() string
	now            func() time.Time

	// mu guards the latest core quota GitHub reported for the installation
	// and the reset window already warned about.
	mu          sync.Mutex
	observed    bool
	remaining   int64
	resetAt     time.Time
	warnedReset time.Time
}

// newQuotaReserveTransport returns base unchanged when reserve is not
// positive, so an unconfigured reserve adds no layer and refuses nothing.
func newQuotaReserveTransport(base http.RoundTripper, reserve, installationID int64, appSlug func() string) http.RoundTripper {
	if reserve <= 0 {
		return base
	}
	return &quotaReserveTransport{
		base:           base,
		reserve:        reserve,
		installationID: installationID,
		appSlug:        appSlug,
		now:            time.Now,
	}
}

func (t *quotaReserveTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	metricCtx, ok := githubRateLimitContextFrom(req.Context())
	if !ok {
		metricCtx = githubRateLimitContextFromRequest(req)
	}
	if isDeferrablePlanInputRead(metricCtx.operation) {
		if refusal, firstInWindow := t.refusal(metricCtx.operation); refusal != nil {
			t.reportRefusal(req, metricCtx, refusal, firstInWindow)
			if req.Body != nil {
				if err := req.Body.Close(); err != nil {
					slog.WarnContext(req.Context(), "failed to close body of GitHub request refused by quota reserve",
						"operation", metricCtx.operation, "installation_id", t.installationID, "error", err)
				}
			}
			return nil, refusal
		}
	}
	resp, err := t.base.RoundTrip(req)
	if resp != nil {
		t.observe(req, resp)
	}
	return resp, err
}

// observe records the core quota a response reports. Responses can arrive out
// of order, so a later reset window replaces the state, the same window keeps
// the lowest remaining count (it only falls within a window), and an earlier
// window is ignored.
func (t *quotaReserveTransport) observe(req *http.Request, resp *http.Response) {
	if gitHubRateLimitResource(githubRateLimitResourceFromHeaders(req, resp)) != metrics.GitHubRateLimitResourceCore {
		return
	}
	remaining, hasRemaining := parseGitHubRateLimitHeader(resp.Header, headerRateLimitRemaining)
	reset, hasReset := parseGitHubRateLimitHeader(resp.Header, headerRateLimitReset)
	if !hasRemaining || !hasReset {
		return
	}
	resetAt := time.Unix(reset, 0)

	t.mu.Lock()
	defer t.mu.Unlock()
	switch {
	case !t.observed || resetAt.After(t.resetAt):
		t.observed = true
		t.remaining = remaining
		t.resetAt = resetAt
	case resetAt.Equal(t.resetAt):
		t.remaining = min(t.remaining, remaining)
	}
}

// refusal returns the error for a deferrable request when the quota is at its
// reserve, and whether this is the first refusal of the current reset window.
func (t *quotaReserveTransport) refusal(operation string) (*QuotaReservedError, bool) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if !t.quotaAtReserveLocked() {
		return nil, false
	}
	firstInWindow := !t.warnedReset.Equal(t.resetAt)
	t.warnedReset = t.resetAt
	return &QuotaReservedError{
		Operation: operation,
		Remaining: t.remaining,
		Reserve:   t.reserve,
		ResetAt:   t.resetAt,
	}, firstInWindow
}

// quotaAtReserveLocked reports whether GitHub's last report puts the core
// quota at or below the reserve in a window that has not reset yet. Callers
// hold t.mu.
func (t *quotaReserveTransport) quotaAtReserveLocked() bool {
	return t.observed && t.remaining <= t.reserve && t.now().Before(t.resetAt)
}

// reportRefusal counts every refusal and warns once per installation per reset
// window, since every deferrable read in that window is refused the same way.
func (t *quotaReserveTransport) reportRefusal(req *http.Request, metricCtx githubRateLimitContext, refusal *QuotaReservedError, firstInWindow bool) {
	app := ""
	if t.appSlug != nil {
		app = t.appSlug()
	}
	metrics.RecordGitHubPrimaryRateLimitRejection(req.Context(), metrics.GitHubClientMetricContext{
		Operation: metricCtx.operation, Resource: metrics.GitHubRateLimitResourceCore,
		GitHubApp: app, InstallationID: t.installationID,
	}, metrics.GitHubRejectionOriginReserve)
	if !firstInWindow {
		return
	}
	if app == "" {
		app = "unknown"
	}
	repository := metricCtx.repository
	if repository == "" {
		repository = "unknown"
	}
	slog.WarnContext(req.Context(), "GitHub core quota is at its reserve; schema file and tree reads are refused until the quota resets, and check runs, pull request reads, and comments still go through",
		"installation_id", t.installationID, "github_app", app,
		"remaining", refusal.Remaining, "reserve", refusal.Reserve,
		"reset_time", refusal.ResetAt.UTC().Format(time.RFC3339),
		"operation", refusal.Operation, "repo", repository)
}
