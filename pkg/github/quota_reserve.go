package github

import (
	"context"
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

// quotaResetHorizon bounds how far ahead a reported reset time is believed.
// GitHub's core quota resets every hour, so a reset further out than this is
// a garbled header, and accepting it would hold the reserve until that time.
const quotaResetHorizon = 2 * time.Hour

// checkPublicationReadsKey marks a context whose GitHub reads decide which
// Check Run to post.
type checkPublicationReadsKey struct{}

// WithCheckPublicationReads marks ctx as the context of a check publication.
// The schema file, blob and tree reads made under it decide what the Check Run
// says, such as which files a pull request proposes and which environments its
// configs cover, so the quota reserve sends them: the reserve exists so that
// checks keep posting. Their number per publication is bounded by the pull
// request's schema and config files (AV-15).
func WithCheckPublicationReads(ctx context.Context) context.Context {
	return context.WithValue(ctx, checkPublicationReadsKey{}, true)
}

// CheckPublicationReads reports whether ctx was marked by
// WithCheckPublicationReads.
func CheckPublicationReads(ctx context.Context) bool {
	marked, _ := ctx.Value(checkPublicationReadsKey{}).(bool)
	return marked
}

// isDeferrablePlanInputRead reports whether a request only reads schema or
// plan inputs, so it can wait for the quota to reset without blocking a merge.
// Directory listings use the same Contents API route as file reads and so
// share fetch_file_content. Everything else — check runs, pull request reads,
// changed-file lists, comments, token exchanges, and unattributed requests —
// is never refused, because check publication depends on it. The same reads
// made for a check publication are never refused either; see
// WithCheckPublicationReads.
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

	// mu guards the latest core quota GitHub reported for the installation,
	// the requests sent and not yet answered, and the reset window already
	// warned about.
	mu          sync.Mutex
	observed    bool
	remaining   int64
	resetAt     time.Time
	inFlight    int64
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
	deferrable := isDeferrablePlanInputRead(metricCtx.operation) && !CheckPublicationReads(req.Context())
	if refusal, firstInWindow := t.admit(metricCtx.operation, deferrable); refusal != nil {
		t.reportRefusal(req, metricCtx, refusal, firstInWindow)
		if req.Body != nil {
			if err := req.Body.Close(); err != nil {
				slog.WarnContext(req.Context(), "failed to close body of GitHub request refused by quota reserve",
					"operation", metricCtx.operation, "installation_id", t.installationID, "error", err)
			}
		}
		return nil, refusal
	}
	resp, err := t.base.RoundTrip(req)
	t.settle(req, resp)
	return resp, err
}

// admit decides whether a request may be sent and, when it may, counts it as
// in flight in the same critical section, so concurrent requests cannot all
// pass on one stale reading of the quota. Only a deferrable request is
// refused; every admitted request counts, because each one spends quota.
func (t *quotaReserveTransport) admit(operation string, deferrable bool) (*QuotaReservedError, bool) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if deferrable && t.quotaAtReserveLocked() {
		firstInWindow := !t.warnedReset.Equal(t.resetAt)
		t.warnedReset = t.resetAt
		return &QuotaReservedError{
			Operation: operation,
			Remaining: t.remaining,
			Reserve:   t.reserve,
			ResetAt:   t.resetAt,
		}, firstInWindow
	}
	t.inFlight++
	return nil, false
}

// settle stops counting an admitted request as in flight and records the core
// quota its response reports. A request that got no response spent quota or
// did not; either way the next response reports the count.
func (t *quotaReserveTransport) settle(req *http.Request, resp *http.Response) {
	if resp != nil {
		t.observe(req, resp)
	}
	t.mu.Lock()
	t.inFlight--
	t.mu.Unlock()
}

// observe records the core quota a response reports. Responses can arrive out
// of order, so a later reset window replaces the state, the same window keeps
// the lowest remaining count (it only falls within a window), and an earlier
// window is ignored. A reset further out than quotaResetHorizon is ignored as
// a garbled header.
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
	if horizon := t.now().Add(quotaResetHorizon); resetAt.After(horizon) {
		slog.WarnContext(req.Context(), "ignoring a GitHub core quota report whose reset time is further out than GitHub's hourly window; the quota reserve keeps its previous reading",
			"installation_id", t.installationID, "remaining", remaining,
			"reset_time", resetAt.UTC().Format(time.RFC3339), "horizon", horizon.UTC().Format(time.RFC3339))
		return
	}

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

// quotaAtReserveLocked reports whether GitHub's last report, less the requests
// already sent and not yet answered, puts the core quota at or below the
// reserve in a window that has not reset yet. Callers hold t.mu.
func (t *quotaReserveTransport) quotaAtReserveLocked() bool {
	return t.observed && t.remaining-t.inFlight <= t.reserve && t.now().Before(t.resetAt)
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
	slog.WarnContext(req.Context(), "GitHub core quota is at its reserve; plan reads of schema files and trees are refused until the quota resets, and check runs, the reads that decide them, pull request reads, and comments still go through",
		"installation_id", t.installationID, "github_app", app,
		"remaining", refusal.Remaining, "reserve", refusal.Reserve,
		"reset_time", refusal.ResetAt.UTC().Format(time.RFC3339),
		"operation", refusal.Operation, "repo", repository)
}
