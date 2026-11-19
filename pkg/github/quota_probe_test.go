package github

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/gofri/go-github-ratelimit/v2/github_ratelimit/github_primary_ratelimit"
	gh "github.com/google/go-github/v86/github"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"

	"github.com/block/schemabot/pkg/metrics"
)

const quotaProbeTestDeadline = 5 * time.Second

// recordingQuotaProbe returns a probe over fixed installations whose budget
// reads are reported on a channel, and a function that fails reads for an
// installation.
func recordingQuotaProbe(t *testing.T, listErr error, ids ...int64) (*QuotaProbe, <-chan int64, func(int64)) {
	t.Helper()
	reads := make(chan int64, 64)
	var mu sync.Mutex
	failing := map[int64]bool{}
	probe := &QuotaProbe{
		logger:          slog.New(slog.DiscardHandler),
		interval:        time.Hour,
		listTimeout:     quotaProbeTestDeadline,
		readTimeout:     quotaProbeTestDeadline,
		readConcurrency: 1,
		listInstallations: func(context.Context) (quotaTargets, error) {
			if listErr != nil {
				return quotaTargets{}, listErr
			}
			return quotaTargets{installations: ids}, nil
		},
		reportTargets: func(quotaTargets) {},
		readQuota: func(_ context.Context, id int64) error {
			reads <- id
			mu.Lock()
			defer mu.Unlock()
			if failing[id] {
				return fmt.Errorf("installation %d refused the token exchange", id)
			}
			return nil
		},
	}
	fail := func(id int64) {
		mu.Lock()
		defer mu.Unlock()
		failing[id] = true
	}
	return probe, reads, fail
}

func receiveRead(t *testing.T, reads <-chan int64) int64 {
	t.Helper()
	select {
	case id := <-reads:
		return id
	case <-time.After(quotaProbeTestDeadline):
		require.FailNow(t, "quota probe did not read an installation budget in time")
		return 0
	}
}

// A server that has just started, before any webhook delivery arrives, reads
// every installation's budget immediately, so the quota is known from the
// first collection instead of only after the first delivery.
func TestQuotaProbeReadsEveryInstallationAtStart(t *testing.T) {
	probe, reads, _ := recordingQuotaProbe(t, nil, 11, 22)
	probe.Start(t.Context())
	t.Cleanup(probe.Stop)

	assert.Equal(t, int64(11), receiveRead(t, reads))
	assert.Equal(t, int64(22), receiveRead(t, reads))
}

// One installation that cannot answer, such as one whose token exchange
// fails, does not hide the budget of the installations after it.
func TestQuotaProbeKeepsReadingPastAFailedInstallation(t *testing.T) {
	probe, reads, fail := recordingQuotaProbe(t, nil, 11, 22)
	fail(11)
	probe.probeOnce(t.Context())

	assert.Equal(t, int64(11), receiveRead(t, reads))
	assert.Equal(t, int64(22), receiveRead(t, reads))
}

// An installation whose read hangs costs only its own read: the reads after it
// in the same pass still run, so a hung installation cannot keep a healthy one
// unknown pass after pass.
func TestQuotaProbeHungInstallationDoesNotStarveTheRest(t *testing.T) {
	probe, reads, _ := recordingQuotaProbe(t, nil, 11, 22, 33)
	record := probe.readQuota
	probe.readTimeout = 50 * time.Millisecond
	probe.readQuota = func(ctx context.Context, id int64) error {
		if id == 11 {
			<-ctx.Done()
			return fmt.Errorf("installation %d did not answer: %w", id, ctx.Err())
		}
		return record(ctx, id)
	}
	probe.probeOnce(t.Context())

	assert.Equal(t, int64(22), receiveRead(t, reads))
	assert.Equal(t, int64(33), receiveRead(t, reads))
}

// A pass reads installations concurrently, so a slow installation does not hold
// up the ones next to it.
func TestQuotaProbeReadsInstallationsConcurrently(t *testing.T) {
	probe, _, _ := recordingQuotaProbe(t, nil, 11, 22)
	probe.readConcurrency = 2
	started := make(chan struct{})
	overlapped := make(chan bool, 1)
	probe.readQuota = func(ctx context.Context, id int64) error {
		if id == 22 {
			close(started)
			return nil
		}
		select {
		case <-started:
			overlapped <- true
			return nil
		case <-ctx.Done():
			overlapped <- false
			return fmt.Errorf("installation 22 was not read while installation 11 was in flight: %w", ctx.Err())
		}
	}
	probe.probeOnce(t.Context())

	assert.True(t, <-overlapped, "installation 22 was read only after installation 11 finished")
}

// A panic inside the probe, such as one raised while GitHub returns something
// unexpected during an outage, is contained to the read that raised it: the
// process keeps running and the other installations are still read.
func TestQuotaProbeContainsAPanickingRead(t *testing.T) {
	probe, reads, _ := recordingQuotaProbe(t, nil, 11, 22)
	record := probe.readQuota
	probe.readQuota = func(ctx context.Context, id int64) error {
		if id == 11 {
			panic("unexpected GitHub response")
		}
		return record(ctx, id)
	}
	require.NotPanics(t, func() { probe.probeOnce(t.Context()) })

	assert.Equal(t, int64(22), receiveRead(t, reads))
}

// A panic while listing installations ends that pass, not the process, and the
// next pass lists again.
func TestQuotaProbeContainsAPanickingList(t *testing.T) {
	probe, reads, _ := recordingQuotaProbe(t, nil, 11)
	list := probe.listInstallations
	var calls int
	probe.listInstallations = func(ctx context.Context) (quotaTargets, error) {
		calls++
		if calls == 1 {
			panic("unexpected installation list")
		}
		return list(ctx)
	}
	require.NotPanics(t, func() { probe.probeOnce(t.Context()) })
	probe.probeOnce(t.Context())

	assert.Equal(t, int64(11), receiveRead(t, reads))
}

// While GitHub is unreachable every list fails; the probe logs, reads nothing,
// and keeps running until it is stopped.
func TestQuotaProbeKeepsRunningWhileGitHubIsDown(t *testing.T) {
	probe, _, _ := recordingQuotaProbe(t, nil)
	probe.interval = 10 * time.Millisecond
	lists := make(chan struct{}, 64)
	probe.listInstallations = func(context.Context) (quotaTargets, error) {
		lists <- struct{}{}
		return quotaTargets{}, errors.New("dial tcp: connect: connection refused")
	}
	probe.Start(t.Context())
	t.Cleanup(probe.Stop)

	for range 3 {
		select {
		case <-lists:
		case <-time.After(quotaProbeTestDeadline):
			require.FailNow(t, "quota probe stopped listing while GitHub was down")
		}
	}
}

// Canceling the context the probe was started with stops it, the same as Stop,
// so the probe ends with the server's lifecycle.
func TestQuotaProbeStopsWhenItsContextIsCanceled(t *testing.T) {
	probe, reads, _ := recordingQuotaProbe(t, nil, 11)
	ctx, cancel := context.WithCancel(t.Context())
	probe.Start(ctx)
	t.Cleanup(probe.Stop)
	assert.Equal(t, int64(11), receiveRead(t, reads))

	cancel()
	exited := make(chan struct{})
	go func() {
		probe.wg.Wait()
		close(exited)
	}()
	select {
	case <-exited:
	case <-time.After(quotaProbeTestDeadline):
		require.FailNow(t, "quota probe kept running after its context was canceled")
	}
}

// When the installation list cannot be read, the pass reads nothing; the
// budgets already exported age out to unknown rather than being guessed.
func TestQuotaProbeReadsNothingWhenInstallationsCannotBeListed(t *testing.T) {
	probe, reads, _ := recordingQuotaProbe(t, errors.New("GitHub returned 502"), 11)
	probe.probeOnce(t.Context())

	select {
	case id := <-reads:
		assert.Failf(t, "unexpected budget read", "installation %d was read although listing failed", id)
	default:
	}
}

// The probe reads again on every interval until stopped. A second Start does
// not launch a second loop, and Stop can be called again after the probe has
// exited.
func TestQuotaProbeRepeatsUntilStopped(t *testing.T) {
	probe, reads, _ := recordingQuotaProbe(t, nil, 11)
	probe.interval = 10 * time.Millisecond
	probe.Start(t.Context())
	probe.Start(t.Context())

	for range 3 {
		assert.Equal(t, int64(11), receiveRead(t, reads))
	}
	probe.Stop()
	probe.Stop()
	assert.Nil(t, probe.cancel)
}

// Every pass publishes the installations it is responsible for, including the
// owners it could not resolve, before it reads any budget, so the count of
// unobserved installations reflects this pass even if a read then hangs.
func TestQuotaProbeReportsItsTargetsEachPass(t *testing.T) {
	probe, reads, _ := recordingQuotaProbe(t, nil)
	probe.listInstallations = func(context.Context) (quotaTargets, error) {
		return quotaTargets{installations: []int64{11}, unresolved: 1}, nil
	}
	var reported []quotaTargets
	probe.reportTargets = func(targets quotaTargets) { reported = append(reported, targets) }

	probe.probeOnce(t.Context())

	assert.Equal(t, int64(11), receiveRead(t, reads))
	assert.Equal(t, []quotaTargets{{installations: []int64{11}, unresolved: 1}}, reported)
}

// A pass that cannot list installations reports nothing, so the targets from
// the last good pass stay in place and their budgets age out to unobserved.
func TestQuotaProbeKeepsPreviousTargetsWhenListingFails(t *testing.T) {
	probe, _, _ := recordingQuotaProbe(t, errors.New("GitHub returned 502"))
	probe.reportTargets = func(targets quotaTargets) {
		assert.Failf(t, "unexpected target report", "targets %+v were reported although listing failed", targets)
	}

	probe.probeOnce(t.Context())
}

// An App installed on an account the server serves no repository of has a
// budget nobody spends. The serving probe resolves installations from the
// configured repositories, so that installation is never read: its untouched
// budget cannot stand in for the serving installation's lower one. One lookup
// answers each owner.
func TestServingInstallationsSkipInstallationsServingNoConfiguredRepo(t *testing.T) {
	client, server := newInstallationLookupClient(t, installedFor(map[string]int64{"octo": 7, "idle": 9}))

	targets := client.servingInstallations(t.Context(), []string{"octo/a", "octo/b", "octo/c"})

	assert.Equal(t, quotaTargets{installations: []int64{7}}, targets)
	assert.Equal(t, 1, server.total(), "one lookup answers the owner")
	assert.Zero(t, server.count("idle/anything"))
}

// When GitHub cannot say which installation serves an owner, the owner counts
// as unobserved instead of being dropped, so a gate on the budget fails closed.
// The owner's remaining repositories are not each looked up in turn, so GitHub
// being down costs one lookup per owner per pass.
func TestServingInstallationsCountUnresolvedOwnerAsUnobserved(t *testing.T) {
	client, server := newInstallationLookupClient(t, func(owner, _ string) (int, int64, string) {
		if owner == "octo" {
			return http.StatusBadGateway, 0, ""
		}
		return http.StatusOK, 9, owner
	})

	targets := client.servingInstallations(t.Context(), []string{"octo/a", "octo/b", "other/c"})

	assert.Equal(t, quotaTargets{installations: []int64{9}, unresolved: 1}, targets)
	assert.Equal(t, 1, server.count("octo/a"))
	assert.Zero(t, server.count("octo/b"))
}

// Every pass resolves each owner afresh, so an App that was uninstalled and
// reinstalled is probed under its new installation on the next pass rather
// than under the retired one until the server restarts. The fresh answer also
// replaces the cached installation webhook dispatch reads.
func TestServingInstallationsFollowAReinstall(t *testing.T) {
	ids := map[string]int64{"octo": 7}
	var mu sync.Mutex
	client, server := newInstallationLookupClient(t, func(owner, _ string) (int, int64, string) {
		mu.Lock()
		defer mu.Unlock()
		return http.StatusOK, ids[owner], owner
	})

	assert.Equal(t, quotaTargets{installations: []int64{7}}, client.servingInstallations(t.Context(), []string{"octo/a", "octo/b"}))
	mu.Lock()
	ids["octo"] = 8
	mu.Unlock()
	assert.Equal(t, quotaTargets{installations: []int64{8}}, client.servingInstallations(t.Context(), []string{"octo/a", "octo/b"}))

	assert.Equal(t, 2, server.total(), "one lookup per owner per pass")
	cached, ok := client.cachedOwnerInstallation("octo")
	require.True(t, ok)
	assert.Equal(t, int64(8), cached)
}

// An installation on an account other than the repository's owner answers only
// the repository asked about, so the owner's other repositories are resolved
// too and every installation serving them is read.
func TestServingInstallationsResolveEachRepositoryWhenAnswerIsNotOwnerWide(t *testing.T) {
	client, server := newInstallationLookupClient(t, func(owner, name string) (int, int64, string) {
		if name == "a" {
			return http.StatusOK, 5, "another-account"
		}
		return http.StatusOK, 7, owner
	})

	targets := client.servingInstallations(t.Context(), []string{"octo/a", "octo/b", "octo/c"})

	assert.Equal(t, quotaTargets{installations: []int64{5, 7}}, targets)
	assert.Equal(t, 1, server.count("octo/a"))
	assert.Equal(t, 1, server.count("octo/b"))
	assert.Zero(t, server.count("octo/c"), "the owner-wide answer stands for the rest")
}

// Once the pass's deadline has passed, no further lookups start, and every
// owner not yet resolved counts as unobserved, so a slow GitHub fails the gate
// closed instead of leaving owners out of it.
func TestServingInstallationsCountOwnersLeftAfterTheDeadlineAsUnobserved(t *testing.T) {
	client, server := newInstallationLookupClient(t, installedFor(map[string]int64{"octo": 7, "other": 9}))
	ctx, cancel := context.WithCancel(t.Context())
	cancel()

	targets := client.servingInstallations(ctx, []string{"octo/a", "other/b"})

	assert.Equal(t, quotaTargets{unresolved: 2}, targets)
	assert.Zero(t, server.total())
}

// A configured repository the App is not installed on is served by no
// installation, so it has no budget to observe and is not counted as
// unobserved. The owner's other repositories are still tried, since an
// installation limited to selected repositories may cover them.
func TestServingInstallationsSkipRepositoryAppIsNotInstalledOn(t *testing.T) {
	client, server := newInstallationLookupClient(t, func(owner, name string) (int, int64, string) {
		if name == "a" {
			return http.StatusNotFound, 0, ""
		}
		return http.StatusOK, 7, owner
	})

	targets := client.servingInstallations(t.Context(), []string{"octo/a", "octo/b"})

	assert.Equal(t, quotaTargets{installations: []int64{7}}, targets)
	assert.Equal(t, 1, server.count("octo/a"))
	assert.Equal(t, 1, server.count("octo/b"))
}

// Listing pages through every installation and skips suspended ones, whose
// token exchange GitHub refuses.
func TestActiveInstallationIDsPagesAndSkipsSuspended(t *testing.T) {
	var pages []string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, "/app/installations", r.URL.Path)
		page := r.URL.Query().Get("page")
		pages = append(pages, page)
		w.Header().Set("Content-Type", "application/json")
		switch page {
		case "", "1":
			next := *r.URL
			q := next.Query()
			q.Set("page", "2")
			next.RawQuery = q.Encode()
			w.Header().Set("Link", fmt.Sprintf(`<http://%s%s>; rel="next"`, r.Host, next.RequestURI()))
			_, _ = io.WriteString(w, `[{"id": 11}, {"id": 12, "suspended_at": "2026-01-02T03:04:05Z"}]`)
		case "2":
			_, _ = io.WriteString(w, `[{"id": 13}]`)
		default:
			assert.Failf(t, "unexpected page", "page %q", page)
		}
	}))
	t.Cleanup(srv.Close)

	client := gh.NewClient(srv.Client())
	base, err := url.Parse(srv.URL + "/")
	require.NoError(t, err)
	client.BaseURL = base

	ids, err := activeInstallationIDs(t.Context(), client, slog.New(slog.DiscardHandler))
	require.NoError(t, err)
	assert.Equal(t, []int64{11, 13}, ids)
	assert.Equal(t, []string{"", "2"}, pages)
}

// A budget read through an installation client records the installation's
// quota snapshot from the /rate_limit response headers, the same series the
// headers of any other response refresh.
func TestRateLimitReadRecordsInstallationSnapshot(t *testing.T) {
	reader := newGitHubMetricsTestReader(t)
	reset := time.Now().Add(time.Hour).Unix()
	rt := NewMetricsTransport(roundTripFunc(func(req *http.Request) (*http.Response, error) {
		assert.Equal(t, "/rate_limit", req.URL.Path)
		header := http.Header{}
		header.Set("Content-Type", "application/json")
		header.Set(headerRateLimitLimit, "15000")
		header.Set(headerRateLimitRemaining, "14250")
		header.Set(headerRateLimitUsed, "750")
		header.Set(headerRateLimitReset, strconv.FormatInt(reset, 10))
		header.Set(headerRateLimitResource, "core")
		body := fmt.Sprintf(`{"resources": {"core": {"limit": 15000, "remaining": 14250, "used": 750, "reset": %d}}}`, reset)
		return &http.Response{StatusCode: http.StatusOK, Header: header, Body: io.NopCloser(strings.NewReader(body)), Request: req}, nil
	}), 42, func() string { return "example-app" })
	ic := NewInstallationClient(gh.NewClient(&http.Client{Transport: rt}), slog.New(slog.DiscardHandler))

	remaining, limit, _, err := ic.CoreRateLimit(t.Context())
	require.NoError(t, err)
	assert.Equal(t, 14250, remaining)
	assert.Equal(t, 15000, limit)

	snapshot := installationQuotaSnapshot(t, reader)
	require.NotNil(t, snapshot, "no installation quota snapshot was recorded")
	assert.Equal(t, int64(14250), snapshot.Value)
	installation, _ := snapshot.Attributes.Value("installation_id")
	assert.Equal(t, "42", installation.AsString())
	app, _ := snapshot.Attributes.Value("github_app")
	assert.Equal(t, "example-app", app.AsString())
	resource, _ := snapshot.Attributes.Value("resource")
	assert.Equal(t, "core", resource.AsString())
}

// installationQuotaSnapshot returns the exported installation quota snapshot,
// or nil when none is exported.
func installationQuotaSnapshot(t *testing.T, reader *sdkmetric.ManualReader) *metricdata.DataPoint[int64] {
	t.Helper()
	var data metricdata.ResourceMetrics
	require.NoError(t, reader.Collect(t.Context(), &data))
	for _, scope := range data.ScopeMetrics {
		for _, m := range scope.Metrics {
			if m.Name != "schemabot.github.rate_limit.remaining" {
				continue
			}
			gauge, ok := m.Data.(metricdata.Gauge[int64])
			require.True(t, ok)
			for i, point := range gauge.DataPoints {
				if op, _ := point.Attributes.Value("operation"); op.AsString() == metrics.GitHubOperationRateLimitSnapshot {
					return &gauge.DataPoints[i]
				}
			}
		}
	}
	return nil
}

// An installation that has spent its whole budget still has that budget read.
// After GitHub answers an ordinary call with the primary quota error, further
// ordinary calls are refused locally until the reset, but the probe's
// GET /rate_limit still reaches GitHub and records the zero, so the exhausted
// installation reports 0 instead of aging out to unknown.
func TestRateLimitReadReachesGitHubAfterQuotaExhaustion(t *testing.T) {
	reader := newGitHubMetricsTestReader(t)
	reset := time.Now().Add(time.Hour).Unix()
	var mu sync.Mutex
	var sent []string
	rt := metricTestTransport(roundTripFunc(func(req *http.Request) (*http.Response, error) {
		mu.Lock()
		sent = append(sent, req.Method+" "+req.URL.Path)
		mu.Unlock()
		header := http.Header{}
		header.Set("Content-Type", "application/json")
		header.Set(headerRateLimitLimit, "15000")
		header.Set(headerRateLimitRemaining, "0")
		header.Set(headerRateLimitUsed, "15000")
		header.Set(headerRateLimitReset, strconv.FormatInt(reset, 10))
		header.Set(headerRateLimitResource, "core")
		if req.URL.Path == "/rate_limit" {
			body := fmt.Sprintf(`{"resources": {"core": {"limit": 15000, "remaining": 0, "used": 15000, "reset": %d}}}`, reset)
			return &http.Response{StatusCode: http.StatusOK, Header: header, Body: io.NopCloser(strings.NewReader(body)), Request: req}, nil
		}
		return &http.Response{StatusCode: http.StatusForbidden, Header: header, Body: io.NopCloser(strings.NewReader(`{"message": "API rate limit exceeded"}`)), Request: req}, nil
	}))
	pullRequest := "https://api.github.com/repos/octocat/example/pulls/1"
	readPullRequest := func() error {
		resp, err := rt.RoundTrip(throttlingRequest(t, t.Context(), http.MethodGet, pullRequest))
		if resp != nil {
			require.NoError(t, resp.Body.Close())
		}
		var quotaErr *github_primary_ratelimit.RateLimitReachedError
		if errors.As(err, &quotaErr) {
			require.NoError(t, quotaErr.Response.Body.Close())
		}
		return err
	}

	var quotaErr *github_primary_ratelimit.RateLimitReachedError
	require.ErrorAs(t, readPullRequest(), &quotaErr, "GitHub's quota error should exhaust the installation's core budget")

	ic := NewInstallationClient(gh.NewClient(&http.Client{Transport: rt}), slog.New(slog.DiscardHandler))
	remaining, limit, _, err := ic.CoreRateLimit(t.Context())
	require.NoError(t, err, "the budget read must not be refused locally while the budget is exhausted")
	assert.Equal(t, 0, remaining)
	assert.Equal(t, 15000, limit)

	require.ErrorAs(t, readPullRequest(), &quotaErr, "ordinary calls stay refused until the reset")
	mu.Lock()
	assert.Equal(t, []string{"GET /repos/octocat/example/pulls/1", "GET /rate_limit"}, sent,
		"only the budget read may reach GitHub while the budget is exhausted")
	mu.Unlock()

	snapshot := installationQuotaSnapshot(t, reader)
	require.NotNil(t, snapshot, "the exhausted budget should be exported, not unknown")
	assert.Equal(t, int64(0), snapshot.Value)
	installation, _ := snapshot.Attributes.Value("installation_id")
	assert.Equal(t, "42", installation.AsString())
}

// A budget read whose installation client must first retry the App slug fetch
// stops as soon as its context is canceled, rather than waiting out the App
// client's own timeout, so stopping the probe stays bounded.
func TestInstallationClientSlugRetryHonorsCancellation(t *testing.T) {
	slugRequested := make(chan struct{})
	var once sync.Once
	c := &Client{
		appID:         1,
		privateKey:    testInstallationPrivateKey(t),
		logger:        slog.New(slog.DiscardHandler),
		installations: map[int64]*InstallationClient{},
		appTransportBase: roundTripFunc(func(req *http.Request) (*http.Response, error) {
			assert.Equal(t, "/app", req.URL.Path)
			once.Do(func() { close(slugRequested) })
			<-req.Context().Done()
			return nil, req.Context().Err()
		}),
	}
	c.storeAppSlug("")

	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	done := make(chan struct{})
	go func() {
		defer close(done)
		_, err := c.forInstallation(ctx, 42)
		assert.NoError(t, err)
	}()
	select {
	case <-slugRequested:
	case <-time.After(quotaProbeTestDeadline):
		require.FailNow(t, "the App slug fetch was never sent")
	}
	cancel()
	select {
	case <-done:
	case <-time.After(time.Second):
		require.FailNow(t, "installation client acquisition kept waiting on the slug fetch after cancellation")
	}
}
