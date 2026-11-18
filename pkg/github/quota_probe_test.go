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

	gh "github.com/google/go-github/v86/github"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
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
		listInstallations: func(context.Context) ([]int64, error) {
			if listErr != nil {
				return nil, listErr
			}
			return ids, nil
		},
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

	var data metricdata.ResourceMetrics
	require.NoError(t, reader.Collect(t.Context(), &data))
	var snapshot *metricdata.DataPoint[int64]
	for _, scope := range data.ScopeMetrics {
		for _, m := range scope.Metrics {
			if m.Name != "schemabot.github.rate_limit.remaining" {
				continue
			}
			gauge, ok := m.Data.(metricdata.Gauge[int64])
			require.True(t, ok)
			for i, point := range gauge.DataPoints {
				if op, _ := point.Attributes.Value("operation"); op.AsString() == metrics.GitHubOperationRateLimitSnapshot {
					snapshot = &gauge.DataPoints[i]
				}
			}
		}
	}
	require.NotNil(t, snapshot, "no installation quota snapshot was recorded")
	assert.Equal(t, int64(14250), snapshot.Value)
	installation, _ := snapshot.Attributes.Value("installation_id")
	assert.Equal(t, "42", installation.AsString())
	app, _ := snapshot.Attributes.Value("github_app")
	assert.Equal(t, "example-app", app.AsString())
	resource, _ := snapshot.Attributes.Value("resource")
	assert.Equal(t, "core", resource.AsString())
}
