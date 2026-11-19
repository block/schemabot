package livee2e

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel"
	otelprom "go.opentelemetry.io/otel/exporters/prometheus"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"

	"github.com/block/schemabot/pkg/metrics"
)

// The runner reads the counter SchemaBot serves, under the name SchemaBot's
// Prometheus endpoint gives it, summed across every label set.
func TestSumCounterReadsSchemaBotsGitHubRequests(t *testing.T) {
	registry := prometheus.NewRegistry()
	exporter, err := otelprom.New(otelprom.WithRegisterer(registry))
	require.NoError(t, err)
	provider := sdkmetric.NewMeterProvider(sdkmetric.WithReader(exporter))
	previous := otel.GetMeterProvider()
	otel.SetMeterProvider(provider)
	t.Cleanup(func() {
		otel.SetMeterProvider(previous)
		require.NoError(t, provider.Shutdown(context.WithoutCancel(t.Context())))
	})

	for _, op := range []string{metrics.GitHubOperationCreateCheckRun, metrics.GitHubOperationCreateCheckRun, metrics.GitHubOperationAddCommentReaction} {
		metrics.RecordGitHubRequest(t.Context(), metrics.GitHubRequestSample{
			Operation: op, Category: "rest", Status: "success",
			Repository: "octocat/hello-world", GitHubApp: "example-app", InstallationID: 42,
		})
	}
	server := httptest.NewServer(promhttp.HandlerFor(registry, promhttp.HandlerOpts{}))
	t.Cleanup(server.Close)

	got, err := scrapeGitHubRequests(t.Context(), server.Client(), server.URL)
	require.NoError(t, err)
	assert.InDelta(t, 3, got, 0)
}

func TestSumCounterWithoutSeriesIsZero(t *testing.T) {
	got, err := sumCounter(strings.NewReader("# TYPE other_total counter\nother_total 7\n"), GitHubRequestsMetric)
	require.NoError(t, err)
	assert.InDelta(t, 0, got, 0)
}

func TestGitHubCallsDuring(t *testing.T) {
	calls := func(process string, n float64) PodCalls { return PodCalls{Process: process, Calls: n} }
	tests := []struct {
		name          string
		before, after CallSnapshot
		want          int64
		noCount       bool
	}{
		{name: "sums each pod's increase",
			before: CallSnapshot{"a": calls("a1", 100), "b": calls("b1", 40)},
			after:  CallSnapshot{"a": calls("a1", 310), "b": calls("b1", 75)}, want: 245},
		{name: "a replaced pod voids the count",
			before: CallSnapshot{"a": calls("a1", 100), "b": calls("b1", 40)},
			after:  CallSnapshot{"a": calls("a1", 310), "c": calls("c1", 5)}, noCount: true},
		{name: "an added pod voids the count",
			before: CallSnapshot{"a": calls("a1", 100)},
			after:  CallSnapshot{"a": calls("a1", 310), "c": calls("c1", 5)}, noCount: true},
		{name: "a removed pod voids the count",
			before: CallSnapshot{"a": calls("a1", 100), "b": calls("b1", 40)},
			after:  CallSnapshot{"a": calls("a1", 310)}, noCount: true},
		{name: "a counter that went down voids the count",
			before: CallSnapshot{"a": calls("a1", 100), "b": calls("b1", 40)},
			after:  CallSnapshot{"a": calls("a1", 310), "b": calls("b1", 2)}, noCount: true},
		// 1,000 calls before the restart and 150 after: the counter reads
		// above its baseline, but the pod made 1,150 calls, not 50.
		{name: "a restarted container voids the count even when its counter climbed past the baseline",
			before: CallSnapshot{"a": calls("a1", 100)},
			after:  CallSnapshot{"a": calls("a2", 150)}, noCount: true},
		{name: "no calls voids the count",
			before: CallSnapshot{"a": calls("a1", 100)},
			after:  CallSnapshot{"a": calls("a1", 100)}, noCount: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := GitHubCallsDuring(tt.before, tt.after)
			if tt.noCount {
				require.ErrorIs(t, err, ErrNoCallCount)
				return
			}
			require.NoError(t, err)
			assert.Equal(t, tt.want, got)
		})
	}
}

func TestKubernetesPodsListsRunningPodsInEveryNamespace(t *testing.T) {
	var paths []string
	api := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, "Bearer test-token", r.Header.Get("Authorization"))
		assert.Equal(t, "app.kubernetes.io/name=schemabot", r.URL.Query().Get("labelSelector"))
		paths = append(paths, r.URL.Path)
		ns := strings.Split(r.URL.Path, "/")[4]
		_, err := fmt.Fprintf(w, `{"items":[
			{"metadata":{"name":"schemabot-1","namespace":%[1]q,"uid":"%[1]s-uid-1"},"status":{"phase":"Running","podIP":"10.0.0.1",
				"containerStatuses":[{"name":"schemabot","containerID":"containerd://%[1]s-sb"},{"name":"istio-proxy","containerID":"containerd://%[1]s-ip"}]}},
			{"metadata":{"name":"schemabot-4","namespace":%[1]q,"uid":"%[1]s-uid-4"},"status":{"phase":"Running","podIP":"10.0.0.4",
				"containerStatuses":[{"name":"schemabot","containerID":""}]}},
			{"metadata":{"name":"schemabot-2","namespace":%[1]q,"uid":"%[1]s-uid-2"},"status":{"phase":"Pending","podIP":""}},
			{"metadata":{"name":"schemabot-3","namespace":%[1]q,"uid":"%[1]s-uid-3"},"status":{"phase":"Running","podIP":""}}
		]}`, ns)
		assert.NoError(t, err)
	}))
	t.Cleanup(api.Close)

	pods, err := NewKubernetesPods(api.URL, "test-token", api.Client(), []string{"control", "data"}, "app.kubernetes.io/name=schemabot", 9102)
	require.NoError(t, err)
	got, err := pods.ListPods(t.Context())
	require.NoError(t, err)

	assert.Equal(t, []string{"/api/v1/namespaces/control/pods", "/api/v1/namespaces/data/pods"}, paths)
	assert.Equal(t, []Pod{
		{UID: "control-uid-1", Name: "control/schemabot-1", MetricsURL: "http://10.0.0.1:9102/metrics",
			Process: "istio-proxy=containerd://control-ip,schemabot=containerd://control-sb"},
		{UID: "control-uid-4", Name: "control/schemabot-4", MetricsURL: "http://10.0.0.4:9102/metrics"},
		{UID: "data-uid-1", Name: "data/schemabot-1", MetricsURL: "http://10.0.0.1:9102/metrics",
			Process: "istio-proxy=containerd://data-ip,schemabot=containerd://data-sb"},
		{UID: "data-uid-4", Name: "data/schemabot-4", MetricsURL: "http://10.0.0.4:9102/metrics"},
	}, got)
}

func TestKubernetesPodsReportsAPIRefusal(t *testing.T) {
	api := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		http.Error(w, `pods is forbidden: cannot list resource "pods"`, http.StatusForbidden)
	}))
	t.Cleanup(api.Close)

	pods, err := NewKubernetesPods(api.URL, "test-token", api.Client(), []string{"control"}, "app=schemabot", 9102)
	require.NoError(t, err)
	_, err = pods.ListPods(t.Context())
	require.Error(t, err)
	assert.Contains(t, err.Error(), "namespace control")
	assert.Contains(t, err.Error(), "HTTP 403")
	assert.Contains(t, err.Error(), "cannot list resource")
}

// metricsPod serves a GitHub request counter whose value the test sets.
type metricsPod struct {
	value  atomic.Int64
	status atomic.Int32
	server *httptest.Server
}

func newMetricsPod(t *testing.T, value int64) *metricsPod {
	t.Helper()
	p := &metricsPod{}
	p.value.Store(value)
	p.status.Store(http.StatusOK)
	p.server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(int(p.status.Load()))
		_, err := fmt.Fprintf(w, "# TYPE %[1]s counter\n%[1]s{operation=\"create_check_run\"} %[2]d\n%[1]s{operation=\"get_pull_request\"} 1\n",
			GitHubRequestsMetric, p.value.Load())
		assert.NoError(t, err)
	}))
	t.Cleanup(p.server.Close)
	return p
}

// fakePods returns the pods its pods field holds at the time of each call,
// after running onList, which a test uses to change the pods mid-read.
type fakePods struct {
	pods   []Pod
	onList func(call int)
	calls  int
}

func (f *fakePods) ListPods(context.Context) ([]Pod, error) {
	f.calls++
	if f.onList != nil {
		f.onList(f.calls)
	}
	return f.pods, nil
}

func pod(uid, process, url string) Pod {
	return Pod{UID: uid, Name: "ns/" + uid, Process: process, MetricsURL: url}
}

type sentGauge struct {
	name  string
	value float64
	tags  []string
}

type fakeGauge struct{ sent []sentGauge }

func (f *fakeGauge) Gauge(name string, value float64, tags []string, _ float64) error {
	f.sent = append(f.sent, sentGauge{name, value, tags})
	return nil
}

func discardLogger() *slog.Logger { return slog.New(slog.NewTextHandler(io.Discard, nil)) }

// A run whose pods stay up sends one gauge with the calls made between the
// two reads, tagged for the deploy under test.
func TestGitHubCallCounterSendsTheRunsCalls(t *testing.T) {
	a, b := newMetricsPod(t, 100), newMetricsPod(t, 40)
	pods := &fakePods{pods: []Pod{pod("a", "a1", a.server.URL), pod("b", "b1", b.server.URL)}}
	gauge := &fakeGauge{}
	counter := NewGitHubCallCounter(pods, http.DefaultClient, gauge, []string{"values_sha:abc123", "stage:control-plane"}, discardLogger())

	counter.Start(t.Context())
	a.value.Store(310)
	b.value.Store(75)
	calls, err := counter.Finish(t.Context())

	require.NoError(t, err)
	assert.Equal(t, int64(245), calls)
	assert.Equal(t, []sentGauge{{GitHubCallsGauge, 245, []string{"values_sha:abc123", "stage:control-plane"}}}, gauge.sent)
}

// A pod replaced during the run starts its counter at zero, so the count would
// read low; nothing is sent and the gate holds the release.
func TestGitHubCallCounterSendsNothingWhenAPodIsReplaced(t *testing.T) {
	a, b := newMetricsPod(t, 100), newMetricsPod(t, 40)
	pods := &fakePods{pods: []Pod{pod("a", "a1", a.server.URL), pod("b", "b1", b.server.URL)}}
	gauge := &fakeGauge{}
	counter := NewGitHubCallCounter(pods, http.DefaultClient, gauge, nil, discardLogger())

	counter.Start(t.Context())
	replacement := newMetricsPod(t, 5)
	pods.pods = []Pod{pod("a", "a1", a.server.URL), pod("c", "c1", replacement.server.URL)}
	a.value.Store(310)
	_, err := counter.Finish(t.Context())

	require.ErrorIs(t, err, ErrNoCallCount)
	assert.Empty(t, gauge.sent)
}

// A pod whose endpoint fails would drop out of the sum, so the count is not
// taken at all.
func TestGitHubCallCounterSendsNothingWhenAPodCannotBeRead(t *testing.T) {
	a, b := newMetricsPod(t, 100), newMetricsPod(t, 40)
	pods := &fakePods{pods: []Pod{pod("a", "a1", a.server.URL), pod("b", "b1", b.server.URL)}}
	gauge := &fakeGauge{}
	counter := NewGitHubCallCounter(pods, http.DefaultClient, gauge, nil, discardLogger())

	counter.Start(t.Context())
	a.value.Store(310)
	b.status.Store(http.StatusInternalServerError)
	_, err := counter.Finish(t.Context())

	require.Error(t, err)
	assert.Contains(t, err.Error(), "ns/b")
	assert.Contains(t, err.Error(), "HTTP 500")
	assert.Empty(t, gauge.sent)
}

// A failed read before the run leaves nothing to subtract from, so Finish
// sends nothing even when the pods answer afterwards.
func TestGitHubCallCounterSendsNothingWhenTheFirstReadFailed(t *testing.T) {
	a := newMetricsPod(t, 100)
	a.status.Store(http.StatusServiceUnavailable)
	pods := &fakePods{pods: []Pod{pod("a", "a1", a.server.URL)}}
	gauge := &fakeGauge{}
	counter := NewGitHubCallCounter(pods, http.DefaultClient, gauge, nil, discardLogger())

	counter.Start(t.Context())
	a.status.Store(http.StatusOK)
	a.value.Store(310)
	_, err := counter.Finish(t.Context())

	require.Error(t, err)
	assert.Contains(t, err.Error(), "before the run")
	assert.Contains(t, err.Error(), "HTTP 503")
	assert.Empty(t, gauge.sent)
}

func TestReadGitHubCallsWithNoPodsHasNoCount(t *testing.T) {
	_, err := ReadGitHubCalls(t.Context(), &fakePods{}, http.DefaultClient)
	require.ErrorIs(t, err, ErrNoCallCount)
}

// A container restart keeps the pod UID. The new process's counter can climb
// past the old baseline before the second read (100 before the run, 1,000
// calls, a restart, then 150 more), so the difference would read 50 for 1,150
// calls. The changed container identity voids the count instead.
func TestGitHubCallCounterSendsNothingWhenAContainerRestarts(t *testing.T) {
	a := newMetricsPod(t, 100)
	pods := &fakePods{pods: []Pod{pod("a", "a1", a.server.URL)}}
	gauge := &fakeGauge{}
	counter := NewGitHubCallCounter(pods, http.DefaultClient, gauge, nil, discardLogger())

	counter.Start(t.Context())
	pods.pods = []Pod{pod("a", "a2", a.server.URL)}
	a.value.Store(150)
	_, err := counter.Finish(t.Context())

	require.ErrorIs(t, err, ErrNoCallCount)
	assert.Contains(t, err.Error(), "restarted a container")
	assert.Empty(t, gauge.sent)
}

// A container that restarts between listing the pods and scraping them would
// hand over the new process's counter under the old identity, so a snapshot
// lists the pods again after scraping and fails when anything changed.
func TestReadGitHubCallsFailsWhenAContainerRestartsDuringTheRead(t *testing.T) {
	a := newMetricsPod(t, 100)
	pods := &fakePods{pods: []Pod{pod("a", "a1", a.server.URL)}}
	pods.onList = func(call int) {
		if call == 2 {
			pods.pods = []Pod{pod("a", "a2", a.server.URL)}
		}
	}

	_, err := ReadGitHubCalls(t.Context(), pods, http.DefaultClient)

	require.ErrorIs(t, err, ErrNoCallCount)
	assert.Contains(t, err.Error(), "ns/a restarted a container")
}

// Without a container identity a restart cannot be told apart from a counter
// that kept climbing, so the snapshot is not taken.
func TestReadGitHubCallsFailsWithoutAContainerIdentity(t *testing.T) {
	a := newMetricsPod(t, 100)
	pods := &fakePods{pods: []Pod{pod("a", "", a.server.URL)}}

	_, err := ReadGitHubCalls(t.Context(), pods, http.DefaultClient)

	require.ErrorIs(t, err, ErrNoCallCount)
	assert.Contains(t, err.Error(), "no container identity")
}
