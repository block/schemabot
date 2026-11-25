package livee2e

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/url"
	"os"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/prometheus/common/expfmt"
	"github.com/prometheus/common/model"
)

// GitHubRequestsMetric is the Prometheus name of SchemaBot's counter of GitHub
// API request attempts, schemabot.github.requests_total.
const GitHubRequestsMetric = "schemabot_github_requests_total"

// GitHubCallsGauge is the gauge a run sends with the GitHub API calls
// SchemaBot made during it. A deploy gate holds the release when the gauge is
// over its budget or missing.
const GitHubCallsGauge = "schemabot.e2e.github_calls"

// ErrNoCallCount reports that the GitHub calls a run caused cannot be counted.
// A count taken across a restarted container or a replaced pod would read low
// and let a release that multiplies GitHub calls through, so no count is sent
// and the gate, seeing no data, holds the release.
var ErrNoCallCount = errors.New("no GitHub call count")

// Pod is one SchemaBot process whose GitHub request counter a run reads.
type Pod struct {
	// UID identifies the pod. A pod replaced during the run has a new UID.
	UID string
	// Process identifies the processes running in the pod: every container's
	// name and container ID. A container restart keeps the pod UID but gets a
	// new container ID, and its counters start again from zero, so a changed
	// Process is a counter reset even when the new counter has climbed past
	// the old one. Empty means the identity is unknown, which voids a count.
	Process string
	// Name is the pod's namespace/name, for logs.
	Name string
	// MetricsURL is the pod's Prometheus endpoint.
	MetricsURL string
}

// PodLister lists the SchemaBot pods whose GitHub calls a run counts.
type PodLister interface {
	ListPods(ctx context.Context) ([]Pod, error)
}

// PodCalls is one pod's GitHub request counter and the processes that served it.
type PodCalls struct {
	Process string
	Calls   float64
}

// CallSnapshot is each pod's GitHub request counter at one moment, by pod UID.
type CallSnapshot map[string]PodCalls

// ReadGitHubCalls lists the pods, reads every pod's GitHub request counter,
// and lists the pods again. Any pod that cannot be read fails the snapshot,
// because a count missing a pod would read low. So does a pod whose identity
// is unknown, or whose containers changed between the two lists, because the
// counter read may belong to a process that has just started.
func ReadGitHubCalls(ctx context.Context, pods PodLister, client *http.Client) (CallSnapshot, error) {
	list, err := pods.ListPods(ctx)
	if err != nil {
		return nil, fmt.Errorf("list SchemaBot pods: %w", err)
	}
	if len(list) == 0 {
		return nil, fmt.Errorf("list SchemaBot pods: none running: %w", ErrNoCallCount)
	}
	snap := make(CallSnapshot, len(list))
	for _, pod := range list {
		if pod.Process == "" {
			return nil, fmt.Errorf("pod %s has no container identity: %w", pod.Name, ErrNoCallCount)
		}
		n, err := scrapeGitHubRequests(ctx, client, pod.MetricsURL)
		if err != nil {
			return nil, fmt.Errorf("read GitHub requests from pod %s (%s): %w", pod.Name, pod.MetricsURL, err)
		}
		snap[pod.UID] = PodCalls{Process: pod.Process, Calls: n}
	}
	again, err := pods.ListPods(ctx)
	if err != nil {
		return nil, fmt.Errorf("list SchemaBot pods after reading them: %w", err)
	}
	if err := sameProcesses(list, again); err != nil {
		return nil, fmt.Errorf("SchemaBot pods changed while being read: %w", err)
	}
	return snap, nil
}

// sameProcesses reports whether two pod lists hold the same pods running the
// same containers.
func sameProcesses(a, b []Pod) error {
	if len(a) != len(b) {
		return fmt.Errorf("%d pods, then %d: %w", len(a), len(b), ErrNoCallCount)
	}
	process := make(map[string]string, len(b))
	for _, pod := range b {
		process[pod.UID] = pod.Process
	}
	for _, pod := range a {
		now, ok := process[pod.UID]
		if !ok {
			return fmt.Errorf("pod %s went away: %w", pod.Name, ErrNoCallCount)
		}
		if now != pod.Process {
			return fmt.Errorf("pod %s restarted a container: %w", pod.Name, ErrNoCallCount)
		}
	}
	return nil
}

// GitHubCallsDuring returns the GitHub calls made between two snapshots: the
// sum of each pod's increase. It returns ErrNoCallCount when the set of pods
// changed or a pod restarted a container, because a new process's counter
// starts at zero, and when the sum is not positive, because every run makes
// calls.
//
// Two snapshots cannot see a pod that started and went away between them, so
// the count holds only while the replica set stays fixed for the run: no
// autoscaling and no rollout of the deployment under test.
func GitHubCallsDuring(before, after CallSnapshot) (int64, error) {
	if !samePods(before, after) {
		return 0, fmt.Errorf("SchemaBot pods changed during the run (%d before, %d after): %w",
			len(before), len(after), ErrNoCallCount)
	}
	var total float64
	for uid, now := range after {
		then := before[uid]
		if now.Process != then.Process {
			return 0, fmt.Errorf("pod %s restarted a container during the run: %w", uid, ErrNoCallCount)
		}
		if now.Calls < then.Calls {
			return 0, fmt.Errorf("pod %s counter went from %v to %v: %w", uid, then.Calls, now.Calls, ErrNoCallCount)
		}
		total += now.Calls - then.Calls
	}
	if total <= 0 {
		return 0, fmt.Errorf("SchemaBot made %v GitHub calls during the run: %w", total, ErrNoCallCount)
	}
	return int64(total), nil
}

func samePods(a, b CallSnapshot) bool {
	if len(a) != len(b) {
		return false
	}
	for uid := range a {
		if _, ok := b[uid]; !ok {
			return false
		}
	}
	return true
}

// scrapeGitHubRequests reads a Prometheus endpoint and sums every series of
// the GitHub request counter.
func scrapeGitHubRequests(ctx context.Context, client *http.Client, endpoint string) (float64, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint, nil)
	if err != nil {
		return 0, fmt.Errorf("build request: %w", err)
	}
	resp, err := client.Do(req)
	if err != nil {
		return 0, fmt.Errorf("scrape: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return 0, fmt.Errorf("scrape: HTTP %d", resp.StatusCode)
	}
	return sumCounter(resp.Body, GitHubRequestsMetric)
}

// sumCounter sums every series of a counter in Prometheus text exposition. A
// process that has made no GitHub calls yet exposes no series, which is zero.
func sumCounter(r io.Reader, name string) (float64, error) {
	parser := expfmt.NewTextParser(model.UTF8Validation)
	families, err := parser.TextToMetricFamilies(r)
	if err != nil {
		return 0, fmt.Errorf("parse metrics: %w", err)
	}
	family, ok := families[name]
	if !ok {
		return 0, nil
	}
	var total float64
	for _, m := range family.GetMetric() {
		if m.GetCounter() == nil {
			return 0, fmt.Errorf("metric %s is not a counter", name)
		}
		total += m.GetCounter().GetValue()
	}
	return total, nil
}

const serviceAccountDir = "/var/run/secrets/kubernetes.io/serviceaccount"

// KubernetesPods lists running pods that match a label selector in each
// namespace, through the Kubernetes API with the pod's service account. The
// service account needs to list pods in every namespace.
type KubernetesPods struct {
	apiURL      string
	token       string
	client      *http.Client
	namespaces  []string
	selector    string
	metricsPort int
}

// NewInClusterPods reaches the API at host and port, which a pod gets as
// KUBERNETES_SERVICE_HOST and KUBERNETES_SERVICE_PORT, with the credentials of
// the mounted service account.
func NewInClusterPods(host, port string, namespaces []string, selector string, metricsPort int) (*KubernetesPods, error) {
	if host == "" || port == "" {
		return nil, errors.New("KUBERNETES_SERVICE_HOST and KUBERNETES_SERVICE_PORT are not set; counting GitHub calls needs to run in a pod")
	}
	token, err := os.ReadFile(serviceAccountDir + "/token")
	if err != nil {
		return nil, fmt.Errorf("read the service account token: %w", err)
	}
	ca, err := os.ReadFile(serviceAccountDir + "/ca.crt")
	if err != nil {
		return nil, fmt.Errorf("read the service account CA: %w", err)
	}
	pool := x509.NewCertPool()
	if !pool.AppendCertsFromPEM(ca) {
		return nil, errors.New("the service account CA holds no certificate")
	}
	client := &http.Client{
		Timeout:   30 * time.Second,
		Transport: &http.Transport{TLSClientConfig: &tls.Config{RootCAs: pool, MinVersion: tls.VersionTLS12}},
	}
	return NewKubernetesPods("https://"+net.JoinHostPort(host, port), strings.TrimSpace(string(token)), client,
		namespaces, selector, metricsPort)
}

// NewKubernetesPods lists pods through the API at apiURL with a bearer token.
func NewKubernetesPods(apiURL, token string, client *http.Client, namespaces []string, selector string, metricsPort int) (*KubernetesPods, error) {
	if len(namespaces) == 0 {
		return nil, errors.New("no namespaces to list SchemaBot pods in")
	}
	if selector == "" {
		return nil, errors.New("no label selector for SchemaBot pods")
	}
	if metricsPort <= 0 {
		return nil, fmt.Errorf("invalid metrics port %d", metricsPort)
	}
	return &KubernetesPods{apiURL: apiURL, token: token, client: client,
		namespaces: namespaces, selector: selector, metricsPort: metricsPort}, nil
}

type podList struct {
	Items []struct {
		Metadata struct {
			Name      string `json:"name"`
			Namespace string `json:"namespace"`
			UID       string `json:"uid"`
		} `json:"metadata"`
		Status struct {
			Phase             string `json:"phase"`
			PodIP             string `json:"podIP"`
			ContainerStatuses []struct {
				Name        string `json:"name"`
				ContainerID string `json:"containerID"`
			} `json:"containerStatuses"`
		} `json:"status"`
	} `json:"items"`
}

// ListPods returns the running pods, terminating ones included: a pod still
// serving counts its calls, and one that goes away during the run changes the
// set, which voids the count.
func (k *KubernetesPods) ListPods(ctx context.Context) ([]Pod, error) {
	var pods []Pod
	for _, ns := range k.namespaces {
		list, err := k.listNamespace(ctx, ns)
		if err != nil {
			return nil, fmt.Errorf("list pods in namespace %s: %w", ns, err)
		}
		for _, item := range list.Items {
			if item.Status.Phase != "Running" || item.Status.PodIP == "" {
				continue
			}
			var containers []string
			for _, c := range item.Status.ContainerStatuses {
				if c.ContainerID == "" {
					containers = nil
					break
				}
				containers = append(containers, c.Name+"="+c.ContainerID)
			}
			slices.Sort(containers)
			pods = append(pods, Pod{
				UID:     item.Metadata.UID,
				Name:    item.Metadata.Namespace + "/" + item.Metadata.Name,
				Process: strings.Join(containers, ","),
				MetricsURL: "http://" + net.JoinHostPort(item.Status.PodIP, strconv.Itoa(k.metricsPort)) +
					"/metrics",
			})
		}
	}
	slices.SortFunc(pods, func(a, b Pod) int { return strings.Compare(a.Name, b.Name) })
	return pods, nil
}

func (k *KubernetesPods) listNamespace(ctx context.Context, ns string) (*podList, error) {
	endpoint := fmt.Sprintf("%s/api/v1/namespaces/%s/pods?labelSelector=%s",
		k.apiURL, url.PathEscape(ns), url.QueryEscape(k.selector))
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint, nil)
	if err != nil {
		return nil, fmt.Errorf("build request: %w", err)
	}
	req.Header.Set("Authorization", "Bearer "+k.token)
	resp, err := k.client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("request: %w", err)
	}
	defer resp.Body.Close()
	var list podList
	if err := decodePodList(resp, &list); err != nil {
		return nil, err
	}
	return &list, nil
}

func decodePodList(resp *http.Response, list *podList) error {
	if resp.StatusCode != http.StatusOK {
		body, err := io.ReadAll(io.LimitReader(resp.Body, 512))
		if err != nil {
			return fmt.Errorf("HTTP %d, and reading the body failed: %w", resp.StatusCode, err)
		}
		return fmt.Errorf("HTTP %d: %s", resp.StatusCode, strings.TrimSpace(string(body)))
	}
	if err := json.NewDecoder(resp.Body).Decode(list); err != nil {
		return fmt.Errorf("decode pod list: %w", err)
	}
	return nil
}

// Gauge sends one gauge value. The DogStatsD client satisfies it.
type Gauge interface {
	Gauge(name string, value float64, tags []string, rate float64) error
}

// GitHubCallCounter counts the GitHub calls SchemaBot makes during a run and
// sends the count as GitHubCallsGauge. Start reads every pod's counter before
// the run and Finish reads it again after. The count assumes the replica set
// stays fixed for the run (see GitHubCallsDuring).
type GitHubCallCounter struct {
	pods   PodLister
	client *http.Client
	gauge  Gauge
	tags   []string
	logger *slog.Logger

	before    CallSnapshot
	beforeErr error
}

// NewGitHubCallCounter counts through pods, scraping each with client, and
// sends the count to gauge with tags, which tie it to the deploy under test.
func NewGitHubCallCounter(pods PodLister, client *http.Client, gauge Gauge, tags []string, logger *slog.Logger) *GitHubCallCounter {
	return &GitHubCallCounter{pods: pods, client: client, gauge: gauge, tags: tags, logger: logger}
}

// Start reads every pod's counter. A failed read is kept for Finish, which
// then sends no count: the run itself still goes ahead.
func (c *GitHubCallCounter) Start(ctx context.Context) {
	c.before, c.beforeErr = ReadGitHubCalls(ctx, c.pods, c.client)
	if c.beforeErr != nil {
		c.logger.Error("reading GitHub calls before the run failed; no count will be sent and the gate will hold the release",
			"error", c.beforeErr)
		return
	}
	c.logger.Info("read GitHub calls before the run", "pods", len(c.before))
}

// Finish reads every pod's counter again and sends the difference. When the
// count cannot be taken it sends nothing, so the gate sees no data and holds
// the release, and it returns the reason.
func (c *GitHubCallCounter) Finish(ctx context.Context) (int64, error) {
	if c.beforeErr != nil {
		return 0, fmt.Errorf("before the run: %w", c.beforeErr)
	}
	after, err := ReadGitHubCalls(ctx, c.pods, c.client)
	if err != nil {
		return 0, fmt.Errorf("after the run: %w", err)
	}
	calls, err := GitHubCallsDuring(c.before, after)
	if err != nil {
		return 0, err
	}
	if err := c.gauge.Gauge(GitHubCallsGauge, float64(calls), c.tags, 1); err != nil {
		return 0, fmt.Errorf("send %s: %w", GitHubCallsGauge, err)
	}
	c.logger.Info("sent GitHub calls", "metric", GitHubCallsGauge, "calls", calls, "pods", len(after), "tags", c.tags)
	return calls, nil
}
