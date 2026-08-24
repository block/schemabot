//go:build e2e

package k8s

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/block/schemabot/e2e/testutil"
	"github.com/block/spirit/pkg/utils"
	"github.com/stretchr/testify/require"
)

// k8sNamespace is the namespace holding the stack under test. The runner
// script derives a per-run namespace so concurrent suites on one cluster
// deploy isolated stacks, and hands it to the tests here; the fixed default
// covers running the tests by hand against a manually deployed stack.
var k8sNamespace = func() string {
	if ns := os.Getenv("E2E_K8S_NAMESPACE"); ns != "" {
		return ns
	}
	return "schemabot-e2e"
}()

func runKubectl(t *testing.T, args ...string) string {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, "kubectl", args...)
	output, err := cmd.CombinedOutput()
	require.NoErrorf(t, err, "kubectl %s failed\nOutput: %s", strings.Join(args, " "), string(output))
	return string(output)
}

func crashPod(t *testing.T, instance string) string {
	t.Helper()
	pods := podNamesForInstance(t, instance)
	require.NotEmpty(t, pods, "expected pod for %s", instance)
	pod := pods[0]

	runKubectl(t, "delete", "pod", "-n", k8sNamespace, pod, "--grace-period=0", "--force", "--wait=false")
	return pod
}

func crashPods(t *testing.T, instance string) []string {
	t.Helper()
	pods := podNamesForInstance(t, instance)
	require.NotEmpty(t, pods, "expected pods for %s", instance)

	args := []string{"delete", "pod", "-n", k8sNamespace}
	args = append(args, pods...)
	args = append(args, "--grace-period=0", "--force", "--wait=false")
	runKubectl(t, args...)
	return pods
}

func podNamesForInstance(t *testing.T, instance string) []string {
	t.Helper()
	selector := "app.kubernetes.io/instance=" + instance
	output := runKubectl(t, "get", "pod", "-n", k8sNamespace, "-l", selector, "-o", "jsonpath={range .items[*]}{.metadata.name}{\"\\n\"}{end}")
	var pods []string
	for line := range strings.SplitSeq(output, "\n") {
		pod := strings.TrimSpace(line)
		if pod != "" {
			pods = append(pods, pod)
		}
	}
	return pods
}

func desiredReplicasForInstance(t *testing.T, instance string) int {
	t.Helper()
	selector := "app.kubernetes.io/instance=" + instance
	output := strings.TrimSpace(runKubectl(t, "get", "deployment", "-n", k8sNamespace, "-l", selector, "-o", "jsonpath={.items[0].spec.replicas}"))
	replicas, err := strconv.Atoi(output)
	require.NoError(t, err, "parse desired replicas for %s", instance)
	require.Positive(t, replicas, "expected desired replicas for %s", instance)
	return replicas
}

func waitForPodsReadyAfterDeletion(t *testing.T, instance string, previousPods []string, timeout time.Duration) {
	t.Helper()
	desiredReplicas := desiredReplicasForInstance(t, instance)
	previous := make(map[string]bool, len(previousPods))
	for _, pod := range previousPods {
		previous[pod] = true
	}

	selector := "app.kubernetes.io/instance=" + instance
	var readyPods []string
	testutil.Poll(t, timeout, 500*time.Millisecond,
		func() bool {
			readyPods = readyPods[:0]
			output := runKubectl(t, "get", "pod", "-n", k8sNamespace, "-l", selector, "-o", "json")

			var podList struct {
				Items []struct {
					Metadata struct {
						Name string `json:"name"`
					} `json:"metadata"`
					Status struct {
						Conditions []struct {
							Type   string `json:"type"`
							Status string `json:"status"`
						} `json:"conditions"`
					} `json:"status"`
				} `json:"items"`
			}
			require.NoError(t, json.Unmarshal([]byte(output), &podList))

			for _, pod := range podList.Items {
				if previous[pod.Metadata.Name] {
					continue
				}
				if podReady(pod.Status.Conditions) {
					readyPods = append(readyPods, pod.Metadata.Name)
				}
			}
			return len(readyPods) >= desiredReplicas
		},
		func() string {
			return fmt.Sprintf("timeout waiting for %d ready replacement %s pods after deleting %s, ready replacements: %s",
				desiredReplicas, instance, strings.Join(previousPods, ","), strings.Join(readyPods, ","))
		},
	)
}

func podReady(conditions []struct {
	Type   string `json:"type"`
	Status string `json:"status"`
}) bool {
	for _, condition := range conditions {
		if condition.Type == "Ready" && condition.Status == "True" {
			return true
		}
	}
	return false
}

func waitForReplacementPodReady(t *testing.T, instance, previousPod string, timeout time.Duration) {
	t.Helper()
	waitForPodsReadyAfterDeletion(t, instance, []string{previousPod}, timeout)
}

func waitForTernHealth(t *testing.T, endpoint, deployment, environment string, timeout time.Duration) {
	t.Helper()
	url := fmt.Sprintf("%s/tern-health/%s/%s", endpoint, deployment, environment)
	waitForHTTPStatus(t, url, http.StatusOK, timeout)
}

func waitForHTTPStatus(t *testing.T, url string, expectedStatus int, timeout time.Duration) {
	t.Helper()
	var (
		lastStatus int
		lastErr    error
	)
	testutil.Poll(t, timeout, 500*time.Millisecond,
		func() bool {
			ctx, cancel := context.WithTimeout(t.Context(), 3*time.Second)
			defer cancel()
			req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
			require.NoError(t, err)
			resp, err := http.DefaultClient.Do(req)
			if err != nil {
				lastErr = err
				return false
			}
			lastStatus = resp.StatusCode
			require.NoError(t, resp.Body.Close())
			return lastStatus == expectedStatus
		},
		func() string {
			return fmt.Sprintf("timeout waiting for %s to return status %d, last status: %d, last error: %v", url, expectedStatus, lastStatus, lastErr)
		},
	)
}

func freeLocalPort(t *testing.T) int {
	t.Helper()
	var listenConfig net.ListenConfig
	listener, err := listenConfig.Listen(t.Context(), "tcp", "127.0.0.1:0")
	require.NoError(t, err)
	defer utils.CloseAndLog(listener)
	addr, ok := listener.Addr().(*net.TCPAddr)
	require.True(t, ok, "expected TCP listener address")
	return addr.Port
}

var (
	controlPlaneEndpointMu     sync.Mutex
	cachedControlPlaneEndpoint string
	cachedControlPlaneForward  *exec.Cmd
)

// TestMain owns the suite's shared control-plane port-forward. The forward
// deliberately outlives the test that started it, so no test cleanup can end
// it; without an owner here, a run against a stack that is not torn down
// afterwards (a manual `go test ./e2e/k8s/...`) would leave stray kubectl
// processes behind for every forward the suite had to replace.
func TestMain(m *testing.M) {
	code := m.Run()
	controlPlaneEndpointMu.Lock()
	stopControlPlaneForwardLocked()
	controlPlaneEndpointMu.Unlock()
	os.Exit(code)
}

// stopControlPlaneForwardLocked ends the cached forward, if this suite started
// one. Callers must hold controlPlaneEndpointMu.
func stopControlPlaneForwardLocked() {
	if cachedControlPlaneForward == nil {
		return
	}
	if cachedControlPlaneForward.Process != nil {
		// The reaping goroutine's Wait collects it; killing an already-exited
		// process is the expected case when its pod went away on its own.
		_ = cachedControlPlaneForward.Process.Kill()
	}
	cachedControlPlaneForward = nil
	cachedControlPlaneEndpoint = ""
}

// controlPlaneEndpoint returns a health-checked HTTP endpoint for the base
// stack's control plane. It starts from the suite-wide E2E_SCHEMABOT_URL
// port-forward and lazily replaces it with a fresh forward to
// svc/control-plane-schemabot when the current endpoint stops answering
// /health — a test that crashes the control-plane pod kills whichever forward
// was serving the endpoint, and every later test must self-heal rather than
// inherit the dead forward. The etre/vitess resolver suites deploy
// differently-named control-plane services, so their tests read
// testutil.Endpoint directly instead of using this helper.
func controlPlaneEndpoint(t *testing.T) string {
	t.Helper()
	controlPlaneEndpointMu.Lock()
	defer controlPlaneEndpointMu.Unlock()

	if cachedControlPlaneEndpoint == "" {
		cachedControlPlaneEndpoint = testutil.Endpoint(t)
	}
	if controlPlaneHealthy(t, cachedControlPlaneEndpoint) {
		return cachedControlPlaneEndpoint
	}

	t.Logf("control-plane endpoint %s is not answering /health; starting a fresh port-forward", cachedControlPlaneEndpoint)
	stopControlPlaneForwardLocked()
	cachedControlPlaneEndpoint, cachedControlPlaneForward = startControlPlanePortForward(t)
	return cachedControlPlaneEndpoint
}

// controlPlaneHealthy reports whether the endpoint answers /health with 200.
// A single failed probe is treated as an unhealthy endpoint: the caller
// replaces the forward, which is safe to do even on a transient failure.
func controlPlaneHealthy(t *testing.T, endpoint string) bool {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 3*time.Second)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint+"/health", nil)
	require.NoError(t, err)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Logf("control-plane health probe for %s failed: %v", endpoint, err)
		return false
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Logf("control-plane health probe for %s returned status %d", endpoint, resp.StatusCode)
		return false
	}
	return true
}

// startControlPlanePortForward forwards a free local port to the base stack's
// control-plane service, waits for it to answer /health, and returns the
// endpoint alongside the process serving it. The forward is deliberately not
// tied to the calling test's lifecycle: controlPlaneEndpoint caches it as the
// shared suite endpoint, so killing it when the creating test finishes would
// strand every later test. It is instead owned by the suite, and ended in
// TestMain. The kubectl process can also exit on its own when its pinned pod
// goes away (another control-plane crash, or the suite teardown deleting the
// namespace) — controlPlaneEndpoint then detects the dead endpoint and
// forwards again.
func startControlPlanePortForward(t *testing.T) (string, *exec.Cmd) {
	t.Helper()
	port := freeLocalPort(t)
	// WithoutCancel detaches the process from the creating test's lifetime —
	// the forward must keep serving after that test finishes.
	cmd := exec.CommandContext(context.WithoutCancel(t.Context()), "kubectl", "port-forward", "-n", k8sNamespace, "svc/control-plane-schemabot", fmt.Sprintf("%d:8080", port))
	require.NoError(t, cmd.Start())
	// Reap the kubectl process whenever it exits so a dead forward does not
	// linger as a zombie for the rest of the test binary. Its exit error is
	// expected (the pinned pod went away, or TestMain killed it) and outlives
	// the creating test, so there is no test to report it to.
	go func() { _ = cmd.Wait() }()

	endpoint := fmt.Sprintf("http://localhost:%d", port)
	waitForHTTPStatus(t, endpoint+"/health", http.StatusOK, testutil.PollDeadline)
	return endpoint, cmd
}

func startDataPlanePodGRPCPortForward(t *testing.T, pod string) string {
	t.Helper()
	port := freeLocalPort(t)
	ctx, cancel := context.WithCancel(context.WithoutCancel(t.Context()))
	cmd := exec.CommandContext(ctx, "kubectl", "port-forward", "-n", k8sNamespace, "pod/"+pod, fmt.Sprintf("%d:13370", port))
	require.NoError(t, cmd.Start())
	t.Cleanup(func() {
		cancel()
		if cmd.Process != nil {
			_ = cmd.Process.Kill()
		}
		_ = cmd.Wait()
	})

	return fmt.Sprintf("127.0.0.1:%d", port)
}

func startDataPlaneServiceGRPCPortForward(t *testing.T) string {
	t.Helper()
	port := freeLocalPort(t)
	ctx, cancel := context.WithCancel(context.WithoutCancel(t.Context()))
	cmd := exec.CommandContext(ctx, "kubectl", "port-forward", "-n", k8sNamespace, "svc/data-plane-schemabot", fmt.Sprintf("%d:13370", port))
	require.NoError(t, cmd.Start())
	t.Cleanup(func() {
		cancel()
		if cmd.Process != nil {
			_ = cmd.Process.Kill()
		}
		_ = cmd.Wait()
	})

	return fmt.Sprintf("127.0.0.1:%d", port)
}
