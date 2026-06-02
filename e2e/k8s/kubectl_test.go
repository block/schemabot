//go:build e2e

package k8s

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"os/exec"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/block/schemabot/e2e/testutil"
	"github.com/block/spirit/pkg/utils"
	"github.com/stretchr/testify/require"
)

const k8sNamespace = "schemabot-e2e"

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

func startControlPlanePortForward(t *testing.T) string {
	t.Helper()
	port := freeLocalPort(t)
	ctx, cancel := context.WithCancel(context.WithoutCancel(t.Context()))
	cmd := exec.CommandContext(ctx, "kubectl", "port-forward", "-n", k8sNamespace, "svc/control-plane-schemabot", fmt.Sprintf("%d:8080", port))
	require.NoError(t, cmd.Start())
	t.Cleanup(func() {
		cancel()
		if cmd.Process != nil {
			_ = cmd.Process.Kill()
		}
		_ = cmd.Wait()
	})

	endpoint := fmt.Sprintf("http://localhost:%d", port)
	waitForHTTPStatus(t, endpoint+"/health", http.StatusOK, testutil.PollDeadline)
	return endpoint
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
