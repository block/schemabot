package localscale

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/moby/moby/api/types/network"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const (
	adminRequestTestTimeout = 75 * time.Millisecond
	adminRequestHandlerWait = 500 * time.Millisecond
)

func TestLocalScaleContainerAdminRequestDefaultDeadline(t *testing.T) {
	oldTimeout := localScaleAdminRequestTimeout
	localScaleAdminRequestTimeout = adminRequestTestTimeout
	t.Cleanup(func() {
		localScaleAdminRequestTimeout = oldTimeout
	})

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		time.Sleep(adminRequestHandlerWait)
		w.WriteHeader(http.StatusGatewayTimeout)
	}))
	t.Cleanup(server.Close)

	container := NewTestHelper(server.URL)
	_, err := container.VtgateExec(t.Context(), "test-org", "test-db", "test_keyspace", "SELECT 1")

	require.Error(t, err)
	assert.Contains(t, err.Error(), "vtgate exec test-org/test-db/test_keyspace")
	assert.Contains(t, err.Error(), `query "SELECT 1"`)
	assert.Contains(t, err.Error(), "POST /admin/vtgate-exec")
	assert.ErrorIs(t, err, context.DeadlineExceeded)
}

func TestLocalScaleContainerAdminRequestKeepsCallerDeadline(t *testing.T) {
	oldTimeout := localScaleAdminRequestTimeout
	localScaleAdminRequestTimeout = 10 * time.Second
	t.Cleanup(func() {
		localScaleAdminRequestTimeout = oldTimeout
	})

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		time.Sleep(100 * time.Millisecond)
		w.WriteHeader(http.StatusGatewayTimeout)
	}))
	t.Cleanup(server.Close)

	ctx, cancel := context.WithTimeout(t.Context(), adminRequestTestTimeout)
	defer cancel()

	container := NewTestHelper(server.URL)
	err := container.ResetState(ctx)

	require.Error(t, err)
	assert.Contains(t, err.Error(), "reset LocalScale state")
	assert.Contains(t, err.Error(), "POST /admin/reset-state")
	assert.ErrorIs(t, err, context.DeadlineExceeded)
}

// shortProxyPortMapTimeout shrinks the lookup budget so the failure paths
// resolve in test time instead of the startup budget.
func shortProxyPortMapTimeout(t *testing.T, d time.Duration) {
	t.Helper()
	old := proxyPortMapTimeout
	proxyPortMapTimeout = d
	t.Cleanup(func() {
		proxyPortMapTimeout = old
	})
}

func TestResolveProxyPortMapRetriesTransientFailure(t *testing.T) {
	calls := 0
	lookup := func(ctx context.Context, port string) (network.Port, error) {
		calls++
		assert.Equal(t, "19101", port)
		if calls < 3 {
			return network.Port{}, errors.New("inspect: docker under load")
		}
		return network.ParsePort("55555")
	}

	portMap, err := resolveProxyPortMap(t.Context(), 19101, 19101, lookup)

	require.NoError(t, err)
	assert.Equal(t, map[int]int{19101: 55555}, portMap)
	assert.Equal(t, 3, calls)
}

func TestResolveProxyPortMapFailsWhenPortStaysUnmapped(t *testing.T) {
	shortProxyPortMapTimeout(t, 250*time.Millisecond)

	lookupErr := errors.New("inspect: docker under load")
	lookup := func(ctx context.Context, port string) (network.Port, error) {
		return network.Port{}, lookupErr
	}

	_, err := resolveProxyPortMap(t.Context(), 19101, 19101, lookup)

	require.Error(t, err)
	assert.Contains(t, err.Error(), "proxy port 19101")
	assert.ErrorIs(t, err, lookupErr)
}

// A wedged Docker fails every port alike, so the whole range shares one lookup
// budget: startup reports the failure on the scale of a single port's retries
// rather than multiplying it by the size of the range.
func TestResolveProxyPortMapSharesOneBudgetAcrossPorts(t *testing.T) {
	const budget = 250 * time.Millisecond
	shortProxyPortMapTimeout(t, budget)

	var ports []string
	lookup := func(ctx context.Context, port string) (network.Port, error) {
		ports = append(ports, port)
		return network.Port{}, errors.New("inspect: docker daemon not responding")
	}

	began := time.Now()
	_, err := resolveProxyPortMap(t.Context(), 19100, 19150, lookup)
	took := time.Since(began)

	require.Error(t, err)
	assert.Less(t, took, 2*budget, "51 ports must not each get their own budget")
	assert.Contains(t, err.Error(), "proxy port 19100", "the first port to exhaust the budget is the one reported")
	assert.NotContains(t, ports, "19101", "the range is abandoned once the budget is gone")
}

func TestResolveProxyPortMapCancelsHungLookup(t *testing.T) {
	shortProxyPortMapTimeout(t, 250*time.Millisecond)

	lookup := func(ctx context.Context, port string) (network.Port, error) {
		<-ctx.Done()
		return network.Port{}, fmt.Errorf("inspect: %w", ctx.Err())
	}

	_, err := resolveProxyPortMap(t.Context(), 19101, 19101, lookup)

	require.Error(t, err)
	assert.Contains(t, err.Error(), "proxy port 19101")
	assert.ErrorIs(t, err, context.DeadlineExceeded)
}

func TestResolveProxyPortMapHonorsContextCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	lookup := func(ctx context.Context, port string) (network.Port, error) {
		cancel()
		return network.Port{}, errors.New("inspect: docker under load")
	}

	_, err := resolveProxyPortMap(ctx, 19101, 19101, lookup)

	require.Error(t, err)
	assert.Contains(t, err.Error(), "proxy port 19101")
	assert.ErrorIs(t, err, context.Canceled)
}
