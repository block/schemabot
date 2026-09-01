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

func TestResolveProxyPortRetriesTransientFailure(t *testing.T) {
	calls := 0
	lookup := func(ctx context.Context, port string) (network.Port, error) {
		calls++
		assert.Equal(t, "19101", port)
		if calls < 3 {
			return network.Port{}, errors.New("inspect: docker under load")
		}
		return network.ParsePort("55555")
	}

	hostPort, err := resolveProxyPort(t.Context(), 19101, lookup)

	require.NoError(t, err)
	assert.Equal(t, 55555, hostPort)
	assert.Equal(t, 3, calls)
}

func TestResolveProxyPortFailsWhenPortStaysUnmapped(t *testing.T) {
	oldTimeout := proxyPortLookupTimeout
	proxyPortLookupTimeout = 250 * time.Millisecond
	t.Cleanup(func() {
		proxyPortLookupTimeout = oldTimeout
	})

	lookupErr := errors.New("inspect: docker under load")
	lookup := func(ctx context.Context, port string) (network.Port, error) {
		return network.Port{}, lookupErr
	}

	_, err := resolveProxyPort(t.Context(), 19101, lookup)

	require.Error(t, err)
	assert.Contains(t, err.Error(), "proxy port 19101")
	assert.ErrorIs(t, err, lookupErr)
}

func TestResolveProxyPortCancelsHungLookup(t *testing.T) {
	oldTimeout := proxyPortLookupTimeout
	proxyPortLookupTimeout = 250 * time.Millisecond
	t.Cleanup(func() {
		proxyPortLookupTimeout = oldTimeout
	})

	lookup := func(ctx context.Context, port string) (network.Port, error) {
		<-ctx.Done()
		return network.Port{}, fmt.Errorf("inspect: %w", ctx.Err())
	}

	_, err := resolveProxyPort(t.Context(), 19101, lookup)

	require.Error(t, err)
	assert.Contains(t, err.Error(), "proxy port 19101")
	assert.ErrorIs(t, err, context.DeadlineExceeded)
}

func TestResolveProxyPortHonorsContextCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	lookup := func(ctx context.Context, port string) (network.Port, error) {
		cancel()
		return network.Port{}, errors.New("inspect: docker under load")
	}

	_, err := resolveProxyPort(ctx, 19101, lookup)

	require.Error(t, err)
	assert.Contains(t, err.Error(), "proxy port 19101")
	assert.ErrorIs(t, err, context.Canceled)
}
