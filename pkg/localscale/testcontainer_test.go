package localscale

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

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
