package storagetest

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/block/schemabot/pkg/storage"
)

// TestOperatorServers runs the behavioral parity suite for
// storage.OperatorServerStore.
func TestOperatorServers(t *testing.T, h Harness) {
	// HeartbeatRegistersAndUpdates verifies a server's first heartbeat creates
	// its row and later ones replace its driver count, and that Deregister
	// removes it, including a second time.
	t.Run("HeartbeatRegistersAndUpdates", func(t *testing.T) {
		ctx := t.Context()
		store := h.NewStorage(t)
		servers := store.OperatorServers()

		require.NoError(t, servers.Heartbeat(ctx, &storage.OperatorServer{ServerID: "host-b/1/operator", Drivers: 4}))
		require.NoError(t, servers.Heartbeat(ctx, &storage.OperatorServer{ServerID: "host-a/1/operator", Drivers: 1}))
		require.NoError(t, servers.Heartbeat(ctx, &storage.OperatorServer{ServerID: "host-b/1/operator", Drivers: 8}))

		live, err := servers.ListLive(ctx)
		require.NoError(t, err)
		require.Len(t, live, 2)
		assert.Equal(t, "host-a/1/operator", live[0].ServerID)
		assert.Equal(t, 1, live[0].Drivers)
		assert.Equal(t, "host-b/1/operator", live[1].ServerID)
		assert.Equal(t, 8, live[1].Drivers)
		assert.False(t, live[1].UpdatedAt.IsZero())

		require.NoError(t, servers.Deregister(ctx, "host-b/1/operator"))
		require.NoError(t, servers.Deregister(ctx, "host-b/1/operator"), "deregistering a missing row is not an error")
		live, err = servers.ListLive(ctx)
		require.NoError(t, err)
		require.Len(t, live, 1)
		assert.Equal(t, "host-a/1/operator", live[0].ServerID)
	})

	t.Run("HeartbeatRequiresServerID", func(t *testing.T) {
		store := h.NewStorage(t)
		require.Error(t, store.OperatorServers().Heartbeat(t.Context(), &storage.OperatorServer{Drivers: 4}))
	})
}
