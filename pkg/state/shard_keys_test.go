package state

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestShardWorkKey(t *testing.T) {
	ns, shard, table, ok := ShardWorkKey("cdb_resolute_sharded/-40/mutes")
	require.True(t, ok)
	assert.Equal(t, "cdb_resolute_sharded", ns)
	assert.Equal(t, "-40", shard)
	assert.Equal(t, "mutes", table)

	for _, key := range []string{"", "cdb_resolute_sharded/group_finalizer", "deployment-only", "ns//table", "ns/-40/table/extra"} {
		_, _, _, ok := ShardWorkKey(key)
		assert.False(t, ok, "key %q must not parse as a shard work key", key)
	}
}

func TestNamespaceFinalizerKey(t *testing.T) {
	ns, ok := NamespaceFinalizerKey("cdb_resolute_sharded/group_finalizer")
	require.True(t, ok)
	assert.Equal(t, "cdb_resolute_sharded", ns)

	for _, key := range []string{"cdb_resolute_sharded/-40/mutes", "group_finalizer", "", "orders-001/ns_0/group_finalizer"} {
		_, ok := NamespaceFinalizerKey(key)
		assert.False(t, ok, "key %q must not parse as a namespace finalizer key", key)
	}
}
