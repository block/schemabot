package planetscale

import (
	"context"
	"log/slog"
	"testing"

	ps "github.com/planetscale/planetscale-go/planetscale"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/block/schemabot/pkg/ddl"
	"github.com/block/schemabot/pkg/engine"
	"github.com/block/schemabot/pkg/psclient"
)

// keyspaceListingClient fakes only ListKeyspaces; other PSClient methods are
// inherited from the embedded nil interface and must not be called.
type keyspaceListingClient struct {
	psclient.PSClient
	keyspaces []*ps.Keyspace
	err       error
}

func (c *keyspaceListingClient) ListKeyspaces(context.Context, *ps.ListKeyspacesRequest) ([]*ps.Keyspace, error) {
	return c.keyspaces, c.err
}

func TestFetchKeyspaceShardCounts(t *testing.T) {
	e := New(slog.Default())
	client := &keyspaceListingClient{keyspaces: []*ps.Keyspace{
		{Name: "commerce", Shards: 1},
		{Name: "commerce_sharded", Shards: 4},
	}}

	counts, err := e.fetchKeyspaceShardCounts(t.Context(), client, "org", "db", "main")
	require.NoError(t, err)
	assert.Equal(t, map[string]int{"commerce": 1, "commerce_sharded": 4}, counts)
}

// Without a vtgate DSN the row probe cannot run: the change still carries the
// API's shard count, and the absent row estimate renders as explicitly
// unavailable downstream.
func TestAttachTableSizesWithoutDSNKeepsAPIShardCount(t *testing.T) {
	e := New(slog.Default())
	changes := []engine.TableChange{{
		Table:     "customers",
		Operation: ddl.StatementAlterTable,
		DDL:       "ALTER TABLE `customers` ADD COLUMN `loyalty_tier` varchar(20)",
	}}

	e.attachTableSizes(t.Context(), &engine.Credentials{}, "commerce_sharded", 4, changes)

	assert.Equal(t, 4, changes[0].ShardCount)
	assert.Nil(t, changes[0].EstimatedRows)
	assert.Nil(t, changes[0].LargestShardRows)
	assert.Nil(t, changes[0].EstimatedBytes)
}
