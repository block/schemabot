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

// A table present in the branch metrics carries its storage bytes alongside
// the API's shard count. The metrics endpoint reports no row counts, so the
// row estimate fields stay absent and render as unavailable downstream.
func TestAttachTableSizesCarriesBranchMetricsBytes(t *testing.T) {
	changes := []engine.TableChange{{
		Table:     "customers",
		Operation: ddl.StatementAlterTable,
		DDL:       "ALTER TABLE `customers` ADD COLUMN `loyalty_tier` varchar(20)",
	}}

	attachTableSizes(4, map[string]int64{"customers": 14_319_353_856}, changes)

	assert.Equal(t, 4, changes[0].ShardCount)
	require.NotNil(t, changes[0].EstimatedBytes)
	assert.Equal(t, int64(14_319_353_856), *changes[0].EstimatedBytes)
	assert.Nil(t, changes[0].EstimatedRows)
	assert.Nil(t, changes[0].LargestShardRows)
}

// Without branch metrics (the API call failed or timed out) the change still
// carries the API's shard count, and the absent byte estimate renders as
// explicitly unavailable downstream.
func TestAttachTableSizesWithoutMetricsKeepsAPIShardCount(t *testing.T) {
	changes := []engine.TableChange{{
		Table:     "customers",
		Operation: ddl.StatementAlterTable,
		DDL:       "ALTER TABLE `customers` ADD COLUMN `loyalty_tier` varchar(20)",
	}}

	attachTableSizes(4, nil, changes)

	assert.Equal(t, 4, changes[0].ShardCount)
	assert.Nil(t, changes[0].EstimatedRows)
	assert.Nil(t, changes[0].LargestShardRows)
	assert.Nil(t, changes[0].EstimatedBytes)
}

// A table the plan creates does not exist in the branch metrics yet: it keeps
// the shard count but carries no byte estimate, while sibling changes on
// existing tables keep theirs.
func TestAttachTableSizesSkipsTablesAbsentFromMetrics(t *testing.T) {
	changes := []engine.TableChange{
		{Table: "customers", Operation: ddl.StatementAlterTable},
		{Table: "loyalty_events", Operation: ddl.StatementCreateTable},
	}

	attachTableSizes(2, map[string]int64{"customers": 1_048_576}, changes)

	require.NotNil(t, changes[0].EstimatedBytes)
	assert.Equal(t, int64(1_048_576), *changes[0].EstimatedBytes)
	assert.Equal(t, 2, changes[0].ShardCount)
	assert.Nil(t, changes[1].EstimatedBytes)
	assert.Equal(t, 2, changes[1].ShardCount)
}
