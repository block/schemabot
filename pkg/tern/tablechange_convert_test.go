package tern

import (
	"log/slog"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/block/schemabot/pkg/ddl"
	"github.com/block/schemabot/pkg/engine"
	"github.com/block/schemabot/pkg/storage"
)

func TestNamespacesFromEngineChangesPreservesBlockedNonShardedSteps(t *testing.T) {
	client := &LocalClient{logger: slog.New(slog.DiscardHandler)}
	reason := "requires privileges unavailable to the engine"
	changes := []engine.SchemaChange{{
		Namespace: "public",
		TableChanges: []engine.TableChange{
			{Table: "users", DDL: "ALTER TABLE users ADD COLUMN name text", Operation: ddl.StatementAlterTable},
			{Table: "users", DDL: "CREATE INDEX users_name_idx ON users (name)", Operation: ddl.StatementCreateIndex, ExecutionMode: engine.ExecutionModeBlocked, ModeReason: reason},
		},
	}}

	namespaces, shards := client.namespacesFromEngineChanges(changes, nil)
	require.Empty(t, shards)
	require.Contains(t, namespaces, "public")
	require.Len(t, namespaces["public"].Tables, 2)
	assert.Equal(t, engine.ExecutionModeBlocked, namespaces["public"].Tables[1].ExecutionMode)
	assert.Equal(t, reason, namespaces["public"].Tables[1].ModeReason)
}

// Every conversion between the engine, proto, and storage table changes copies
// the table-size estimates. Each field carries a distinct value, so a dropped
// or swapped field fails the test.
func TestTableChangeConversionsPreserveSizeEstimates(t *testing.T) {
	const (
		rows    int64 = 48_200_000
		shards        = 4
		largest int64 = 13_100_000
		bytes   int64 = 23_400_000_000
	)
	engineChange := engine.TableChange{
		Table:            "orders",
		DDL:              "ALTER TABLE `orders` ADD INDEX `created_at`(`created_at`)",
		Operation:        ddl.StatementAlterTable,
		EstimatedRows:    new(rows),
		ShardCount:       shards,
		LargestShardRows: new(largest),
		EstimatedBytes:   new(bytes),
	}

	protoChange := protoTableChangeFromEngine(engineChange, "commerce")
	require.NotNil(t, protoChange.EstimatedRows)
	assert.Equal(t, rows, *protoChange.EstimatedRows)
	assert.Equal(t, int32(shards), protoChange.ShardCount)
	require.NotNil(t, protoChange.LargestShardRows)
	assert.Equal(t, largest, *protoChange.LargestShardRows)
	require.NotNil(t, protoChange.EstimatedBytes)
	assert.Equal(t, bytes, *protoChange.EstimatedBytes)

	for name, stored := range map[string]storage.TableChange{
		"from engine": storageTableChangeFromEngine(engineChange, "commerce"),
		"from proto":  StorageTableChangeFromProto(protoChange, "commerce", protoChange.TableName, protoChange.Ddl, "alter"),
	} {
		require.NotNil(t, stored.EstimatedRows, name)
		assert.Equal(t, rows, *stored.EstimatedRows, name)
		assert.Equal(t, shards, stored.ShardCount, name)
		require.NotNil(t, stored.LargestShardRows, name)
		assert.Equal(t, largest, *stored.LargestShardRows, name)
		require.NotNil(t, stored.EstimatedBytes, name)
		assert.Equal(t, bytes, *stored.EstimatedBytes, name)
	}
}

// Every conversion between the engine, proto, and storage table changes copies
// the collation changes field by field, each field with a distinct value.
func TestTableChangeConversionsPreserveCollationChanges(t *testing.T) {
	engineChange := engine.TableChange{
		Table:     "products",
		DDL:       "ALTER TABLE `products` MODIFY COLUMN `sku` varchar(64) COLLATE utf8mb4_bin NOT NULL",
		Operation: ddl.StatementAlterTable,
		CollationChanges: []engine.CollationChange{{
			Column:         "sku",
			From:           "utf8mb4_general_ci",
			To:             "utf8mb4_bin",
			Case:           engine.ComparisonBecomesSensitive,
			TrailingSpaces: engine.ComparisonUnknown,
			UniqueIndexes:  []string{"uk_sku"},
		}},
	}
	want := []storage.CollationChange{{
		Column:         "sku",
		From:           "utf8mb4_general_ci",
		To:             "utf8mb4_bin",
		Case:           "becomes_sensitive",
		TrailingSpaces: "unknown",
		UniqueIndexes:  []string{"uk_sku"},
	}}

	protoChange := protoTableChangeFromEngine(engineChange, "commerce")
	for name, stored := range map[string]storage.TableChange{
		"from engine": storageTableChangeFromEngine(engineChange, "commerce"),
		"from proto":  StorageTableChangeFromProto(protoChange, "commerce", protoChange.TableName, protoChange.Ddl, "alter"),
	} {
		assert.Equal(t, want, stored.CollationChanges, name)
	}
}
