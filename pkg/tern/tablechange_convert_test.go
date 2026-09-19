package tern

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/block/schemabot/pkg/ddl"
	"github.com/block/schemabot/pkg/engine"
)

func TestNamespacesFromEngineChangesPreservesBlockedNonShardedSteps(t *testing.T) {
	client := &LocalClient{}
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
