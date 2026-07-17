package tern

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/block/schemabot/pkg/storage"
)

// A shard-scoped dispatch keys its single operation from the one
// (namespace, table) its changes target; several changes to the same table
// share that key. A dispatch mixing tables or namespaces cannot be represented
// by one shard operation key and must fail closed.
func TestShardScopedDispatchOperationKey(t *testing.T) {
	key, err := shardScopedDispatchOperationKey([]storage.TableChange{
		{Namespace: "commerce", Table: "users", DDL: "ALTER TABLE `users` ADD COLUMN `email` varchar(255)", Operation: "alter"},
		{Namespace: "commerce", Table: "users", DDL: "ALTER TABLE `users` ADD COLUMN `phone` varchar(32)", Operation: "alter"},
	}, "-80")
	require.NoError(t, err)
	assert.Equal(t, "commerce/-80/users", key)

	_, err = shardScopedDispatchOperationKey([]storage.TableChange{
		{Namespace: "commerce", Table: "users", DDL: "ALTER TABLE `users` ADD COLUMN `email` varchar(255)", Operation: "alter"},
		{Namespace: "commerce", Table: "orders", DDL: "ALTER TABLE `orders` ADD COLUMN `note` text", Operation: "alter"},
	}, "-80")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "mixes tables")

	_, err = shardScopedDispatchOperationKey(nil, "-80")
	require.Error(t, err)

	// A component containing the key's "/" delimiter would produce a key that
	// splits into more than three parts, so readers would no longer classify
	// the operation as shard-scoped work. Stamping must refuse it.
	_, err = shardScopedDispatchOperationKey([]storage.TableChange{
		{Namespace: "commerce", Table: "users/archive", DDL: "ALTER TABLE `users/archive` ADD COLUMN `email` varchar(255)", Operation: "alter"},
	}, "-80")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "delimiter")

	_, err = shardScopedDispatchOperationKey([]storage.TableChange{
		{Namespace: "commerce", Table: "users", DDL: "ALTER TABLE `users` ADD COLUMN `email` varchar(255)", Operation: "alter"},
	}, "-80/0")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "delimiter")
}
