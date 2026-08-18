package vschema

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const shardedVSchema = `{
	"sharded": true,
	"vindexes": {
		"hash": {"type": "hash"},
		"email_lookup": {
			"type": "consistent_lookup_unique",
			"params": {"table": "email_lookup", "from": "email", "to": "keyspace_id"},
			"owner": "users"
		}
	},
	"tables": {
		"users": {
			"column_vindexes": [
				{"column": "id", "name": "hash"},
				{"column": "email", "name": "email_lookup"}
			]
		},
		"orders": {
			"column_vindexes": [
				{"column": "user_id", "name": "hash"}
			]
		}
	}
}`

func TestDeletions_NoChange(t *testing.T) {
	deletions, err := Deletions(shardedVSchema, shardedVSchema)
	require.NoError(t, err)
	assert.Empty(t, deletions)
}

func TestDeletions_EmptyCurrent(t *testing.T) {
	// A new keyspace has no current VSchema, so nothing can be removed.
	for _, current := range []string{"", "   ", "{}"} {
		deletions, err := Deletions(current, shardedVSchema)
		require.NoError(t, err)
		assert.Empty(t, deletions)
	}
}

func TestDeletions_AdditionsOnly(t *testing.T) {
	desired := `{
		"sharded": true,
		"vindexes": {
			"hash": {"type": "hash"},
			"email_lookup": {
				"type": "consistent_lookup_unique",
				"params": {"table": "email_lookup", "from": "email", "to": "keyspace_id"},
				"owner": "users"
			},
			"region_idx": {"type": "hash"}
		},
		"tables": {
			"users": {
				"column_vindexes": [
					{"column": "id", "name": "hash"},
					{"column": "email", "name": "email_lookup"}
				]
			},
			"orders": {
				"column_vindexes": [
					{"column": "user_id", "name": "hash"},
					{"column": "region", "name": "region_idx"}
				]
			},
			"payments": {
				"column_vindexes": [
					{"column": "id", "name": "hash"}
				]
			}
		}
	}`
	deletions, err := Deletions(shardedVSchema, desired)
	require.NoError(t, err)
	assert.Empty(t, deletions)
}

func TestDeletions_RemovedVindex(t *testing.T) {
	// email_lookup vindex removed entirely, along with the users column that
	// referenced it.
	desired := `{
		"sharded": true,
		"vindexes": {
			"hash": {"type": "hash"}
		},
		"tables": {
			"users": {
				"column_vindexes": [
					{"column": "id", "name": "hash"}
				]
			},
			"orders": {
				"column_vindexes": [
					{"column": "user_id", "name": "hash"}
				]
			}
		}
	}`
	deletions, err := Deletions(shardedVSchema, desired)
	require.NoError(t, err)
	require.Len(t, deletions, 2)

	assert.Equal(t, DeletionKindVindex, deletions[0].Kind)
	assert.Equal(t, "email_lookup", deletions[0].Name)
	assert.Contains(t, deletions[0].Reason, `lookup vindex "email_lookup"`)
	assert.Contains(t, deletions[0].Reason, `backing table "email_lookup"`)
	assert.Contains(t, deletions[0].Reason, "goes stale")

	assert.Equal(t, DeletionKindColumnVindex, deletions[1].Kind)
	assert.Equal(t, "users.email_lookup", deletions[1].Name)
}

func TestDeletions_RemovedTable(t *testing.T) {
	desired := `{
		"sharded": true,
		"vindexes": {
			"hash": {"type": "hash"},
			"email_lookup": {
				"type": "consistent_lookup_unique",
				"params": {"table": "email_lookup", "from": "email", "to": "keyspace_id"},
				"owner": "users"
			}
		},
		"tables": {
			"users": {
				"column_vindexes": [
					{"column": "id", "name": "hash"},
					{"column": "email", "name": "email_lookup"}
				]
			}
		}
	}`
	deletions, err := Deletions(shardedVSchema, desired)
	require.NoError(t, err)
	require.Len(t, deletions, 1)
	assert.Equal(t, DeletionKindTable, deletions[0].Kind)
	assert.Equal(t, "orders", deletions[0].Name)
	assert.Contains(t, deletions[0].Reason, "orders")
	assert.Contains(t, deletions[0].Reason, "routing entry")
}

func TestDeletions_RemovedColumnVindex(t *testing.T) {
	// The email_lookup vindex definition survives, but the users table no
	// longer routes through it — the association removal alone is unsafe.
	desired := `{
		"sharded": true,
		"vindexes": {
			"hash": {"type": "hash"},
			"email_lookup": {
				"type": "consistent_lookup_unique",
				"params": {"table": "email_lookup", "from": "email", "to": "keyspace_id"},
				"owner": "users"
			}
		},
		"tables": {
			"users": {
				"column_vindexes": [
					{"column": "id", "name": "hash"}
				]
			},
			"orders": {
				"column_vindexes": [
					{"column": "user_id", "name": "hash"}
				]
			}
		}
	}`
	deletions, err := Deletions(shardedVSchema, desired)
	require.NoError(t, err)
	require.Len(t, deletions, 1)
	assert.Equal(t, DeletionKindColumnVindex, deletions[0].Kind)
	assert.Equal(t, "users.email_lookup", deletions[0].Name)
	assert.Contains(t, deletions[0].Reason, `"users"`)
	assert.Contains(t, deletions[0].Reason, `"email_lookup"`)
}

func TestDeletions_UnknownFieldsTolerated(t *testing.T) {
	// A VSchema served by a newer Vitess can carry fields the vendored proto
	// does not know about; they must not fail deletion detection.
	current := `{"sharded": true, "future_field": {"x": 1}, "vindexes": {"hash": {"type": "hash"}}, "tables": {"users": {"column_vindexes": [{"column": "id", "name": "hash"}]}}}`
	desired := `{"sharded": true, "vindexes": {"hash": {"type": "hash"}}, "tables": {}}`
	deletions, err := Deletions(current, desired)
	require.NoError(t, err)
	require.Len(t, deletions, 1)
	assert.Equal(t, DeletionKindTable, deletions[0].Kind)
	assert.Equal(t, "users", deletions[0].Name)
}

func TestDeletions_UnparseableCurrentFailsClosed(t *testing.T) {
	_, err := Deletions("{not json", shardedVSchema)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "parse current VSchema")
}

func TestDeletions_UnparseableDesiredFailsClosed(t *testing.T) {
	_, err := Deletions(shardedVSchema, "{not json")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "parse desired VSchema")
}

func TestDeletions_UnshardedTableRemoval(t *testing.T) {
	// Unsharded keyspaces list tables without vindexes; removing one still
	// drops its routing entry.
	current := `{"tables": {"widgets": {}, "gadgets": {}}}`
	desired := `{"tables": {"widgets": {}}}`
	deletions, err := Deletions(current, desired)
	require.NoError(t, err)
	require.Len(t, deletions, 1)
	assert.Equal(t, DeletionKindTable, deletions[0].Kind)
	assert.Equal(t, "gadgets", deletions[0].Name)
}

func TestDeletions_FunctionalVindexRemovalReason(t *testing.T) {
	current := `{"sharded": true, "vindexes": {"hash": {"type": "hash"}, "region": {"type": "region_json"}}, "tables": {"users": {"column_vindexes": [{"column": "id", "name": "hash"}]}}}`
	desired := `{"sharded": true, "vindexes": {"hash": {"type": "hash"}}, "tables": {"users": {"column_vindexes": [{"column": "id", "name": "hash"}]}}}`
	deletions, err := Deletions(current, desired)
	require.NoError(t, err)
	require.Len(t, deletions, 1)
	assert.Contains(t, deletions[0].Reason, "stops using it for routing")
	assert.NotContains(t, deletions[0].Reason, "backing table")
}
