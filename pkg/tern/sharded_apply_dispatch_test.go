package tern

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	ternv1 "github.com/block/schemabot/pkg/proto/ternv1"
)

// A shard-scoped dispatch carries the control plane's authoritative,
// already-scoped DDL changes for one apply_operation; the data plane executes
// exactly those. This is what lets a per-(shard, table) operation drive only its
// own table change.
func TestScopedDispatchDDLChangesHonorsDispatchedScope(t *testing.T) {
	got, err := scopedDispatchDDLChanges([]*ternv1.TableChange{{
		Namespace:  "cdb_resolute_sharded",
		TableName:  "mutes",
		Ddl:        "ALTER TABLE `mutes` ADD INDEX (`created_at`)",
		ChangeType: ternv1.ChangeType_CHANGE_TYPE_ALTER,
	}})
	require.NoError(t, err)
	require.Len(t, got, 1)
	assert.Equal(t, "mutes", got[0].Table)
	assert.Equal(t, "cdb_resolute_sharded", got[0].Namespace)
	assert.Equal(t, "alter", got[0].Operation, "proto change type round-trips to the plan's DDL action")
	assert.Contains(t, got[0].DDL, "ADD INDEX")
}

// A shard-scoped dispatch is already scoped by the control plane, so it must
// carry valid, non-empty changes. Anything malformed fails closed rather than
// falling back to the whole plan (which would apply unrelated tables on the
// targeted shard).
func TestDispatchTargetShard(t *testing.T) {
	t.Run("single shard is trimmed", func(t *testing.T) {
		shard, err := dispatchTargetShard([]string{"  -80  "})
		require.NoError(t, err)
		assert.Equal(t, "-80", shard)
	})
	t.Run("zero shards", func(t *testing.T) {
		_, err := dispatchTargetShard(nil)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "exactly one shard")
	})
	t.Run("more than one shard", func(t *testing.T) {
		_, err := dispatchTargetShard([]string{"-80", "80-"})
		require.Error(t, err)
		assert.Contains(t, err.Error(), "exactly one shard")
	})
	t.Run("empty after trim fails closed", func(t *testing.T) {
		_, err := dispatchTargetShard([]string{"   "})
		require.Error(t, err)
		assert.Contains(t, err.Error(), "empty target shard")
	})
}

func TestScopedDispatchDDLChangesFailsClosed(t *testing.T) {
	t.Run("no changes", func(t *testing.T) {
		_, err := scopedDispatchDDLChanges(nil)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "no ddl_changes")
	})
	t.Run("nil entry", func(t *testing.T) {
		_, err := scopedDispatchDDLChanges([]*ternv1.TableChange{nil})
		require.Error(t, err)
		assert.Contains(t, err.Error(), "nil")
	})
	t.Run("empty namespace", func(t *testing.T) {
		// The namespace is authoritative scope for a shard-scoped dispatch.
		_, err := scopedDispatchDDLChanges([]*ternv1.TableChange{{TableName: "mutes", Ddl: "x", ChangeType: ternv1.ChangeType_CHANGE_TYPE_ALTER}})
		require.Error(t, err)
		assert.Contains(t, err.Error(), "empty namespace")
	})
	t.Run("empty table or DDL", func(t *testing.T) {
		_, err := scopedDispatchDDLChanges([]*ternv1.TableChange{{Namespace: "ks", TableName: "mutes", ChangeType: ternv1.ChangeType_CHANGE_TYPE_ALTER}})
		require.Error(t, err)
		assert.Contains(t, err.Error(), "empty table or DDL")
	})
	t.Run("unsupported change type", func(t *testing.T) {
		_, err := scopedDispatchDDLChanges([]*ternv1.TableChange{{Namespace: "ks", TableName: "mutes", Ddl: "ALTER TABLE `mutes` ADD INDEX (`x`)", ChangeType: ternv1.ChangeType_CHANGE_TYPE_OTHER}})
		require.Error(t, err)
		assert.Contains(t, err.Error(), "unsupported change type")
	})
	t.Run("vschema is not shard-scoped", func(t *testing.T) {
		// A VSchema update is keyspace-wide (applied by the task-less
		// group_finalizer), never shard-scoped — reject it here.
		_, err := scopedDispatchDDLChanges([]*ternv1.TableChange{{Namespace: "ks", TableName: "mutes", Ddl: "x", ChangeType: ternv1.ChangeType_CHANGE_TYPE_VSCHEMA}})
		require.Error(t, err)
		assert.Contains(t, err.Error(), "unsupported change type")
	})
	t.Run("values are trimmed before storing", func(t *testing.T) {
		got, err := scopedDispatchDDLChanges([]*ternv1.TableChange{{
			Namespace:    "  ks  ",
			TableName:    "  mutes  ",
			Ddl:          "  ALTER TABLE `mutes` ADD INDEX (`x`)  ",
			ChangeType:   ternv1.ChangeType_CHANGE_TYPE_ALTER,
			IsUnsafe:     true,
			UnsafeReason: "DROP COLUMN removes data",
		}})
		require.NoError(t, err)
		require.Len(t, got, 1)
		assert.Equal(t, "ks", got[0].Namespace)
		assert.Equal(t, "mutes", got[0].Table)
		assert.Equal(t, "ALTER TABLE `mutes` ADD INDEX (`x`)", got[0].DDL, "surrounding whitespace must not leak into operation keys/tasks")
		assert.True(t, got[0].IsUnsafe)
		assert.Equal(t, "DROP COLUMN removes data", got[0].UnsafeReason)
	})
}
