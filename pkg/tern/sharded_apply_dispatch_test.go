package tern

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	ternv1 "github.com/block/schemabot/pkg/proto/ternv1"
	"github.com/block/schemabot/pkg/storage"
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

// A VSchema-only dispatch — every change VSCHEMA-typed — is the control
// plane's task-less finalizer shape. Any table DDL in the set makes it a work
// dispatch instead, so the detection returns nil rather than misrouting work
// into the finalizer path.
func TestVSchemaOnlyDispatchNamespaces(t *testing.T) {
	vschemaChange := func(ns string) *ternv1.TableChange {
		return &ternv1.TableChange{
			Namespace:  ns,
			TableName:  "VSchema: " + ns,
			ChangeType: ternv1.ChangeType_CHANGE_TYPE_VSCHEMA,
		}
	}
	t.Run("single namespace", func(t *testing.T) {
		got := vschemaOnlyDispatchNamespaces([]*ternv1.TableChange{vschemaChange("cdb_resolute_sharded")})
		assert.Equal(t, []string{"cdb_resolute_sharded"}, got)
	})
	t.Run("multiple namespaces deduplicated", func(t *testing.T) {
		got := vschemaOnlyDispatchNamespaces([]*ternv1.TableChange{
			vschemaChange("ks_a"), vschemaChange("ks_b"), vschemaChange("ks_a"),
		})
		assert.Equal(t, []string{"ks_a", "ks_b"}, got)
	})
	t.Run("table DDL makes it a work dispatch", func(t *testing.T) {
		got := vschemaOnlyDispatchNamespaces([]*ternv1.TableChange{
			vschemaChange("ks_a"),
			{Namespace: "ks_a", TableName: "mutes", Ddl: "ALTER TABLE `mutes` ADD COLUMN `c` int", ChangeType: ternv1.ChangeType_CHANGE_TYPE_ALTER},
		})
		assert.Nil(t, got)
	})
	t.Run("no changes", func(t *testing.T) {
		assert.Nil(t, vschemaOnlyDispatchNamespaces(nil))
	})
	t.Run("empty namespace fails the shape", func(t *testing.T) {
		assert.Nil(t, vschemaOnlyDispatchNamespaces([]*ternv1.TableChange{vschemaChange("  ")}))
	})
}

// A group_finalizer dispatch resolves to a namespace scope (one namespace) or
// a deployment scope (every VSchema-changed namespace of a VSchema-only plan,
// applied as one engine apply). The stored plan must carry each dispatched
// namespace's artifact, and a multi-namespace dispatch must cover the plan's
// full VSchema set — otherwise the operation would be created only to apply
// something other than what was dispatched, or to fail at drive time with
// nothing to apply.
func TestFinalizerDispatchScope(t *testing.T) {
	plan := &storage.Plan{
		ID:             7,
		PlanIdentifier: "plan-scope-test",
		Namespaces: map[string]*storage.NamespacePlanData{
			"ks_a": {Artifacts: map[string]string{storage.VSchemaArtifactName: `{"sharded": true}`}},
			"ks_b": {},
		},
	}
	t.Run("single namespace is namespace-scoped", func(t *testing.T) {
		ns, err := finalizerDispatchScope(plan, []string{"ks_a"}, nil)
		require.NoError(t, err)
		assert.Equal(t, "ks_a", ns)
	})
	t.Run("single namespace with a namespace-keyed manifest stays namespace-scoped", func(t *testing.T) {
		ns, err := finalizerDispatchScope(plan, []string{"ks_a"}, []string{"ks_a/-80/users", "ks_a/group_finalizer"})
		require.NoError(t, err)
		assert.Equal(t, "ks_a", ns)
	})
	t.Run("single namespace with a deployment-keyed manifest is deployment-scoped", func(t *testing.T) {
		soloPlan := &storage.Plan{
			ID:             10,
			PlanIdentifier: "plan-scope-solo",
			Namespaces: map[string]*storage.NamespacePlanData{
				"ks_a": {Artifacts: map[string]string{storage.VSchemaArtifactName: `{"sharded": true}`}},
			},
		}
		ns, err := finalizerDispatchScope(soloPlan, []string{"ks_a"}, []string{"group_finalizer"})
		require.NoError(t, err)
		assert.Empty(t, ns)
	})
	t.Run("deployment-keyed manifest with partial namespace coverage fails closed", func(t *testing.T) {
		twoPlan := &storage.Plan{
			ID:             11,
			PlanIdentifier: "plan-scope-two",
			Namespaces: map[string]*storage.NamespacePlanData{
				"ks_a": {Artifacts: map[string]string{storage.VSchemaArtifactName: `{"sharded": true}`}},
				"ks_b": {Artifacts: map[string]string{storage.VSchemaArtifactName: `{"tables": {}}`}},
			},
		}
		_, err := finalizerDispatchScope(twoPlan, []string{"ks_a"}, []string{"group_finalizer"})
		require.Error(t, err)
		assert.Contains(t, err.Error(), "must cover the plan's full VSchema set")
	})
	t.Run("full VSchema set is deployment-scoped", func(t *testing.T) {
		multiPlan := &storage.Plan{
			ID:             8,
			PlanIdentifier: "plan-scope-multi",
			Namespaces: map[string]*storage.NamespacePlanData{
				"ks_a": {Artifacts: map[string]string{storage.VSchemaArtifactName: `{"sharded": true}`}},
				"ks_b": {Artifacts: map[string]string{storage.VSchemaArtifactName: `{"tables": {}}`}},
			},
		}
		ns, err := finalizerDispatchScope(multiPlan, []string{"ks_b", "ks_a"}, nil)
		require.NoError(t, err)
		assert.Empty(t, ns)
	})
	t.Run("partial multi-namespace dispatch fails closed", func(t *testing.T) {
		multiPlan := &storage.Plan{
			ID:             9,
			PlanIdentifier: "plan-scope-partial",
			Namespaces: map[string]*storage.NamespacePlanData{
				"ks_a": {Artifacts: map[string]string{storage.VSchemaArtifactName: `{"sharded": true}`}},
				"ks_b": {Artifacts: map[string]string{storage.VSchemaArtifactName: `{"tables": {}}`}},
				"ks_c": {Artifacts: map[string]string{storage.VSchemaArtifactName: `{"tables": {}}`}},
			},
		}
		_, err := finalizerDispatchScope(multiPlan, []string{"ks_a", "ks_b"}, nil)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "must cover the plan's full VSchema set")
	})
	t.Run("no namespaces fails closed", func(t *testing.T) {
		_, err := finalizerDispatchScope(plan, nil, nil)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "names no namespaces")
	})
	t.Run("namespace without a VSchema artifact fails closed", func(t *testing.T) {
		_, err := finalizerDispatchScope(plan, []string{"ks_b"}, nil)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "no VSchema artifact")
	})
}
