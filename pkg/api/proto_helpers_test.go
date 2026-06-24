package api

import (
	"encoding/json"
	"testing"

	ternv1 "github.com/block/schemabot/pkg/proto/ternv1"
	"github.com/block/schemabot/pkg/storage"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestProtoSchemaFilesToAPIIgnoresNilNamespaces(t *testing.T) {
	result := protoSchemaFilesToAPI(map[string]*ternv1.SchemaFiles{
		"orders": nil,
		"users":  {Files: map[string]string{"users.sql": "CREATE TABLE `users` (`id` bigint);\n"}},
	})

	require.Contains(t, result, "orders")
	assert.Empty(t, result["orders"].Files)
	assert.Equal(t, "CREATE TABLE `users` (`id` bigint);\n", result["users"].Files["users.sql"])
}

func TestChangeTypeRoundTrip(t *testing.T) {
	// Proto → storage → proto should round-trip correctly
	for _, ct := range []ternv1.ChangeType{
		ternv1.ChangeType_CHANGE_TYPE_CREATE,
		ternv1.ChangeType_CHANGE_TYPE_ALTER,
		ternv1.ChangeType_CHANGE_TYPE_DROP,
		ternv1.ChangeType_CHANGE_TYPE_VSCHEMA,
		ternv1.ChangeType_CHANGE_TYPE_OTHER,
	} {
		op := protoChangeTypeToOperation(ct)
		result := changeTypeToProto(op)
		assert.Equal(t, ct, result, "round-trip failed for %v (op=%q)", ct, op)
	}
}

func TestChangeTypeToProto_CaseInsensitive(t *testing.T) {
	assert.Equal(t, ternv1.ChangeType_CHANGE_TYPE_ALTER, changeTypeToProto("alter"))
	assert.Equal(t, ternv1.ChangeType_CHANGE_TYPE_ALTER, changeTypeToProto("ALTER"))
	assert.Equal(t, ternv1.ChangeType_CHANGE_TYPE_CREATE, changeTypeToProto("Create"))
	assert.Equal(t, ternv1.ChangeType_CHANGE_TYPE_OTHER, changeTypeToProto("unknown"))
}

func TestPlanResponseFromProto_ChangeType(t *testing.T) {
	resp := &ternv1.PlanResponse{
		Changes: []*ternv1.SchemaChange{
			{
				Namespace: "testapp",
				TableChanges: []*ternv1.TableChange{
					{TableName: "users", Ddl: "CREATE TABLE users (id int)", ChangeType: ternv1.ChangeType_CHANGE_TYPE_CREATE},
					{TableName: "orders", Ddl: "ALTER TABLE orders ADD col int", ChangeType: ternv1.ChangeType_CHANGE_TYPE_ALTER},
					{TableName: "old_table", Ddl: "DROP TABLE old_table", ChangeType: ternv1.ChangeType_CHANGE_TYPE_DROP},
				},
			},
		},
	}

	result := planResponseFromProto(resp)
	tables := result.FlatTables()

	assert.Equal(t, "create", tables[0].ChangeType, "CREATE should be lowercase 'create', not proto enum string")
	assert.Equal(t, "alter", tables[1].ChangeType, "ALTER should be lowercase 'alter', not proto enum string")
	assert.Equal(t, "drop", tables[2].ChangeType, "DROP should be lowercase 'drop', not proto enum string")
}

// A null namespace value in the proto map (e.g. JSON `{"default": null}`)
// converts to an empty namespace rather than dereferencing a nil pointer.
func TestProtoToSchemaFiles_NilNamespaceValue(t *testing.T) {
	result := protoToSchemaFiles(map[string]*ternv1.SchemaFiles{
		"default": nil,
		"payments": {Files: map[string]string{
			"users.sql": "CREATE TABLE users (id bigint primary key)",
		}},
	})

	require.Contains(t, result, "default")
	require.NotNil(t, result["default"])
	assert.Empty(t, result["default"].Files)

	require.Contains(t, result, "payments")
	require.NotNil(t, result["payments"])
	assert.Equal(t, map[string]string{
		"users.sql": "CREATE TABLE users (id bigint primary key)",
	}, result["payments"].Files)
}

func TestProtoChangesToNamespacesPreservesVSchemaFiles(t *testing.T) {
	namespaces, err := protoChangesToNamespaces([]*ternv1.SchemaChange{{
		Namespace: "commerce",
		Metadata:  map[string]string{"vschema_changed": "true"},
		TableChanges: []*ternv1.TableChange{{
			TableName:  "users",
			Ddl:        "ALTER TABLE `users` ADD COLUMN `email` varchar(255)",
			ChangeType: ternv1.ChangeType_CHANGE_TYPE_ALTER,
		}},
		OriginalFiles: map[string]string{
			"users.sql":    "CREATE TABLE `users` (`id` bigint unsigned NOT NULL)",
			"vschema.json": `{"tables":{"users":{"column_vindexes":[{"column":"old_id","name":"hash"}]}}}`,
		},
		OriginalFilesCaptured: true,
	}}, map[string]*ternv1.SchemaFiles{
		"commerce": {Files: map[string]string{
			"users.sql":    "CREATE TABLE `users` (`id` bigint unsigned NOT NULL, `email` varchar(255))",
			"vschema.json": `{"tables":{"users":{"column_vindexes":[{"column":"id","name":"hash"}]}}}`,
		}},
	})

	require.NoError(t, err)
	require.Contains(t, namespaces, "commerce")
	nsData := namespaces["commerce"]
	assert.Equal(t, "CREATE TABLE `users` (`id` bigint unsigned NOT NULL)", nsData.OriginalFiles["users.sql"])
	assert.True(t, nsData.OriginalFilesCaptured)
	assert.JSONEq(t, `{"tables":{"users":{"column_vindexes":[{"column":"old_id","name":"hash"}]}}}`, nsData.OriginalFiles["vschema.json"])
	assert.JSONEq(t, `{"tables":{"users":{"column_vindexes":[{"column":"id","name":"hash"}]}}}`, nsData.Artifacts["vschema.json"])

	var decoded map[string]any
	require.NoError(t, json.Unmarshal([]byte(nsData.OriginalFiles["vschema.json"]), &decoded))
}

func TestProtoChangesToNamespacesPreservesOriginalFiles(t *testing.T) {
	namespaces, err := protoChangesToNamespaces([]*ternv1.SchemaChange{{
		Namespace: "commerce",
		Metadata:  map[string]string{"vschema_changed": "true"},
		OriginalFiles: map[string]string{
			"schema.sql": "CREATE SCHEMA commerce;",
		},
		OriginalFilesCaptured: true,
	}}, map[string]*ternv1.SchemaFiles{
		"commerce": {Files: map[string]string{"vschema.json": `{"tables":{"users":{}}}`}},
	})

	require.NoError(t, err)
	require.Contains(t, namespaces, "commerce")
	nsData := namespaces["commerce"]
	assert.Equal(t, "CREATE SCHEMA commerce;", nsData.OriginalFiles["schema.sql"])
	assert.True(t, nsData.OriginalFilesCaptured)
}

func TestProtoChangesToNamespacesRejectsDuplicateNamespaces(t *testing.T) {
	_, err := protoChangesToNamespaces([]*ternv1.SchemaChange{
		{Namespace: "commerce"},
		{Namespace: "commerce"},
	}, nil)

	require.Error(t, err)
	assert.Contains(t, err.Error(), `duplicate schema change namespace "commerce"`)
}

func TestProtoChangesToNamespacesRejectsDuplicateDefaultNamespaces(t *testing.T) {
	_, err := protoChangesToNamespaces([]*ternv1.SchemaChange{
		{},
		{Namespace: "default"},
	}, nil)

	require.Error(t, err)
	assert.Contains(t, err.Error(), `duplicate schema change namespace "default"`)
}

func TestProtoShardPlansToStorage(t *testing.T) {
	got, err := protoShardPlansToStorage([]*ternv1.ShardPlan{
		{Namespace: "commerce", Shard: "-80", NeedsChange: true},
		{Shard: "80-", NeedsChange: false},
	})

	require.NoError(t, err)
	assert.Equal(t, []storage.ShardPlan{
		{Namespace: "commerce", Shard: "-80", NeedsChange: true},
		{Namespace: "default", Shard: "80-", NeedsChange: false},
	}, got)
}

func TestProtoShardPlansToStorageRejectsNilShardPlan(t *testing.T) {
	_, err := protoShardPlansToStorage([]*ternv1.ShardPlan{nil})

	require.Error(t, err)
	assert.Contains(t, err.Error(), "shard plan 0 is null")
}

func TestProtoShardPlansToStorageRejectsEmptyShard(t *testing.T) {
	_, err := protoShardPlansToStorage([]*ternv1.ShardPlan{{Namespace: "commerce", Shard: "  "}})

	require.Error(t, err)
	assert.Contains(t, err.Error(), "shard plan 0 has empty shard")
}

// schema_files with a null namespace value is rejected as a hard validation
// error so the request gets a clear 4xx instead of reaching the converter.
func TestValidateSchemaFiles_NullNamespaceRejected(t *testing.T) {
	warning, err := validateSchemaFiles(map[string]*ternv1.SchemaFiles{
		"default": nil,
	})

	require.Error(t, err)
	assert.Empty(t, warning)
	assert.Contains(t, err.Error(), `schema_files["default"] is null`)
}

// An empty (but non-null) files map is valid input and only produces a
// warning, since it legitimately signals "drop all tables" in a namespace.
func TestValidateSchemaFiles_EmptyFilesWarnsButAllows(t *testing.T) {
	warning, err := validateSchemaFiles(map[string]*ternv1.SchemaFiles{
		"default": {Files: map[string]string{}},
	})

	require.NoError(t, err)
	assert.Contains(t, warning, `schema_files["default"] has no files`)
}

// Missing schema_files is rejected with a clear required-field error.
func TestValidateSchemaFiles_MissingRejected(t *testing.T) {
	warning, err := validateSchemaFiles(nil)

	require.Error(t, err)
	assert.Empty(t, warning)
	assert.Contains(t, err.Error(), "schema_files is required")
}
