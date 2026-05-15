package api

import (
	"testing"

	ternv1 "github.com/block/schemabot/pkg/proto/ternv1"
	"github.com/stretchr/testify/assert"
)

func TestChangeTypeRoundTrip(t *testing.T) {
	// Proto → storage → proto should round-trip correctly
	for _, ct := range []ternv1.ChangeType{
		ternv1.ChangeType_CHANGE_TYPE_CREATE,
		ternv1.ChangeType_CHANGE_TYPE_ALTER,
		ternv1.ChangeType_CHANGE_TYPE_DROP,
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
