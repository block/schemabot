package ternconv

import (
	"fmt"
	"strings"
	"testing"

	"github.com/block/schemabot/pkg/ddl"
	ternv1 "github.com/block/schemabot/pkg/proto/ternv1"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestChangeTypeToStatementType(t *testing.T) {
	tests := []struct {
		changeType    ternv1.ChangeType
		statementType ddl.StatementType
		mapped        bool
	}{
		{ternv1.ChangeType_CHANGE_TYPE_OTHER, ddl.StatementUnknown, false},
		{ternv1.ChangeType_CHANGE_TYPE_CREATE, ddl.StatementCreateTable, true},
		{ternv1.ChangeType_CHANGE_TYPE_ALTER, ddl.StatementAlterTable, true},
		{ternv1.ChangeType_CHANGE_TYPE_DROP, ddl.StatementDropTable, true},
		{ternv1.ChangeType_CHANGE_TYPE_VSCHEMA, ddl.StatementUnknown, false},
		{ternv1.ChangeType_CHANGE_TYPE_CREATE_INDEX, ddl.StatementCreateIndex, true},
		{ternv1.ChangeType_CHANGE_TYPE_DROP_INDEX, ddl.StatementDropIndex, true},
		{ternv1.ChangeType_CHANGE_TYPE_RENAME, ddl.StatementRenameTable, true},
		{ternv1.ChangeType_CHANGE_TYPE_TRUNCATE, ddl.StatementTruncateTable, true},
		{ternv1.ChangeType_CHANGE_TYPE_CREATE_VIEW, ddl.StatementCreateView, true},
		{ternv1.ChangeType(100), ddl.StatementUnknown, false},
	}

	for _, tt := range tests {
		t.Run(tt.changeType.String(), func(t *testing.T) {
			statementType, mapped := ChangeTypeToStatementType(tt.changeType)
			assert.Equal(t, tt.statementType, statementType)
			assert.Equal(t, tt.mapped, mapped)
		})
	}
}

func TestStatementTypeToChangeType(t *testing.T) {
	tests := []struct {
		statementType ddl.StatementType
		changeType    ternv1.ChangeType
	}{
		{ddl.StatementUnknown, ternv1.ChangeType_CHANGE_TYPE_OTHER},
		{ddl.StatementAlterTable, ternv1.ChangeType_CHANGE_TYPE_ALTER},
		{ddl.StatementCreateTable, ternv1.ChangeType_CHANGE_TYPE_CREATE},
		{ddl.StatementDropTable, ternv1.ChangeType_CHANGE_TYPE_DROP},
		{ddl.StatementRenameTable, ternv1.ChangeType_CHANGE_TYPE_RENAME},
		{ddl.StatementTruncateTable, ternv1.ChangeType_CHANGE_TYPE_TRUNCATE},
		{ddl.StatementCreateIndex, ternv1.ChangeType_CHANGE_TYPE_CREATE_INDEX},
		{ddl.StatementDropIndex, ternv1.ChangeType_CHANGE_TYPE_DROP_INDEX},
		{ddl.StatementCreateView, ternv1.ChangeType_CHANGE_TYPE_CREATE_VIEW},
		{ddl.StatementInsert, ternv1.ChangeType_CHANGE_TYPE_OTHER},
		{ddl.StatementUpdate, ternv1.ChangeType_CHANGE_TYPE_OTHER},
		{ddl.StatementDelete, ternv1.ChangeType_CHANGE_TYPE_OTHER},
		{ddl.StatementType(100), ternv1.ChangeType_CHANGE_TYPE_OTHER},
	}

	for _, tt := range tests {
		t.Run(tt.statementType.String(), func(t *testing.T) {
			assert.Equal(t, tt.changeType, StatementTypeToChangeType(tt.statementType))
		})
	}
}

// changeTypesOutsideDDLVocabulary are the proto values that intentionally have
// no StatementType: OTHER is the catch-all and VSCHEMA is not a SQL statement.
// Every other value the proto declares must map, so adding a value to the enum
// without extending the mapping fails here rather than degrading to OTHER at
// every boundary.
var changeTypesOutsideDDLVocabulary = map[ternv1.ChangeType]bool{
	ternv1.ChangeType_CHANGE_TYPE_OTHER:   true,
	ternv1.ChangeType_CHANGE_TYPE_VSCHEMA: true,
}

func TestEveryDeclaredChangeTypeRoundTrips(t *testing.T) {
	for value, name := range ternv1.ChangeType_name {
		ct := ternv1.ChangeType(value)
		t.Run(name, func(t *testing.T) {
			st, mapped := ChangeTypeToStatementType(ct)
			if changeTypesOutsideDDLVocabulary[ct] {
				assert.False(t, mapped, "%s sits outside the DDL vocabulary and must not map", name)
				assert.Equal(t, ddl.StatementUnknown, st)
				return
			}
			require.True(t, mapped, "%s is declared by the proto but has no StatementType mapping", name)
			assert.True(t, st.IsDDL(), "%s maps to %s, which is not DDL", name, st)
			assert.Equal(t, ct, StatementTypeToChangeType(st), "%s does not round-trip through %s", name, st)
		})
	}
}

// statementTypeWalkLimit bounds the StatementType values the completeness test
// visits. It sits well past the last declared value so a kind appended to the
// const block is walked even when nothing else names it yet.
const statementTypeWalkLimit = ddl.StatementUnknown + 64

// TestEveryDDLStatementTypeRoundTrips walks a fixed range of StatementType
// values and asks IsDDL, the vocabulary's own definition of DDL, which of them
// must map. A DDL kind added to pkg/ddl without a proto representation fails
// here instead of being reported as OTHER on the wire, including a kind that
// String() does not name yet.
func TestEveryDDLStatementTypeRoundTrips(t *testing.T) {
	for st := range statementTypeWalkLimit {
		t.Run(fmt.Sprintf("%d_%s", int(st), st), func(t *testing.T) {
			ct := StatementTypeToChangeType(st)
			if !st.IsDDL() {
				assert.Equal(t, ternv1.ChangeType_CHANGE_TYPE_OTHER, ct, "StatementType(%d) is not DDL and must map to OTHER", int(st))
				return
			}
			require.NotEqual(t, ternv1.ChangeType_CHANGE_TYPE_OTHER, ct, "StatementType(%d) is DDL but has no proto ChangeType", int(st))
			back, mapped := ChangeTypeToStatementType(ct)
			require.True(t, mapped)
			assert.Equal(t, st, back, "StatementType(%d) does not round-trip through %s", int(st), ct)
		})
	}
}

// TestOpRoundTripsThroughChangeType pins the operation strings each boundary
// stores: every change type with an operation, vschema included, comes back as
// the same lowercase string after a trip through the proto value.
func TestOpRoundTripsThroughChangeType(t *testing.T) {
	for value, name := range ternv1.ChangeType_name {
		ct := ternv1.ChangeType(value)
		t.Run(name, func(t *testing.T) {
			op, ok := ChangeTypeToOp(ct)
			if ct == ternv1.ChangeType_CHANGE_TYPE_OTHER {
				assert.False(t, ok, "OTHER has no operation string")
				assert.Empty(t, op)
				return
			}
			require.True(t, ok, "%s has no operation string", name)
			assert.Equal(t, strings.ToLower(op), op, "%s operation %q is not lowercase", name, op)
			assert.Equal(t, ct, OpToChangeType(op), "%s does not round-trip through %q", name, op)
		})
	}
	assert.Equal(t, OpVSchemaUpdate, mustOp(t, ternv1.ChangeType_CHANGE_TYPE_VSCHEMA))
	assert.Equal(t, ddl.StatementTypeToOp(ddl.StatementDropIndex), mustOp(t, ternv1.ChangeType_CHANGE_TYPE_DROP_INDEX))
}

func mustOp(t *testing.T, ct ternv1.ChangeType) string {
	t.Helper()
	op, ok := ChangeTypeToOp(ct)
	require.True(t, ok)
	return op
}

// TestOpToChangeTypeIgnoresCase pins the one case rule every boundary applies:
// an operation read back from storage or a request matches regardless of case,
// for vschema exactly as for the DDL vocabulary.
func TestOpToChangeTypeIgnoresCase(t *testing.T) {
	tests := []struct {
		op         string
		changeType ternv1.ChangeType
	}{
		{"alter", ternv1.ChangeType_CHANGE_TYPE_ALTER},
		{"ALTER", ternv1.ChangeType_CHANGE_TYPE_ALTER},
		{"Create", ternv1.ChangeType_CHANGE_TYPE_CREATE},
		{"drop_index", ternv1.ChangeType_CHANGE_TYPE_DROP_INDEX},
		{"vschema_update", ternv1.ChangeType_CHANGE_TYPE_VSCHEMA},
		{"VSCHEMA_UPDATE", ternv1.ChangeType_CHANGE_TYPE_VSCHEMA},
		{"VSchema_Update", ternv1.ChangeType_CHANGE_TYPE_VSCHEMA},
		{"vschema", ternv1.ChangeType_CHANGE_TYPE_OTHER},
		{"unknown", ternv1.ChangeType_CHANGE_TYPE_OTHER},
		{"other", ternv1.ChangeType_CHANGE_TYPE_OTHER},
		{"", ternv1.ChangeType_CHANGE_TYPE_OTHER},
	}
	for _, tt := range tests {
		t.Run(tt.op, func(t *testing.T) {
			assert.Equal(t, tt.changeType, OpToChangeType(tt.op))
		})
	}
}
