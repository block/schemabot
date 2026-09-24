package ternconv

import (
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

// TestEveryDDLStatementTypeRoundTrips walks the StatementType vocabulary from
// its first value until String() stops naming values, so a DDL kind added to
// pkg/ddl without a proto representation fails here instead of being reported
// as OTHER on the wire.
func TestEveryDDLStatementTypeRoundTrips(t *testing.T) {
	unnamed := ddl.StatementType(-1).String()
	for st := ddl.StatementUnknown + 1; st.String() != unnamed; st++ {
		t.Run(st.String(), func(t *testing.T) {
			ct := StatementTypeToChangeType(st)
			if !st.IsDDL() {
				assert.Equal(t, ternv1.ChangeType_CHANGE_TYPE_OTHER, ct, "%s is not DDL and must map to OTHER", st)
				return
			}
			require.NotEqual(t, ternv1.ChangeType_CHANGE_TYPE_OTHER, ct, "%s is DDL but has no proto ChangeType", st)
			back, mapped := ChangeTypeToStatementType(ct)
			require.True(t, mapped)
			assert.Equal(t, st, back, "%s does not round-trip through %s", st, ct)
		})
	}
}
