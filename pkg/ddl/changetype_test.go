package ddl

import (
	"testing"

	ternv1 "github.com/block/schemabot/pkg/proto/ternv1"
	"github.com/stretchr/testify/assert"
)

func TestChangeTypeToStatementType(t *testing.T) {
	tests := []struct {
		changeType    ternv1.ChangeType
		statementType StatementType
		mapped        bool
	}{
		{ternv1.ChangeType_CHANGE_TYPE_OTHER, StatementUnknown, false},
		{ternv1.ChangeType_CHANGE_TYPE_CREATE, StatementCreateTable, true},
		{ternv1.ChangeType_CHANGE_TYPE_ALTER, StatementAlterTable, true},
		{ternv1.ChangeType_CHANGE_TYPE_DROP, StatementDropTable, true},
		{ternv1.ChangeType_CHANGE_TYPE_VSCHEMA, StatementUnknown, false},
		{ternv1.ChangeType_CHANGE_TYPE_CREATE_INDEX, StatementCreateIndex, true},
		{ternv1.ChangeType_CHANGE_TYPE_DROP_INDEX, StatementDropIndex, true},
		{ternv1.ChangeType_CHANGE_TYPE_RENAME, StatementRenameTable, true},
		{ternv1.ChangeType_CHANGE_TYPE_TRUNCATE, StatementTruncateTable, true},
		{ternv1.ChangeType_CHANGE_TYPE_CREATE_VIEW, StatementCreateView, true},
		{ternv1.ChangeType(100), StatementUnknown, false},
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
		statementType StatementType
		changeType    ternv1.ChangeType
	}{
		{StatementUnknown, ternv1.ChangeType_CHANGE_TYPE_OTHER},
		{StatementAlterTable, ternv1.ChangeType_CHANGE_TYPE_ALTER},
		{StatementCreateTable, ternv1.ChangeType_CHANGE_TYPE_CREATE},
		{StatementDropTable, ternv1.ChangeType_CHANGE_TYPE_DROP},
		{StatementRenameTable, ternv1.ChangeType_CHANGE_TYPE_RENAME},
		{StatementTruncateTable, ternv1.ChangeType_CHANGE_TYPE_TRUNCATE},
		{StatementCreateIndex, ternv1.ChangeType_CHANGE_TYPE_CREATE_INDEX},
		{StatementDropIndex, ternv1.ChangeType_CHANGE_TYPE_DROP_INDEX},
		{StatementCreateView, ternv1.ChangeType_CHANGE_TYPE_CREATE_VIEW},
		{StatementInsert, ternv1.ChangeType_CHANGE_TYPE_OTHER},
		{StatementUpdate, ternv1.ChangeType_CHANGE_TYPE_OTHER},
		{StatementDelete, ternv1.ChangeType_CHANGE_TYPE_OTHER},
		{StatementType(100), ternv1.ChangeType_CHANGE_TYPE_OTHER},
	}

	for _, tt := range tests {
		t.Run(tt.statementType.String(), func(t *testing.T) {
			assert.Equal(t, tt.changeType, StatementTypeToChangeType(tt.statementType))
		})
	}
}
