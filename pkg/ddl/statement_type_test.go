package ddl

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

// IsDDL gates which statements drift comparison accepts: every schema-changing
// type must report true — dropping one would fail closed on legitimate DDL and
// permanently block any schema change containing it — while DML and unknown
// report false, since an unclassifiable statement must be treated as
// unverifiable rather than assumed to change schema.
func TestStatementTypeIsDDL(t *testing.T) {
	tests := []struct {
		statement StatementType
		isDDL     bool
	}{
		{statement: StatementAlterTable, isDDL: true},
		{statement: StatementCreateTable, isDDL: true},
		{statement: StatementDropTable, isDDL: true},
		{statement: StatementRenameTable, isDDL: true},
		{statement: StatementTruncateTable, isDDL: true},
		{statement: StatementCreateIndex, isDDL: true},
		{statement: StatementDropIndex, isDDL: true},
		{statement: StatementCreateView, isDDL: true},
		{statement: StatementUnknown, isDDL: false},
		{statement: StatementInsert, isDDL: false},
		{statement: StatementUpdate, isDDL: false},
		{statement: StatementDelete, isDDL: false},
	}

	for _, tt := range tests {
		t.Run(tt.statement.String(), func(t *testing.T) {
			assert.Equal(t, tt.isDDL, tt.statement.IsDDL())
		})
	}
}

func TestIndexStatementOperationRoundTrip(t *testing.T) {
	tests := []struct {
		statement StatementType
		op        string
	}{
		{statement: StatementCreateIndex, op: "create_index"},
		{statement: StatementDropIndex, op: "drop_index"},
	}

	for _, tt := range tests {
		t.Run(tt.op, func(t *testing.T) {
			assert.Equal(t, tt.op, StatementTypeToOp(tt.statement))
			assert.Equal(t, tt.statement, OpToStatementType(tt.op))
		})
	}
}
