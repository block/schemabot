package ddl

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

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
