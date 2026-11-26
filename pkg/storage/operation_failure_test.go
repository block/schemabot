package storage

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Tasks without an apply_operation_id (legacy rows) are not attributable to an
// operation and are dropped from the per-operation grouping, which keeps each
// operation's tasks in order.
func TestTasksByOperationSkipsUnlinkedTasks(t *testing.T) {
	grouped := TasksByOperation([]*Task{
		{ApplyOperationID: new(int64(1)), TableName: "a"},
		{ApplyOperationID: nil, TableName: "orphan"},
		{ApplyOperationID: new(int64(1)), TableName: "b"},
	})
	require.Len(t, grouped[1], 2)
	assert.Equal(t, "a", grouped[1][0].TableName)
	assert.Equal(t, "b", grouped[1][1].TableName)
	assert.Len(t, grouped, 1)
}

// A failed operation's reason is its own error when the row carries one, and
// otherwise the first error among its tasks, since a remote failure records
// its error on the tasks and may never stamp the row.
func TestOperationFailureMessage(t *testing.T) {
	tasks := []*Task{
		{TableName: "orders"},
		{TableName: "items", ErrorMessage: "dial tcp 10.0.0.7:3306: connect: connection refused"},
		{TableName: "users", ErrorMessage: "lock wait timeout exceeded"},
	}

	t.Run("row error wins", func(t *testing.T) {
		op := &ApplyOperation{ErrorMessage: "copy failed on orders"}
		assert.Equal(t, "copy failed on orders", op.FailureMessage(tasks))
	})
	t.Run("first task error when the row has none", func(t *testing.T) {
		op := &ApplyOperation{}
		assert.Equal(t, "dial tcp 10.0.0.7:3306: connect: connection refused", op.FailureMessage(tasks))
	})
	t.Run("no reason anywhere", func(t *testing.T) {
		op := &ApplyOperation{}
		assert.Empty(t, op.FailureMessage([]*Task{{TableName: "orders"}}))
	})
}
