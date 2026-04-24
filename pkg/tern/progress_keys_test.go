package tern

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/block/schemabot/pkg/engine"
	"github.com/block/schemabot/pkg/storage"
)

func TestEngineProgressForTaskUsesNamespace(t *testing.T) {
	progress := indexEngineTableProgress([]engine.TableProgress{
		{
			Namespace: "commerce_sharded",
			Table:     "orders",
			Progress:  25,
		},
		{
			Namespace: "commerce_sharded_006",
			Table:     "orders",
			Progress:  100,
		},
	})

	tp, ok := engineProgressForTask(progress, &storage.Task{
		Namespace: "commerce_sharded_006",
		TableName: "orders",
	})
	require.True(t, ok)
	require.Equal(t, "commerce_sharded_006", tp.Namespace)
	require.Equal(t, 100, tp.Progress)
}

func TestEngineProgressForTaskRequiresNamespaceMatch(t *testing.T) {
	progress := indexEngineTableProgress([]engine.TableProgress{
		{
			Table:    "users",
			Progress: 50,
		},
	})

	tp, ok := engineProgressForTask(progress, &storage.Task{
		Namespace: "app",
		TableName: "users",
	})
	require.False(t, ok)
	require.Nil(t, tp)
}
