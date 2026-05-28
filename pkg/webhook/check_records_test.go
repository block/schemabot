package webhook

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/block/schemabot/pkg/state"
	"github.com/block/schemabot/pkg/storage"
)

func TestLatestApplyByCheckKey(t *testing.T) {
	applies := []*storage.Apply{
		{
			ID:           1,
			Database:     "orders",
			DatabaseType: "mysql",
			Environment:  "staging",
			State:        state.Apply.Completed,
		},
		{
			ID:           2,
			Database:     "orders",
			DatabaseType: "mysql",
			Environment:  "staging",
			State:        state.Apply.Running,
		},
		{
			ID:           3,
			Database:     "orders",
			DatabaseType: "vitess",
			Environment:  "staging",
			State:        state.Apply.Completed,
		},
		{
			ID:           5,
			Database:     "users",
			DatabaseType: "mysql",
			Environment:  "staging",
			State:        state.Apply.Failed,
		},
		{
			ID:           4,
			Database:     "users",
			DatabaseType: "mysql",
			Environment:  "staging",
			State:        state.Apply.Completed,
		},
	}

	latest := latestApplyByCheckKey(applies)

	mysqlOrders := latest[applyCheckKey{environment: "staging", databaseType: "mysql", databaseName: "orders"}]
	require.NotNil(t, mysqlOrders)
	assert.Equal(t, state.Apply.Running, mysqlOrders.State)

	vitessOrders := latest[applyCheckKey{environment: "staging", databaseType: "vitess", databaseName: "orders"}]
	require.NotNil(t, vitessOrders)
	assert.Equal(t, state.Apply.Completed, vitessOrders.State)

	mysqlUsers := latest[applyCheckKey{environment: "staging", databaseType: "mysql", databaseName: "users"}]
	require.NotNil(t, mysqlUsers)
	assert.Equal(t, int64(5), mysqlUsers.ID)
	assert.Equal(t, state.Apply.Failed, mysqlUsers.State)
}
