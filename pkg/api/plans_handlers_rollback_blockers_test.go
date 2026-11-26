package api

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/block/schemabot/pkg/apitypes"
	ternv1 "github.com/block/schemabot/pkg/proto/ternv1"
	"github.com/block/schemabot/pkg/storage"
)

// A plan that could not save a namespace's current schema is stored with the
// tables that are why, so GET /api/plans/{plan_id} says why its apply cannot
// be rolled back, as the response to the plan request did.
func TestStoredPlanSaysWhyItsApplyCannotBeRolledBack(t *testing.T) {
	namespaces, err := protoChangesToNamespaces([]*ternv1.SchemaChange{{
		Namespace: "public",
		TableChanges: []*ternv1.TableChange{{
			TableName: "users", Ddl: "ALTER TABLE users ADD COLUMN email text", ChangeType: ternv1.ChangeType_CHANGE_TYPE_ALTER,
		}},
		RollbackBlockers: []*ternv1.SkippedTable{{Table: "orders", Reason: "holds a foreign key added NOT VALID, which a schema file cannot declare"}},
	}}, nil)
	require.NoError(t, err)

	// Plan data is persisted as JSON, so read it back the way storage does.
	encoded, err := json.Marshal(namespaces)
	require.NoError(t, err)
	var stored map[string]*storage.NamespacePlanData
	require.NoError(t, json.Unmarshal(encoded, &stored))

	resp := PlanContentFromStorage(&storage.Plan{
		PlanIdentifier: "plan-rollback-blockers", Database: "orders", DatabaseType: storage.DatabaseTypePostgres, Namespaces: stored,
	})

	require.Len(t, resp.Changes, 1)
	assert.Equal(t, []apitypes.SkippedTable{{Table: "orders", Reason: "holds a foreign key added NOT VALID, which a schema file cannot declare"}},
		resp.Changes[0].RollbackBlockers)
}
