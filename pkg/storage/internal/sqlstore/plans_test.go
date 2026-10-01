//go:build integration

package sqlstore

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/block/schemabot/pkg/storage"
)

// TestPlanStore_RoundTripsNilPlanDataAsNull reads the raw plan_data column:
// a plan without namespace data stores JSON null rather than an empty
// container, so later readers cannot mistake absence for a recorded verdict.
func TestPlanStore_RoundTripsNilPlanDataAsNull(t *testing.T) {
	clearTables(t)
	ctx := t.Context()
	store := NewMySQL(testDB)

	_, err := store.Plans().Create(ctx, &storage.Plan{
		PlanIdentifier: "plan_nil_data",
		Database:       "commerce",
		DatabaseType:   storage.DatabaseTypeMySQL,
		Deployment:     "primary",
		Target:         "commerce-target",
		Repository:     "org/repo",
		PullRequest:    123,
		SchemaPath:     "schema/commerce",
		Environment:    "staging",
		CreatedAt:      time.Now(),
	})
	require.NoError(t, err)

	var planData string
	err = testDB.QueryRowContext(ctx, "SELECT plan_data FROM plans WHERE plan_identifier = ?", "plan_nil_data").Scan(&planData)
	require.NoError(t, err)
	assert.Equal(t, "null", planData)
}

// UpdateRoute restamps a stored plan with the rollout member it was planned for
// and leaves the rest of the row, the plan's content included, as it was stored.
func TestPlanStore_UpdateRouteRestampsOnlyTheRoute(t *testing.T) {
	clearTables(t)
	ctx := t.Context()
	store := NewMySQL(testDB)

	_, err := store.Plans().Create(ctx, &storage.Plan{
		PlanIdentifier: "plan_route",
		Database:       "commerce",
		DatabaseType:   storage.DatabaseTypeMySQL,
		Deployment:     "commerce",
		Target:         "commerce-001",
		Repository:     "org/repo",
		PullRequest:    123,
		Environment:    "staging",
		HeadSHA:        "abc123",
		Namespaces: map[string]*storage.NamespacePlanData{
			"commerce": {Tables: []storage.TableChange{{Table: "orders", Operation: "alter", DDL: "ALTER TABLE `orders` ADD COLUMN `region` varchar(16)"}}},
		},
		CreatedAt: time.Now(),
	})
	require.NoError(t, err)

	require.NoError(t, store.Plans().UpdateRoute(ctx, "plan_route", "eu", "commerce-002", ""))

	plan, err := store.Plans().Get(ctx, "plan_route")
	require.NoError(t, err)
	require.NotNil(t, plan)
	assert.Equal(t, "eu", plan.Deployment)
	assert.Equal(t, "commerce-002", plan.Target)
	assert.Equal(t, "commerce", plan.Database)
	assert.Equal(t, "abc123", plan.HeadSHA)
	changes := plan.FlatDDLChanges()
	require.Len(t, changes, 1)
	assert.Equal(t, "ALTER TABLE `orders` ADD COLUMN `region` varchar(16)", changes[0].DDL)
}
