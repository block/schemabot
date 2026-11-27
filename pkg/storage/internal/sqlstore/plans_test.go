//go:build integration

package sqlstore

import (
	"strings"
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

// A plan of a database with many shards carries plan data wider than the
// server's sort buffer. Listing plans must keep that data out of the sort, so
// the plans page, the review round lookup, and the PR lookup each return such
// a plan newest first instead of failing every request that reaches its row.
func TestPlanStore_ListsPlanWiderThanSortBuffer(t *testing.T) {
	clearTables(t)
	ctx := t.Context()
	store := NewMySQL(testDB)

	var sortBufferSize int
	require.NoError(t, testDB.QueryRowContext(ctx, `SELECT @@sort_buffer_size`).Scan(&sortBufferSize))
	wideDDL := "ALTER TABLE `orders` COMMENT '" + strings.Repeat("a", 2*sortBufferSize) + "'"

	base := time.Now().Add(-time.Hour).Truncate(time.Second)
	create := func(identifier, primary string, age time.Duration, ddl string) {
		t.Helper()
		_, err := store.Plans().Create(ctx, &storage.Plan{
			PlanIdentifier:        identifier,
			Database:              "commerce",
			DatabaseType:          storage.DatabaseTypeMySQL,
			Repository:            "org/repo",
			PullRequest:           7,
			Environment:           "staging",
			PrimaryPlanIdentifier: primary,
			Namespaces: map[string]*storage.NamespacePlanData{
				"commerce": {Tables: []storage.TableChange{{Table: "orders", Operation: "alter", DDL: ddl}}},
			},
			CreatedAt: base.Add(-age),
		})
		require.NoError(t, err)
	}
	narrowDDL := "ALTER TABLE `orders` ADD COLUMN `region` varchar(16)"
	create("plan_oldest", "", 3*time.Minute, narrowDDL)
	create("plan_primary", "", 2*time.Minute, wideDDL)
	create("plan_member", "plan_primary", time.Minute, wideDDL)

	identifiers := func(plans []*storage.Plan) []string {
		out := make([]string, 0, len(plans))
		for _, plan := range plans {
			out = append(out, plan.PlanIdentifier)
		}
		return out
	}

	page, err := store.Plans().List(ctx, storage.ListPlansOptions{Database: "commerce", Environment: "staging", Limit: 2})
	require.NoError(t, err)
	assert.Equal(t, []string{"plan_member", "plan_primary"}, identifiers(page))
	require.Len(t, page, 2)
	changes := page[0].FlatDDLChanges()
	require.Len(t, changes, 1)
	assert.Equal(t, wideDDL, changes[0].DDL)

	round, err := store.Plans().List(ctx, storage.ListPlansOptions{PrimaryPlanIdentifier: "plan_primary"})
	require.NoError(t, err)
	assert.Equal(t, []string{"plan_member"}, identifiers(round))

	byPR, err := store.Plans().GetByPR(ctx, "org/repo", 7)
	require.NoError(t, err)
	assert.Equal(t, []string{"plan_member", "plan_primary", "plan_oldest"}, identifiers(byPR))
}
