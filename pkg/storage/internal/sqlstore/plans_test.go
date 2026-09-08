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
