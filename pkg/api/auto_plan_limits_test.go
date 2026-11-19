package api

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestAutoPlanLimitsExceeded(t *testing.T) {
	defaults := AutoPlanLimitsConfig{}
	for _, tc := range []struct {
		name   string
		limits AutoPlanLimitsConfig
		size   AutoPlanSize
		want   bool
	}{
		{"default database limit holds", defaults, AutoPlanSize{Databases: DefaultAutoPlanMaxDatabases, SchemaFiles: 20}, false},
		{"one database over the default limit", defaults, AutoPlanSize{Databases: DefaultAutoPlanMaxDatabases + 1, SchemaFiles: 11}, true},
		{"default schema file limit holds", defaults, AutoPlanSize{Databases: 2, SchemaFiles: DefaultAutoPlanMaxSchemaFiles}, false},
		{"one schema file over the default limit", defaults, AutoPlanSize{Databases: 2, SchemaFiles: DefaultAutoPlanMaxSchemaFiles + 1}, true},
		{"one database is never over the schema file limit", defaults, AutoPlanSize{Databases: 1, SchemaFiles: 5000}, false},
		{"configured database limit", AutoPlanLimitsConfig{MaxDatabases: 3}, AutoPlanSize{Databases: 4, SchemaFiles: 4}, true},
		{"configured schema file limit", AutoPlanLimitsConfig{MaxSchemaFiles: 5}, AutoPlanSize{Databases: 2, SchemaFiles: 6}, true},
		{"a PR with no schema changes", defaults, AutoPlanSize{}, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.want, tc.limits.Exceeded(tc.size))
		})
	}
}

func TestAutoPlanLimitsRejectNegativeValues(t *testing.T) {
	require.ErrorContains(t, AutoPlanLimitsConfig{MaxDatabases: -1}.validate(), "auto_plan_limits.max_databases must not be negative, got -1")
	require.ErrorContains(t, AutoPlanLimitsConfig{MaxSchemaFiles: -2}.validate(), "auto_plan_limits.max_schema_files must not be negative, got -2")
	require.NoError(t, AutoPlanLimitsConfig{}.validate())
}
