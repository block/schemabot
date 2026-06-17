package schema

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Flat layout tests — SQL files directly in the schema directory.
// The directory name is used as the namespace key.

func TestGroupFilesByNamespace_FlatLayout_UsesDirectoryName(t *testing.T) {
	// aurora_coffeeshop_exemplar/
	// ├── schemabot.yaml   (skipped)
	// ├── baristas.sql     → namespace: "aurora_coffeeshop_exemplar"
	// └── customers.sql    → namespace: "aurora_coffeeshop_exemplar"
	files := map[string]string{
		"baristas.sql":  "CREATE TABLE baristas (...);",
		"customers.sql": "CREATE TABLE customers (...);",
	}

	result, err := GroupFilesByNamespace(files, "aurora_coffeeshop_exemplar", "development")
	require.NoError(t, err)

	require.Len(t, result, 1)
	assert.Contains(t, result, "aurora_coffeeshop_exemplar")
	assert.Len(t, result["aurora_coffeeshop_exemplar"].Files, 2)
	assert.Equal(t, "CREATE TABLE baristas (...);", result["aurora_coffeeshop_exemplar"].Files["baristas.sql"])
	assert.Equal(t, "CREATE TABLE customers (...);", result["aurora_coffeeshop_exemplar"].Files["customers.sql"])
}

func TestGroupFilesByNamespace_FlatLayout_SkipsNonSchemaFiles(t *testing.T) {
	// Only .sql and vschema.json are included — everything else is skipped.
	files := map[string]string{
		"users.sql":      "CREATE TABLE users (...);",
		"schemabot.yaml": "database: myapp\ntype: mysql",
		"README.md":      "# Schema docs",
		".gitkeep":       "",
	}

	result, err := GroupFilesByNamespace(files, "myapp", "development")
	require.NoError(t, err)
	require.Len(t, result, 1)
	assert.Len(t, result["myapp"].Files, 1)
	assert.Contains(t, result["myapp"].Files, "users.sql")
}

func TestGroupFilesByNamespace_FlatLayout_IncludesVSchemaJSON(t *testing.T) {
	// vschema.json is a valid schema file (Vitess VSchema definition).
	files := map[string]string{
		"orders.sql":   "CREATE TABLE orders (...);",
		"vschema.json": `{"sharded": true}`,
	}

	result, err := GroupFilesByNamespace(files, "commerce", "development")
	require.NoError(t, err)
	require.Len(t, result, 1)
	assert.Len(t, result["commerce"].Files, 2)
	assert.Contains(t, result["commerce"].Files, "vschema.json")
}

// Subdirectory layout tests — each subdirectory becomes a namespace.
// defaultNamespace is not used.

func TestGroupFilesByNamespace_SubdirLayout_UsesSubdirNames(t *testing.T) {
	// schema/
	// ├── payments/
	// │   ├── transactions.sql   → namespace: "payments"
	// │   └── refunds.sql        → namespace: "payments"
	// └── payments_audit/
	//     └── audit_log.sql      → namespace: "payments_audit"
	files := map[string]string{
		"payments/transactions.sql":    "CREATE TABLE transactions (...);",
		"payments/refunds.sql":         "CREATE TABLE refunds (...);",
		"payments_audit/audit_log.sql": "CREATE TABLE audit_log (...);",
	}

	result, err := GroupFilesByNamespace(files, "ignored_because_subdirs_exist", "development")
	require.NoError(t, err)
	require.Len(t, result, 2)
	assert.Contains(t, result, "payments")
	assert.Contains(t, result, "payments_audit")
	assert.Len(t, result["payments"].Files, 2)
	assert.Len(t, result["payments_audit"].Files, 1)
}

func TestGroupFilesByNamespace_SubdirLayout_VSchemaInSubdir(t *testing.T) {
	// Vitess layout with vschema.json inside keyspace subdirectories.
	files := map[string]string{
		"commerce/orders.sql":   "CREATE TABLE orders (...);",
		"commerce/vschema.json": `{"sharded": true}`,
		"customers/users.sql":   "CREATE TABLE users (...);",
	}

	result, err := GroupFilesByNamespace(files, "ignored", "development")
	require.NoError(t, err)
	require.Len(t, result, 2)
	assert.Len(t, result["commerce"].Files, 2)
	assert.Contains(t, result["commerce"].Files, "vschema.json")
	assert.Len(t, result["customers"].Files, 1)
}

// Mixed layout — rejected as ambiguous.

func TestGroupFilesByNamespace_MixedLayout_Rejected(t *testing.T) {
	// Flat files alongside subdirectories is ambiguous.
	files := map[string]string{
		"standalone.sql":            "CREATE TABLE standalone (...);",
		"payments/transactions.sql": "CREATE TABLE transactions (...);",
	}

	_, err := GroupFilesByNamespace(files, "mydb", "development")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "both flat files and namespace subdirectories")
}

func TestGroupFilesByNamespace_SubdirLayout_NameMatchesDefault(t *testing.T) {
	// A subdirectory name that matches defaultNamespace should NOT trigger
	// the mixed-layout rejection — all files are in subdirectories.
	files := map[string]string{
		"schema/tables.sql": "CREATE TABLE tables (...);",
		"other/items.sql":   "CREATE TABLE items (...);",
	}

	result, err := GroupFilesByNamespace(files, "schema", "development")
	require.NoError(t, err)
	require.Len(t, result, 2)
	assert.Contains(t, result, "schema")
	assert.Contains(t, result, "other")
}

// Edge cases.

func TestGroupFilesByNamespace_EmptyInput(t *testing.T) {
	result, err := GroupFilesByNamespace(map[string]string{}, "mydb", "development")
	require.NoError(t, err)
	assert.Empty(t, result)
}

func TestGroupFilesByNamespace_OnlyNonSchemaFiles(t *testing.T) {
	// All files are skipped — returns empty result (no error).
	files := map[string]string{
		"schemabot.yaml": "database: myapp\ntype: mysql",
		"README.md":      "# docs",
	}

	result, err := GroupFilesByNamespace(files, "myapp", "development")
	require.NoError(t, err)
	assert.Empty(t, result)
}

// $ENV substitution tests.

func TestGroupFilesByNamespace_EnvSubstitution_SubdirLayout(t *testing.T) {
	// Subdirectory named "bikeshare_$ENV" becomes "bikeshare_staging" when
	// environment is "staging".
	files := map[string]string{
		"bikeshare_$ENV/bikes.sql":    "CREATE TABLE bikes (...);",
		"bikeshare_$ENV/stations.sql": "CREATE TABLE stations (...);",
	}

	result, err := GroupFilesByNamespace(files, "ignored", "staging")
	require.NoError(t, err)

	require.Len(t, result, 1)
	assert.Contains(t, result, "bikeshare_staging")
	assert.Len(t, result["bikeshare_staging"].Files, 2)
	assert.Equal(t, "CREATE TABLE bikes (...);", result["bikeshare_staging"].Files["bikes.sql"])
	assert.Equal(t, "CREATE TABLE stations (...);", result["bikeshare_staging"].Files["stations.sql"])
}

func TestGroupFilesByNamespace_EnvSubstitution_FlatLayout(t *testing.T) {
	// Flat layout where the defaultNamespace (directory name) contains $ENV.
	// With environment="production", "bikeshare_$ENV" → "bikeshare_production".
	files := map[string]string{
		"bikes.sql":    "CREATE TABLE bikes (...);",
		"stations.sql": "CREATE TABLE stations (...);",
	}

	result, err := GroupFilesByNamespace(files, "bikeshare_$ENV", "production")
	require.NoError(t, err)

	require.Len(t, result, 1)
	assert.Contains(t, result, "bikeshare_production")
	assert.Len(t, result["bikeshare_production"].Files, 2)
}

func TestGroupFilesByNamespace_EnvSubstitution_EmptyEnvNoChange(t *testing.T) {
	// When environment is empty, $ENV is left as-is (no substitution).
	files := map[string]string{
		"bikeshare_$ENV/bikes.sql": "CREATE TABLE bikes (...);",
	}

	result, err := GroupFilesByNamespace(files, "ignored", "")
	require.NoError(t, err)

	require.Len(t, result, 1)
	assert.Contains(t, result, "bikeshare_$ENV")
}

func TestGroupFilesByNamespace_EnvSubstitution_MultipleNamespaces(t *testing.T) {
	// Multiple subdirectories, some with $ENV, some without.
	files := map[string]string{
		"app_$ENV/users.sql":   "CREATE TABLE users (...);",
		"analytics/events.sql": "CREATE TABLE events (...);",
	}

	result, err := GroupFilesByNamespace(files, "ignored", "staging")
	require.NoError(t, err)

	require.Len(t, result, 2)
	assert.Contains(t, result, "app_staging")
	assert.Contains(t, result, "analytics")
}

func TestIsReservedPullNamespace(t *testing.T) {
	tests := []struct {
		name      string
		namespace string
		want      bool
	}{
		{name: "pending drops", namespace: "_pending_drops", want: true},
		{name: "system schema", namespace: "information_schema", want: true},
		{name: "performance schema", namespace: "performance_schema", want: true},
		{name: "mysql system db", namespace: "mysql", want: true},
		{name: "sys schema", namespace: "sys", want: true},
		{name: "aurora dbadmin", namespace: "dbadmin", want: true},
		{name: "aurora innodb", namespace: "innodb", want: true},
		{name: "aurora tmp", namespace: "tmp", want: true},
		{name: "rds polt", namespace: "polt", want: true},
		{name: "rds rdsmon", namespace: "rdsmon", want: true},
		{name: "rds topo", namespace: "topo", want: true},
		{name: "uppercase system schema", namespace: "INNODB", want: true},
		{name: "schemabot storage", namespace: "schemabot", want: true},
		{name: "underscore prefix", namespace: "_scratch", want: true},
		{name: "application namespace", namespace: "orders_production", want: false},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.want, IsReservedPullNamespace(tc.namespace))
		})
	}
}
