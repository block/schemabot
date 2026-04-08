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

	result, err := GroupFilesByNamespace(files, "aurora_coffeeshop_exemplar")
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

	result, err := GroupFilesByNamespace(files, "myapp")
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

	result, err := GroupFilesByNamespace(files, "commerce")
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

	result, err := GroupFilesByNamespace(files, "ignored_because_subdirs_exist")
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

	result, err := GroupFilesByNamespace(files, "ignored")
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

	_, err := GroupFilesByNamespace(files, "mydb")
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

	result, err := GroupFilesByNamespace(files, "schema")
	require.NoError(t, err)
	require.Len(t, result, 2)
	assert.Contains(t, result, "schema")
	assert.Contains(t, result, "other")
}

// Edge cases.

func TestGroupFilesByNamespace_EmptyInput(t *testing.T) {
	result, err := GroupFilesByNamespace(map[string]string{}, "mydb")
	require.NoError(t, err)
	assert.Empty(t, result)
}

func TestGroupFilesByNamespace_OnlyNonSchemaFiles(t *testing.T) {
	// All files are skipped — returns empty result (no error).
	files := map[string]string{
		"schemabot.yaml": "database: myapp\ntype: mysql",
		"README.md":      "# docs",
	}

	result, err := GroupFilesByNamespace(files, "myapp")
	require.NoError(t, err)
	assert.Empty(t, result)
}
