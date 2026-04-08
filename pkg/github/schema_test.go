package github

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Flat layout tests — the webhook strips the schemaPath prefix, leaving bare
// filenames. path.Base(schemaPath) becomes the defaultNamespace.

func TestGroupFilesByNamespace_FlatLayout_UsesDirectoryName(t *testing.T) {
	// schema/payments/
	// ├── schemabot.yaml
	// ├── transactions.sql  → namespace: "payments" (from path.Base("schema/payments"))
	// └── refunds.sql       → namespace: "payments"
	files := []GitHubFile{
		{Path: "schema/payments/schemabot.yaml", Name: "schemabot.yaml", Content: "database: payments\ntype: mysql"},
		{Path: "schema/payments/transactions.sql", Name: "transactions.sql", Content: "CREATE TABLE transactions (...);"},
		{Path: "schema/payments/refunds.sql", Name: "refunds.sql", Content: "CREATE TABLE refunds (...);"},
	}

	result, err := groupFilesByNamespace(files, "schema/payments")

	require.NoError(t, err)
	require.Len(t, result, 1)
	assert.Contains(t, result, "payments")
	assert.Len(t, result["payments"].Files, 2)
	assert.Contains(t, result["payments"].Files, "transactions.sql")
	assert.Contains(t, result["payments"].Files, "refunds.sql")
}

func TestGroupFilesByNamespace_FlatLayout_IssueScenario(t *testing.T) {
	// The exact scenario from the issue — flat files in a directory whose name
	// IS the database name. Previously this used "default" which Tern rejected.
	//
	// schema/aurora_coffeeshop_exemplar/
	// ├── schemabot.yaml
	// ├── baristas.sql   → namespace: "aurora_coffeeshop_exemplar"
	// └── customers.sql  → namespace: "aurora_coffeeshop_exemplar"
	files := []GitHubFile{
		{Path: "schema/aurora_coffeeshop_exemplar/schemabot.yaml", Name: "schemabot.yaml", Content: "database: aurora_coffeeshop_exemplar\ntype: mysql"},
		{Path: "schema/aurora_coffeeshop_exemplar/baristas.sql", Name: "baristas.sql", Content: "CREATE TABLE baristas (...);"},
		{Path: "schema/aurora_coffeeshop_exemplar/customers.sql", Name: "customers.sql", Content: "CREATE TABLE customers (...);"},
	}

	result, err := groupFilesByNamespace(files, "schema/aurora_coffeeshop_exemplar")

	require.NoError(t, err)
	require.Len(t, result, 1)
	assert.Contains(t, result, "aurora_coffeeshop_exemplar")
	assert.Len(t, result["aurora_coffeeshop_exemplar"].Files, 2)
}

func TestGroupFilesByNamespace_FlatLayout_TopLevelSchemaDir(t *testing.T) {
	// schema/ is the schemaPath — flat files use "schema" as namespace.
	// This is a less common layout but valid.
	files := []GitHubFile{
		{Path: "schema/schemabot.yaml", Name: "schemabot.yaml", Content: "database: myapp\ntype: mysql"},
		{Path: "schema/users.sql", Name: "users.sql", Content: "CREATE TABLE users (...);"},
		{Path: "schema/orders.sql", Name: "orders.sql", Content: "CREATE TABLE orders (...);"},
	}

	result, err := groupFilesByNamespace(files, "schema")

	require.NoError(t, err)
	require.Len(t, result, 1)
	assert.Contains(t, result, "schema")
	assert.Len(t, result["schema"].Files, 2)
}

// Subdirectory layout tests — subdirectory names become namespace keys.
// defaultNamespace is not used.

func TestGroupFilesByNamespace_SubdirLayout_MultipleNamespaces(t *testing.T) {
	// schema/
	// ├── schemabot.yaml
	// ├── payments/
	// │   ├── transactions.sql   → namespace: "payments"
	// │   └── refunds.sql        → namespace: "payments"
	// └── payments_audit/
	//     └── audit_log.sql      → namespace: "payments_audit"
	files := []GitHubFile{
		{Path: "schema/schemabot.yaml", Name: "schemabot.yaml", Content: "database: payments\ntype: mysql"},
		{Path: "schema/payments/transactions.sql", Name: "transactions.sql", Content: "CREATE TABLE transactions (...);"},
		{Path: "schema/payments/refunds.sql", Name: "refunds.sql", Content: "CREATE TABLE refunds (...);"},
		{Path: "schema/payments_audit/audit_log.sql", Name: "audit_log.sql", Content: "CREATE TABLE audit_log (...);"},
	}

	result, err := groupFilesByNamespace(files, "schema")
	require.NoError(t, err)

	require.Len(t, result, 2)
	assert.Contains(t, result, "payments")
	assert.Contains(t, result, "payments_audit")
	assert.Len(t, result["payments"].Files, 2)
	assert.Len(t, result["payments_audit"].Files, 1)
}

func TestGroupFilesByNamespace_SubdirLayout_Monorepo(t *testing.T) {
	// Deeply nested schema path in a monorepo — subdirectory names still work.
	files := []GitHubFile{
		{Path: "payments-service/mysql/schema/schemabot.yaml", Name: "schemabot.yaml", Content: "database: payments\ntype: mysql"},
		{Path: "payments-service/mysql/schema/payments/transactions.sql", Name: "transactions.sql", Content: "CREATE TABLE transactions (...);"},
		{Path: "payments-service/mysql/schema/payments_audit/audit_log.sql", Name: "audit_log.sql", Content: "CREATE TABLE audit_log (...);"},
	}

	result, err := groupFilesByNamespace(files, "payments-service/mysql/schema")
	require.NoError(t, err)

	require.Len(t, result, 2)
	assert.Contains(t, result, "payments")
	assert.Contains(t, result, "payments_audit")
}

func TestGroupFilesByNamespace_SubdirLayout_VitessKeyspaces(t *testing.T) {
	// Vitess layout with vschema.json inside keyspace subdirectories.
	files := []GitHubFile{
		{Path: "schema/schemabot.yaml", Name: "schemabot.yaml", Content: "database: commerce\ntype: vitess"},
		{Path: "schema/commerce/orders.sql", Name: "orders.sql", Content: "CREATE TABLE orders (...);"},
		{Path: "schema/commerce/vschema.json", Name: "vschema.json", Content: "{}"},
		{Path: "schema/customers/users.sql", Name: "users.sql", Content: "CREATE TABLE users (...);"},
	}

	result, err := groupFilesByNamespace(files, "schema")
	require.NoError(t, err)

	require.Len(t, result, 2)
	assert.Contains(t, result, "commerce")
	assert.Contains(t, result, "customers")
	assert.Len(t, result["commerce"].Files, 2)
	assert.Contains(t, result["commerce"].Files, "vschema.json")
}

// Mixed layout — rejected.

func TestGroupFilesByNamespace_MixedLayout_Rejected(t *testing.T) {
	files := []GitHubFile{
		{Path: "schema/schemabot.yaml", Name: "schemabot.yaml", Content: "database: myapp\ntype: mysql"},
		{Path: "schema/standalone.sql", Name: "standalone.sql", Content: "CREATE TABLE standalone (...);"},
		{Path: "schema/payments/transactions.sql", Name: "transactions.sql", Content: "CREATE TABLE transactions (...);"},
	}

	_, err := groupFilesByNamespace(files, "schema")

	require.Error(t, err)
	assert.Contains(t, err.Error(), "both flat files and namespace subdirectories")
}
