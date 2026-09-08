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

	result, _, err := GroupFilesByNamespace(files, "aurora_coffeeshop_exemplar", "development", nil)
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

	result, _, err := GroupFilesByNamespace(files, "myapp", "development", nil)
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

	result, _, err := GroupFilesByNamespace(files, "commerce", "development", nil)
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

	result, _, err := GroupFilesByNamespace(files, "ignored_because_subdirs_exist", "development", nil)
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

	result, _, err := GroupFilesByNamespace(files, "ignored", "development", nil)
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

	_, _, err := GroupFilesByNamespace(files, "mydb", "development", nil)
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

	result, _, err := GroupFilesByNamespace(files, "schema", "development", nil)
	require.NoError(t, err)
	require.Len(t, result, 2)
	assert.Contains(t, result, "schema")
	assert.Contains(t, result, "other")
}

// Edge cases.

func TestGroupFilesByNamespace_EmptyInput(t *testing.T) {
	result, _, err := GroupFilesByNamespace(map[string]string{}, "mydb", "development", nil)
	require.NoError(t, err)
	assert.Empty(t, result)
}

func TestGroupFilesByNamespace_OnlyNonSchemaFiles(t *testing.T) {
	// All files are skipped — returns empty result (no error).
	files := map[string]string{
		"schemabot.yaml": "database: myapp\ntype: mysql",
		"README.md":      "# docs",
	}

	result, _, err := GroupFilesByNamespace(files, "myapp", "development", nil)
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

	result, _, err := GroupFilesByNamespace(files, "ignored", "staging", nil)
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

	result, _, err := GroupFilesByNamespace(files, "bikeshare_$ENV", "production", nil)
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

	result, _, err := GroupFilesByNamespace(files, "ignored", "", nil)
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

	result, _, err := GroupFilesByNamespace(files, "ignored", "staging", nil)
	require.NoError(t, err)

	require.Len(t, result, 2)
	assert.Contains(t, result, "app_staging")
	assert.Contains(t, result, "analytics")
}

func TestGroupFilesByNamespace_IgnoreNamespaces(t *testing.T) {
	// A namespace directory listed in ignore_namespaces is excluded from the
	// result — its files never reach plans, applies, or checks — while the
	// other namespaces are grouped normally.
	files := map[string]string{
		"payments/users.sql":         "CREATE TABLE users (...);",
		"payments/vschema.json":      `{"tables": {}}`,
		"local_fixtures/widgets.sql": "CREATE TABLE widgets (...);",
	}

	result, removed, err := GroupFilesByNamespace(files, "ignored", "development", []string{"local_fixtures"})
	require.NoError(t, err)

	require.Len(t, result, 1)
	require.Contains(t, result, "payments")
	assert.Contains(t, result["payments"].Files, "users.sql")
	assert.Contains(t, result["payments"].Files, "vschema.json")
	assert.NotContains(t, result, "local_fixtures")
	assert.Equal(t, []string{"local_fixtures"}, removed)
}

func TestGroupFilesByNamespace_IgnoreNamespacesEnvSubstitution(t *testing.T) {
	// Ignore entries receive the same $ENV substitution as directory names,
	// so "fixtures_$ENV" excludes the "fixtures_staging" namespace when
	// planning for staging.
	files := map[string]string{
		"app_$ENV/users.sql":        "CREATE TABLE users (...);",
		"fixtures_$ENV/widgets.sql": "CREATE TABLE widgets (...);",
	}

	result, removed, err := GroupFilesByNamespace(files, "ignored", "staging", []string{"fixtures_$ENV"})
	require.NoError(t, err)

	require.Len(t, result, 1)
	assert.Contains(t, result, "app_staging")
	assert.Equal(t, []string{"fixtures_staging"}, removed)
}

func TestGroupFilesByNamespace_IgnoreNamespacesNoMatch(t *testing.T) {
	// An ignore entry that matches no namespace directory removes nothing; the
	// removed list stays empty and the entry surfaces via
	// UnmatchedIgnoreEntries so callers can warn instead of implying an
	// exclusion that is not happening.
	files := map[string]string{
		"payments/users.sql": "CREATE TABLE users (...);",
	}

	result, removed, err := GroupFilesByNamespace(files, "ignored", "development", []string{"nonexistent"})
	require.NoError(t, err)

	require.Len(t, result, 1)
	assert.Contains(t, result, "payments")
	assert.Empty(t, removed)
	assert.Equal(t, []string{"nonexistent"}, UnmatchedIgnoreEntries([]string{"nonexistent"}, "development", removed))
}

func TestGroupFilesByNamespace_IgnoreNamespacesCaseSensitive(t *testing.T) {
	// Matching is exact and case-sensitive: an entry that differs from the
	// directory name only by case removes nothing, and is reported unmatched
	// so the mismatch is visible rather than silently reconciling the
	// namespace it was meant to exclude.
	files := map[string]string{
		"payments/users.sql": "CREATE TABLE users (...);",
	}

	result, removed, err := GroupFilesByNamespace(files, "ignored", "development", []string{"Payments"})
	require.NoError(t, err)

	require.Len(t, result, 1)
	assert.Contains(t, result, "payments")
	assert.Empty(t, removed)
	assert.Equal(t, []string{"Payments"}, UnmatchedIgnoreEntries([]string{"Payments"}, "development", removed))
}

func TestUnmatchedIgnoreEntries(t *testing.T) {
	assert.Nil(t, UnmatchedIgnoreEntries(nil, "staging", nil))

	// Entries are resolved with $ENV before comparison against the removed
	// keys, so a matched $ENV entry is not reported.
	assert.Nil(t, UnmatchedIgnoreEntries(
		[]string{"fixtures_$ENV"}, "staging", []string{"fixtures_staging"}))
	assert.Equal(t, []string{"typo"}, UnmatchedIgnoreEntries(
		[]string{"fixtures_$ENV", "typo"}, "staging", []string{"fixtures_staging"}))
}

func TestValidateIgnoreNamespaces(t *testing.T) {
	assert.NoError(t, ValidateIgnoreNamespaces(nil))
	assert.NoError(t, ValidateIgnoreNamespaces([]string{"local_fixtures", "fixtures_$ENV"}))

	err := ValidateIgnoreNamespaces([]string{""})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "must not be blank")

	err = ValidateIgnoreNamespaces([]string{"  "})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "must not be blank")

	err = ValidateIgnoreNamespaces([]string{"local_fixtures "})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "whitespace")

	err = ValidateIgnoreNamespaces([]string{" local_fixtures"})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "whitespace")

	err = ValidateIgnoreNamespaces([]string{"schema/local_fixtures"})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "not a path")

	err = ValidateIgnoreNamespaces([]string{`schema\local_fixtures`})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "not a path")
}

func TestResolveIgnoreNamespaces(t *testing.T) {
	assert.Nil(t, ResolveIgnoreNamespaces(nil, "staging"))
	assert.Equal(t, []string{"fixtures_staging", "local_fixtures"},
		ResolveIgnoreNamespaces([]string{"fixtures_$ENV", "local_fixtures"}, "staging"))
	assert.Equal(t, []string{"fixtures_$ENV"},
		ResolveIgnoreNamespaces([]string{"fixtures_$ENV"}, ""))
}
