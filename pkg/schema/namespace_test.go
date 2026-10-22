package schema

import (
	"fmt"
	"maps"
	"slices"
	"strings"
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

// Environment placeholder substitution tests.

func TestGroupFilesByNamespace_BraceEnvSubstitution(t *testing.T) {
	files := map[string]string{
		"bikeshare_{env}/bikes.sql":    "CREATE TABLE bikes (...);",
		"bikeshare_{env}/stations.sql": "CREATE TABLE stations (...);",
	}
	for _, environment := range []string{"staging", "production"} {
		t.Run(environment, func(t *testing.T) {
			result, _, err := GroupFilesByNamespace(files, "ignored", environment, nil)
			require.NoError(t, err)
			require.Len(t, result, 1)
			assert.Len(t, result["bikeshare_"+environment].Files, 2)
		})
	}
}

func TestGroupFilesByNamespace_BraceEnvFlatLayout(t *testing.T) {
	result, _, err := GroupFilesByNamespace(map[string]string{"bikes.sql": "CREATE TABLE bikes (...);"}, "bikeshare_{env}", "production", nil)
	require.NoError(t, err)
	assert.Contains(t, result, "bikeshare_production")
}

func TestGroupFilesByNamespace_BraceEnvEmptyNamespace(t *testing.T) {
	result, _, err := GroupFilesByNamespace(map[string]string{"bikeshare_{env}/schema.sql": EmptyNamespaceDeclaration}, "ignored", "staging", nil)
	require.NoError(t, err)
	require.Contains(t, result, "bikeshare_staging")
	assert.Empty(t, result["bikeshare_staging"].Files)
}

func TestGroupFilesByNamespace_EnvAliasesCannotCollide(t *testing.T) {
	files := map[string]string{
		"bikeshare_$ENV/bikes.sql":  "CREATE TABLE bikes (id bigint);",
		"bikeshare_{env}/bikes.sql": "CREATE TABLE bikes (id int);",
	}
	_, _, err := GroupFilesByNamespace(files, "ignored", "staging", nil)
	require.ErrorContains(t, err, "both resolve to namespace")
	assert.ErrorContains(t, err, "bikeshare_staging")
}

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

func TestGroupFilesByNamespace_IgnoreNamespacesBraceEnv(t *testing.T) {
	files := map[string]string{
		"app_{env}/users.sql":        "CREATE TABLE users (...);",
		"fixtures_{env}/widgets.sql": "CREATE TABLE widgets (...);",
	}
	result, removed, err := GroupFilesByNamespace(files, "ignored", "staging", []string{"fixtures_{env}"})
	require.NoError(t, err)
	assert.Contains(t, result, "app_staging")
	assert.NotContains(t, result, "fixtures_staging")
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
	unmatched, err := UnmatchedIgnoreEntries([]string{"nonexistent"}, "development", removed)
	require.NoError(t, err)
	assert.Equal(t, []string{"nonexistent"}, unmatched)
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
	unmatched, err := UnmatchedIgnoreEntries([]string{"Payments"}, "development", removed)
	require.NoError(t, err)
	assert.Equal(t, []string{"Payments"}, unmatched)
}

func TestUnmatchedIgnoreEntries(t *testing.T) {
	unmatched, err := UnmatchedIgnoreEntries(nil, "staging", nil)
	require.NoError(t, err)
	assert.Nil(t, unmatched)

	// Entries are resolved with $ENV before comparison against the removed
	// keys, so a matched $ENV entry is not reported.
	unmatched, err = UnmatchedIgnoreEntries([]string{"fixtures_$ENV"}, "staging", []string{"fixtures_staging"})
	require.NoError(t, err)
	assert.Nil(t, unmatched)
	unmatched, err = UnmatchedIgnoreEntries([]string{"fixtures_$ENV", "typo"}, "staging", []string{"fixtures_staging"})
	require.NoError(t, err)
	assert.Equal(t, []string{"typo"}, unmatched)

	// A pattern is matched when it withheld any namespace, and is reported as
	// written when it withheld none, as one written for another environment
	// does.
	unmatched, err = UnmatchedIgnoreEntries([]string{"/^shop_[0-9]+_staging$/", "/^shop_[0-9]+_production$/"}, "staging", []string{"shop_2_staging", "shop_3_staging"})
	require.NoError(t, err)
	assert.Equal(t, []string{"/^shop_[0-9]+_production$/"}, unmatched)

	_, err = UnmatchedIgnoreEntries([]string{"/shop_(/"}, "staging", nil)
	require.ErrorContains(t, err, `ignore_namespaces entry "/shop_(/" is not a valid regular expression`)
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
	assert.Contains(t, err.Error(), "wrap a regular expression in slashes")

	assert.NoError(t, ValidateIgnoreNamespaces([]string{"local_fixtures", "/^shop_[0-9]+_production$/"}))

	err = ValidateIgnoreNamespaces([]string{`schema\local_fixtures`})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "not a path")
}

func TestCompileIgnoreNamespaces_PlainEntriesResolveTokens(t *testing.T) {
	tests := []struct {
		name        string
		entries     []string
		environment string
		ignored     []string
		kept        []string
	}{
		{name: "legacy token", entries: []string{"fixtures_$ENV"}, environment: "staging", ignored: []string{"fixtures_staging"}, kept: []string{"fixtures_$ENV", "fixtures_production"}},
		{name: "brace token", entries: []string{"fixtures_{env}"}, environment: "staging", ignored: []string{"fixtures_staging"}, kept: []string{"fixtures_{env}"}},
		{name: "no environment keeps the token", entries: []string{"fixtures_{env}"}, ignored: []string{"fixtures_{env}"}, kept: []string{"fixtures_staging"}},
		{name: "literal", entries: []string{"local_fixtures"}, environment: "staging", ignored: []string{"local_fixtures"}, kept: []string{"Local_fixtures", "local_fixtures_2"}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ignore, err := CompileIgnoreNamespaces(tt.entries, tt.environment)
			require.NoError(t, err)
			for _, ns := range tt.ignored {
				assert.True(t, ignore.Ignores(ns), "should ignore %s", ns)
			}
			for _, ns := range tt.kept {
				assert.False(t, ignore.Ignores(ns), "should keep %s", ns)
			}
		})
	}
}

// TestCompileIgnoreNamespaces_Patterns pins how a pattern entry matches: the
// whole name, case-sensitively, so a pattern written for one family of shard
// schemas cannot reach a namespace that merely contains a match.
func TestCompileIgnoreNamespaces_Patterns(t *testing.T) {
	tests := []struct {
		name    string
		entry   string
		ignored []string
		kept    []string
	}{
		{
			name:    "unanchored pattern matches whole names only",
			entry:   "/shop_[0-9]+_production/",
			ignored: []string{"shop_1_production", "shop_32_production"},
			kept:    []string{"shop_production", "shop_1_staging", "old_shop_1_production", "shop_1_production_backup"},
		},
		{
			name:    "numeric range",
			entry:   "/^shop_([4-9]|[12][0-9]|3[01])_staging$/",
			ignored: []string{"shop_4_staging", "shop_19_staging", "shop_31_staging"},
			kept:    []string{"shop_staging", "shop_0_staging", "shop_3_staging", "shop_32_staging"},
		},
		{
			name:    "alternation stays inside the anchors",
			entry:   "/shop_1_staging|shop_2_staging/",
			ignored: []string{"shop_1_staging", "shop_2_staging"},
			kept:    []string{"shop_1_staging_x", "x_shop_2_staging"},
		},
		{
			name:    "case-sensitive unless asked",
			entry:   "/shop_[0-9]+_staging/",
			ignored: []string{"shop_1_staging"},
			kept:    []string{"Shop_1_Staging"},
		},
		{
			name:    "case folding on request",
			entry:   "/(?i)shop_[0-9]+_staging/",
			ignored: []string{"shop_1_staging", "Shop_1_Staging"},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ignore, err := CompileIgnoreNamespaces([]string{tt.entry}, "staging")
			require.NoError(t, err)
			for _, ns := range tt.ignored {
				assert.True(t, ignore.Ignores(ns), "should ignore %s", ns)
			}
			for _, ns := range tt.kept {
				assert.False(t, ignore.Ignores(ns), "should keep %s", ns)
			}
		})
	}
}

func TestCompileIgnoreNamespacePattern_Refusals(t *testing.T) {
	tests := []struct {
		name  string
		entry string
		want  string
	}{
		{name: "empty pattern", entry: "//", want: `ignore_namespaces entry "//" is an empty pattern`},
		{name: "invalid expression", entry: "/shop_(/", want: `ignore_namespaces entry "/shop_(/" is not a valid regular expression`},
		{name: "expression that only parses once wrapped", entry: "/a)|(b/", want: `ignore_namespaces entry "/a)|(b/" is not a valid regular expression`},
		{name: "brace token", entry: "/shop_[0-9]+_{env}/", want: `ignore_namespaces entry "/shop_[0-9]+_{env}/" is a pattern and cannot use {env} or $ENV`},
		{name: "legacy token", entry: "/shop_[0-9]+_$ENV/", want: `ignore_namespaces entry "/shop_[0-9]+_$ENV/" is a pattern and cannot use {env} or $ENV`},
		{name: "path", entry: "/schema/shop_1/", want: `ignore_namespaces entry "/schema/shop_1/" must be a namespace name or a pattern, not a path`},
		{name: "too long", entry: "/" + strings.Repeat("a", maxIgnoreNamespacePatternBytes) + "/", want: "is longer than 256 bytes"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := CompileIgnoreNamespacePattern(tt.entry)
			require.ErrorContains(t, err, tt.want)
			require.ErrorContains(t, ValidateIgnoreNamespaces([]string{tt.entry}), tt.want, "validation refuses what a plan would")
		})
	}
}

func TestValidateIgnoreNamespaces_PatternBudget(t *testing.T) {
	pattern := func(i int) string { return fmt.Sprintf("/^shop_%03d_%s$/", i, strings.Repeat("x", 180)) }
	within := []string{pattern(1), pattern(2), pattern(3), pattern(3), pattern(4), pattern(5)}
	require.NoError(t, ValidateIgnoreNamespaces(within), "a repeated pattern counts once")

	err := ValidateIgnoreNamespaces(append(within, pattern(6)))
	require.ErrorContains(t, err, "ignore_namespaces pattern entries total")
	require.ErrorContains(t, err, "over the 1024-byte limit")
}

// TestGroupFilesByNamespace_IgnoreNamespacesPattern models a sharded database
// whose schema root holds one directory per shard schema, all linked to one
// definition, while staging runs fewer shards than production. One pattern
// per environment withholds the shards that environment does not run, and the
// removed list names every namespace a pattern withheld so the plan can
// disclose each one.
func TestGroupFilesByNamespace_IgnoreNamespacesPattern(t *testing.T) {
	files := map[string]string{}
	for _, dir := range []string{"shop_{env}", "shop_0_{env}", "shop_1_{env}", "shop_2_{env}", "shop_3_{env}"} {
		files[dir+"/orders.sql"] = "CREATE TABLE orders (id bigint);"
	}
	entries := []string{"/^shop_[23]_staging$/"}

	staging, removed, err := GroupFilesByNamespace(files, "shop", "staging", entries)
	require.NoError(t, err)
	assert.Equal(t, []string{"shop_2_staging", "shop_3_staging"}, removed)
	assert.ElementsMatch(t, []string{"shop_staging", "shop_0_staging", "shop_1_staging"}, slices.Collect(maps.Keys(staging)))
	unmatched, err := UnmatchedIgnoreEntries(entries, "staging", removed)
	require.NoError(t, err)
	assert.Empty(t, unmatched)

	production, removed, err := GroupFilesByNamespace(files, "shop", "production", entries)
	require.NoError(t, err)
	assert.Empty(t, removed)
	assert.Len(t, production, 5)
	unmatched, err = UnmatchedIgnoreEntries(entries, "production", removed)
	require.NoError(t, err)
	assert.Equal(t, []string{"/^shop_[23]_staging$/"}, unmatched)
}

func TestGroupFilesByNamespace_InvalidIgnorePattern(t *testing.T) {
	_, _, err := GroupFilesByNamespace(map[string]string{"shop/orders.sql": "CREATE TABLE orders (id bigint);"}, "shop", "staging", []string{"/shop_(/"})
	require.ErrorContains(t, err, `ignore_namespaces entry "/shop_(/" is not a valid regular expression`)
}

func TestGroupFilesByNamespaceEmptyDeclaration(t *testing.T) {
	grouped, _, err := GroupFilesByNamespace(map[string]string{"public/schema.sql": EmptyNamespaceDeclaration}, "app", "development", nil)
	require.NoError(t, err)
	require.Contains(t, grouped, "public")
	require.Empty(t, grouped["public"].Files)
	// User SQL after the marker must never be skipped.
	edited := EmptyNamespaceDeclaration + "CREATE TABLE users (id bigint);"
	grouped, _, err = GroupFilesByNamespace(map[string]string{"public/schema.sql": edited}, "app", "development", nil)
	require.NoError(t, err)
	require.Equal(t, edited, grouped["public"].Files["schema.sql"])
}

func TestEmptyNamespaceDeclarationSurvivesEditorWhitespace(t *testing.T) {
	for _, content := range []string{strings.TrimSpace(EmptyNamespaceDeclaration), strings.ReplaceAll(EmptyNamespaceDeclaration, "\n", "\r\n"), EmptyNamespaceDeclaration + " \t\n"} {
		grouped, _, err := GroupFilesByNamespace(map[string]string{"public/schema.sql": content}, "app", "development", nil)
		require.NoError(t, err)
		require.Contains(t, grouped, "public")
		require.Empty(t, grouped["public"].Files)
	}
	content := EmptyNamespaceDeclaration + "\r\nCREATE TABLE users (id bigint);"
	grouped, _, err := GroupFilesByNamespace(map[string]string{"public/schema.sql": content}, "app", "development", nil)
	require.NoError(t, err)
	require.Equal(t, content, grouped["public"].Files["schema.sql"])
}

func TestEmptyNamespaceDeclarationPreservesIntent(t *testing.T) {
	for _, content := range []string{EmptyNamespaceDeclaration, "  \n" + EmptyNamespaceDeclaration + "\n "} {
		got, _, err := GroupFilesByNamespace(map[string]string{"public/schema.sql": content}, "app", "", nil)
		require.NoError(t, err)
		require.Contains(t, got, "public")
		require.NotNil(t, got["public"].Files)
		require.Empty(t, got["public"].Files)
	}
	for _, content := range []string{"-- a user comment", EmptyNamespaceDeclaration + "\n-- another comment", EmptyNamespaceDeclaration + "\nCREATE TABLE orders (id bigint);"} {
		got, _, err := GroupFilesByNamespace(map[string]string{"public/schema.sql": content}, "app", "", nil)
		require.NoError(t, err)
		require.Equal(t, content, got["public"].Files["schema.sql"])
	}
}
