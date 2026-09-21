package commands

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/block/schemabot/pkg/apitypes"
	"github.com/block/schemabot/pkg/cmd/client"
	"github.com/block/schemabot/pkg/repoconfig"
	"github.com/block/schemabot/pkg/schema"
)

func TestBuildOnboardWritePlanWritesConfigAndNamespaceFiles(t *testing.T) {
	root := t.TempDir()
	plan, err := buildOnboardWritePlan(root, &apitypes.PullSchemaResponse{
		Database:    "orders",
		Type:        "mysql",
		Environment: "production",
		TableCount:  2,
		Namespaces: map[string]*apitypes.PulledNamespace{
			"orders": {
				Tables: map[string]string{
					"users":  "CREATE TABLE `users` (`id` bigint NOT NULL);\n",
					"orders": "CREATE TABLE `orders` (`id` bigint NOT NULL);\n",
				},
			},
		},
	}, client.PlanExclusions{})
	require.NoError(t, err)
	require.NoError(t, plan.checkConflicts(false))
	require.NoError(t, plan.write())

	config, err := os.ReadFile(filepath.Join(root, "schemabot.yaml"))
	require.NoError(t, err)
	assert.Equal(t, "database: orders\ntype: mysql\n", string(config))

	users, err := os.ReadFile(filepath.Join(root, "orders", "users.sql"))
	require.NoError(t, err)
	assert.Equal(t, "CREATE TABLE `users` (`id` bigint NOT NULL);\n", string(users))

	orders, err := os.ReadFile(filepath.Join(root, "orders", "orders.sql"))
	require.NoError(t, err)
	assert.Equal(t, "CREATE TABLE `orders` (`id` bigint NOT NULL);\n", string(orders))
}

func TestBuildOnboardWritePlanWritesVitessKeyspaceArtifacts(t *testing.T) {
	root := t.TempDir()
	plan, err := buildOnboardWritePlan(root, &apitypes.PullSchemaResponse{
		Database:    "commerce",
		Type:        "vitess",
		Environment: "production",
		TableCount:  1,
		Namespaces: map[string]*apitypes.PulledNamespace{
			"commerce_sharded": {
				Tables: map[string]string{
					"users": "CREATE TABLE `users` (`id` bigint NOT NULL);\n",
				},
				Artifacts: map[string]string{
					"vschema.json": "{\"sharded\":true}",
				},
			},
		},
	}, client.PlanExclusions{})
	require.NoError(t, err)
	require.NoError(t, plan.checkConflicts(false))
	require.NoError(t, plan.write())

	config, err := os.ReadFile(filepath.Join(root, "schemabot.yaml"))
	require.NoError(t, err)
	assert.Equal(t, "database: commerce\ntype: vitess\n", string(config))

	users, err := os.ReadFile(filepath.Join(root, "commerce_sharded", "users.sql"))
	require.NoError(t, err)
	assert.Equal(t, "CREATE TABLE `users` (`id` bigint NOT NULL);\n", string(users))

	vschema, err := os.ReadFile(filepath.Join(root, "commerce_sharded", "vschema.json"))
	require.NoError(t, err)
	assert.JSONEq(t, "{\"sharded\":true}", string(vschema))
}

// Re-onboarding over an existing schema root must not drop the exclusions an
// operator configured: the rewritten schemabot.yaml carries both lists forward,
// and the plan verification excludes the same namespaces and withholds the same
// tables a real plan would.
func TestBuildOnboardWritePlanPreservesExclusions(t *testing.T) {
	root := t.TempDir()
	plan, err := buildOnboardWritePlan(root, &apitypes.PullSchemaResponse{
		Database:    "orders",
		Type:        "mysql",
		Environment: "production",
		TableCount:  1,
		Namespaces: map[string]*apitypes.PulledNamespace{
			"orders": {Tables: map[string]string{"users": "CREATE TABLE `users` (`id` bigint NOT NULL);\n"}},
		},
	}, client.PlanExclusions{
		Namespaces: []string{"local_fixtures", "fixtures_$ENV"},
		Tables:     []string{"flyway_schema_history"},
	})
	require.NoError(t, err)
	require.NoError(t, plan.write())

	config, err := os.ReadFile(filepath.Join(root, "schemabot.yaml"))
	require.NoError(t, err)
	assert.Equal(t, "database: orders\ntype: mysql\nignore_namespaces:\n  - local_fixtures\n  - fixtures_$ENV\nignore_tables:\n  - flyway_schema_history\n", string(config))
	assert.Equal(t, []string{"local_fixtures", "fixtures_$ENV"}, plan.exclusions.Namespaces)
	assert.Equal(t, []string{"flyway_schema_history"}, plan.exclusions.Tables)
}

// A pull returns the target's whole catalog, the tables ignore_tables withholds
// included, so re-onboarding an already-configured repository is where a table
// can end up both withheld from the planner and declared to it — the
// contradiction every engine refuses, written into the repo by the rewrite
// itself, after the files are already on disk.
func TestBuildOnboardWritePlanDoesNotDeclareAWithheldTable(t *testing.T) {
	root := t.TempDir()
	plan, err := buildOnboardWritePlan(root, &apitypes.PullSchemaResponse{
		Database:    "orders",
		Type:        "mysql",
		Environment: "production",
		TableCount:  2,
		Namespaces: map[string]*apitypes.PulledNamespace{
			"orders": {Tables: map[string]string{
				"users":                 "CREATE TABLE `users` (`id` bigint NOT NULL);\n",
				"flyway_schema_history": "CREATE TABLE `flyway_schema_history` (`installed_rank` int NOT NULL);\n",
			}},
		},
	}, client.PlanExclusions{Tables: []string{"flyway_schema_history"}})
	require.NoError(t, err)
	require.NoError(t, plan.write())

	assert.FileExists(t, filepath.Join(root, "orders", "users.sql"))
	assert.NoFileExists(t, filepath.Join(root, "orders", "flyway_schema_history.sql"),
		"the withheld table is not declared back into the repository it is withheld from")
	assert.NoFileExists(t, filepath.Join(root, "orders", "schema.sql"),
		"the namespace still declares a table, so it needs no empty-scope declaration")
}

// Withholding is exact so an entry never withholds a table it does not name,
// but the engines refuse a declared-and-ignored table with case folded. An
// entry that differs from the live table only in case therefore survives the
// filter, and onboard would write the very file every later plan refuses.
// Onboard refuses first, before anything is on disk, so the operator is never
// left holding a repository no plan accepts.
func TestBuildOnboardWritePlanRefusesACaseDifferingEntry(t *testing.T) {
	for _, tc := range []struct {
		name  string
		entry string
		live  string
	}{
		{"entry folds down to the live table", "databasechangelog", "DATABASECHANGELOG"},
		{"entry folds up to the live table", "Flyway_Schema_History", "flyway_schema_history"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			root := t.TempDir()
			_, err := buildOnboardWritePlan(root, &apitypes.PullSchemaResponse{
				Database:    "orders",
				Type:        "mysql",
				Environment: "production",
				TableCount:  2,
				Namespaces: map[string]*apitypes.PulledNamespace{
					"orders": {Tables: map[string]string{
						"users": "CREATE TABLE `users` (`id` bigint NOT NULL);\n",
						tc.live: "CREATE TABLE `" + tc.live + "` (`id` bigint NOT NULL);\n",
					}},
				},
			}, client.PlanExclusions{Tables: []string{tc.entry}})

			require.Error(t, err)
			assert.Contains(t, err.Error(), tc.entry)
			assert.Contains(t, err.Error(), "is also declared by a schema file in namespace \"orders\"")
			assert.NoDirExists(t, filepath.Join(root, "orders"),
				"the refusal lands before anything reaches disk")
		})
	}
}

// A namespace whose every table is withheld still belongs to the plan, so it
// keeps its comment-only declaration rather than vanishing from the repository.
func TestBuildOnboardWritePlanKeepsAFullyWithheldNamespaceExplicit(t *testing.T) {
	root := t.TempDir()
	plan, err := buildOnboardWritePlan(root, &apitypes.PullSchemaResponse{
		Database:    "orders",
		Type:        "mysql",
		Environment: "production",
		TableCount:  1,
		Namespaces: map[string]*apitypes.PulledNamespace{
			"bookkeeping": {Tables: map[string]string{
				"flyway_schema_history": "CREATE TABLE `flyway_schema_history` (`installed_rank` int NOT NULL);\n",
			}},
		},
	}, client.PlanExclusions{Tables: []string{"flyway_schema_history"}})
	require.NoError(t, err)
	require.NoError(t, plan.write())

	assert.NoFileExists(t, filepath.Join(root, "bookkeeping", "flyway_schema_history.sql"))
	declaration, err := os.ReadFile(filepath.Join(root, "bookkeeping", "schema.sql"))
	require.NoError(t, err)
	assert.Equal(t, schema.EmptyNamespaceDeclaration, string(declaration))
}

// A config with no exclusions emits neither key: an empty list would read as a
// configured exclusion of nothing.
func TestBuildOnboardWritePlanOmitsEmptyExclusions(t *testing.T) {
	root := t.TempDir()
	plan, err := buildOnboardWritePlan(root, &apitypes.PullSchemaResponse{
		Database:    "orders",
		Type:        "mysql",
		Environment: "production",
		TableCount:  1,
		Namespaces: map[string]*apitypes.PulledNamespace{
			"orders": {Tables: map[string]string{"users": "CREATE TABLE `users` (`id` bigint NOT NULL);\n"}},
		},
	}, client.PlanExclusions{})
	require.NoError(t, err)
	require.NoError(t, plan.write())

	config, err := os.ReadFile(filepath.Join(root, "schemabot.yaml"))
	require.NoError(t, err)
	assert.Equal(t, "database: orders\ntype: mysql\n", string(config))
}

func TestPreservedExclusions(t *testing.T) {
	t.Run("missing config is a fresh onboarding", func(t *testing.T) {
		exclusions, err := preservedExclusions(t.TempDir())
		require.NoError(t, err)
		assert.Equal(t, client.PlanExclusions{}, exclusions)
	})

	t.Run("existing config's entries are preserved", func(t *testing.T) {
		root := t.TempDir()
		require.NoError(t, os.WriteFile(filepath.Join(root, "schemabot.yaml"),
			[]byte("database: orders\ntype: mysql\nignore_namespaces:\n  - local_fixtures\nignore_tables:\n  - flyway_schema_history\n"), 0o644))

		exclusions, err := preservedExclusions(root)
		require.NoError(t, err)
		assert.Equal(t, []string{"local_fixtures"}, exclusions.Namespaces)
		assert.Equal(t, []string{"flyway_schema_history"}, exclusions.Tables)
	})

	t.Run("unreadable config is an error, not a silent drop", func(t *testing.T) {
		root := t.TempDir()
		require.NoError(t, os.WriteFile(filepath.Join(root, "schemabot.yaml"),
			[]byte(": not yaml"), 0o644))

		_, err := preservedExclusions(root)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "preserve ignore_namespaces and ignore_tables")
	})
}

func TestResolveOnboardLegacyBaseline(t *testing.T) {
	commit := "0123456789abcdef0123456789abcdef01234567"

	t.Run("fresh onboarding without legacy verification", func(t *testing.T) {
		baseline, err := resolveOnboardLegacyBaseline(t.TempDir(), "", nil)
		require.NoError(t, err)
		assert.Nil(t, baseline)
	})

	t.Run("refresh without metadata needs no backfill", func(t *testing.T) {
		root := t.TempDir()
		require.NoError(t, os.WriteFile(filepath.Join(root, "schemabot.yaml"), []byte("database: orders\ntype: mysql\n"), 0o644))
		baseline, err := resolveOnboardLegacyBaseline(root, "", nil)
		require.NoError(t, err)
		assert.Nil(t, baseline)
	})

	t.Run("partial flags are rejected", func(t *testing.T) {
		_, err := resolveOnboardLegacyBaseline(t.TempDir(), commit, nil)
		require.ErrorContains(t, err, "must be supplied together")
		_, err = resolveOnboardLegacyBaseline(t.TempDir(), "", []string{"db/changes"})
		require.ErrorContains(t, err, "must be supplied together")
	})

	t.Run("invalid existing metadata is not silently removed", func(t *testing.T) {
		root := t.TempDir()
		require.NoError(t, os.WriteFile(filepath.Join(root, "schemabot.yaml"), []byte("database: orders\ntype: mysql\nlegacy_baseline: {}\n"), 0o644))
		_, err := resolveOnboardLegacyBaseline(root, "", nil)
		require.ErrorContains(t, err, "legacy_baseline.version")
	})

	t.Run("explicit anchor is validated", func(t *testing.T) {
		baseline, err := resolveOnboardLegacyBaseline(t.TempDir(), commit, []string{"service/db/changes"})
		require.NoError(t, err)
		assert.Equal(t, &repoconfig.LegacyBaseline{
			Version: 1, BaseCommit: commit, LegacyPaths: []string{"service/db/changes"},
		}, baseline)
	})

	t.Run("existing anchor is preserved", func(t *testing.T) {
		root := t.TempDir()
		require.NoError(t, os.WriteFile(filepath.Join(root, "schemabot.yaml"), []byte(`database: orders
type: mysql
legacy_baseline:
  version: 1
  base_commit: 0123456789abcdef0123456789abcdef01234567
  legacy_paths:
    - service/db/changes
`), 0o644))

		baseline, err := resolveOnboardLegacyBaseline(root, "", nil)
		require.NoError(t, err)
		assert.Equal(t, commit, baseline.BaseCommit)
		assert.Equal(t, []string{"service/db/changes"}, baseline.LegacyPaths)
	})
}

func TestBuildOnboardWritePlanWritesLegacyBaseline(t *testing.T) {
	root := t.TempDir()
	baseline := &repoconfig.LegacyBaseline{
		Version: 1, BaseCommit: "0123456789abcdef0123456789abcdef01234567",
		LegacyPaths: []string{"service/db/changes", "service/db/schema.rb"},
	}
	plan, err := buildOnboardWritePlanWithBaseline(root, validPullSchemaResponse(), client.PlanExclusions{}, baseline)
	require.NoError(t, err)
	require.NoError(t, plan.write())

	config, err := os.ReadFile(filepath.Join(root, "schemabot.yaml"))
	require.NoError(t, err)
	assert.Equal(t, `database: orders
type: mysql
legacy_baseline:
  version: 1
  base_commit: 0123456789abcdef0123456789abcdef01234567
  legacy_paths:
    - service/db/changes
    - service/db/schema.rb
`, string(config))
}

func TestOnboardPullNamespacesUseConcreteLiveNamespaces(t *testing.T) {
	pullNamespaces, err := onboardPullNamespaces([]string{"orders_production", "orders_audit_production"})
	require.NoError(t, err)

	assert.Equal(t, []string{"orders_production", "orders_audit_production"}, pullNamespaces)

	_, err = onboardPullNamespaces([]string{"orders_$ENV"})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "must be a concrete live namespace")
}

func TestRewriteOnboardNamespacesInfersEnvironmentTemplate(t *testing.T) {
	resp := &apitypes.PullSchemaResponse{
		Database:    "orders-logical",
		Type:        "mysql",
		Environment: "production",
		Namespaces: map[string]*apitypes.PulledNamespace{
			"orders_production":       {Tables: map[string]string{"users": "CREATE TABLE `users` (`id` bigint NOT NULL);\n"}},
			"orders_audit_production": {Tables: map[string]string{"events": "CREATE TABLE `events` (`id` bigint NOT NULL);\n"}},
		},
	}
	require.NoError(t, rewriteOnboardNamespaces(resp, "production", true))
	assert.Contains(t, resp.Namespaces, "orders_$ENV")
	assert.Contains(t, resp.Namespaces, "orders_audit_$ENV")
	assert.NotContains(t, resp.Namespaces, "orders_production")
}

func TestRewriteOnboardNamespacesKeepsConcreteNamesByDefault(t *testing.T) {
	resp := &apitypes.PullSchemaResponse{
		Database:    "orders-logical",
		Type:        "mysql",
		Environment: "production",
		Namespaces: map[string]*apitypes.PulledNamespace{
			"orders_production": {Tables: map[string]string{"users": "CREATE TABLE `users` (`id` bigint NOT NULL);\n"}},
		},
	}
	require.NoError(t, rewriteOnboardNamespaces(resp, "production", false))
	assert.Contains(t, resp.Namespaces, "orders_production")
	assert.NotContains(t, resp.Namespaces, "orders_$ENV")
}

func TestOnboardWritePlanRefusesExistingFilesWithoutForce(t *testing.T) {
	root := t.TempDir()
	require.NoError(t, os.WriteFile(filepath.Join(root, "schemabot.yaml"), []byte("database: old\ntype: mysql\n"), 0o644))
	plan, err := buildOnboardWritePlan(root, &apitypes.PullSchemaResponse{
		Database:    "orders",
		Type:        "mysql",
		Environment: "production",
		Namespaces: map[string]*apitypes.PulledNamespace{
			"orders": {Tables: map[string]string{"users": "CREATE TABLE `users` (`id` bigint NOT NULL);\n"}},
		},
	}, client.PlanExclusions{})
	require.NoError(t, err)

	err = plan.checkConflicts(false)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "refusing to overwrite existing files")
	assert.Contains(t, err.Error(), filepath.Join(root, "schemabot.yaml"))

	require.NoError(t, plan.checkConflicts(true))
}

func TestBuildOnboardWritePlanRejectsUnsafeResponsePaths(t *testing.T) {
	_, err := buildOnboardWritePlan(t.TempDir(), &apitypes.PullSchemaResponse{
		Database:    "orders",
		Type:        "mysql",
		Environment: "production",
		Namespaces: map[string]*apitypes.PulledNamespace{
			"orders": {Tables: map[string]string{"../users": "CREATE TABLE `users` (`id` bigint NOT NULL);\n"}},
		},
	}, client.PlanExclusions{})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "table")
}

func TestBuildOnboardWritePlanRejectsInvalidPullResponse(t *testing.T) {
	tests := []struct {
		name       string
		schemaRoot string
		resp       *apitypes.PullSchemaResponse
		want       string
	}{
		{
			name:       "empty schema root",
			schemaRoot: "",
			resp:       validPullSchemaResponse(),
			want:       "schema root is required",
		},
		{
			name:       "empty database",
			schemaRoot: t.TempDir(),
			resp: func() *apitypes.PullSchemaResponse {
				resp := validPullSchemaResponse()
				resp.Database = ""
				return resp
			}(),
			want: "database is empty",
		},
		{
			name:       "empty pull",
			schemaRoot: t.TempDir(),
			resp: &apitypes.PullSchemaResponse{
				Database:    "orders",
				Type:        "mysql",
				Environment: "production",
			},
			want: "returned no tables",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			plan, err := buildOnboardWritePlan(tt.schemaRoot, tt.resp, client.PlanExclusions{})
			require.Error(t, err)
			assert.Nil(t, plan)
			assert.Contains(t, err.Error(), tt.want)
		})
	}
}

func TestFileStatusForDryRunTreatsStatErrorsAsExisting(t *testing.T) {
	root := t.TempDir()
	existing := filepath.Join(root, "schemabot.yaml")
	require.NoError(t, os.WriteFile(existing, []byte("database: orders\ntype: mysql\n"), 0o644))

	exists, err := fileStatusForDryRun(existing)
	require.NoError(t, err)
	assert.True(t, exists)

	exists, err = fileStatusForDryRun(filepath.Join(root, "missing.sql"))
	require.NoError(t, err)
	assert.False(t, exists)

	exists, err = fileStatusForDryRun(filepath.Join(existing, "child.sql"))
	require.Error(t, err)
	assert.True(t, exists)
}

func TestValidateOnboardPlanResult(t *testing.T) {
	assert.NoError(t, validateOnboardPlanResult(&apitypes.PlanResponse{Environment: "production"}, "orders", "production"))
	assert.NoError(t, validateOnboardPlanResult(&apitypes.PlanResponse{
		Environment: "production",
		LintResults: []*apitypes.LintViolationResponse{{Severity: "error", Message: "existing lint violation"}},
	}, "orders", "production"))

	err := validateOnboardPlanResult(&apitypes.PlanResponse{
		Environment: "production",
		Changes: []*apitypes.SchemaChangeResponse{
			{
				Namespace: "orders",
				TableChanges: []*apitypes.TableChangeResponse{{
					TableName:  "users",
					ChangeType: "ALTER",
					DDL:        "ALTER TABLE `users`\n  ADD COLUMN `email` varchar(255)",
				}},
			},
		},
	}, "orders", "production")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "database orders environment production")
	assert.Contains(t, err.Error(), "still produce schema changes")
	assert.Contains(t, err.Error(), "orders/users (alter): ALTER TABLE `users` ADD COLUMN `email` varchar(255)")

	err = validateOnboardPlanResult(&apitypes.PlanResponse{Errors: []string{"syntax error"}}, "orders", "production")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "database orders environment production")
	assert.Contains(t, err.Error(), "plan returned errors")
	assert.Contains(t, err.Error(), "syntax error")

	err = validateOnboardPlanResult(nil, "orders", "production")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "database orders environment production")
	assert.Contains(t, err.Error(), "plan response is empty")
}

func TestDescribeOnboardPlanChangesIncludesVSchemaAndClampsDDL(t *testing.T) {
	longDDL := "ALTER TABLE `users` ADD COLUMN " + strings.Repeat("`c` varchar(255), ", 20)
	lines := describeOnboardPlanChanges(&apitypes.PlanResponse{
		Changes: []*apitypes.SchemaChangeResponse{
			{
				Namespace:    "orders",
				TableChanges: []*apitypes.TableChangeResponse{{TableName: "users", ChangeType: "ALTER", DDL: longDDL}},
				Metadata:     map[string]string{"vschema": "{\"sharded\": true}"},
			},
		},
	})
	require.Len(t, lines, 2)
	assert.True(t, strings.HasPrefix(lines[0], "orders/users (alter): ALTER TABLE `users` ADD COLUMN"), lines[0])
	assert.True(t, strings.HasSuffix(lines[0], "…"), lines[0])
	assert.LessOrEqual(t, len([]rune(lines[0])), onboardVerifyDDLPreviewLimit+len("orders/users (alter): ")+len("…"))
	assert.Equal(t, "orders: vschema change", lines[1])
}

// A leftover schema file for a table that no longer exists in the target is
// the classic cause of a failed onboarding verification: the pull rewrites
// every table in the namespace but never deletes strays, so the stale file
// resurfaces as a spurious create. strayFiles must name exactly those files —
// and ignore non-schema files — so the operator knows what to delete.
func TestOnboardWritePlanStrayFiles(t *testing.T) {
	root := t.TempDir()
	plan, err := buildOnboardWritePlan(root, &apitypes.PullSchemaResponse{
		Database:    "orders",
		Type:        "mysql",
		Environment: "production",
		Namespaces: map[string]*apitypes.PulledNamespace{
			"orders": {Tables: map[string]string{"users": "CREATE TABLE `users` (`id` bigint NOT NULL);\n"}},
		},
	}, client.PlanExclusions{})
	require.NoError(t, err)

	// Namespace directory absent (dry run before any write): nothing to scan.
	strays, withheldStrays, err := plan.strayFiles()
	require.NoError(t, err)
	assert.Empty(t, strays)
	assert.Empty(t, withheldStrays)

	require.NoError(t, plan.write())
	strays, withheldStrays, err = plan.strayFiles()
	require.NoError(t, err)
	assert.Empty(t, strays)
	assert.Empty(t, withheldStrays)

	require.NoError(t, os.WriteFile(filepath.Join(root, "orders", "legacy_bak.sql"), []byte("CREATE TABLE `legacy_bak` (`id` bigint NOT NULL);\n"), 0o644))
	require.NoError(t, os.WriteFile(filepath.Join(root, "orders", "vschema.json"), []byte("{}"), 0o644))
	require.NoError(t, os.WriteFile(filepath.Join(root, "orders", "README.md"), []byte("docs"), 0o644))

	// MySQL planning never reads vschema.json, so only the table file is stray.
	strays, withheldStrays, err = plan.strayFiles()
	require.NoError(t, err)
	assert.Equal(t, []string{
		filepath.Join(root, "orders", "legacy_bak.sql"),
	}, strays)
	assert.Empty(t, withheldStrays, "no entry withholds legacy_bak")
}

// Adding an ignore_tables entry to an already-onboarded repository leaves the
// table's old schema file behind: the rewrite stops writing it but never
// deletes it. That file is not the ordinary stray, whose table is absent from
// the target and whose remedy is to restore it. This table is present and
// deliberately unmanaged, so the file states the contradiction the engines
// refuse and deleting it is the only fix. Reporting it under the ordinary
// warning would send the operator to restore a table that never left.
func TestOnboardWritePlanSeparatesAWithheldTablesLeftoverFile(t *testing.T) {
	root := t.TempDir()
	pulled := &apitypes.PullSchemaResponse{
		Database:    "orders",
		Type:        "mysql",
		Environment: "production",
		Namespaces: map[string]*apitypes.PulledNamespace{
			"orders": {Tables: map[string]string{
				"users":                 "CREATE TABLE `users` (`id` bigint NOT NULL);\n",
				"flyway_schema_history": "CREATE TABLE `flyway_schema_history` (`installed_rank` int NOT NULL);\n",
			}},
		},
	}
	before, err := buildOnboardWritePlan(root, pulled, client.PlanExclusions{})
	require.NoError(t, err)
	require.NoError(t, before.write())
	require.FileExists(t, filepath.Join(root, "orders", "flyway_schema_history.sql"))

	after, err := buildOnboardWritePlan(root, pulled, client.PlanExclusions{Tables: []string{"flyway_schema_history"}})
	require.NoError(t, err)
	require.NoError(t, after.write())

	strays, withheldStrays, err := after.strayFiles()
	require.NoError(t, err)
	assert.Empty(t, strays, "the leftover file is not an absent table to restore")
	assert.Equal(t, []string{filepath.Join(root, "orders", "flyway_schema_history.sql")}, withheldStrays)
}

// Onboard prints the server's unfiltered table count, so a table missing from
// the written files has no stated reason unless onboard says what it withheld.
// It records the exemption the way a plan carries one, so the disclosure reads
// the same on both surfaces.
func TestBuildOnboardWritePlanRecordsWhatItWithheld(t *testing.T) {
	plan, err := buildOnboardWritePlan(t.TempDir(), &apitypes.PullSchemaResponse{
		Database:    "orders",
		Type:        "mysql",
		Environment: "production",
		TableCount:  2,
		Namespaces: map[string]*apitypes.PulledNamespace{
			"orders": {Tables: map[string]string{
				"users":                 "CREATE TABLE `users` (`id` bigint NOT NULL);\n",
				"flyway_schema_history": "CREATE TABLE `flyway_schema_history` (`installed_rank` int NOT NULL);\n",
			}},
		},
	}, client.PlanExclusions{Tables: []string{"flyway_schema_history", "legacy_audit_log"}})
	require.NoError(t, err)

	require.Len(t, plan.withheld, 1)
	assert.Equal(t, "orders", plan.withheld[0].Namespace)
	assert.Equal(t, []string{"flyway_schema_history"}, plan.withheld[0].Tables)
	assert.Equal(t, apitypes.ExemptReasonIgnoreTables, plan.withheld[0].Reason)
}

// For Vitess, vschema.json is a schema input: a leftover copy the pull did not
// write proposes a VSchema the target doesn't have, so it must be flagged
// alongside stray table files.
func TestOnboardWritePlanStrayFilesFlagsVSchemaForVitess(t *testing.T) {
	root := t.TempDir()
	plan, err := buildOnboardWritePlan(root, &apitypes.PullSchemaResponse{
		Database:    "orders",
		Type:        "vitess",
		Environment: "production",
		Namespaces: map[string]*apitypes.PulledNamespace{
			"orders": {Tables: map[string]string{"users": "CREATE TABLE `users` (`id` bigint NOT NULL);\n"}},
		},
	}, client.PlanExclusions{})
	require.NoError(t, err)
	require.NoError(t, plan.write())

	require.NoError(t, os.WriteFile(filepath.Join(root, "orders", "vschema.json"), []byte("{\"sharded\": true}"), 0o644))

	strays, withheldStrays, err := plan.strayFiles()
	require.NoError(t, err)
	assert.Equal(t, []string{filepath.Join(root, "orders", "vschema.json")}, strays)
	assert.Empty(t, withheldStrays)
}

func validPullSchemaResponse() *apitypes.PullSchemaResponse {
	return &apitypes.PullSchemaResponse{
		Database:    "orders",
		Type:        "mysql",
		Environment: "production",
		Namespaces: map[string]*apitypes.PulledNamespace{
			"orders": {Tables: map[string]string{"users": "CREATE TABLE `users` (`id` bigint NOT NULL);\n"}},
		},
	}
}

// PostgreSQL pull output includes quoted identifiers and index statements in the
// table file. Import it verbatim so verification plans the same desired schema.
func TestBuildOnboardWritePlanPostgres(t *testing.T) {
	root := t.TempDir()
	ddl := "CREATE TABLE \"Order\" (\n  \"id\" bigint NOT NULL PRIMARY KEY,\n  \"name\" text NOT NULL\n);\nCREATE INDEX \"idx_name\" ON \"Order\" (\"name\");\n"
	plan, err := buildOnboardWritePlan(root, &apitypes.PullSchemaResponse{
		Database: "app", Type: "postgres", Environment: "development", TableCount: 1,
		Namespaces: map[string]*apitypes.PulledNamespace{
			"public": {Tables: map[string]string{"Order": ddl}},
		},
	}, client.PlanExclusions{})
	require.NoError(t, err)
	require.NoError(t, plan.write())
	config, err := os.ReadFile(filepath.Join(root, "schemabot.yaml"))
	require.NoError(t, err)
	assert.Equal(t, "database: app\ntype: postgres\n", string(config))
	contents, err := os.ReadFile(filepath.Join(root, "public", "Order.sql"))
	require.NoError(t, err)
	assert.Equal(t, ddl, string(contents))
	assert.Equal(t, "postgres", plan.databaseType)
}

func TestBuildOnboardWritePlanEmptyNamespace(t *testing.T) {
	for _, engine := range []string{"mysql", "postgres"} {
		t.Run(engine, func(t *testing.T) {
			plan, err := buildOnboardWritePlan(t.TempDir(), &apitypes.PullSchemaResponse{Database: "app", Type: engine, Namespaces: map[string]*apitypes.PulledNamespace{"app": {Tables: map[string]string{}}}}, client.PlanExclusions{})
			require.NoError(t, err)
			require.NoError(t, plan.write())
			require.Equal(t, "-- This namespace is empty. Add CREATE TABLE declarations here.\n", plan.files[filepath.Join("app", "schema.sql")])
		})
	}
}

func TestOnboardRetainsEmptyNamespacesAndRejectsCaseCollisions(t *testing.T) {
	response := &apitypes.PullSchemaResponse{Database: "app", Type: "postgres", Environment: "development", Namespaces: map[string]*apitypes.PulledNamespace{
		"public": {Tables: map[string]string{}}, "billing": {Tables: map[string]string{"orders": "CREATE TABLE orders (id bigint);"}},
	}}
	root := t.TempDir()
	plan, err := buildOnboardWritePlan(root, response, client.PlanExclusions{})
	require.NoError(t, err)
	require.Contains(t, plan.files, filepath.Join("public", "schema.sql"))
	response.Namespaces["billing"].Tables["Orders"] = "CREATE TABLE \"Orders\" (id bigint);"
	_, err = buildOnboardWritePlan(root, response, client.PlanExclusions{})
	require.ErrorContains(t, err, "case-insensitive filesystem")
}

// A table name the target's catalog allows but YAML would reinterpret has to
// survive the write. Written bare, a name opening with a comment marker is
// read back as a comment: the config would state an exclusion that no longer
// loads, and the next plan would propose dropping the table it names.
func TestOnboardConfigYAML_ExclusionsRoundTripThroughQuoting(t *testing.T) {
	exclusions := client.PlanExclusions{
		Namespaces: []string{"#reporting", "app"},
		Tables:     []string{"#audit", "flyway_schema_history", "yes"},
	}

	out, err := onboardConfigYAML("testapp", "mysql", exclusions, nil)
	require.NoError(t, err)

	dir := t.TempDir()
	require.NoError(t, os.WriteFile(filepath.Join(dir, "schemabot.yaml"), []byte(out), 0o600))
	cfg, err := LoadCLIConfig(dir)
	require.NoError(t, err)

	assert.Equal(t, "testapp", cfg.Database)
	assert.Equal(t, exclusions.Namespaces, cfg.PlanExclusions().Namespaces)
	assert.Equal(t, exclusions.Tables, cfg.PlanExclusions().Tables)
}

// A name needing no quoting is written plain, so an ordinary onboarded config
// reads the way an operator would have written it by hand.
func TestOnboardConfigYAML_OrdinaryNamesStayUnquoted(t *testing.T) {
	out, err := onboardConfigYAML("testapp", "mysql", client.PlanExclusions{
		Tables: []string{"flyway_schema_history"},
	}, nil)
	require.NoError(t, err)
	assert.Contains(t, out, "ignore_tables:\n  - flyway_schema_history\n")
}

// An empty exclusion list is omitted: a bare key reads as a configured
// exclusion of nothing rather than as no configuration at all.
func TestOnboardConfigYAML_EmptyExclusionsOmitTheirKeys(t *testing.T) {
	out, err := onboardConfigYAML("testapp", "mysql", client.PlanExclusions{}, nil)
	require.NoError(t, err)
	assert.Equal(t, "database: testapp\ntype: mysql\n", out)
}
