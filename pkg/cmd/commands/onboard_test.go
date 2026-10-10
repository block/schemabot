package commands

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/block/schemabot/pkg/apitypes"
	"github.com/block/schemabot/pkg/cmd/client"
	"github.com/block/schemabot/pkg/ddl"
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
	assert.Equal(t, "CREATE TABLE `users` (\n    `id` bigint NOT NULL\n);\n", string(users))

	orders, err := os.ReadFile(filepath.Join(root, "orders", "orders.sql"))
	require.NoError(t, err)
	assert.Equal(t, "CREATE TABLE `orders` (\n    `id` bigint NOT NULL\n);\n", string(orders))
}

func TestBuildOnboardWritePlanFormatsSQLWithoutChangingContent(t *testing.T) {
	root := t.TempDir()
	original := "CREATE TABLE `events` (`id` bigint NOT NULL, `note` varchar(64) DEFAULT 'Keep INT, comma') ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;"
	plan, err := buildOnboardWritePlan(root, &apitypes.PullSchemaResponse{
		Database:    "orders",
		Type:        "mysql",
		Environment: "production",
		TableCount:  1,
		Namespaces: map[string]*apitypes.PulledNamespace{
			"orders": {Tables: map[string]string{"events": original}},
		},
	}, client.PlanExclusions{})
	require.NoError(t, err)

	formatted := plan.files[filepath.Join("orders", "events.sql")]
	assert.Contains(t, strings.TrimSuffix(formatted, "\n"), "\n")
	assert.Contains(t, formatted, "'Keep INT, comma'")
	assert.Equal(t, ddl.Canonicalize(original), ddl.Canonicalize(formatted))
}

// Supported MySQL-family targets keep uncommon table options while writing every
// table across multiple lines.
func TestBuildOnboardWritePlanPreservesMySQLTableOptions(t *testing.T) {
	for _, databaseType := range []string{"mysql", "vitess"} {
		t.Run(databaseType, func(t *testing.T) {
			root := t.TempDir()
			plan, err := buildOnboardWritePlan(root, &apitypes.PullSchemaResponse{
				Database: "app", Type: databaseType, TableCount: 2,
				Namespaces: map[string]*apitypes.PulledNamespace{
					"app": {Tables: map[string]string{
						"plain":     "CREATE TABLE plain (id int)",
						"encrypted": "CREATE TABLE encrypted (id int) ENGINE=InnoDB ENCRYPTION='Y'",
					}},
				},
			}, client.PlanExclusions{})
			require.NoError(t, err)
			require.NoError(t, plan.write())
			for _, table := range []string{"plain", "encrypted"} {
				content, err := os.ReadFile(filepath.Join(root, "app", table+".sql"))
				require.NoError(t, err)
				assert.Contains(t, string(content), "\n    `id` int\n)")
				if table == "encrypted" {
					assert.Contains(t, string(content), "ENCRYPTION = 'Y'")
				}
			}
		})
	}
}

// Formatting refusals name the affected table and prevent a partial plan or
// filesystem changes, even when an earlier table could be formatted.
func TestBuildOnboardWritePlanRefusesLossyFormatting(t *testing.T) {
	for _, databaseType := range []string{"mysql", "vitess"} {
		for _, suffix := range []string{"/* keep me */", "/*!50100 PARTITION BY HASH (id) PARTITIONS 4 */", "ENGINE_ATTRIBUTE='{}'"} {
			t.Run(databaseType+"/"+suffix, func(t *testing.T) {
				root := t.TempDir()
				plan, err := buildOnboardWritePlan(root, &apitypes.PullSchemaResponse{
					Database: "app", Type: databaseType, TableCount: 2,
					Namespaces: map[string]*apitypes.PulledNamespace{
						"app": {Tables: map[string]string{
							"a": "CREATE TABLE a (id int)",
							"b": "CREATE TABLE b (id int) " + suffix,
						}},
					},
				}, client.PlanExclusions{})
				require.ErrorContains(t, err, "namespace app table b")
				assert.Nil(t, plan)
				entries, err := os.ReadDir(root)
				require.NoError(t, err)
				assert.Empty(t, entries)
			})
		}
	}
}

// All table-formatting failures are reported in namespace/table order, while
// both existing files and successfully formatted tables remain unwritten.
func TestBuildOnboardWritePlanReportsAllFormattingFailures(t *testing.T) {
	root := t.TempDir()
	existing := filepath.Join(root, "schemabot.yaml")
	require.NoError(t, os.WriteFile(existing, []byte("database: existing\n"), 0o644))
	plan, err := buildOnboardWritePlan(root, &apitypes.PullSchemaResponse{
		Database: "app", Type: "mysql", Environment: "staging", TableCount: 4,
		Namespaces: map[string]*apitypes.PulledNamespace{
			"z": {Tables: map[string]string{"c": "CREATE TABLE c (id int) ENGINE_ATTRIBUTE='{}'"}},
			"a": {Tables: map[string]string{
				"good": "CREATE TABLE good (id int)",
				"b":    "CREATE TABLE b (id int) ENGINE_ATTRIBUTE='{\"k\": 1}'",
				"a":    "CREATE TABLE a (id int) /* keep me */",
			}},
		},
	}, client.PlanExclusions{})
	require.Error(t, err)
	assert.Nil(t, plan)
	lines := strings.Split(err.Error(), "\n")
	require.GreaterOrEqual(t, len(lines), 6)
	assert.Equal(t, "onboarding refused; no files were written:", lines[0])
	assert.Contains(t, lines[1], "namespace a table a: cannot preserve MySQL comments")
	assert.Contains(t, lines[2], "namespace a table b: formatter could not prove statement 1 preserved its SQL")
	assert.Contains(t, lines[3], "namespace z table c: formatter could not prove statement 1 preserved its SQL")
	assert.Contains(t, err.Error(), "pull with -o json")
	assert.Contains(t, err.Error(), "plan against the source environment and require no schema changes")
	assert.Contains(t, err.Error(), "onboard pulls the source again")
	content, err := os.ReadFile(existing)
	require.NoError(t, err)
	assert.Equal(t, "database: existing\n", string(content))
	entries, err := os.ReadDir(root)
	require.NoError(t, err)
	require.Len(t, entries, 1)
	assert.Equal(t, "schemabot.yaml", entries[0].Name())
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
	assert.Equal(t, "CREATE TABLE `users` (\n    `id` bigint NOT NULL\n);\n", string(users))

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

func TestOnboardPullNamespacesUseConcreteLiveNamespaces(t *testing.T) {
	pullNamespaces, err := onboardPullNamespaces([]string{"orders_production", "orders_audit_production"})
	require.NoError(t, err)

	assert.Equal(t, []string{"orders_production", "orders_audit_production"}, pullNamespaces)

	_, err = onboardPullNamespaces([]string{"orders_$ENV"})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "must be a concrete live namespace")
	_, err = onboardPullNamespaces([]string{"orders_{env}"})
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
	assert.Contains(t, resp.Namespaces, "orders_{env}")
	assert.Contains(t, resp.Namespaces, "orders_audit_{env}")
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
	assert.NotContains(t, resp.Namespaces, "orders_{env}")
}

func TestOnboardRejectsDiscoveredNamespacePlaceholders(t *testing.T) {
	for _, namespace := range []string{"orders_{env}", "orders_$ENV"} {
		for _, templateEnvSuffix := range []bool{false, true} {
			t.Run(namespace+"/template="+strconv.FormatBool(templateEnvSuffix), func(t *testing.T) {
				server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					require.Equal(t, http.MethodPost, r.Method)
					assert.Equal(t, "/api/pull", r.URL.Path)
					w.Header().Set("Content-Type", "application/json")
					require.NoError(t, json.NewEncoder(w).Encode(&apitypes.PullSchemaResponse{
						Database:    "orders",
						Type:        "mysql",
						Environment: "production",
						Namespaces: map[string]*apitypes.PulledNamespace{
							namespace: {Tables: map[string]string{"users": "CREATE TABLE `users` (`id` bigint NOT NULL);\n"}},
						},
					}))
				}))
				t.Cleanup(server.Close)

				root := t.TempDir()
				cmd := &OnboardCmd{
					Database: "orders", Environment: "production", SchemaDir: root,
					TemplateEnvSuffix: templateEnvSuffix, SkipVerify: true,
				}
				err := cmd.Run(&Globals{Endpoint: server.URL})
				require.Error(t, err)
				assert.Contains(t, err.Error(), "must be a concrete live namespace")
				assert.Contains(t, err.Error(), namespace)
				assert.NoFileExists(t, filepath.Join(root, "schemabot.yaml"))
			})
		}
	}
}

// Onboarding verifies the pulled files on every rollout member's plan, so its
// plan says it reads the rollout, and the server answers it with every
// member's plan rather than refusing a caller that would see only the
// primary's.
func TestVerifyOnboardPlan_SaysItReadsTheRollout(t *testing.T) {
	var got apitypes.PlanRequest
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, "/api/plan", r.URL.Path)
		require.NoError(t, json.NewDecoder(r.Body).Decode(&got))
		w.Header().Set("Content-Type", "application/json")
		require.NoError(t, json.NewEncoder(w).Encode(&apitypes.PlanResponse{PlanID: "plan-verify"}))
	}))
	t.Cleanup(server.Close)

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
	require.NoError(t, plan.write())

	require.NoError(t, verifyOnboardPlan(server.URL, "orders", "production", plan))
	assert.Equal(t, "orders", got.Database)
	assert.True(t, got.RendersRollout, "onboarding's verification reads every rollout member's plan")
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

// Onboarding a database whose environment has several targets verifies every
// target, not only the primary the plan is made against. A primary already at
// the pulled schema passes verification only when every other target is too:
// a target that still needs a change fails it, naming that target and the
// change, and a target that could not be planned fails it as unverified.
func TestValidateOnboardPlanResult_VerifiesEveryRolloutMember(t *testing.T) {
	alterUsers := []*apitypes.SchemaChangeResponse{{
		Namespace: "orders",
		TableChanges: []*apitypes.TableChangeResponse{{
			TableName: "users", ChangeType: "ALTER", DDL: "ALTER TABLE `users` ADD COLUMN `email` varchar(255)",
		}},
	}}
	converged := &apitypes.PlanResponse{Environment: "production", Rollout: &apitypes.PlanRolloutResponse{
		Members:     3,
		Independent: true,
		Groups:      []*apitypes.PlanMemberGroupResponse{{Primary: true, Members: []string{"orders-001", "orders-002", "orders-003"}}},
	}}
	assert.NoError(t, validateOnboardPlanResult(converged, "orders", "production"))

	pending := &apitypes.PlanResponse{Environment: "production", Rollout: &apitypes.PlanRolloutResponse{
		Members:     3,
		Independent: true,
		Groups: []*apitypes.PlanMemberGroupResponse{
			{Primary: true, Members: []string{"orders-001", "orders-002"}},
			{Members: []string{"orders-003"}, Changes: alterUsers},
		},
	}}
	err := validateOnboardPlanResult(pending, "orders", "production")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "still produce schema changes")
	assert.Contains(t, err.Error(), "orders-003: orders/users (alter): ALTER TABLE `users` ADD COLUMN `email` varchar(255)")
	assert.NotContains(t, err.Error(), "orders-001:")

	unplanned := &apitypes.PlanResponse{Environment: "production", Rollout: &apitypes.PlanRolloutResponse{
		Members:     3,
		Independent: true,
		Groups:      []*apitypes.PlanMemberGroupResponse{{Primary: true, Members: []string{"orders-001", "orders-003"}}},
		Attention: []*apitypes.PlanMemberAttentionResponse{{
			Member: "orders-002", Reason: apitypes.PlanMemberUnplanned, Detail: "could not be planned; see server logs for the cause, then plan again",
		}},
	}}
	err = validateOnboardPlanResult(unplanned, "orders", "production")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "1 of 3 rollout members could not be verified")
	assert.Contains(t, err.Error(), "orders-002: could not be planned; see server logs for the cause, then plan again")
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

// Onboarding verification fails when the pulled files still plan work. A
// keyspace whose only work is the engine's finalize is named as such, so the
// failure never lists nothing under "still produce schema changes".
func TestDescribeOnboardPlanChangesNamesFinalizeOnlyKeyspace(t *testing.T) {
	lines := describeOnboardPlanChanges(&apitypes.PlanResponse{
		Changes: []*apitypes.SchemaChangeResponse{
			{Namespace: "payments", Metadata: map[string]string{apitypes.NeedsFinalizerMetadataKey: "true"}},
		},
	})
	assert.Equal(t, []string{"payments: engine finalize requested"}, lines)
}

// A keyspace whose only VSchema work is generated from its DDL is named by
// that DDL alone, the same way the plan shows it.
func TestDescribeOnboardPlanChangesNamesGeneratedVSchemaChangeByItsDDL(t *testing.T) {
	lines := describeOnboardPlanChanges(&apitypes.PlanResponse{
		Changes: []*apitypes.SchemaChangeResponse{
			{
				Namespace:    "payments",
				TableChanges: []*apitypes.TableChangeResponse{{TableName: "refund_notes", ChangeType: "create", DDL: "CREATE TABLE `refund_notes` (`id` bigint NOT NULL, PRIMARY KEY (`id`))"}},
				Metadata: map[string]string{
					apitypes.VSchemaChangedMetadataKey:       "true",
					apitypes.VSchemaGeneratedOnlyMetadataKey: "true",
					apitypes.NeedsFinalizerMetadataKey:       "true",
				},
			},
		},
	})
	assert.Equal(t, []string{"payments/refund_notes (create): CREATE TABLE `refund_notes` (`id` bigint NOT NULL, PRIMARY KEY (`id`))"}, lines)
}

// A keyspace that still plans a table and is finalized afterward is named by
// its table alone: the finalize is part of that work.
func TestDescribeOnboardPlanChangesNamesOnlyTheTableOfAFinalizedKeyspace(t *testing.T) {
	lines := describeOnboardPlanChanges(&apitypes.PlanResponse{
		Changes: []*apitypes.SchemaChangeResponse{
			{
				Namespace:    "payments",
				TableChanges: []*apitypes.TableChangeResponse{{TableName: "refund_notes", ChangeType: "create", DDL: "CREATE TABLE `refund_notes` (`id` bigint NOT NULL, PRIMARY KEY (`id`))"}},
				Metadata:     map[string]string{apitypes.NeedsFinalizerMetadataKey: "true"},
			},
		},
	})
	assert.Equal(t, []string{"payments/refund_notes (create): CREATE TABLE `refund_notes` (`id` bigint NOT NULL, PRIMARY KEY (`id`))"}, lines)
}

// A keyspace on its only shard carries its DDL on that shard. It is named by
// that DDL alone, with no finalize line, the same way the plan shows it.
func TestDescribeOnboardPlanChangesNamesAShardCarriedTableOfAFinalizedKeyspace(t *testing.T) {
	lines := describeOnboardPlanChanges(&apitypes.PlanResponse{
		Changes: []*apitypes.SchemaChangeResponse{
			{Namespace: "payments", Metadata: map[string]string{apitypes.NeedsFinalizerMetadataKey: "true"}},
		},
		Shards: []*apitypes.ShardPlanResponse{{Namespace: "payments", Shard: "-", Changes: []*apitypes.TableChangeResponse{
			{TableName: "refunds", ChangeType: "alter", DDL: "ALTER TABLE `refunds` ADD COLUMN `note` text"},
		}}},
	})
	assert.Equal(t, []string{"payments/refunds (alter): ALTER TABLE `refunds` ADD COLUMN `note` text"}, lines)
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

// Re-onboarding a repository that withholds a runtime table family with a
// pattern keeps the pattern in the rewritten config, round-tripped as written,
// and writes no file for any member of the family the pull returned, so the
// rewrite never declares a table the config withholds.
func TestBuildOnboardWritePlanPreservesAndHonorsPatternEntries(t *testing.T) {
	root := t.TempDir()
	const pattern = `/^relay_\d+_feed$/`
	plan, err := buildOnboardWritePlan(root, &apitypes.PullSchemaResponse{
		Database:    "orders",
		Type:        "mysql",
		Environment: "production",
		TableCount:  3,
		Namespaces: map[string]*apitypes.PulledNamespace{
			"orders": {Tables: map[string]string{
				"users":         "CREATE TABLE `users` (`id` bigint NOT NULL);\n",
				"relay_1_feed":  "CREATE TABLE `relay_1_feed` (`id` bigint NOT NULL);\n",
				"relay_17_feed": "CREATE TABLE `relay_17_feed` (`id` bigint NOT NULL);\n",
			}},
		},
	}, client.PlanExclusions{Tables: []string{pattern}})
	require.NoError(t, err)
	require.NoError(t, plan.write())

	assert.FileExists(t, filepath.Join(root, "orders", "users.sql"))
	assert.NoFileExists(t, filepath.Join(root, "orders", "relay_1_feed.sql"))
	assert.NoFileExists(t, filepath.Join(root, "orders", "relay_17_feed.sql"))
	require.Len(t, plan.withheld, 1)
	assert.Equal(t, []string{"relay_17_feed", "relay_1_feed"}, plan.withheld[0].Tables)

	cfg, err := LoadCLIConfig(root)
	require.NoError(t, err, "the rewritten config is read back by the same parser plans use")
	assert.Equal(t, []string{pattern}, cfg.IgnoreTables)
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

func TestBuildOnboardWritePlanPostgresPreservesRowSecurity(t *testing.T) {
	root := t.TempDir()
	definition := "CREATE TABLE documents (\n  id bigint PRIMARY KEY\n);\nALTER TABLE documents ENABLE ROW LEVEL SECURITY;\nALTER TABLE documents FORCE ROW LEVEL SECURITY;\nCREATE POLICY readers ON documents FOR SELECT USING (id = 1);\nCOMMENT ON POLICY readers ON documents IS 'Read your documents';\n"
	plan, err := buildOnboardWritePlan(root, &apitypes.PullSchemaResponse{
		Database: "app", Type: "postgres", Environment: "development", TableCount: 1,
		Namespaces: map[string]*apitypes.PulledNamespace{
			"public": {Tables: map[string]string{"documents": definition}},
		},
	}, client.PlanExclusions{})
	require.NoError(t, err)
	require.NoError(t, plan.write())

	contents, err := os.ReadFile(filepath.Join(root, "public", "documents.sql"))
	require.NoError(t, err)
	assert.Equal(t, definition, string(contents))
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

	out, err := onboardConfigYAML("testapp", "mysql", exclusions)
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
	})
	require.NoError(t, err)
	assert.Contains(t, out, "ignore_tables:\n  - flyway_schema_history\n")
}

// An empty exclusion list is omitted: a bare key reads as a configured
// exclusion of nothing rather than as no configuration at all.
func TestOnboardConfigYAML_EmptyExclusionsOmitTheirKeys(t *testing.T) {
	out, err := onboardConfigYAML("testapp", "mysql", client.PlanExclusions{})
	require.NoError(t, err)
	assert.Equal(t, "database: testapp\ntype: mysql\n", out)
}

// An empty Vitess keyspace still has routing metadata. Importing it must keep
// both its VSchema and an editable SQL starter, without inventing a table.
func TestOnboardEmptyVitessKeyspaceThenFirstTable(t *testing.T) {
	root := t.TempDir()
	const vschema = `{"sharded":false,"tables":{}}`
	plan, err := buildOnboardWritePlan(root, &apitypes.PullSchemaResponse{
		Database: "shop", Type: "vitess", Environment: "development",
		Namespaces: map[string]*apitypes.PulledNamespace{
			"commerce": {Tables: map[string]string{}, Artifacts: map[string]string{"vschema.json": vschema}},
		},
	}, client.PlanExclusions{})
	require.NoError(t, err)
	require.NoError(t, plan.write())
	starter := filepath.Join(root, "commerce", "schema.sql")
	contents, err := os.ReadFile(starter)
	require.NoError(t, err)
	require.Equal(t, schema.EmptyNamespaceDeclaration, string(contents))
	files, _, err := client.ReadSchemaFiles(root, "development", nil)
	require.NoError(t, err)
	require.Len(t, files, 1)
	require.Equal(t, map[string]string{"vschema.json": vschema}, files["commerce"].Files)
	const ddl = "CREATE TABLE customers (id bigint NOT NULL PRIMARY KEY);\n"
	require.NoError(t, os.WriteFile(starter, []byte(ddl), 0600))
	files, _, err = client.ReadSchemaFiles(root, "development", nil)
	require.NoError(t, err)
	require.Equal(t, map[string]string{"schema.sql": ddl, "vschema.json": vschema}, files["commerce"].Files)
}
