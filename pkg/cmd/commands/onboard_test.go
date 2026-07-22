package commands

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/block/schemabot/pkg/apitypes"
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
	})
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
	})
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
	})
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
	})
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
		{
			name:       "empty namespace",
			schemaRoot: t.TempDir(),
			resp: &apitypes.PullSchemaResponse{
				Database:    "orders",
				Type:        "mysql",
				Environment: "production",
				Namespaces: map[string]*apitypes.PulledNamespace{
					"orders": {Tables: map[string]string{}},
				},
			},
			want: "contains no tables or artifacts",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			plan, err := buildOnboardWritePlan(tt.schemaRoot, tt.resp)
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
	})
	require.NoError(t, err)

	// Namespace directory absent (dry run before any write): nothing to scan.
	strays, err := plan.strayFiles()
	require.NoError(t, err)
	assert.Empty(t, strays)

	require.NoError(t, plan.write())
	strays, err = plan.strayFiles()
	require.NoError(t, err)
	assert.Empty(t, strays)

	require.NoError(t, os.WriteFile(filepath.Join(root, "orders", "legacy_bak.sql"), []byte("CREATE TABLE `legacy_bak` (`id` bigint NOT NULL);\n"), 0o644))
	require.NoError(t, os.WriteFile(filepath.Join(root, "orders", "vschema.json"), []byte("{}"), 0o644))
	require.NoError(t, os.WriteFile(filepath.Join(root, "orders", "README.md"), []byte("docs"), 0o644))

	strays, err = plan.strayFiles()
	require.NoError(t, err)
	assert.Equal(t, []string{
		filepath.Join(root, "orders", "legacy_bak.sql"),
		filepath.Join(root, "orders", "vschema.json"),
	}, strays)
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
