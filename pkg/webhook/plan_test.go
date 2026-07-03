package webhook

import (
	"errors"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/block/schemabot/pkg/api"
	"github.com/block/schemabot/pkg/apitypes"
	ghclient "github.com/block/schemabot/pkg/github"
	"github.com/block/schemabot/pkg/storage"
	"github.com/block/schemabot/pkg/webhook/templates"
)

// A sharded plan response's per-shard changes are threaded into the keyspace's
// Shards, so the plan comment can render "what applies where" — not just the
// collapsed namespace-level Changes.
func TestBuildPlanCommentData_CarriesPerShardChanges(t *testing.T) {
	schema := &ghclient.SchemaRequestResult{Database: "cdb_resolute", Type: "strata"}
	const mutes = "ALTER TABLE `mutes` ADD INDEX `created_at`(`created_at`)"
	const mutesDrift = "ALTER TABLE `mutes` ADD INDEX `created_at`(`created_at`), ADD COLUMN `reason` varchar(255)"
	planResp := &apitypes.PlanResponse{
		Changes: []*apitypes.SchemaChangeResponse{{
			Namespace:    "cdb_resolute_sharded",
			TableChanges: []*apitypes.TableChangeResponse{{TableName: "mutes", DDL: mutes, ChangeType: "alter"}},
		}},
		Shards: []*apitypes.ShardPlanResponse{
			{Namespace: "cdb_resolute_sharded", Shard: "-40", Changes: []*apitypes.TableChangeResponse{{TableName: "mutes", DDL: mutes, ChangeType: "alter"}}},
			{Namespace: "cdb_resolute_sharded", Shard: "40-80", Changes: []*apitypes.TableChangeResponse{{TableName: "mutes", DDL: mutesDrift, ChangeType: "alter"}}},
		},
	}

	data := buildPlanCommentData(schema, planResp, "staging", "", "testuser")

	require.Len(t, data.Changes, 1)
	require.Len(t, data.Changes[0].Shards, 2, "per-shard changes are threaded into the keyspace")
	assert.Equal(t, "-40", data.Changes[0].Shards[0].Shard)
	assert.Equal(t, []string{mutesDrift}, data.Changes[0].Shards[1].Statements, "the drifted shard keeps its own DDL")
}

// An unsafe change on a single shard (per-shard plan) is surfaced with its shard,
// even when the collapsed namespace-level Changes don't carry it.
func TestBuildPlanCommentData_PerShardUnsafe(t *testing.T) {
	schema := &ghclient.SchemaRequestResult{Database: "cdb_resolute", Type: "strata"}
	planResp := &apitypes.PlanResponse{
		Changes: []*apitypes.SchemaChangeResponse{{
			Namespace:    "cdb_resolute_sharded",
			TableChanges: []*apitypes.TableChangeResponse{{TableName: "mutes", DDL: "ALTER TABLE `mutes` ADD INDEX a", ChangeType: "alter"}},
		}},
		Shards: []*apitypes.ShardPlanResponse{
			{Namespace: "cdb_resolute_sharded", Shard: "-40", Changes: []*apitypes.TableChangeResponse{{TableName: "mutes", DDL: "ALTER TABLE `mutes` ADD INDEX a", ChangeType: "alter"}}},
			// One combined ALTER per table; the drifted shard's single mutes change
			// also drops a column and is flagged unsafe.
			{Namespace: "cdb_resolute_sharded", Shard: "40-80", Changes: []*apitypes.TableChangeResponse{
				{TableName: "mutes", DDL: "ALTER TABLE `mutes` ADD INDEX a, DROP COLUMN x", ChangeType: "alter", IsUnsafe: true, UnsafeReason: "DROP COLUMN removes data"},
			}},
		},
	}

	data := buildPlanCommentData(schema, planResp, "staging", "", "testuser")

	assert.True(t, data.HasUnsafeChanges)
	require.Len(t, data.UnsafeChanges, 1)
	assert.Equal(t, "mutes", data.UnsafeChanges[0].Table)
	assert.Equal(t, []string{"40-80"}, data.UnsafeChanges[0].Shards, "the unsafe change is scoped to the drifted shard")
}

// A shard that already matches the desired schema while siblings change is
// carried as a satisfied (no-change) group, so a partially-applied keyspace
// renders its divergent state instead of hiding the shard.
func TestBuildPlanCommentData_CarriesSatisfiedShard(t *testing.T) {
	schema := &ghclient.SchemaRequestResult{Database: "cdb_resolute", Type: "strata"}
	const mutes = "ALTER TABLE `mutes` ADD INDEX `created_at`(`created_at`)"
	planResp := &apitypes.PlanResponse{
		Changes: []*apitypes.SchemaChangeResponse{{
			Namespace:    "cdb_resolute_sharded",
			TableChanges: []*apitypes.TableChangeResponse{{TableName: "mutes", DDL: mutes, ChangeType: "alter"}},
		}},
		Shards: []*apitypes.ShardPlanResponse{
			{Namespace: "cdb_resolute_sharded", Shard: "-40"}, // already matches — no changes
			{Namespace: "cdb_resolute_sharded", Shard: "40-80", Changes: []*apitypes.TableChangeResponse{{TableName: "mutes", DDL: mutes, ChangeType: "alter"}}},
		},
	}

	data := buildPlanCommentData(schema, planResp, "staging", "", "testuser")

	require.Len(t, data.Changes, 1)
	require.Len(t, data.Changes[0].Shards, 2, "the satisfied shard is carried, not dropped")
	assert.Equal(t, "-40", data.Changes[0].Shards[0].Shard)
	assert.True(t, data.Changes[0].Shards[0].Satisfied, "a shard with no changes is marked satisfied")
	assert.Empty(t, data.Changes[0].Shards[0].Statements)
	assert.False(t, data.Changes[0].Shards[1].Satisfied, "the changing shard is not satisfied")
	assert.Empty(t, data.Errors, "a satisfied shard is not an error")
}

// A shard that reports changes but produces no usable DDL is malformed: the plan
// is incomplete for that shard. It is surfaced as an error rather than silently
// dropped, which would hide the divergent state the sharded view exists to show.
func TestBuildPlanCommentData_MalformedShardSurfacesError(t *testing.T) {
	schema := &ghclient.SchemaRequestResult{Database: "cdb_resolute", Type: "strata"}
	const mutes = "ALTER TABLE `mutes` ADD INDEX `created_at`(`created_at`)"
	planResp := &apitypes.PlanResponse{
		Changes: []*apitypes.SchemaChangeResponse{{
			Namespace:    "cdb_resolute_sharded",
			TableChanges: []*apitypes.TableChangeResponse{{TableName: "mutes", DDL: mutes, ChangeType: "alter"}},
		}},
		Shards: []*apitypes.ShardPlanResponse{
			{Namespace: "cdb_resolute_sharded", Shard: "-40", Changes: []*apitypes.TableChangeResponse{{TableName: "mutes", DDL: mutes, ChangeType: "alter"}}},
			// Reports a change but yields no DDL — malformed.
			{Namespace: "cdb_resolute_sharded", Shard: "40-80", Changes: []*apitypes.TableChangeResponse{{TableName: "mutes", DDL: "", ChangeType: "alter"}}},
		},
	}

	data := buildPlanCommentData(schema, planResp, "staging", "", "testuser")

	require.Len(t, data.Changes, 1)
	require.Len(t, data.Changes[0].Shards, 1, "the malformed shard is not carried into the rendered shards")
	assert.Equal(t, "-40", data.Changes[0].Shards[0].Shard)
	require.Len(t, data.Errors, 1, "the malformed shard is surfaced as an error")
	assert.Contains(t, data.Errors[0], `shard "40-80"`)
	assert.Contains(t, data.Errors[0], `keyspace "cdb_resolute_sharded"`)
	assert.Contains(t, data.Errors[0], "no DDL")
}

func TestBuildPlanCommentData_UnsafeChangesPopulated(t *testing.T) {
	schema := &ghclient.SchemaRequestResult{
		Database: "testdb",
		Type:     "mysql",
	}

	planResp := &apitypes.PlanResponse{
		Changes: []*apitypes.SchemaChangeResponse{{
			Namespace: "testdb",
			TableChanges: []*apitypes.TableChangeResponse{{
				TableName:    "orders",
				DDL:          "ALTER TABLE `orders` DROP INDEX `idx_status`",
				ChangeType:   "alter",
				IsUnsafe:     true,
				UnsafeReason: "DROP INDEX without making invisible first",
			}},
		}},
		LintResults: []*apitypes.LintViolationResponse{{
			Message:  "Index 'idx_status' should be made invisible before dropping",
			Table:    "orders",
			Linter:   "invisible_index_before_drop",
			Severity: "error",
		}},
	}

	data := buildPlanCommentData(schema, planResp, "staging", "", "testuser")

	assert.True(t, data.HasUnsafeChanges, "expected HasUnsafeChanges=true when plan contains unsafe table changes")
	require.Len(t, data.UnsafeChanges, 1)
	assert.Equal(t, "orders", data.UnsafeChanges[0].Table)
	assert.Equal(t, "DROP INDEX without making invisible first", data.UnsafeChanges[0].Reason)
}

func TestBuildPlanCommentData_TableDropIsUnsafeWithoutEngineFlag(t *testing.T) {
	schema := &ghclient.SchemaRequestResult{
		Database: "testdb",
		Type:     "mysql",
	}

	planResp := &apitypes.PlanResponse{
		Changes: []*apitypes.SchemaChangeResponse{{
			Namespace: "testdb",
			TableChanges: []*apitypes.TableChangeResponse{{
				TableName:  "users",
				DDL:        "DROP TABLE `users`",
				ChangeType: "drop",
			}},
		}},
	}

	data := buildPlanCommentData(schema, planResp, "staging", "", "testuser")

	assert.True(t, data.HasUnsafeChanges)
	require.Len(t, data.UnsafeChanges, 1)
	assert.Equal(t, "users", data.UnsafeChanges[0].Table)
	assert.Equal(t, "DROP TABLE removes all data", data.UnsafeChanges[0].Reason)
}

func TestBuildPlanCommentData_NoUnsafeChanges(t *testing.T) {
	schema := &ghclient.SchemaRequestResult{
		Database:   "testdb",
		Type:       "mysql",
		HeadSHA:    "abcdef1234567890",
		Repository: "block/schemabot",
	}

	planResp := &apitypes.PlanResponse{
		Changes: []*apitypes.SchemaChangeResponse{{
			Namespace: "testdb",
			TableChanges: []*apitypes.TableChangeResponse{{
				TableName:  "orders",
				DDL:        "ALTER TABLE `orders` ADD COLUMN `status2` varchar(50)",
				ChangeType: "alter",
				IsUnsafe:   false,
			}},
		}},
	}

	data := buildPlanCommentData(schema, planResp, "staging", "", "testuser")

	assert.False(t, data.HasUnsafeChanges)
	assert.Empty(t, data.UnsafeChanges)
	assert.Equal(t, "mysql", data.DatabaseType)
	assert.Equal(t, "abcdef1234567890", data.HeadSHA)
	assert.Equal(t, "block/schemabot", data.Repository)
}

func TestBuildPlanCommentData_MixedSafeAndUnsafe(t *testing.T) {
	schema := &ghclient.SchemaRequestResult{
		Database: "testdb",
		Type:     "mysql",
	}

	planResp := &apitypes.PlanResponse{
		Changes: []*apitypes.SchemaChangeResponse{{
			Namespace: "testdb",
			TableChanges: []*apitypes.TableChangeResponse{
				{
					TableName:  "orders",
					DDL:        "ALTER TABLE `orders` ADD COLUMN `status2` varchar(50)",
					ChangeType: "alter",
					IsUnsafe:   false,
				},
				{
					TableName:    "users",
					DDL:          "DROP TABLE `users`",
					ChangeType:   "drop",
					IsUnsafe:     true,
					UnsafeReason: "DROP TABLE removes all data",
				},
			},
		}},
	}

	data := buildPlanCommentData(schema, planResp, "staging", "", "testuser")

	assert.True(t, data.HasUnsafeChanges)
	require.Len(t, data.UnsafeChanges, 1)
	assert.Equal(t, "users", data.UnsafeChanges[0].Table)
}

func TestRenderPlanComment_ShowsUnsafeWarning(t *testing.T) {
	data := templates.PlanCommentData{
		Database:    "testdb",
		Environment: "staging",
		IsMySQL:     true,
		Changes: []templates.KeyspaceChangeData{{
			Keyspace:   "testdb",
			Statements: []string{"ALTER TABLE `orders` DROP INDEX `idx_status`"},
		}},
		HasUnsafeChanges: true,
		UnsafeChanges: []templates.UnsafeChangeData{{
			Table:  "orders",
			Reason: "DROP INDEX without making invisible first",
		}},
	}

	rendered := templates.RenderPlanComment(data)

	assert.Contains(t, rendered, "⛔ Unsafe Changes Detected")
	assert.Contains(t, rendered, "`orders`")
	assert.Contains(t, rendered, "DROP INDEX without making invisible first")
}

func TestRenderPlanComment_TenantScopedHints(t *testing.T) {
	t.Run("plan hint preserves tenant", func(t *testing.T) {
		data := templates.PlanCommentData{
			Database:    "testdb",
			Environment: "staging",
			Tenant:      "alpha",
			IsMySQL:     true,
			Changes: []templates.KeyspaceChangeData{{
				Keyspace:   "testdb",
				Statements: []string{"ALTER TABLE `orders` ADD COLUMN `x` INT"},
			}},
		}

		rendered := templates.RenderPlanComment(data)

		assert.Contains(t, rendered, "**Tenant**: `alpha`")
		assert.Contains(t, rendered, "schemabot apply -e staging --tenant alpha")
	})

	t.Run("automatic apply preserves tenant metadata without showing apply-confirm", func(t *testing.T) {
		data := templates.PlanCommentData{
			Database:    "testdb",
			Environment: "staging",
			Tenant:      "alpha",
			IsMySQL:     true,
			IsLocked:    true,
			Changes: []templates.KeyspaceChangeData{{
				Keyspace:   "testdb",
				Statements: []string{"ALTER TABLE `orders` ADD COLUMN `x` INT"},
			}},
		}

		rendered := templates.RenderPlanComment(data)

		assert.Contains(t, rendered, "**Tenant**: `alpha`")
		assert.Contains(t, rendered, "Applying automatically")
		assert.NotContains(t, rendered, "schemabot apply-confirm -e staging --tenant alpha")
	})

	t.Run("downgrade hint preserves tenant", func(t *testing.T) {
		data := templates.PlanCommentData{
			Database:                   "testdb",
			Environment:                "staging",
			Tenant:                     "alpha",
			IsMySQL:                    true,
			IsLocked:                   true,
			AutoConfirmDowngradeReason: "Schema changes differ from auto-plan — review and confirm manually",
			Changes: []templates.KeyspaceChangeData{{
				Keyspace:   "testdb",
				Statements: []string{"ALTER TABLE `orders` ADD COLUMN `x` INT"},
			}},
		}

		rendered := templates.RenderPlanComment(data)

		assert.Contains(t, rendered, "**Tenant**: `alpha`")
		assert.Contains(t, rendered, "Automatic apply paused")
		assert.Contains(t, rendered, "schemabot apply-confirm -e staging --tenant alpha")
	})

	t.Run("preview shows tenant metadata without putting tenant in title", func(t *testing.T) {
		rendered := templates.PreviewCommentPlanTenant()
		firstLine, _, _ := strings.Cut(rendered, "\n")

		assert.Equal(t, "## Schema Change Plan — Staging", firstLine)
		assert.Contains(t, rendered, "**Tenant**: `alpha`")
		assert.Contains(t, rendered, "schemabot apply -e staging --tenant alpha")
		assert.NotContains(t, firstLine, "alpha")
	})
}

func TestRenderPlanComment_EnvironmentScopedTitle(t *testing.T) {
	t.Run("plan title includes environment without tenant", func(t *testing.T) {
		data := templates.PlanCommentData{
			Database:     "testdb",
			Environment:  "production",
			Tenant:       "alpha",
			DatabaseType: storage.DatabaseTypeStrata,
			Changes: []templates.KeyspaceChangeData{{
				Keyspace:   "testdb",
				Statements: []string{"ALTER TABLE `orders` ADD COLUMN `x` INT"},
			}},
		}

		rendered := templates.RenderPlanComment(data)
		firstLine, _, _ := strings.Cut(rendered, "\n")

		assert.Equal(t, "## Schema Change Plan — Production", firstLine)
		assert.Contains(t, rendered, "**Type**: `Strata`")
		assert.NotContains(t, firstLine, "alpha")
	})

	t.Run("locked apply title includes environment", func(t *testing.T) {
		data := templates.PlanCommentData{
			Database:    "testdb",
			Environment: "staging",
			IsMySQL:     true,
			IsLocked:    true,
			Changes: []templates.KeyspaceChangeData{{
				Keyspace:   "testdb",
				Statements: []string{"ALTER TABLE `orders` ADD COLUMN `x` INT"},
			}},
		}

		rendered := templates.RenderPlanComment(data)
		firstLine, _, _ := strings.Cut(rendered, "\n")

		assert.Equal(t, "## Schema Change Apply — Staging", firstLine)
		assert.Contains(t, rendered, "**Type**: `MySQL`")
	})

	t.Run("environment suffix preserves identifier separators", func(t *testing.T) {
		data := templates.PlanCommentData{
			Database:    "testdb",
			Environment: "prod_us-east",
			IsMySQL:     true,
			Changes: []templates.KeyspaceChangeData{{
				Keyspace:   "testdb",
				Statements: []string{"ALTER TABLE `orders` ADD COLUMN `x` INT"},
			}},
		}

		rendered := templates.RenderPlanComment(data)
		firstLine, _, _ := strings.Cut(rendered, "\n")

		assert.Equal(t, "## Schema Change Plan — Prod_us-east", firstLine)
	})

	t.Run("multi environment plan title stays generic", func(t *testing.T) {
		data := templates.MultiEnvPlanCommentData{
			Database:     "testdb",
			IsMySQL:      true,
			Environments: []string{"staging", "production"},
			Plans: map[string]*templates.PlanCommentData{
				"staging":    {Database: "testdb", Environment: "staging", IsMySQL: true},
				"production": {Database: "testdb", Environment: "production", IsMySQL: true},
			},
			Errors: map[string]string{},
		}

		rendered := templates.RenderMultiEnvPlanComment(data)
		firstLine, _, _ := strings.Cut(rendered, "\n")

		assert.Equal(t, "## Schema Change Plan", firstLine)
		assert.Contains(t, rendered, "**Type**: `MySQL`")
	})

	t.Run("single environment multi-env plan title includes environment", func(t *testing.T) {
		data := templates.MultiEnvPlanCommentData{
			Database:     "testdb",
			IsMySQL:      true,
			Environments: []string{"staging"},
			Plans: map[string]*templates.PlanCommentData{
				"staging": {
					Database:    "testdb",
					Environment: "staging",
					IsMySQL:     true,
					Changes: []templates.KeyspaceChangeData{{
						Keyspace:   "testdb",
						Statements: []string{"ALTER TABLE `orders` ADD COLUMN `x` INT"},
					}},
				},
			},
			Errors: map[string]string{},
		}

		rendered := templates.RenderMultiEnvPlanComment(data)
		firstLine, _, _ := strings.Cut(rendered, "\n")

		assert.Equal(t, "## Schema Change Plan — Staging", firstLine)
		assert.Contains(t, rendered, "**Type**: `MySQL`")
		assert.NotContains(t, rendered, "### Staging")
		assert.Contains(t, rendered, "schemabot apply -e staging")
	})

	t.Run("single environment tenant plan keeps tenant in production command", func(t *testing.T) {
		data := templates.MultiEnvPlanCommentData{
			Database:     "testdb",
			IsMySQL:      true,
			Tenant:       "alpha",
			Environments: []string{"production"},
			Plans: map[string]*templates.PlanCommentData{
				"production": {
					Database:    "testdb",
					Environment: "production",
					IsMySQL:     true,
					Changes: []templates.KeyspaceChangeData{{
						Keyspace:   "testdb",
						Statements: []string{"ALTER TABLE `orders` ADD COLUMN `x` INT"},
					}},
				},
			},
			Errors: map[string]string{},
		}

		rendered := templates.RenderMultiEnvPlanComment(data)
		firstLine, _, _ := strings.Cut(rendered, "\n")

		assert.Equal(t, "## Schema Change Plan — Production", firstLine)
		assert.Contains(t, rendered, "**Tenant**: `alpha`")
		assert.Contains(t, rendered, "schemabot apply -e production --tenant alpha")
	})

	t.Run("multi environment tenant plan keeps tenant in metadata and commands", func(t *testing.T) {
		data := templates.MultiEnvPlanCommentData{
			Database:     "testdb",
			IsMySQL:      true,
			Tenant:       "alpha",
			Environments: []string{"staging", "production"},
			Plans: map[string]*templates.PlanCommentData{
				"staging": {
					Database:    "testdb",
					Environment: "staging",
					IsMySQL:     true,
					Changes: []templates.KeyspaceChangeData{{
						Keyspace:   "testdb",
						Statements: []string{"ALTER TABLE `orders` ADD COLUMN `x` INT"},
					}},
				},
				"production": {
					Database:    "testdb",
					Environment: "production",
					IsMySQL:     true,
					Changes: []templates.KeyspaceChangeData{{
						Keyspace:   "testdb",
						Statements: []string{"ALTER TABLE `orders` ADD COLUMN `x` INT"},
					}},
				},
			},
			Errors: map[string]string{},
		}

		rendered := templates.RenderMultiEnvPlanComment(data)

		assert.Contains(t, rendered, "**Tenant**: `alpha`")
		assert.Contains(t, rendered, "schemabot apply -e staging --tenant alpha")
		assert.Contains(t, rendered, "schemabot apply -e production --tenant alpha")
	})
}

func TestRenderPlanComment_NoUnsafe_NoWarning(t *testing.T) {
	data := templates.PlanCommentData{
		Database:    "testdb",
		Environment: "staging",
		IsMySQL:     true,
		Changes: []templates.KeyspaceChangeData{{
			Keyspace:   "testdb",
			Statements: []string{"ALTER TABLE `orders` ADD COLUMN `x` INT"},
		}},
	}

	rendered := templates.RenderPlanComment(data)

	assert.NotContains(t, rendered, "Unsafe")
}

func TestRenderPlanComment_StrataHeader(t *testing.T) {
	data := templates.PlanCommentData{
		Database:     "testdb",
		Environment:  "staging",
		DatabaseType: storage.DatabaseTypeStrata,
		Changes: []templates.KeyspaceChangeData{{
			Keyspace:   "testdb",
			Statements: []string{"ALTER TABLE `users` ADD COLUMN `email` varchar(255)"},
		}},
	}

	rendered := templates.RenderPlanComment(data)

	assert.Contains(t, rendered, "## Schema Change Plan")
	assert.Contains(t, rendered, "**Type**: `Strata`")
}

func TestRenderPlanComment_CustomDatabaseTypeHeader(t *testing.T) {
	data := templates.PlanCommentData{
		Database:     "testdb",
		Environment:  "staging",
		DatabaseType: "custom-engine",
		Changes: []templates.KeyspaceChangeData{{
			Keyspace:   "testdb",
			Statements: []string{"ALTER TABLE `users` ADD COLUMN `email` varchar(255)"},
		}},
	}

	rendered := templates.RenderPlanComment(data)

	assert.Contains(t, rendered, "## Schema Change Plan")
	assert.Contains(t, rendered, "**Type**: `Custom Engine`")
}

func TestRenderPlanComment_PostgresHeader(t *testing.T) {
	data := templates.PlanCommentData{
		Database:     "testdb",
		Environment:  "staging",
		DatabaseType: "postgres",
		Changes: []templates.KeyspaceChangeData{{
			Keyspace:   "testdb",
			Statements: []string{"ALTER TABLE `users` ADD COLUMN `email` varchar(255)"},
		}},
	}

	rendered := templates.RenderPlanComment(data)

	assert.Contains(t, rendered, "## Schema Change Plan")
	assert.Contains(t, rendered, "**Type**: `PostgreSQL`")
}

func TestRenderPlanComment_ShowsPRHeadSHA(t *testing.T) {
	data := templates.PlanCommentData{
		Database:    "testdb",
		Environment: "staging",
		HeadSHA:     "abcdef1234567890",
		Repository:  "block/schemabot",
		IsMySQL:     true,
	}

	rendered := templates.RenderPlanComment(data)

	assert.Contains(t, rendered, "planned from [`abcdef1`](https://github.com/block/schemabot/commit/abcdef1234567890)")
	assert.NotContains(t, rendered, "**PR head SHA**")
}

func TestRenderPlanComment_ShowsRecoveredApplyOwnedCheckState(t *testing.T) {
	data := templates.PlanCommentData{
		Database:                      "testdb",
		Environment:                   "staging",
		IsMySQL:                       true,
		RecoveredApplyOwnedCheckState: true,
	}

	rendered := templates.RenderPlanComment(data)

	assert.Contains(t, rendered, "✅ **No schema changes detected**")
	assert.Contains(t, rendered, "SchemaBot found stored PR check state")
	assert.Contains(t, rendered, "still marked as an apply in progress")
	assert.Contains(t, rendered, "SchemaBot updated the PR check to passing")
}

func TestRenderMultiEnvPlanComment_ShowsPRHeadSHA(t *testing.T) {
	data := templates.MultiEnvPlanCommentData{
		Database:     "testdb",
		HeadSHA:      "abcdef1234567890",
		Repository:   "block/schemabot",
		IsMySQL:      true,
		Environments: []string{"staging"},
		Plans: map[string]*templates.PlanCommentData{
			"staging": {
				Database:    "testdb",
				Environment: "staging",
				IsMySQL:     true,
			},
		},
		Errors: map[string]string{},
	}

	rendered := templates.RenderMultiEnvPlanComment(data)
	firstLine, _, _ := strings.Cut(rendered, "\n")

	assert.Equal(t, "## Schema Change Plan — Staging", firstLine)
	assert.Contains(t, rendered, "planned from [`abcdef1`](https://github.com/block/schemabot/commit/abcdef1234567890)")
	assert.NotContains(t, rendered, "**PR head SHA**")
}

func TestRenderMultiEnvPlanComment_StrataHeaderWithErrors(t *testing.T) {
	data := templates.MultiEnvPlanCommentData{
		Database:     "testdb",
		DatabaseType: storage.DatabaseTypeStrata,
		Environments: []string{"staging"},
		Plans:        map[string]*templates.PlanCommentData{},
		Errors:       map[string]string{"staging": "resolver unavailable"},
	}

	rendered := templates.RenderMultiEnvPlanComment(data)
	firstLine, _, _ := strings.Cut(rendered, "\n")

	assert.Equal(t, "## Schema Change Plan — Staging", firstLine)
	assert.Contains(t, rendered, "**Type**: `Strata`")
}

// A multi-environment plan collapses its DDL behind a <details> block so the
// SQL doesn't dominate the comment, while the summary line advertises the
// statement count so a reviewer can gauge the change size without expanding it.
func TestRenderMultiEnvPlanComment_CollapsesDDL(t *testing.T) {
	changes := []templates.KeyspaceChangeData{{
		Keyspace: "testdb",
		Statements: []string{
			"CREATE TABLE `customers` (`id` bigint NOT NULL AUTO_INCREMENT PRIMARY KEY)",
			"CREATE TABLE `orders` (`id` bigint NOT NULL AUTO_INCREMENT PRIMARY KEY)",
		},
	}}
	data := templates.MultiEnvPlanCommentData{
		Database:     "testdb",
		IsMySQL:      true,
		Environments: []string{"staging", "production"},
		Plans: map[string]*templates.PlanCommentData{
			"staging":    {Database: "testdb", Environment: "staging", IsMySQL: true, Changes: changes},
			"production": {Database: "testdb", Environment: "production", IsMySQL: true, Changes: changes},
		},
		Errors: map[string]string{},
	}

	rendered := templates.RenderMultiEnvPlanComment(data)

	assert.Contains(t, rendered, "<details>\n<summary>Show SQL (2 statements)</summary>")
	assert.Contains(t, rendered, "</details>")
	assert.Contains(t, rendered, "```sql")
	assert.Contains(t, rendered, "CREATE TABLE `customers`")
	// The DDL fence lives inside the collapsed block, before it closes.
	assert.Less(t, strings.Index(rendered, "```sql"), strings.Index(rendered, "</details>"),
		"the SQL fence should be rendered inside the <details> block")
	// The plan summary stays outside the collapse so it is visible at a glance.
	assert.Contains(t, rendered, "📋 **Plan**: **2** tables to create")
	assert.Greater(t, strings.Index(rendered, "📋 **Plan**"), strings.Index(rendered, "</details>"),
		"the plan summary should stay outside (below) the collapsed DDL")
}

// A single statement uses the singular "statement" in the collapse summary.
func TestRenderMultiEnvPlanComment_CollapseSummarySingular(t *testing.T) {
	changes := []templates.KeyspaceChangeData{{
		Keyspace:   "testdb",
		Statements: []string{"ALTER TABLE `orders` ADD COLUMN `x` INT"},
	}}
	data := templates.MultiEnvPlanCommentData{
		Database:     "testdb",
		IsMySQL:      true,
		Environments: []string{"staging", "production"},
		Plans: map[string]*templates.PlanCommentData{
			"staging":    {Database: "testdb", Environment: "staging", IsMySQL: true, Changes: changes},
			"production": {Database: "testdb", Environment: "production", IsMySQL: true, Changes: changes},
		},
		Errors: map[string]string{},
	}

	rendered := templates.RenderMultiEnvPlanComment(data)

	assert.Contains(t, rendered, "<summary>Show SQL (1 statement)</summary>")
}

func TestUserFacingErrorExplainsNoHealthyUpstream(t *testing.T) {
	err := &api.RemoteDeploymentUnavailableError{
		Deployment: "pie",
		Target:     "orders-staging",
		Err:        status.Error(codes.Unavailable, "no healthy upstream"),
	}

	got := userFacingError(err)

	assert.Contains(t, got, "SchemaBot could not reach the remote deployment `pie`")
	assert.Contains(t, got, "target `orders-staging`")
	assert.Contains(t, got, "service or network path is unavailable")
	assert.Contains(t, got, "Raw error: remote deployment \"pie\" target \"orders-staging\" unavailable: rpc error: code = Unavailable desc = no healthy upstream")
}

func TestUserFacingErrorPreservesNonGRPCErrors(t *testing.T) {
	err := errors.New("invalid DDL")

	got := userFacingError(err)

	assert.Equal(t, "invalid DDL", got)
}

func TestUserFacingErrorExplainsConfigOutsideAllowedDirs(t *testing.T) {
	err := &schemaConfigOutsideAllowedDirsError{
		Database:     "orders",
		DatabaseType: "mysql",
		SchemaPath:   "services/orders/schema",
	}

	got := userFacingError(err)

	assert.Contains(t, got, "SchemaBot found a `schemabot.yaml` configuration")
	assert.Contains(t, got, "Schema directory: `services/orders/schema`")
	assert.Contains(t, got, "`databases.orders.allowed_dirs`")
}

func TestUserFacingErrorDetailDoesNotWrapFormattedRemoteErrors(t *testing.T) {
	formatted := "SchemaBot could not reach the remote deployment `pie` for target `orders-staging`. No healthy upstream is available. Raw error: rpc error: code = Unavailable desc = no healthy upstream"

	got := userFacingErrorDetail(formatted)

	assert.Equal(t, formatted, got)
	assert.Equal(t, 1, strings.Count(got, "SchemaBot could not reach"))
	assert.Equal(t, 1, strings.Count(got, "Raw error:"))
}

func TestRenderUnsafeChangesBlocked_UsedByApplyFlow(t *testing.T) {
	// Verify RenderUnsafeChangesBlocked produces the expected blocking content
	data := templates.PlanCommentData{
		Database:    "testdb",
		Environment: "staging",
		IsMySQL:     true,
		Changes: []templates.KeyspaceChangeData{{
			Keyspace:   "testdb",
			Statements: []string{"DROP TABLE `users`"},
		}},
		HasUnsafeChanges: true,
		UnsafeChanges: []templates.UnsafeChangeData{{
			Table:  "users",
			Reason: "DROP TABLE removes all data",
		}},
	}

	rendered := templates.RenderUnsafeChangesBlocked(data)

	assert.Contains(t, rendered, "⛔ Unsafe Changes Detected")
	assert.Contains(t, rendered, "`users`")
	assert.Contains(t, rendered, "DROP TABLE removes all data")
	assert.Contains(t, rendered, "--allow-unsafe")
	assert.Contains(t, rendered, "schemabot apply -e staging --allow-unsafe")
}

func TestRenderUnsafeChangesBlocked_CustomDatabaseTypeHeader(t *testing.T) {
	data := templates.PlanCommentData{
		Database:     "testdb",
		Environment:  "staging",
		DatabaseType: "custom-engine",
		Changes: []templates.KeyspaceChangeData{{
			Keyspace:   "testdb",
			Statements: []string{"DROP TABLE `users`"},
		}},
		HasUnsafeChanges: true,
		UnsafeChanges: []templates.UnsafeChangeData{{
			Table:  "users",
			Reason: "DROP TABLE removes all data",
		}},
	}

	rendered := templates.RenderUnsafeChangesBlocked(data)

	assert.Contains(t, rendered, "## Schema Change Plan")
	assert.Contains(t, rendered, "schemabot apply -e staging --allow-unsafe")
}

func TestRenderUnsafeChangesBlocked_PreservesTenantInRetryCommand(t *testing.T) {
	data := templates.PlanCommentData{
		Database:    "testdb",
		Environment: "staging",
		Tenant:      "alpha",
		IsMySQL:     true,
		Changes: []templates.KeyspaceChangeData{{
			Keyspace:   "testdb",
			Statements: []string{"DROP TABLE `orders`"},
		}},
		UnsafeChanges: []templates.UnsafeChangeData{{
			Table:  "orders",
			Reason: "DROP TABLE removes all data",
		}},
	}

	rendered := templates.RenderUnsafeChangesBlocked(data)

	assert.Contains(t, rendered, "**Tenant**: `alpha`")
	assert.Contains(t, rendered, "schemabot apply -e staging --tenant alpha --allow-unsafe")
}
