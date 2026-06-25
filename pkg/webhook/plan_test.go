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
	// Plan comment should NOT say "--allow-unsafe enabled" since it wasn't
	assert.NotContains(t, rendered, "--allow-unsafe` enabled")
}

func TestRenderPlanComment_UnsafeWithAllowUnsafe(t *testing.T) {
	data := templates.PlanCommentData{
		Database:    "testdb",
		Environment: "staging",
		IsMySQL:     true,
		IsLocked:    true,
		AllowUnsafe: true,
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

	rendered := templates.RenderPlanComment(data)

	assert.Contains(t, rendered, "--allow-unsafe` enabled")
	assert.Contains(t, rendered, "`users`")
	assert.Contains(t, rendered, "apply-confirm -e staging --allow-unsafe")
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

	t.Run("apply-confirm hint preserves tenant", func(t *testing.T) {
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
		assert.Contains(t, rendered, "schemabot apply-confirm -e staging --tenant alpha")
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

	assert.Contains(t, rendered, "## Strata Schema Change Plan")
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

	assert.Contains(t, rendered, "## Custom Engine Schema Change Plan")
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

	assert.Contains(t, rendered, "## PostgreSQL Schema Change Plan")
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

	assert.Contains(t, rendered, "## Strata Schema Change Plan")
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

	assert.Contains(t, rendered, "## Custom Engine Schema Change Plan")
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
