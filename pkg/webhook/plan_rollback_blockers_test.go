package webhook

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/block/schemabot/pkg/api"
	ghclient "github.com/block/schemabot/pkg/github"
	ternv1 "github.com/block/schemabot/pkg/proto/ternv1"
	"github.com/block/schemabot/pkg/storage"
	"github.com/block/schemabot/pkg/tern"
	"github.com/block/schemabot/pkg/webhook/templates"
)

// rollbackBlockersTernClient answers a plan the way the PostgreSQL engine
// does for a namespace holding a foreign key added NOT VALID: the column is
// planned, and the table that kept the plan from saving the namespace's
// current schema is named with why.
type rollbackBlockersTernClient struct {
	tern.Client
}

func (c *rollbackBlockersTernClient) Plan(context.Context, *ternv1.PlanRequest) (*ternv1.PlanResponse, error) {
	return &ternv1.PlanResponse{
		PlanId: "plan-rollback-blockers",
		Engine: ternv1.Engine_ENGINE_POSTGRES,
		Changes: []*ternv1.SchemaChange{{
			Namespace: "public",
			TableChanges: []*ternv1.TableChange{{
				TableName:  "users",
				Ddl:        "ALTER TABLE users ADD COLUMN email text",
				ChangeType: ternv1.ChangeType_CHANGE_TYPE_ALTER,
			}},
			RollbackBlockers: []*ternv1.SkippedTable{{
				Table:  "orders",
				Reason: "holds a foreign key added NOT VALID, which a schema file cannot declare",
			}},
		}},
	}, nil
}

func (c *rollbackBlockersTernClient) IsRemote() bool   { return true }
func (c *rollbackBlockersTernClient) Endpoint() string { return "remote:9090" }

// A PostgreSQL namespace holds a table whose schema no schema file can
// describe, and a pull request adds a column to another table. The PR plan
// comment shows the column and says, before anyone applies, that the apply
// cannot be rolled back, which table is why, and how to undo the change
// instead. The plan travels through the server's plan path, so the comment
// shows what the server actually returns.
func TestPlanComment_SaysWhyTheApplyCannotBeRolledBack(t *testing.T) {
	cfg := &api.ServerConfig{
		Databases: map[string]api.DatabaseConfig{
			"orders": {Type: storage.DatabaseTypePostgres, Environments: map[string]api.EnvironmentConfig{
				"staging": {Deployment: "remote", Target: "orders-target"},
			}},
		},
		TernDeployments: api.TernConfig{"remote": {"staging": "remote:9090"}},
	}
	h := &Handler{
		service: api.New(&noUnsettledAppliesStorage{}, cfg, map[string]tern.Client{"remote/staging": &rollbackBlockersTernClient{}}, testLogger()),
		logger:  testLogger(),
	}
	planReq := planRetryTestRequest()
	planReq.Type = storage.DatabaseTypePostgres
	planReq.SchemaFiles = map[string]*ternv1.SchemaFiles{"public": {Files: map[string]string{
		"users.sql": "CREATE TABLE users (id bigint PRIMARY KEY, email text)",
	}}}

	_, planResp, err := h.executePlanProtoWithTransientRetry(t.Context(), planReq, "octocat/orders", 7)
	require.NoError(t, err)

	schema := &ghclient.SchemaRequestResult{Database: "orders", Type: storage.DatabaseTypePostgres}
	comment := templates.RenderPlanComment(buildPlanCommentData(schema, planResp, "staging", "", "testuser", "", ""))

	assert.Contains(t, comment, "ALTER TABLE users ADD COLUMN email text")
	assert.Contains(t, comment,
		"⚠️ No rollback for namespace `public`: SchemaBot couldn't save the current schema of the tables below, so an apply of this plan can't be rolled back. To undo the change, revert the schema files in a new pull request.\n\n"+
			"- `orders`: holds a foreign key added NOT VALID, which a schema file cannot declare\n")
}
