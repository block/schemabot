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

// unmatchedDropTablesTernClient answers a plan the way the MySQL engine does
// for a drop_tables list with one entry naming a live leftover and one with a
// typo: the leftover is planned as a drop, and the typo is reported as naming
// no table.
type unmatchedDropTablesTernClient struct {
	tern.Client
}

func (c *unmatchedDropTablesTernClient) Plan(context.Context, *ternv1.PlanRequest) (*ternv1.PlanResponse, error) {
	return &ternv1.PlanResponse{
		PlanId: "plan-drop-tables",
		Engine: ternv1.Engine_ENGINE_SPIRIT,
		Changes: []*ternv1.SchemaChange{{
			Namespace: "orders",
			TableChanges: []*ternv1.TableChange{{
				TableName:    "_jobs_old",
				Ddl:          "DROP TABLE `_jobs_old`",
				ChangeType:   ternv1.ChangeType_CHANGE_TYPE_DROP,
				IsUnsafe:     true,
				UnsafeReason: "DROP TABLE removes all data",
			}},
		}},
		UnmatchedDropTables: []string{"_jobs_olds"},
	}, nil
}

func (c *unmatchedDropTablesTernClient) IsRemote() bool   { return true }
func (c *unmatchedDropTablesTernClient) Endpoint() string { return "remote:9090" }

// noUnsettledAppliesStorage holds no apply that could still resume, so a
// drop_tables entry names no working table the server must protect.
type noUnsettledAppliesStorage struct {
	planRetryStorage
}

func (s *noUnsettledAppliesStorage) Applies() storage.ApplyStore { return noUnsettledApplyStore{} }

type noUnsettledApplyStore struct {
	storage.ApplyStore
}

func (noUnsettledApplyStore) GetUnsettled(context.Context) ([]*storage.Apply, error) { return nil, nil }

// A repository lists two leftovers under drop_tables, one spelled right and
// one with a typo. The PR plan comment shows the drop of the first and says
// the second names no table and drops nothing, so the typo does not hide
// behind an otherwise ordinary plan. The plan travels through the server's
// plan path, so the comment shows what the server actually returns.
func TestPlanComment_SaysWhichDropTablesEntryNamesNoTable(t *testing.T) {
	cfg := &api.ServerConfig{
		Databases: map[string]api.DatabaseConfig{
			"orders": {Type: storage.DatabaseTypeMySQL, Environments: map[string]api.EnvironmentConfig{
				"staging": {Deployment: "remote", Target: "orders-target"},
			}},
		},
		TernDeployments: api.TernConfig{"remote": {"staging": "remote:9090"}},
	}
	h := &Handler{
		service: api.New(&noUnsettledAppliesStorage{}, cfg, map[string]tern.Client{"remote/staging": &unmatchedDropTablesTernClient{}}, testLogger()),
		logger:  testLogger(),
	}
	planReq := planRetryTestRequest()
	planReq.DropTables = []string{"_jobs_old", "_jobs_olds"}

	_, planResp, err := h.executePlanProtoWithTransientRetry(t.Context(), planReq, "octocat/orders", 7)
	require.NoError(t, err)

	schema := &ghclient.SchemaRequestResult{Database: "orders", Type: "mysql"}
	comment := templates.RenderPlanComment(buildPlanCommentData(schema, planResp, "staging", "", "testuser", "", ""))

	assert.Contains(t, comment, "DROP TABLE `_jobs_old`")
	assert.Contains(t, comment,
		"⚠️ `drop_tables` entry `_jobs_olds` names no table on the target, so this plan drops nothing for it. Fix the name, or remove the entry if the table is already gone.\n")
	assert.NotContains(t, comment, "entry `_jobs_old` names no table", "the entry that planned a drop is not reported")
}
