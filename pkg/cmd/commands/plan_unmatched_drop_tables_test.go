package commands

import (
	"context"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/block/schemabot/pkg/api"
	"github.com/block/schemabot/pkg/apitypes"
	ternv1 "github.com/block/schemabot/pkg/proto/ternv1"
	"github.com/block/schemabot/pkg/storage"
	"github.com/block/schemabot/pkg/tern"
)

const cliUnmatchedDropLine = `⚠️  drop_tables entry "_jobs_olds" names no table on the target, so this plan drops nothing for it. Fix the name, or remove the entry if the table is already gone.`

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
			Namespace: "testdb",
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

// planOnlyStorage stores plans without keeping them and holds no apply that
// could still resume. Only the calls a plan request makes are implemented.
type planOnlyStorage struct {
	storage.Storage
}

func (planOnlyStorage) Plans() storage.PlanStore    { return discardingPlanStore{} }
func (planOnlyStorage) Applies() storage.ApplyStore { return noUnsettledApplyStore{} }

type discardingPlanStore struct {
	storage.PlanStore
}

func (discardingPlanStore) Create(context.Context, *storage.Plan) (int64, error) { return 1, nil }

type noUnsettledApplyStore struct {
	storage.ApplyStore
}

func (noUnsettledApplyStore) GetUnsettled(context.Context) ([]*storage.Apply, error) { return nil, nil }

// A repository lists two leftovers under drop_tables, one spelled right and
// one with a typo. `schemabot plan` shows the drop of the first and says the
// second names no table and drops nothing, the same sentence the PR plan
// comment carries. The CLI talks to the server's own plan endpoint, so the
// output shows what the server actually returns.
func TestPlanCmd_SaysWhichDropTablesEntryNamesNoTable(t *testing.T) {
	cfg := &api.ServerConfig{
		Databases: map[string]api.DatabaseConfig{
			"testdb": {Type: storage.DatabaseTypeMySQL, Environments: map[string]api.EnvironmentConfig{
				"staging": {Deployment: "remote", Target: "testdb"},
			}},
		},
		TernDeployments: api.TernConfig{"remote": {"staging": "remote:9090"}},
	}
	svc := api.New(planOnlyStorage{}, cfg, map[string]tern.Client{"remote/staging": &unmatchedDropTablesTernClient{}}, slog.New(slog.DiscardHandler))
	mux := http.NewServeMux()
	svc.ConfigureRoutes(mux)
	server := httptest.NewServer(mux)
	t.Cleanup(server.Close)

	dir := writeTestSchemaDir(t)
	require.NoError(t, os.WriteFile(filepath.Join(dir, "schemabot.yaml"),
		[]byte("database: testdb\ntype: mysql\ndrop_tables:\n  - _jobs_old\n  - _jobs_olds\n"), 0o644))

	cmd := PlanCmd{SchemaDir: dir, Environment: "staging"}
	var runErr error
	out := stripAnsi(captureStdout(func() { runErr = cmd.Run(&Globals{Endpoint: server.URL}) }))
	require.NoError(t, runErr, out)

	assert.Contains(t, out, "DROP TABLE `_jobs_old`", out)
	assert.Contains(t, out, cliUnmatchedDropLine+"\n", out)
	assert.NotContains(t, out, `entry "_jobs_old" names no table`, "the entry that planned a drop is not reported")
}

// The plan body reports an entry that names no table on both of its branches,
// in the plan and apply layouts: under the summary when there are changes,
// and under the no-changes message when there are none, which is where a
// typo would otherwise pass for a clean plan.
func TestWritePlanBody_UnmatchedDropTablesReported(t *testing.T) {
	withChanges := planWithTablesAndEngine("spirit", &apitypes.TableChangeResponse{
		TableName: "_jobs_old", DDL: "DROP TABLE `_jobs_old`;", ChangeType: "drop", IsUnsafe: true, UnsafeReason: "DROP TABLE removes all data",
	})
	withChanges.UnmatchedDropTables = []string{"_jobs_olds"}
	noChanges := &apitypes.PlanResponse{Engine: "spirit", UnmatchedDropTables: []string{"_jobs_olds"}}

	for name, isApply := range map[string]bool{"plan": false, "apply": true} {
		t.Run(name, func(t *testing.T) {
			out := stripAnsi(captureStdout(func() { writePlanBody(withChanges, isApply) }))
			summaryAt := strings.Index(out, "📋 Plan: 1 table to drop")
			lineAt := strings.Index(out, cliUnmatchedDropLine)
			assert.Greater(t, lineAt, summaryAt, "the line follows the summary:\n%s", out)

			out = stripAnsi(captureStdout(func() { writePlanBody(noChanges, isApply) }))
			noChangesAt := strings.Index(out, "No schema changes detected")
			lineAt = strings.Index(out, cliUnmatchedDropLine)
			assert.Greater(t, lineAt, noChangesAt, "the line follows the no-changes message:\n%s", out)
		})
	}
}

// Two environments with the same DDL but different unmatched entries do not
// read the same, so they render as separate sections.
func TestOutputMultiEnvPlanResult_UnmatchedDropTablesDivergeSections(t *testing.T) {
	staging := planWithTablesAndEngine("spirit", createUsers())
	staging.UnmatchedDropTables = []string{"_jobs_olds"}
	production := planWithTablesAndEngine("spirit", createUsers())

	out := stripAnsi(captureStdout(func() {
		outputMultiEnvPlanResult(map[string]*apitypes.PlanResponse{"staging": staging, "production": production}, "testdb", "testdb")
	}))

	assert.NotContains(t, out, "Staging & Production")
	assert.Equal(t, 1, strings.Count(out, cliUnmatchedDropLine), out)
}
