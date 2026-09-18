package api

import (
	"io"
	"log/slog"
	"testing"

	"github.com/block/schemabot/pkg/apitypes"
	"github.com/block/schemabot/pkg/engine"
	ternv1 "github.com/block/schemabot/pkg/proto/ternv1"
	"github.com/block/schemabot/pkg/storage"
	"github.com/block/schemabot/pkg/tern"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestPlanResponseFromProtoCarriesExemptTables(t *testing.T) {
	response := planResponseFromProto(&ternv1.PlanResponse{ExemptTables: []*ternv1.ExemptTables{{
		Namespace: "app", Tables: []string{"orders_archive_2024"}, Reason: "archive naming",
	}}})
	require.Len(t, response.ExemptTables, 1)
	assert.Equal(t, "app", response.ExemptTables[0].Namespace)
	assert.Equal(t, []string{"orders_archive_2024"}, response.ExemptTables[0].Tables)
	assert.Equal(t, "archive naming", response.ExemptTables[0].Reason)
}

// apitypes carries its own copy of the exemption-reason vocabulary because it
// is dependency-free by design. This package imports both, so it is where the
// two are held together: a rename on the engine side that missed the wire side
// would leave the unmatched-entry report unable to tell a configured exclusion
// that withheld a table from one that withheld nothing.
func TestExemptReasonIgnoreTablesMatchesTheEngine(t *testing.T) {
	assert.Equal(t, engine.ExemptReasonIgnoreTables, apitypes.ExemptReasonIgnoreTables)
}

// Only the exclusions the repository asked for answer whether a configured
// ignore_tables entry matched anything, so the engine's own exemptions are not
// counted as tables the config withheld.
func TestWithheldTablesFromProtoCountsOnlyConfiguredExclusions(t *testing.T) {
	assert.Empty(t, withheldTablesFromProto(nil))

	withheld := withheldTablesFromProto([]*ternv1.ExemptTables{
		{Namespace: "app", Tables: []string{"legacy_audit_log"}, Reason: engine.ExemptReasonIgnoreTables},
		{Namespace: "app", Tables: []string{"orders_archive_2024"}, Reason: "archive naming"},
		nil,
		{Namespace: "billing", Tables: []string{"legacy_audit_log", "flyway_schema_history"}, Reason: engine.ExemptReasonIgnoreTables},
	})
	assert.Equal(t, []string{"flyway_schema_history", "legacy_audit_log"}, withheld)
}

func TestPlanResponseWithheldTables(t *testing.T) {
	assert.Nil(t, (*apitypes.PlanResponse)(nil).WithheldTables())

	response := planResponseFromProto(&ternv1.PlanResponse{ExemptTables: []*ternv1.ExemptTables{
		{Namespace: "app", Tables: []string{"legacy_audit_log"}, Reason: engine.ExemptReasonIgnoreTables},
		{Namespace: "app", Tables: []string{"orders_archive_2024"}, Reason: "archive naming"},
		{Namespace: "billing", Tables: []string{"legacy_audit_log"}, Reason: engine.ExemptReasonIgnoreTables},
	}})
	assert.Equal(t, []string{"legacy_audit_log"}, response.WithheldTables())
}

// The repository's ignore_tables travels to the data plane on the plan request
// — the tables live on the target, so the planner is the only thing that can
// withhold them — and the tables it reports withholding are recorded on the
// stored plan, so a rollback or resume re-plan withholds the same ones instead
// of proposing to drop the tables the plan never captured.
func TestExecutePlanCarriesIgnoreTablesAndRecordsWhatWasWithheld(t *testing.T) {
	plans := &capturingPlanStore{}
	mockClient := &mockTernClient{planResp: &ternv1.PlanResponse{
		PlanId: "plan-ignore-tables",
		Changes: []*ternv1.SchemaChange{{
			Namespace: "payments",
			TableChanges: []*ternv1.TableChange{{
				TableName: "users", ChangeType: ternv1.ChangeType_CHANGE_TYPE_ALTER,
				Ddl: "ALTER TABLE `users` ADD COLUMN `note` varchar(50)",
			}},
		}},
		ExemptTables: []*ternv1.ExemptTables{
			{Namespace: "payments", Tables: []string{"flyway_schema_history"}, Reason: engine.ExemptReasonIgnoreTables},
			{Namespace: "payments", Tables: []string{"orders_archive_2024"}, Reason: "archive naming"},
		},
	}}
	cfg := &ServerConfig{
		Databases: map[string]DatabaseConfig{
			"payments": {
				Type:         storage.DatabaseTypeMySQL,
				Environments: map[string]EnvironmentConfig{"staging": {Target: "payments-staging-target", Deployment: DefaultDeployment}},
			},
		},
		TernDeployments: TernConfig{DefaultDeployment: {"staging": "localhost:9090"}},
	}
	svc := New(&mockStorageWithPlanLookup{plans: plans}, cfg, map[string]tern.Client{
		DefaultDeployment + "/staging": mockClient,
	}, slog.New(slog.NewTextHandler(io.Discard, nil)))

	resp, err := svc.ExecutePlan(t.Context(), PlanRequest{
		Database:    "payments",
		Environment: "staging",
		Type:        storage.DatabaseTypeMySQL,
		SchemaFiles: map[string]*ternv1.SchemaFiles{
			"payments": {Files: map[string]string{"users.sql": "CREATE TABLE users (id bigint primary key, note varchar(50))"}},
		},
		// "never_existed" withholds nothing; the plan proceeds and the caller
		// is left able to report the unmatched entry.
		IgnoreTables: []string{"flyway_schema_history", "never_existed"},
	})
	require.NoError(t, err)

	require.NotNil(t, mockClient.planReq)
	assert.Equal(t, []string{"flyway_schema_history", "never_existed"}, mockClient.planReq.GetIgnoreTables())

	require.NotNil(t, resp)
	assert.Equal(t, []string{"flyway_schema_history"}, resp.WithheldTables(),
		"only the config's own exclusions answer whether a configured entry matched anything")

	require.NotNil(t, plans.created)
	assert.Equal(t, []string{"flyway_schema_history"}, plans.created.WithheldTables(),
		"the re-plan of this stored plan withholds the same tables")
}
