package api

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"testing"

	"github.com/block/schemabot/pkg/apitypes"
	"github.com/block/schemabot/pkg/engine"
	ternv1 "github.com/block/schemabot/pkg/proto/ternv1"
	"github.com/block/schemabot/pkg/state"
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
// withhold them — and the reviewed list is recorded on the stored plan, so a
// rollback, a resume, or a member's drift check is asked for the same
// exclusions instead of proposing to drop the tables the plan never captured.
func TestExecutePlanCarriesIgnoreTablesAndRecordsTheReviewedConfig(t *testing.T) {
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
	assert.Equal(t, []string{"flyway_schema_history", "never_existed"}, plans.created.IgnoreTables(),
		"the stored plan keeps the reviewed config, not the subset that matched here: "+
			"an entry this target lacks still names a table a member target holds, and a member "+
			"re-planning under the narrower subset would propose dropping it")
}

// An entry that withheld nothing while the plan proposes dropping the very
// table it names can only mean the exclusion never reached the planner: the
// target holds the table, so a data plane that honored the request would have
// withheld it. Every other unmatched shape names a table that is not there to
// drop. The plan is refused rather than stored and reviewed as a drop, which
// is the change the entry was written to prevent.
func TestExecutePlanRefusesADropOfAWithheldTable(t *testing.T) {
	plans := &capturingPlanStore{}
	mockClient := &mockTernClient{planResp: &ternv1.PlanResponse{
		PlanId: "plan-unhonored",
		Changes: []*ternv1.SchemaChange{{
			Namespace: "payments",
			TableChanges: []*ternv1.TableChange{{
				TableName: "flyway_schema_history", ChangeType: ternv1.ChangeType_CHANGE_TYPE_DROP,
				Ddl: "DROP TABLE `flyway_schema_history`",
			}},
		}},
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

	_, err := svc.ExecutePlan(t.Context(), PlanRequest{
		Database:    "payments",
		Environment: "staging",
		Type:        storage.DatabaseTypeMySQL,
		SchemaFiles: map[string]*ternv1.SchemaFiles{
			"payments": {Files: map[string]string{"users.sql": "CREATE TABLE users (id bigint primary key)"}},
		},
		IgnoreTables: []string{"flyway_schema_history"},
	})

	require.Error(t, err)
	assert.Contains(t, err.Error(), "flyway_schema_history")
	assert.Contains(t, err.Error(), "ignore_tables withholds")
	assert.Contains(t, err.Error(), "Upgrade that deployment to a build that supports ignore_tables",
		"the error names the remedy")
	assert.Nil(t, plans.created, "a plan that would drop a withheld table is not stored")
}

// An entry that matched nothing because the table is not on the target is the
// ordinary typo, and it is reported rather than refused: the plan proceeds and
// the unmatched entry reaches the pull request comment. Only a drop of the
// entry's own name proves the exclusion was not applied.
func TestExecutePlanKeepsPlanningWhenAnUnmatchedEntryIsNotDropped(t *testing.T) {
	plans := &capturingPlanStore{}
	mockClient := &mockTernClient{planResp: &ternv1.PlanResponse{
		PlanId: "plan-typo",
		Changes: []*ternv1.SchemaChange{{
			Namespace: "payments",
			TableChanges: []*ternv1.TableChange{{
				TableName: "legacy_audit_log", ChangeType: ternv1.ChangeType_CHANGE_TYPE_DROP,
				Ddl: "DROP TABLE `legacy_audit_log`",
			}},
		}},
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

	// The entry names one table; the plan drops a different one.
	resp, err := svc.ExecutePlan(t.Context(), PlanRequest{
		Database:    "payments",
		Environment: "staging",
		Type:        storage.DatabaseTypeMySQL,
		SchemaFiles: map[string]*ternv1.SchemaFiles{
			"payments": {Files: map[string]string{"users.sql": "CREATE TABLE users (id bigint primary key)"}},
		},
		IgnoreTables: []string{"flyway_schema_hist"},
	})

	require.NoError(t, err)
	require.NotNil(t, resp)
	assert.NotNil(t, plans.created, "the plan is stored and the unmatched entry is reported, not refused")
}

// rollbackSourcePlanStore answers the rollback's source-plan lookup and
// captures the rollback plan it stores.
type rollbackSourcePlanStore struct {
	capturingPlanStore
	source *storage.Plan
}

func (s *rollbackSourcePlanStore) GetByID(context.Context, int64) (*storage.Plan, error) {
	return s.source, nil
}

// newRollbackExemptService wires a service whose rollback re-plan is answered
// by resp, against a completed apply whose source plan was reviewed under an
// ignore_tables config.
func newRollbackExemptService(plans storage.PlanStore, resp *ternv1.PlanResponse) (*Service, *storage.Apply) {
	return newRollbackExemptServiceWithClient(plans, &mockTernClient{planResp: resp})
}

// newRollbackExemptServiceWithClient is newRollbackExemptService for a rollback
// whose re-plan is answered by something other than a plan, such as an engine
// refusal.
func newRollbackExemptServiceWithClient(plans storage.PlanStore, client tern.Client) (*Service, *storage.Apply) {
	apply := &storage.Apply{
		ID: 1, ApplyIdentifier: "apply_rollback", PlanID: 10,
		Database: "payments", DatabaseType: storage.DatabaseTypeMySQL,
		Repository: "org/repo", PullRequest: 7, Environment: "staging",
		Deployment: DefaultDeployment, State: state.Apply.Completed,
	}
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
		DefaultDeployment + "/staging": client,
	}, slog.New(slog.NewTextHandler(io.Discard, nil)))
	return svc, apply
}

// rollbackSourcePlan is a completed apply's plan, reviewed under an
// ignore_tables config. Its captured original files never included the
// withheld table, which is why the rollback must be asked to withhold it again.
func rollbackSourcePlan() *storage.Plan {
	plan := &storage.Plan{
		ID: 10, PlanIdentifier: "plan_source", Database: "payments",
		DatabaseType: storage.DatabaseTypeMySQL, Environment: "staging",
		Deployment: DefaultDeployment, Target: "payments-staging-target",
		Namespaces: map[string]*storage.NamespacePlanData{
			"payments": {
				OriginalFiles:         map[string]string{"users.sql": "CREATE TABLE users (id bigint primary key)"},
				OriginalFilesCaptured: true,
			},
		},
	}
	plan.RecordIgnoreTables([]string{"flyway_schema_history"})
	return plan
}

// A rollback re-plans the source plan's captured files against the live target,
// so it needs the same exclusions — and its own stored plan needs them too: a
// resume or a further rollback reads them back off that row, and a row that
// forgot them re-plans the withheld tables as drops.
func TestExecuteRollbackPlanCarriesAndRecordsTheReviewedIgnoreTables(t *testing.T) {
	plans := &rollbackSourcePlanStore{source: rollbackSourcePlan()}
	svc, apply := newRollbackExemptService(plans, &ternv1.PlanResponse{
		PlanId: "plan_rollback",
		Changes: []*ternv1.SchemaChange{{
			Namespace: "payments",
			TableChanges: []*ternv1.TableChange{{
				TableName: "users", ChangeType: ternv1.ChangeType_CHANGE_TYPE_ALTER,
				Ddl: "ALTER TABLE `users` DROP COLUMN `note`",
			}},
		}},
		ExemptTables: []*ternv1.ExemptTables{{
			Namespace: "payments",
			Tables:    []string{"flyway_schema_history"},
			Reason:    apitypes.ExemptReasonIgnoreTables,
		}},
	})

	resp, err := svc.ExecuteRollbackPlanForApply(t.Context(), apply)
	require.NoError(t, err)
	require.NotNil(t, resp)

	client := svc.ternClients[DefaultDeployment+"/staging"].(*mockTernClient)
	require.NotNil(t, client.planReq)
	assert.Equal(t, []string{"flyway_schema_history"}, client.planReq.GetIgnoreTables(),
		"the rollback re-plan withholds what the source plan was reviewed under")

	require.NotNil(t, plans.created)
	assert.Equal(t, []string{"flyway_schema_history"}, plans.created.IgnoreTables(),
		"the rollback's own stored plan keeps them for a re-plan of itself")
}

// A rollback re-plans schema files the recorded ignore_tables were never
// checked against, so an engine can refuse a contradiction the reviewed plan
// never had. That refusal tells the operator to edit the entry, which does
// nothing here: the rollback reads the entries frozen on the source plan, so
// the same edit leaves the same refusal. The failure says where the entries
// came from, so nobody spends the outage editing config.
func TestExecuteRollbackPlanSaysItsIgnoreTablesCameFromThePlan(t *testing.T) {
	plans := &rollbackSourcePlanStore{source: rollbackSourcePlan()}
	svc, apply := newRollbackExemptServiceWithClient(plans, &mockTernClient{
		planErr: engine.NewIgnoredTables([]string{"flyway_schema_history"}).
			RefuseDeclared("payments", []string{"flyway_schema_history"}),
	})

	_, err := svc.ExecuteRollbackPlanForApply(t.Context(), apply)

	require.Error(t, err)
	assert.Contains(t, err.Error(), "Remove the entry or the schema file",
		"the engine's own refusal still reaches the operator")
	assert.Contains(t, err.Error(), "This rollback uses the plan's recorded ignore_tables, not schemabot.yaml",
		"the remedy above it points at a config edit that cannot change a frozen record")
	assert.True(t, IsTerminalControlError(err))
	assert.Nil(t, plans.created)
}

// A rollback whose source plan recorded nothing has no provenance to explain,
// so its failures read as the engine wrote them.
func TestExecuteRollbackPlanWithoutIgnoreTablesAddsNoNote(t *testing.T) {
	source := rollbackSourcePlan()
	source.Namespaces["payments"].IgnoreTables = nil
	plans := &rollbackSourcePlanStore{source: source}
	svc, apply := newRollbackExemptServiceWithClient(plans,
		&mockTernClient{planErr: errors.New("plan the restored schema: target unreachable")})

	_, err := svc.ExecuteRollbackPlanForApply(t.Context(), apply)

	require.Error(t, err)
	assert.NotContains(t, err.Error(), "recorded ignore_tables")
}

// The refusal that protects a reviewed plan protects a rollback plan the same
// way. A data plane that discards the exclusion answers a rollback with a drop
// of the withheld table, and storing that plan would surface it for review as
// an ordinary drop.
func TestExecuteRollbackPlanRefusesADropOfAWithheldTable(t *testing.T) {
	plans := &rollbackSourcePlanStore{source: rollbackSourcePlan()}
	svc, apply := newRollbackExemptService(plans, &ternv1.PlanResponse{
		PlanId: "plan_rollback_unhonored",
		Changes: []*ternv1.SchemaChange{{
			Namespace: "payments",
			TableChanges: []*ternv1.TableChange{{
				TableName: "flyway_schema_history", ChangeType: ternv1.ChangeType_CHANGE_TYPE_DROP,
				Ddl: "DROP TABLE `flyway_schema_history`",
			}},
		}},
	})

	_, err := svc.ExecuteRollbackPlanForApply(t.Context(), apply)

	require.Error(t, err)
	assert.Contains(t, err.Error(), "ignore_tables withholds")
	assert.Contains(t, err.Error(), "flyway_schema_history")
	assert.True(t, IsTerminalControlError(err),
		"the same data plane answers every retry the same way, so the failure must not be re-driven")
	assert.Nil(t, plans.created, "a rollback plan that would drop a withheld table is not stored")
}
