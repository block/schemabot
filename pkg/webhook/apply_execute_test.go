package webhook

import (
	"errors"
	"fmt"
	"testing"

	"github.com/block/schemabot/pkg/api"
	"github.com/block/schemabot/pkg/apitypes"
	"github.com/block/schemabot/pkg/schema"
	"github.com/block/schemabot/pkg/storage"
	"github.com/stretchr/testify/assert"
)

func TestApplyExecutionErrorMessage(t *testing.T) {
	t.Run("unsupported feature is actionable", func(t *testing.T) {
		err := &api.UnsupportedFeatureError{Database: "orders", DatabaseType: storage.DatabaseTypePostgres, Feature: schema.FeatureDeferredCutover}
		assert.Equal(t, `database "orders": deferred cutover is not supported for database_type: postgres`, applyExecutionErrorMessage(err))
	})

	t.Run("lock intent change remains actionable", func(t *testing.T) {
		assert.Contains(t, applyExecutionErrorMessage(fmt.Errorf("verify lock: %w", storage.ErrLockIntentChanged)), "review the latest plan")
	})

	t.Run("internal error remains sanitized", func(t *testing.T) {
		assert.Equal(t, "Failed to execute apply. See SchemaBot server logs for details.", applyExecutionErrorMessage(errors.New("secret DSN")))
	})
}

func TestDDLMatchesStoredPlan(t *testing.T) {
	tests := []struct {
		name       string
		planResp   *apitypes.PlanResponse
		storedPlan *storage.Plan
		wantMatch  bool
	}{
		{
			name: "identical change matches",
			planResp: &apitypes.PlanResponse{
				Changes: []*apitypes.SchemaChangeResponse{
					{Namespace: "mydb", TableChanges: []*apitypes.TableChangeResponse{
						{TableName: "users", ChangeType: "alter", DDL: "ALTER TABLE `users` ADD COLUMN `email` varchar(255)"},
					}},
				},
			},
			storedPlan: &storage.Plan{
				Namespaces: map[string]*storage.NamespacePlanData{
					"mydb": {Tables: []storage.TableChange{
						{Table: "users", Operation: "alter", DDL: "ALTER TABLE `users` ADD COLUMN `email` varchar(255)"},
					}},
				},
			},
			wantMatch: true,
		},
		{
			name: "extra change does not match",
			planResp: &apitypes.PlanResponse{
				Changes: []*apitypes.SchemaChangeResponse{
					{Namespace: "mydb", TableChanges: []*apitypes.TableChangeResponse{
						{TableName: "users", ChangeType: "alter", DDL: "ALTER TABLE `users` ADD COLUMN `email` varchar(255)"},
						{TableName: "users", ChangeType: "alter", DDL: "ALTER TABLE `users` DROP COLUMN `old_field`"},
					}},
				},
			},
			storedPlan: &storage.Plan{
				Namespaces: map[string]*storage.NamespacePlanData{
					"mydb": {Tables: []storage.TableChange{
						{Table: "users", Operation: "alter", DDL: "ALTER TABLE `users` ADD COLUMN `email` varchar(255)"},
					}},
				},
			},
			wantMatch: false,
		},
		{
			name: "different DDL content does not match",
			planResp: &apitypes.PlanResponse{
				Changes: []*apitypes.SchemaChangeResponse{
					{Namespace: "mydb", TableChanges: []*apitypes.TableChangeResponse{
						{TableName: "users", ChangeType: "alter", DDL: "ALTER TABLE `users` ADD COLUMN `email` varchar(500)"},
					}},
				},
			},
			storedPlan: &storage.Plan{
				Namespaces: map[string]*storage.NamespacePlanData{
					"mydb": {Tables: []storage.TableChange{
						{Table: "users", Operation: "alter", DDL: "ALTER TABLE `users` ADD COLUMN `email` varchar(255)"},
					}},
				},
			},
			wantMatch: false,
		},
		{
			name: "same changes in different order match",
			planResp: &apitypes.PlanResponse{
				Changes: []*apitypes.SchemaChangeResponse{
					{Namespace: "mydb", TableChanges: []*apitypes.TableChangeResponse{
						{TableName: "orders", ChangeType: "alter", DDL: "ALTER TABLE `orders` ADD INDEX `idx_status` (`status`)"},
						{TableName: "users", ChangeType: "alter", DDL: "ALTER TABLE `users` ADD COLUMN `email` varchar(255)"},
					}},
				},
			},
			storedPlan: &storage.Plan{
				Namespaces: map[string]*storage.NamespacePlanData{
					"mydb": {Tables: []storage.TableChange{
						{Table: "users", Operation: "alter", DDL: "ALTER TABLE `users` ADD COLUMN `email` varchar(255)"},
						{Table: "orders", Operation: "alter", DDL: "ALTER TABLE `orders` ADD INDEX `idx_status` (`status`)"},
					}},
				},
			},
			wantMatch: true,
		},
		{
			name: "same DDL under a different namespace does not match",
			planResp: &apitypes.PlanResponse{
				Changes: []*apitypes.SchemaChangeResponse{
					{Namespace: "keyspace_b", TableChanges: []*apitypes.TableChangeResponse{
						{TableName: "accounts", ChangeType: "create", DDL: "CREATE TABLE `accounts` (`id` bigint)"},
					}},
				},
			},
			storedPlan: &storage.Plan{
				Namespaces: map[string]*storage.NamespacePlanData{
					"keyspace_a": {Tables: []storage.TableChange{
						{Table: "accounts", Operation: "create", DDL: "CREATE TABLE `accounts` (`id` bigint)"},
					}},
				},
			},
			wantMatch: false,
		},
		{
			name: "same DDL against a different table does not match",
			planResp: &apitypes.PlanResponse{
				Changes: []*apitypes.SchemaChangeResponse{
					{Namespace: "mydb", TableChanges: []*apitypes.TableChangeResponse{
						{TableName: "customers", ChangeType: "alter", DDL: "ALTER TABLE `t` ADD COLUMN `email` varchar(255)"},
					}},
				},
			},
			storedPlan: &storage.Plan{
				Namespaces: map[string]*storage.NamespacePlanData{
					"mydb": {Tables: []storage.TableChange{
						{Table: "users", Operation: "alter", DDL: "ALTER TABLE `t` ADD COLUMN `email` varchar(255)"},
					}},
				},
			},
			wantMatch: false,
		},
		{
			name: "same DDL with a different operation does not match",
			planResp: &apitypes.PlanResponse{
				Changes: []*apitypes.SchemaChangeResponse{
					{Namespace: "mydb", TableChanges: []*apitypes.TableChangeResponse{
						{TableName: "events", ChangeType: "drop", DDL: "-- events"},
					}},
				},
			},
			storedPlan: &storage.Plan{
				Namespaces: map[string]*storage.NamespacePlanData{
					"mydb": {Tables: []storage.TableChange{
						{Table: "events", Operation: "create", DDL: "-- events"},
					}},
				},
			},
			wantMatch: false,
		},
		{
			name: "operation comparison is case-insensitive",
			planResp: &apitypes.PlanResponse{
				Changes: []*apitypes.SchemaChangeResponse{
					{Namespace: "mydb", TableChanges: []*apitypes.TableChangeResponse{
						{TableName: "users", ChangeType: "ALTER", DDL: "ALTER TABLE `users` ADD COLUMN `email` varchar(255)"},
					}},
				},
			},
			storedPlan: &storage.Plan{
				Namespaces: map[string]*storage.NamespacePlanData{
					"mydb": {Tables: []storage.TableChange{
						{Table: "users", Operation: "alter", DDL: "ALTER TABLE `users` ADD COLUMN `email` varchar(255)"},
					}},
				},
			},
			wantMatch: true,
		},
		{
			name: "empty response namespace matches default-normalized stored namespace",
			planResp: &apitypes.PlanResponse{
				Changes: []*apitypes.SchemaChangeResponse{
					{Namespace: "", TableChanges: []*apitypes.TableChangeResponse{
						{TableName: "users", ChangeType: "alter", DDL: "ALTER TABLE `users` ADD COLUMN `email` varchar(255)"},
					}},
				},
			},
			storedPlan: &storage.Plan{
				Namespaces: map[string]*storage.NamespacePlanData{
					"default": {Tables: []storage.TableChange{
						{Table: "users", Operation: "alter", DDL: "ALTER TABLE `users` ADD COLUMN `email` varchar(255)"},
					}},
				},
			},
			wantMatch: true,
		},
		{
			name: "empty plans match",
			planResp: &apitypes.PlanResponse{
				Changes: []*apitypes.SchemaChangeResponse{},
			},
			storedPlan: &storage.Plan{
				Namespaces: map[string]*storage.NamespacePlanData{},
			},
			wantMatch: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.wantMatch, ddlMatchesStoredPlan(tt.planResp, tt.storedPlan))
		})
	}
}

// The consent recorded on a pending confirmation only speaks for the apply the
// operator was actually shown. The lock carries no environment, so a disclosure
// earned confirming one environment must not disarm the copy gate in another,
// and a confirmation whose plan cannot be loaded must count as no disclosure at
// all rather than as consent.
func TestDisclosureDescribesThisApply(t *testing.T) {
	tests := []struct {
		name        string
		lock        *storage.Lock
		plan        *storage.Plan
		environment string
		want        bool
	}{
		{
			name:        "disclosure shown for this environment is consent",
			lock:        &storage.Lock{DisclosedCopyDiscard: true, PendingPlanID: "plan-1"},
			plan:        &storage.Plan{PlanIdentifier: "plan-1", Environment: "staging"},
			environment: "staging",
			want:        true,
		},
		{
			name:        "disclosure shown for another environment is not consent",
			lock:        &storage.Lock{DisclosedCopyDiscard: true, PendingPlanID: "plan-1"},
			plan:        &storage.Plan{PlanIdentifier: "plan-1", Environment: "staging"},
			environment: "production",
			want:        false,
		},
		{
			name:        "no disclosure is not consent",
			lock:        &storage.Lock{DisclosedCopyDiscard: false, PendingPlanID: "plan-1"},
			plan:        &storage.Plan{PlanIdentifier: "plan-1", Environment: "staging"},
			environment: "staging",
			want:        false,
		},
		{
			name:        "a disclosure whose plan did not load is not consent",
			lock:        &storage.Lock{DisclosedCopyDiscard: true, PendingPlanID: "plan-1"},
			plan:        nil,
			environment: "staging",
			want:        false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, disclosureDescribesThisApply(tt.lock, tt.plan, tt.environment))
		})
	}
}
