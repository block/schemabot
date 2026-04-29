package webhook

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/block/schemabot/pkg/apitypes"
	"github.com/block/schemabot/pkg/storage"
)

func TestDDLMatchesStoredPlan(t *testing.T) {
	tests := []struct {
		name       string
		planResp   *apitypes.PlanResponse
		storedPlan *storage.Plan
		wantMatch  bool
	}{
		{
			name: "identical DDL matches",
			planResp: &apitypes.PlanResponse{
				Changes: []*apitypes.SchemaChangeResponse{
					{TableChanges: []*apitypes.TableChangeResponse{
						{DDL: "ALTER TABLE `users` ADD COLUMN `email` varchar(255)"},
					}},
				},
			},
			storedPlan: &storage.Plan{
				Namespaces: map[string]*storage.NamespacePlanData{
					"mydb": {Tables: []storage.TableChange{
						{DDL: "ALTER TABLE `users` ADD COLUMN `email` varchar(255)"},
					}},
				},
			},
			wantMatch: true,
		},
		{
			name: "different DDL does not match",
			planResp: &apitypes.PlanResponse{
				Changes: []*apitypes.SchemaChangeResponse{
					{TableChanges: []*apitypes.TableChangeResponse{
						{DDL: "ALTER TABLE `users` ADD COLUMN `email` varchar(255)"},
						{DDL: "ALTER TABLE `users` DROP COLUMN `old_field`"},
					}},
				},
			},
			storedPlan: &storage.Plan{
				Namespaces: map[string]*storage.NamespacePlanData{
					"mydb": {Tables: []storage.TableChange{
						{DDL: "ALTER TABLE `users` ADD COLUMN `email` varchar(255)"},
					}},
				},
			},
			wantMatch: false,
		},
		{
			name: "different DDL content does not match",
			planResp: &apitypes.PlanResponse{
				Changes: []*apitypes.SchemaChangeResponse{
					{TableChanges: []*apitypes.TableChangeResponse{
						{DDL: "ALTER TABLE `users` ADD COLUMN `email` varchar(500)"},
					}},
				},
			},
			storedPlan: &storage.Plan{
				Namespaces: map[string]*storage.NamespacePlanData{
					"mydb": {Tables: []storage.TableChange{
						{DDL: "ALTER TABLE `users` ADD COLUMN `email` varchar(255)"},
					}},
				},
			},
			wantMatch: false,
		},
		{
			name: "same DDL in different order matches",
			planResp: &apitypes.PlanResponse{
				Changes: []*apitypes.SchemaChangeResponse{
					{TableChanges: []*apitypes.TableChangeResponse{
						{DDL: "ALTER TABLE `orders` ADD INDEX `idx_status` (`status`)"},
						{DDL: "ALTER TABLE `users` ADD COLUMN `email` varchar(255)"},
					}},
				},
			},
			storedPlan: &storage.Plan{
				Namespaces: map[string]*storage.NamespacePlanData{
					"mydb": {Tables: []storage.TableChange{
						{DDL: "ALTER TABLE `users` ADD COLUMN `email` varchar(255)"},
						{DDL: "ALTER TABLE `orders` ADD INDEX `idx_status` (`status`)"},
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
