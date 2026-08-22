package templates

import (
	"time"

	"github.com/block/schemabot/pkg/ui"
)

func previewPlansListOutput() {
	WritePlansList(PlansListData{
		Limit:    20,
		MaxLimit: 200,
		HasMore:  true,
		Plans: []PlanSummaryData{
			{
				PlanID:      "plan-1700000000000000003",
				Database:    "orders-db",
				Environment: "staging",
				// Sources arrive pre-rendered by the command layer, so the
				// preview mirrors its terminal form: the short name linked to
				// the PR, falling back to the full URL off a terminal.
				Source:    ui.Link("acme/shop#412", "https://github.com/acme/shop/pull/412"),
				CreatedAt: previewTime.Add(-10 * time.Minute),
				Changes:   "3 changes: 1 create, 2 alter · ⚠️ 1 unsafe",
			},
			{
				PlanID:      "plan-1700000000000000002",
				Database:    "users-db",
				Environment: "production",
				Source:      ui.Link("acme/shop#410", "https://github.com/acme/shop/pull/410"),
				CreatedAt:   previewTime.Add(-2 * time.Hour),
				Changes:     "1 change: 1 alter",
			},
			{
				PlanID:      "plan-1700000000000000001",
				Database:    "products-db",
				Environment: "staging",
				Source:      "ad-hoc",
				CreatedAt:   previewTime.Add(-26 * time.Hour),
				Changes:     "no changes",
			},
		},
	})
}
