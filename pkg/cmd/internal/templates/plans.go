// plans.go renders the stored-plan listing for the plans command.
package templates

import (
	"fmt"
	"strings"
	"time"

	"github.com/block/schemabot/pkg/ui"
)

// PlanSummaryData is one stored plan row in the plans list.
type PlanSummaryData struct {
	PlanID      string
	Database    string
	Environment string
	// Source is the plan's provenance: "owner/repo#123" for a PR-generated
	// plan, "ad-hoc" for a plan created without one.
	Source    string
	CreatedAt time.Time
	// Changes is the rendered change summary, such as
	// "3 changes: 1 create, 2 alter · 1 unsafe".
	Changes string
}

// PlansListData drives WritePlansList.
type PlansListData struct {
	Limit    int
	MaxLimit int
	HasMore  bool
	// Last is the operator's creation-window filter, verbatim, for display.
	Last  string
	Plans []PlanSummaryData
}

// WritePlansList renders stored plans newest-first as a column table.
func WritePlansList(data PlansListData) {
	if len(data.Plans) == 0 {
		window := ""
		if data.Last != "" {
			window = fmt.Sprintf(" in the last %s", data.Last)
		}
		fmt.Printf("%sNo plans found%s%s\n", ANSIDim, window, ANSIReset)
		return
	}

	fmt.Printf("%sRecent plans%s%s\n\n", ANSIBold, ANSIReset, plansWindowSuffix(data))

	maxCreated := len("CREATED")
	maxID := len("PLAN ID")
	maxDB := len("DATABASE")
	maxEnv := len("ENV")
	maxSource := len("SOURCE")
	for _, p := range data.Plans {
		maxCreated = maxLen(maxCreated, len(ui.FormatTimeAgo(p.CreatedAt)))
		maxID = maxLen(maxID, len(p.PlanID))
		maxDB = maxLen(maxDB, len(p.Database))
		maxEnv = maxLen(maxEnv, len(p.Environment))
		maxSource = maxLen(maxSource, len(p.Source))
	}

	fmt.Printf("  %s%-*s  %-*s  %-*s  %-*s  %-*s  %s%s\n",
		ANSIDim,
		maxCreated, "CREATED",
		maxID, "PLAN ID",
		maxDB, "DATABASE",
		maxEnv, "ENV",
		maxSource, "SOURCE",
		"CHANGES",
		ANSIReset)
	for _, p := range data.Plans {
		fmt.Printf("  %-*s  %-*s  %-*s  %-*s  %-*s  %s\n",
			maxCreated, ui.FormatTimeAgo(p.CreatedAt),
			maxID, p.PlanID,
			maxDB, p.Database,
			maxEnv, p.Environment,
			maxSource, p.Source,
			p.Changes)
	}
	if data.HasMore {
		fmt.Printf("\n  %sShowing %d plans; more available — raise --limit (max %d) or narrow with filters%s\n",
			ANSIDim, len(data.Plans), data.MaxLimit, ANSIReset)
	}
}

func plansWindowSuffix(data PlansListData) string {
	var parts []string
	if data.Last != "" {
		parts = append(parts, fmt.Sprintf("last %s", data.Last))
	}
	if len(parts) == 0 {
		return ""
	}
	return fmt.Sprintf(" %s(%s)%s", ANSIDim, strings.Join(parts, ", "), ANSIReset)
}
