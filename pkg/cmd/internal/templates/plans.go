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
	// Source is the plan's provenance, pre-rendered for the operator the same
	// way the status list renders an apply's source: the short "owner/repo#pr"
	// as an OSC 8 hyperlink on an interactive terminal (the full URL otherwise),
	// the repository alone when the plan names no PR, and "ad-hoc" for a plan
	// created without either. It may carry zero-width escape bytes, so it
	// renders as the unpadded last column.
	Source    string
	CreatedAt time.Time
	// Changes is the rendered change summary, such as
	// "1 create, 2 alter · ⚠️".
	Changes string
	// UnsafeCount and BlockedCount say which safety markers the change
	// summary carries, so the table can print a legend naming exactly the
	// markers on display and nothing when the listing is clean.
	UnsafeCount  int
	BlockedCount int
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

// WritePlansList renders stored plans newest-first as a column table, in the
// status list's column order: the identifier first, then the target, then what
// the plan holds, with the linkable source last so its escape bytes never sit
// inside a padded column.
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

	maxID := len("PLAN ID")
	maxDB := len("DATABASE")
	maxEnv := len("ENV")
	maxChanges := len("CHANGES")
	maxCreated := len("CREATED")
	for _, p := range data.Plans {
		maxID = maxLen(maxID, len(p.PlanID))
		maxDB = maxLen(maxDB, len(p.Database))
		maxEnv = maxLen(maxEnv, len(p.Environment))
		maxChanges = maxLen(maxChanges, ui.VisibleWidth(p.Changes))
		maxCreated = maxLen(maxCreated, len(ui.FormatTimeAgo(p.CreatedAt)))
	}

	fmt.Printf("  %s%-*s  %-*s  %-*s  %-*s  %-*s  %s%s\n",
		ANSIDim,
		maxID, "PLAN ID",
		maxDB, "DATABASE",
		maxEnv, "ENV",
		maxChanges, "CHANGES",
		maxCreated, "CREATED",
		"SOURCE",
		ANSIReset)
	for _, p := range data.Plans {
		// The change summary can carry emoji markers whose byte, rune, and
		// terminal-cell counts all differ, so it is padded by visible width
		// rather than %-*s's byte count.
		paddedChanges := ui.PadVisible(p.Changes, maxChanges)
		fmt.Printf("  %-*s  %-*s  %-*s  %s  %-*s  %s\n",
			maxID, p.PlanID,
			maxDB, p.Database,
			maxEnv, p.Environment,
			paddedChanges,
			maxCreated, ui.FormatTimeAgo(p.CreatedAt),
			p.Source)
	}
	if legend := changesLegend(data.Plans); legend != "" {
		fmt.Printf("\n  %s%s%s\n", ANSIDim, legend, ANSIReset)
	}
	if data.HasMore {
		fmt.Printf("\n  %sShowing %d plans; more available — raise --limit (max %d) or narrow with filters%s\n",
			ANSIDim, len(data.Plans), data.MaxLimit, ANSIReset)
	}
}

// MarkerUnsafe and MarkerBlocked are the safety markers a plan's change
// summary carries. The summary is built where the plan data is read and the
// legend is printed here, so both sides take the glyph from these constants —
// a legend that named a marker the table no longer prints would be worse than
// no legend at all.
const (
	MarkerUnsafe  = "⚠️"
	MarkerBlocked = "⛔"
)

// changesLegend names the safety markers appearing in the listed plans'
// change summaries, so a marker is never a symbol the operator has to guess
// at. A listing without markers prints no legend at all.
func changesLegend(plans []PlanSummaryData) string {
	var hasUnsafe, hasBlocked bool
	for _, p := range plans {
		hasUnsafe = hasUnsafe || p.UnsafeCount > 0
		hasBlocked = hasBlocked || p.BlockedCount > 0
	}
	var parts []string
	if hasUnsafe {
		parts = append(parts, MarkerUnsafe+" unsafe change")
	}
	if hasBlocked {
		parts = append(parts, MarkerBlocked+" blocked change")
	}
	return strings.Join(parts, " · ")
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
