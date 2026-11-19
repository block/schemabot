package presentation

import (
	"fmt"

	"github.com/block/schemabot/pkg/ddl"
	"github.com/block/schemabot/pkg/glyph"
	"github.com/block/schemabot/pkg/ui"
)

// The phrases below are how a table's line reads in a phase where the CLI and
// the PR comment would otherwise each word it their own way. Both surfaces
// render a table's line from them, so one table reads the same on both
// (UX-7).

// IsInstantAlter reports whether a table should be described as applying
// instantly: its engine flagged the change instant and it is an ALTER. Other
// operations may complete without a row copy, but they are not instant DDL,
// so they keep their generic applying labels. The change type is populated
// for every stored DDL change (storage rejects a blank operation), so an
// empty value means an unknown operation, not a missing field.
func IsInstantAlter(isInstant bool, changeType string) bool {
	return isInstant && ddl.OpToStatementType(changeType) == ddl.StatementAlterTable
}

// createsOrDropsTable reports whether a change creates or drops a table, which
// has no row copy and no cutover of its own.
func createsOrDropsTable(changeType string) bool {
	op := ddl.OpToStatementType(changeType)
	return op == ddl.StatementCreateTable || op == ddl.StatementDropTable
}

// Complete is the state of a table whose change completed, on one target or
// on every target it ran on.
const Complete = "✅ Complete"

// CompletePhrase is the state of a table whose change completed, naming an
// instant change as one.
func CompletePhrase(isInstant bool, changeType string) string {
	if IsInstantAlter(isInstant, changeType) {
		return "⚡ Applied instantly"
	}
	return Complete
}

// ApplyingPhrase is the state of a running table with no rows to count: a
// change with no row copy, or one whose engine has not reported its rows yet.
func ApplyingPhrase(isInstant bool, changeType string) string {
	switch {
	case IsInstantAlter(isInstant, changeType):
		return "Applying instantly..."
	case createsOrDropsTable(changeType):
		return "Applying..."
	default:
		return "Running..."
	}
}

// CuttingOverPhrase is the state of a table its engine is cutting over. A
// table created or dropped has no cutover of its own, so it reads as applying.
func CuttingOverPhrase(changeType string) string {
	if createsOrDropsTable(changeType) {
		return "🔄 Applying..."
	}
	return "🔄 Cutting over..."
}

// FailedTableHeadline is the bar and state of a table whose change failed. A
// bar says row copy happened, so a change that failed before it copied a row,
// such as one a preflight check rejected, reads without one rather than as a
// copy that started and stalled. An instant change has no row copy phase, so
// its failure does not mention one.
func FailedTableHeadline(percent int, rowsCopied int64, isInstant bool) string {
	switch pct := ui.RowCopyDisplayPercent(percent, rowsCopied); {
	case pct > 0:
		return ui.ProgressBarFailed(pct) + " " + glyph.Failed + " Failed"
	case isInstant:
		return glyph.Failed + " Failed"
	default:
		return glyph.Failed + " Failed (before row copy started)"
	}
}

// RetryingTableHeadline is the bar and state of a table interrupted by a
// recoverable failure that SchemaBot retries on its own. The bar is yellow,
// since the normal path is suspended, and is left out when no row was copied,
// for the same reason as a failed table's. attempt is the retry under way,
// counted from 1 of maxAttempts; zero means the retry count is not known, as
// when a data plane retries on its own, so the line names no number.
func RetryingTableHeadline(percent int, rowsCopied int64, attempt, maxAttempts int) string {
	phrase := "🔄 Interrupted — retrying automatically"
	if attempt > 0 {
		phrase += fmt.Sprintf(" (attempt %d/%d)", attempt, maxAttempts)
	}
	if pct := ui.RowCopyDisplayPercent(percent, rowsCopied); pct > 0 {
		return ui.ProgressBar(pct, ui.ColorYellow) + " " + phrase
	}
	return phrase
}

// CancelledTableBar is the bar of a cancelled table that had copied rows, at
// the point the cancel stopped it: orange, as an operator halted it. A table
// cancelled before it copied a row has no bar.
func CancelledTableBar(percent int, rowsCopied int64) (string, bool) {
	pct := ui.RowCopyDisplayPercent(percent, rowsCopied)
	if percent <= 0 && rowsCopied <= 0 {
		return "", false
	}
	return ui.ProgressBar(pct, ui.ColorOrange), true
}
