package templates

import (
	"fmt"
	"slices"
	"strings"

	"github.com/block/schemabot/pkg/engine"
	"github.com/block/schemabot/pkg/glyph"
)

// CollationChangeData is one existing column a plan moves onto another
// collation, as the engine reported it (see engine.CollationChange).
type CollationChangeData struct {
	Table  string
	Column string
	// To is empty when the change leaves the collation to a server default
	// the plan cannot read.
	From, To             string
	Case, TrailingSpaces engine.ComparisonChange
	UniqueIndexes        []string
}

// collationColumnsShown caps the column names one collation line lists, so a
// CONVERT TO CHARACTER SET on a wide table stays one readable line.
const collationColumnsShown = 8

// collationGroup is the columns of one table that make the same collation
// move, rendered as one line.
type collationGroup struct {
	table          string
	from, to       string
	caseChange     engine.ComparisonChange
	trailingSpaces engine.ComparisonChange
	columns        []string
	// unique lists, for each of the columns a unique index covers, the
	// indexes that cover it.
	unique []uniqueColumn
}

type uniqueColumn struct {
	column  string
	indexes []string
}

// makesSameMove reports whether column change c, on table, moves between the
// same collations with the same effect on comparisons as the group.
func (g collationGroup) makesSameMove(table string, c CollationChangeData) bool {
	return g.table == table && g.from == c.From && g.to == c.To &&
		g.caseChange == c.Case && g.trailingSpaces == c.TrailingSpaces
}

// writeCollationChangesSection renders what the plan does to the collation of
// existing columns, directly under the table sizes. A collation decides which
// values sort together and compare equal, so a change to it can change query
// results and make a unique index reject values it accepted before, and none
// of that shows in the DDL. Each line names the move and says how letter case
// and trailing spaces compare afterwards; a property the plan cannot read is
// called out rather than left unsaid.
//
// Like the size section, it renders nothing on a locked comment that applies
// without confirmation, and in a rollout it describes the reviewed target.
func writeCollationChangesSection(sb *strings.Builder, data PlanCommentData) {
	if data.applyingWithoutConfirmation() {
		return
	}
	groups := collationGroups(data.Changes)
	if len(groups) == 0 {
		return
	}
	sb.WriteString("🔤 **Collation changes**")
	if scope := reviewedTargetSizeScope(data.DeploymentDrift, inlineCode); scope != "" {
		fmt.Fprintf(sb, " (%s)", scope)
	}
	sb.WriteString(": these columns sort and compare under a new collation after the apply.\n")
	for _, g := range groups {
		writeCollationGroup(sb, g)
	}
	sb.WriteString("\n")
}

func writeCollationGroup(sb *strings.Builder, g collationGroup) {
	to := "the server's default collation"
	if g.to != "" {
		to = inlineCode(g.to)
	}
	fmt.Fprintf(sb, "- %s on %s: %s → %s\n", collationColumnList(g.columns), inlineCode(g.table), inlineCode(g.from), to)
	if g.to == "" {
		fmt.Fprintf(sb, "  - %s The new collation is not in the plan, so it cannot say how letter case and trailing spaces will compare.\n", glyph.Attention)
	} else {
		writeComparisonLine(sb, g.caseChange, caseComparison)
		writeComparisonLine(sb, g.trailingSpaces, trailingSpaceComparison)
		if g.caseChange == engine.ComparisonUnchanged && g.trailingSpaces == engine.ComparisonUnchanged {
			sb.WriteString("  - Letter case and trailing spaces compare as before. Accented and other characters can still sort and compare differently.\n")
		}
	}
	for _, u := range g.unique {
		noun := "unique index"
		if len(u.indexes) > 1 {
			noun = "unique indexes"
		}
		fmt.Fprintf(sb, "  - %s %s is in %s %s: the apply fails if two existing values compare equal under the new collation.\n",
			glyph.Attention, inlineCode(u.column), noun, strings.Join(inlineCodeList(u.indexes), ", "))
	}
}

// comparisonWording is how the section words one comparison property that a
// collation change moves.
type comparisonWording struct {
	becomesSensitive, becomesInsensitive, unknown string
}

var (
	caseComparison = comparisonWording{
		becomesSensitive:   "Comparisons become case-sensitive: `'abc'` and `'ABC'` stop comparing equal.",
		becomesInsensitive: "Comparisons become case-insensitive: `'abc'` and `'ABC'` start comparing equal.",
		unknown:            "The plan cannot read whether letter case is significant under the new collation.",
	}
	trailingSpaceComparison = comparisonWording{
		becomesSensitive:   "Trailing spaces become significant (NO PAD): `'abc'` and `'abc '` stop comparing equal.",
		becomesInsensitive: "Trailing spaces stop being significant (PAD SPACE): `'abc'` and `'abc '` start comparing equal.",
		unknown:            "The plan cannot read whether trailing spaces are significant under the new collation.",
	}
)

// writeComparisonLine renders one comparison property that moves, and nothing
// for one that does not.
func writeComparisonLine(sb *strings.Builder, change engine.ComparisonChange, wording comparisonWording) {
	var line string
	switch change {
	case engine.ComparisonUnchanged:
		return
	case engine.ComparisonBecomesSensitive:
		line = wording.becomesSensitive
	case engine.ComparisonBecomesInsensitive:
		line = wording.becomesInsensitive
	default:
		line = wording.unknown
	}
	fmt.Fprintf(sb, "  - %s %s\n", glyph.Attention, line)
}

func collationColumnList(columns []string) string {
	shown := columns[:min(len(columns), collationColumnsShown)]
	list := strings.Join(inlineCodeList(shown), ", ")
	if hidden := len(columns) - len(shown); hidden > 0 {
		list += fmt.Sprintf(" and %d more", hidden)
	}
	return list
}

// collationGroups merges the columns of each table that make the same move,
// in plan order. A table is qualified with its keyspace when more than one
// keyspace has collation changes.
func collationGroups(changes []KeyspaceChangeData) []collationGroup {
	keyspaces := 0
	for _, ks := range changes {
		if len(ks.CollationChanges) > 0 {
			keyspaces++
		}
	}
	var groups []collationGroup
	for _, ks := range changes {
		for _, c := range ks.CollationChanges {
			table := c.Table
			if keyspaces > 1 {
				table = ks.Keyspace + "." + c.Table
			}
			i := slices.IndexFunc(groups, func(g collationGroup) bool { return g.makesSameMove(table, c) })
			if i < 0 {
				groups = append(groups, collationGroup{table: table, from: c.From, to: c.To, caseChange: c.Case, trailingSpaces: c.TrailingSpaces})
				i = len(groups) - 1
			}
			groups[i].columns = append(groups[i].columns, c.Column)
			if len(c.UniqueIndexes) > 0 {
				groups[i].unique = append(groups[i].unique, uniqueColumn{column: c.Column, indexes: c.UniqueIndexes})
			}
		}
	}
	return groups
}
