package spirit

import (
	"fmt"
	"log/slog"
	"strings"

	"github.com/block/spirit/pkg/statement"

	"github.com/block/schemabot/pkg/engine"
)

// plannedCollationChanges reports each existing column whose collation the
// ALTER moves, resolved the way MySQL applies the statement: a MODIFY that
// names no collation picks up the table default the same statement sets, and
// CONVERT TO CHARACTER SET re-collates every character column. A column whose
// new collation depends on a default the definitions do not carry is reported
// with an empty To, so the plan says it cannot tell rather than nothing.
func plannedCollationChanges(logger *slog.Logger, alterSQL, currentCreate, desiredCreate string) ([]engine.CollationChange, error) {
	stmts, err := statement.New(alterSQL)
	if err != nil {
		return nil, fmt.Errorf("parse planned ALTER: %w", err)
	}
	if len(stmts) != 1 {
		return nil, fmt.Errorf("planned ALTER parsed into %d statements, expected 1", len(stmts))
	}
	current, err := statement.ParseCreateTable(currentCreate)
	if err != nil {
		return nil, fmt.Errorf("parse current definition: %w", err)
	}
	desired, err := statement.ParseCreateTable(desiredCreate)
	if err != nil {
		return nil, fmt.Errorf("parse desired definition: %w", err)
	}
	tableDefault := current.TableDefault()

	var changes []engine.CollationChange
	for _, col := range current.GetColumns() {
		if !col.CarriesCharset() {
			continue
		}
		cs, collation := col.EffectiveCharsetCollation(current)
		if cs == "" {
			logger.Warn("the current definition does not determine a column's charset; the plan will not report its collation",
				"table", current.TableName, "column", col.Name)
			continue
		}
		change, determined, err := stmts[0].ColumnCollationChange(col.Name, statement.CharsetCollation{Charset: cs, Collation: collation}, tableDefault)
		if err != nil {
			return nil, fmt.Errorf("resolve the collation of column %q after the ALTER: %w", col.Name, err)
		}
		if determined && !change.Changed() {
			continue
		}
		if determined && change.After.Charset == "" {
			logger.Debug("column stops carrying a charset; its type change is not a collation change",
				"table", current.TableName, "column", col.Name)
			continue
		}
		to := change.After.Collation
		if !determined {
			logger.Debug("column's collation after the ALTER depends on a server default; the plan will report it as unknown",
				"table", current.TableName, "column", col.Name)
			to = ""
		}
		before := collationProperties(logger, change.Before.Collation)
		after := collationProperties(logger, to)
		changes = append(changes, engine.CollationChange{
			Column:         col.Name,
			From:           change.Before.Collation,
			To:             to,
			Case:           compareChange(before, after, caseSensitive),
			TrailingSpaces: compareChange(before, after, trailingSpacesSensitive),
			UniqueIndexes:  uniqueIndexesAtRisk(desired, col.Name, change, before, after),
		})
	}
	return changes, nil
}

func caseSensitive(p statement.CollationProperties) bool { return p.CaseSensitive }

// trailingSpacesSensitive is the inverse of PAD SPACE: a PAD SPACE collation
// pads the shorter value with spaces before comparing, so 'abc' = 'abc '.
func trailingSpacesSensitive(p statement.CollationProperties) bool { return !p.PadSpace }

// collationProperties looks up a collation's comparison properties, nil when
// the collation is not known or has no properties this build can read.
func collationProperties(logger *slog.Logger, collation string) *statement.CollationProperties {
	if collation == "" {
		return nil
	}
	props, err := statement.LookupCollationProperties(collation)
	if err != nil {
		logger.Warn("collation properties unavailable; the plan will report the column's comparisons as possibly changing",
			"collation", collation, "error", err)
		return nil
	}
	return &props
}

// compareChange classifies how one comparison property moves between two
// collations. It fails closed: a side whose properties are not known is a
// possible change.
func compareChange(before, after *statement.CollationProperties, sensitive func(statement.CollationProperties) bool) engine.ComparisonChange {
	if before == nil || after == nil {
		return engine.ComparisonUnknown
	}
	switch {
	case sensitive(*before) == sensitive(*after):
		return engine.ComparisonUnchanged
	case sensitive(*after):
		return engine.ComparisonBecomesSensitive
	default:
		return engine.ComparisonBecomesInsensitive
	}
}

// uniqueIndexesAtRisk names the unique indexes covering column when the move
// can make values that compared unequal start comparing equal, since those are
// the indexes the apply fails on if existing rows collide. A move it cannot do
// that for lists none, so the plan does not warn about a collision that cannot
// happen.
func uniqueIndexesAtRisk(table *statement.CreateTable, column string, change statement.ColumnCollationChange, before, after *statement.CollationProperties) []string {
	if !canMergeValues(change, before, after) {
		return nil
	}
	return uniqueIndexesCovering(table, column)
}

// canMergeValues reports whether a collation move can make two values that
// compared unequal start comparing equal. Only a move onto a binary collation
// of the same charset is known not to: it compares code points or bytes, so
// it calls two values equal only when they are identical, apart from the
// trailing spaces it ignores when it pads and the old collation did not. A
// collation the plan cannot read can merge values.
func canMergeValues(change statement.ColumnCollationChange, before, after *statement.CollationProperties) bool {
	if before == nil || after == nil {
		return true
	}
	if !after.Binary || !strings.EqualFold(change.Before.Charset, change.After.Charset) {
		return true
	}
	return after.PadSpace && !before.PadSpace
}

// uniqueIndexesCovering names the primary key and unique indexes that include
// column, in definition order.
func uniqueIndexesCovering(table *statement.CreateTable, column string) []string {
	var names []string
	for _, index := range table.GetIndexes() {
		if !isUniqueIndex(index) {
			continue
		}
		for _, part := range index.ColumnList {
			if strings.EqualFold(part.Name, column) {
				names = append(names, uniqueIndexName(index))
				break
			}
		}
	}
	return names
}

func isUniqueIndex(index statement.Index) bool {
	return strings.EqualFold(index.Type, "PRIMARY KEY") || strings.EqualFold(index.Type, "UNIQUE")
}

func uniqueIndexName(index statement.Index) string {
	if strings.EqualFold(index.Type, "PRIMARY KEY") {
		return "PRIMARY"
	}
	return index.Name
}
