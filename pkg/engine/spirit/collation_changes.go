package spirit

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"github.com/block/spirit/pkg/parser/charset"
	"github.com/block/spirit/pkg/statement"

	"github.com/block/schemabot/pkg/engine"
)

// plannedCollationChanges reports each existing column whose collation the
// ALTER moves, resolved the way MySQL applies the statement: a MODIFY that
// names no collation picks up the table default the same statement sets, and
// CONVERT TO CHARACTER SET re-collates every character column. A definition
// that names a charset and no collation takes the server's default for that
// charset, which defaultCollation reads from the target. A column whose new
// collation depends on a default the plan cannot read is reported with an
// empty To, so the plan says it cannot tell rather than nothing.
func plannedCollationChanges(logger *slog.Logger, alterSQL, currentCreate, desiredCreate string, defaultCollation func(charset string) (string, error)) ([]engine.CollationChange, error) {
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
	resolve := func(column string, cc statement.CharsetCollation) string {
		collation, err := resolvedCollation(cc, defaultCollation)
		if err != nil {
			logger.Warn("the target's default collation for a charset is unavailable; the plan will report the column's comparisons as unknown",
				"table", current.TableName, "column", column, "charset", cc.Charset, "error", err)
		}
		return collation
	}

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
		from := resolve(col.Name, change.Before)
		to := resolve(col.Name, change.After)
		if from != "" && strings.EqualFold(from, to) {
			logger.Debug("column's collation resolves to the one it has; the ALTER does not re-collate it",
				"table", current.TableName, "column", col.Name, "collation", from)
			continue
		}
		if to == "" {
			logger.Debug("column's collation after the ALTER depends on a default the plan cannot read; the plan will report it as unknown",
				"table", current.TableName, "column", col.Name)
		}
		before := collationProperties(logger, from)
		after := collationProperties(logger, to)
		merges := canMergeValues(change, before, after)
		var unique []string
		if merges {
			unique = uniqueIndexesCovering(desired, col.Name)
		}
		changes = append(changes, engine.CollationChange{
			Column:         col.Name,
			From:           from,
			To:             to,
			Case:           compareChange(before, after, caseSensitive),
			TrailingSpaces: compareChange(before, after, trailingSpacesSensitive),
			CanMergeValues: merges,
			UniqueIndexes:  unique,
		})
	}
	return changes, nil
}

// resolvedCollation is the collation a column compares under: the one its
// definition names, or for a definition that names only a charset, the
// server's default for that charset. It is empty when neither is known.
func resolvedCollation(cc statement.CharsetCollation, defaultCollation func(charset string) (string, error)) (string, error) {
	if cc.Collation != "" || cc.Charset == "" {
		return cc.Collation, nil
	}
	collation, err := defaultCollation(cc.Charset)
	if err != nil {
		return "", fmt.Errorf("read the default collation of charset %q: %w", cc.Charset, err)
	}
	return collation, nil
}

// collationDefaultProbeTimeout bounds each read of a charset's default
// collation from the target. The collation report is informational, so a slow
// target leaves a column's comparisons unknown rather than stalling the plan.
const collationDefaultProbeTimeout = 5 * time.Second

// targetCollationDefaults reads the collation the target gives each charset
// that a definition names without a collation. The default differs between
// server versions, so the plan asks the target rather than assuming one. Each
// answer, including a failure, is cached for the plan, so a plan that
// re-collates many columns reads each charset once. It is not safe for
// concurrent use; the plan reports its tables one at a time.
type targetCollationDefaults struct {
	target *lazyTargetDB
	cache  map[string]collationDefault
}

type collationDefault struct {
	collation string
	err       error
}

func (d *targetCollationDefaults) defaultCollation(ctx context.Context, charset string) (string, error) {
	key := strings.ToLower(charset)
	if cached, ok := d.cache[key]; ok {
		return cached.collation, cached.err
	}
	collation, err := d.read(ctx, charset)
	if d.cache == nil {
		d.cache = make(map[string]collationDefault)
	}
	d.cache[key] = collationDefault{collation: collation, err: err}
	return collation, err
}

func (d *targetCollationDefaults) read(ctx context.Context, charset string) (string, error) {
	ctx, cancel := context.WithTimeout(ctx, collationDefaultProbeTimeout)
	defer cancel()
	db, err := d.target.get(ctx)
	if err != nil {
		return "", fmt.Errorf("connect for the default collation of charset %q: %w", charset, err)
	}
	var collation string
	err = db.QueryRowContext(ctx,
		"SELECT DEFAULT_COLLATE_NAME FROM information_schema.CHARACTER_SETS WHERE CHARACTER_SET_NAME = ?", charset).Scan(&collation)
	if errors.Is(err, sql.ErrNoRows) {
		return "", fmt.Errorf("the target has no charset %q", charset)
	}
	if err != nil {
		return "", fmt.Errorf("query the default collation of charset %q: %w", charset, err)
	}
	return collation, nil
}

func caseSensitive(c *charset.Collation) bool { return c.CaseSensitive }

// trailingSpacesSensitive is the inverse of PAD SPACE: a PAD SPACE collation
// pads the shorter value with spaces before comparing, so 'abc' = 'abc '.
func trailingSpacesSensitive(c *charset.Collation) bool { return !padsSpaces(c) }

func padsSpaces(c *charset.Collation) bool { return c.PadAttribute == charset.PadSpace }

// collationProperties looks up how a collation compares strings, nil when the
// collation is not known.
func collationProperties(logger *slog.Logger, collation string) *charset.Collation {
	if collation == "" {
		return nil
	}
	c, err := charset.FindCollationByName(collation)
	if err != nil {
		logger.Warn("collation properties unavailable; the plan will report the column's comparisons as possibly changing",
			"collation", collation, "error", err)
		return nil
	}
	return c
}

// compareChange classifies how one comparison property moves between two
// collations. It fails closed: a side whose properties are not known is a
// possible change.
func compareChange(before, after *charset.Collation, sensitive func(*charset.Collation) bool) engine.ComparisonChange {
	if before == nil || after == nil {
		return engine.ComparisonUnknown
	}
	switch {
	case sensitive(before) == sensitive(after):
		return engine.ComparisonUnchanged
	case sensitive(after):
		return engine.ComparisonBecomesSensitive
	default:
		return engine.ComparisonBecomesInsensitive
	}
}

// canMergeValues reports whether a collation move can make two values that
// compared unequal start comparing equal, which is when a unique index
// covering the column can reject rows it accepted before. Only a move onto a
// binary collation of the same charset is known not to: it compares code
// points or bytes, so it calls two values equal only when they are identical,
// apart from the trailing spaces it ignores when it pads and the old
// collation did not. A collation the plan cannot read can merge values.
func canMergeValues(change statement.ColumnCollationChange, before, after *charset.Collation) bool {
	if before == nil || after == nil {
		return true
	}
	if !after.Binary || !strings.EqualFold(change.Before.Charset, change.After.Charset) {
		return true
	}
	return padsSpaces(after) && !padsSpaces(before)
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
