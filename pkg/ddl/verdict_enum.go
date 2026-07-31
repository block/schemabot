package ddl

import (
	"fmt"

	"github.com/block/spirit/pkg/statement"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/parser/types"
)

// EnumReorderRefusalReason reports whether the MySQL schema-change engine
// (Spirit) deterministically refuses the given ALTER TABLE statement because
// it reorders an existing ENUM column's values or inserts new values before
// retained ones, and the engine's reason.
//
// The engine's enumReorder preflight check guards the row-copy path: the
// binlog replay path receives ENUM values as integer ordinals and decodes
// them against the source table's element list, so retained values must keep
// their relative order and new values must be appended after all retained
// ones. Unlike other preflight checks, no fast path can bypass this one:
// MySQL's native DDL (ALGORITHM=INSTANT, or the safe-INPLACE subset the
// engine attempts first) can only append ENUM members at the end of the
// list, so a change this check refuses always falls through to the row-copy
// path and reaches the check. The refusal is therefore certain, not a race
// with the engine's native-DDL attempt.
//
// currentCreateTable is the target table's live CREATE TABLE statement — the
// same definition the plan diff was computed against. Because the verdict
// must never claim an apply will fail when it can succeed, it errs toward
// silence: a modified column missing from the current definition, a current
// column type that is not ENUM, or a SET column (guarded by a different
// engine check) yields no refusal and surfaces at apply time at worst.
// Agreement with the engine's own check is enforced by test.
func EnumReorderRefusalReason(alterStmt, currentCreateTable string) (string, bool, error) {
	stmts, err := statement.New(alterStmt)
	if err != nil {
		return "", false, fmt.Errorf("parse statement for ENUM reorder verdict %q: %w", statementPreview(alterStmt), err)
	}
	if len(stmts) != 1 {
		return "", false, fmt.Errorf("ENUM reorder verdict requires exactly one statement, got %d in %q", len(stmts), statementPreview(alterStmt))
	}
	abs := stmts[0]
	if !abs.IsAlterTable() {
		return "", false, nil
	}
	alterNode, ok := abs.AsAlterTable()
	if !ok {
		return "", false, fmt.Errorf("statement classified as ALTER TABLE has no alter AST node: %q", statementPreview(alterStmt))
	}

	modified := modifiedEnumColumns(alterNode)
	if len(modified) == 0 {
		return "", false, nil
	}

	existingTypes, err := createTableColumnTypes(currentCreateTable)
	if err != nil {
		return "", false, fmt.Errorf("parse current table definition for ENUM reorder verdict: %w", err)
	}

	for _, col := range modified {
		newElems := col.def.Tp.GetElems()
		if len(newElems) == 0 {
			continue
		}
		existing, ok := existingTypes[col.lookupKey]
		if !ok || existing.GetType() != mysql.TypeEnum {
			// The column is new, renamed, or not currently an ENUM
			// (e.g. VARCHAR → ENUM); the reorder rule compares two ENUM
			// element lists, so it has nothing to say about those shapes.
			continue
		}
		existingElems := existing.GetElems()
		if len(existingElems) == 0 {
			continue
		}
		if !isCompatibleEnumChange(existingElems, newElems) {
			return fmt.Sprintf("unsafe ENUM value reorder on column %q is not supported. "+
				"The binlog replay path uses integer ordinals for ENUM values, so retained values "+
				"must keep their relative order and new values must be appended at the end. "+
				"Dropping existing values from anywhere in the list is allowed",
				col.lookupName), true, nil
		}
	}
	return "", false, nil
}

// modifiedEnumColumn is a column an ALTER TABLE redefines with an ENUM type.
type modifiedEnumColumn struct {
	// lookupName is the name to find the column under in the current table
	// definition: the old column name for CHANGE COLUMN, the column's own
	// name for MODIFY COLUMN. lookupKey is its lowercase form — MySQL
	// column names are case-insensitive — while lookupName preserves the
	// statement's original case for messages.
	lookupName string
	lookupKey  string
	def        *ast.ColumnDef
}

// modifiedEnumColumns extracts the MODIFY COLUMN and CHANGE COLUMN specs
// whose new type is ENUM — the only alter specs that can redefine a column's
// element list.
func modifiedEnumColumns(alterStmt *ast.AlterTableStmt) []modifiedEnumColumn {
	var result []modifiedEnumColumn
	for _, spec := range alterStmt.Specs {
		var def *ast.ColumnDef
		var lookupName, lookupKey string
		switch spec.Tp {
		case ast.AlterTableModifyColumn:
			if len(spec.NewColumns) > 0 {
				def = spec.NewColumns[0]
				lookupName, lookupKey = def.Name.Name.O, def.Name.Name.L
			}
		case ast.AlterTableChangeColumn:
			if len(spec.NewColumns) > 0 {
				def = spec.NewColumns[0]
				if spec.OldColumnName != nil {
					lookupName, lookupKey = spec.OldColumnName.Name.O, spec.OldColumnName.Name.L
				} else {
					lookupName, lookupKey = def.Name.Name.O, def.Name.Name.L
				}
			}
		default:
			continue
		}
		if def == nil || def.Tp == nil || def.Tp.GetType() != mysql.TypeEnum {
			continue
		}
		result = append(result, modifiedEnumColumn{lookupName: lookupName, lookupKey: lookupKey, def: def})
	}
	return result
}

// createTableColumnTypes parses a CREATE TABLE statement and returns each
// column's field type keyed by lowercase column name — MySQL column names
// are case-insensitive.
func createTableColumnTypes(createTable string) (map[string]*types.FieldType, error) {
	ct, err := statement.ParseCreateTable(createTable)
	if err != nil {
		return nil, fmt.Errorf("parse CREATE TABLE %q: %w", statementPreview(createTable), err)
	}
	cols := make(map[string]*types.FieldType, len(ct.Raw.Cols))
	for _, col := range ct.Raw.Cols {
		cols[col.Name.Name.L] = col.Tp
	}
	return cols, nil
}

// isCompatibleEnumChange reports whether newElems is a binlog-ordinal-safe
// modification of existingElems, mirroring the engine's rule:
//
//   - Values present in both lists must appear in the same relative order
//     (existing values may be dropped from anywhere, but not moved).
//   - Values in newElems that are not in existingElems must appear only
//     after all retained existing values (new values are appended at the
//     end, never interleaved among or before existing values).
func isCompatibleEnumChange(existingElems, newElems []string) bool {
	existingSet := make(map[string]struct{}, len(existingElems))
	for _, e := range existingElems {
		existingSet[e] = struct{}{}
	}

	j := 0
	sawNew := false
	for _, e := range newElems {
		if _, isExisting := existingSet[e]; isExisting {
			if sawNew {
				return false // an existing value appears after a brand-new value
			}
			for j < len(existingElems) && existingElems[j] != e {
				j++
			}
			if j >= len(existingElems) {
				return false // existing value out of relative order, or repeated in newElems
			}
			j++
		} else {
			sawNew = true
		}
	}
	return true
}
