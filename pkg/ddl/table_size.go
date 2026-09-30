package ddl

import (
	"log/slog"
	"slices"

	"github.com/block/schemabot/pkg/schema"
)

// TableCostScalesWithSize reports whether any statement a plan runs against a
// table has a cost that grows with the table: an index build, a table copy or
// rebuild, or a full-table validation scan. It uses the real parser for the
// database's dialect. A table's DDL can join several statements, and a sharded
// namespace carries each shard's DDL, so every entry is split and each
// statement inspected. Table sizes are display-only context, so DDL that
// cannot be split or parsed logs a warning and contributes no size line rather
// than failing the plan. logAttrs identify the plan and table in those
// warnings.
func TableCostScalesWithSize(databaseType string, ddls []string, logAttrs ...any) bool {
	parser, err := ParserForDialect(schema.DialectForDatabaseType(databaseType))
	if err != nil {
		slog.Warn("no statement parser for dialect; the plan omits the table-size line",
			slices.Concat(logAttrs, []any{"database_type", databaseType, "error", err})...)
		return false
	}
	for _, d := range ddls {
		stmts, err := parser.Split(d)
		if err != nil {
			slog.Warn("failed to split plan DDL for table-size-scaling cost; its statements get no table-size line",
				slices.Concat(logAttrs, []any{"database_type", databaseType, "error", err})...)
			continue
		}
		for _, stmt := range stmts {
			scales, err := parser.CostScalesWithTableSize(stmt)
			if err != nil {
				slog.Warn("failed to inspect plan statement for table-size-scaling cost; it gets no table-size line",
					slices.Concat(logAttrs, []any{"database_type", databaseType, "error", err})...)
				continue
			}
			if scales {
				return true
			}
		}
	}
	return false
}
