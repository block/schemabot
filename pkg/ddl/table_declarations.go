package ddl

import (
	"fmt"
	"strings"
)

// TableDeclarations records which schema file declares each desired table in
// one diff set, so a table declared twice fails the plan before it reaches the
// differ, with an error that names both files. The differ refuses a desired
// table declared twice as well, since there is no single desired definition to
// review, but it sees table names only, so its refusal is the backstop for a
// caller that skipped this check rather than the message an operator acts on.
// The zero value is ready to use.
type TableDeclarations struct {
	// LowerCaseTableNames is the target server's @@lower_case_table_names.
	// When it is nonzero the server treats names that differ only in case as
	// one table, and so does Declare. Zero compares names exactly.
	LowerCaseTableNames int

	declaredBy map[string]declaration
}

// declaration is the first file to declare a table and the spelling it used.
type declaration struct {
	file  string
	table string
}

// Declare records that the schema file declares the table. It refuses a table
// that an earlier file, or an earlier statement of the same file, already
// declared, naming the table and both files so the operator knows which
// declaration to remove. Table names compare the way the target and the
// differ compare them (see LowerCaseTableNames), so the refusal fires for
// exactly the pairs the differ would refuse as one table and never for a pair
// it keeps apart.
func (d *TableDeclarations) Declare(file, table string) error {
	key := table
	if d.LowerCaseTableNames != 0 {
		key = strings.ToLower(table)
	}
	first, declared := d.declaredBy[key]
	if !declared {
		if d.declaredBy == nil {
			d.declaredBy = make(map[string]declaration)
		}
		d.declaredBy[key] = declaration{file: file, table: table}
		return nil
	}
	if first.table != table {
		return fmt.Errorf("tables %q (schema file %q) and %q (schema file %q) are the same table on this target, which compares table names case-insensitively (lower_case_table_names=%d). Declare each table exactly once", first.table, first.file, table, file, d.LowerCaseTableNames)
	}
	if first.file == file {
		return fmt.Errorf("table %q is declared more than once in schema file %q. Declare each table exactly once", table, file)
	}
	return fmt.Errorf("table %q is declared by both schema files %q and %q. Declare each table in exactly one schema file", table, first.file, file)
}
