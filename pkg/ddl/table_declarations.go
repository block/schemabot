package ddl

import "fmt"

// TableDeclarations records which schema file declares each desired table in
// one diff set, so a table declared twice fails the plan before it reaches the
// differ. The differ keys desired tables by name and keeps a single
// definition, so a second declaration would silently replace the first and the
// plan would drop every column only the replaced one declares. There is no
// single desired definition to review, so the plan refuses instead of picking
// one. The zero value is ready to use.
type TableDeclarations struct {
	declaredBy map[string]string
}

// Declare records that the schema file declares the table. It refuses a table
// that an earlier file, or an earlier statement of the same file, already
// declared, naming the table and both files so the operator knows which
// declaration to remove. Table names compare exactly, the same way the differ
// keys them.
func (d *TableDeclarations) Declare(file, table string) error {
	first, declared := d.declaredBy[table]
	if !declared {
		if d.declaredBy == nil {
			d.declaredBy = make(map[string]string)
		}
		d.declaredBy[table] = file
		return nil
	}
	if first == file {
		return fmt.Errorf("table %q is declared more than once in schema file %q. Declare each table exactly once", table, file)
	}
	return fmt.Errorf("table %q is declared by both schema files %q and %q. Declare each table in exactly one schema file", table, first, file)
}
