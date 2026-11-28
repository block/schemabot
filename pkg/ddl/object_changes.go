package ddl

import (
	"fmt"
	"slices"

	"github.com/block/spirit/pkg/parser/ast"
	"github.com/block/spirit/pkg/statement"
	pgproto "github.com/pganalyze/pg_query_go/v6"
	pgquery "github.com/wasilibs/go-pgquery"
)

// SchemaObjectKind is the kind of named schema object a statement changes.
type SchemaObjectKind string

const (
	ObjectTable      SchemaObjectKind = "table"
	ObjectColumn     SchemaObjectKind = "column"
	ObjectIndex      SchemaObjectKind = "index"
	ObjectConstraint SchemaObjectKind = "constraint"
	ObjectPartition  SchemaObjectKind = "partition"
)

// SchemaObject names a table, or a column, index, constraint or partition of
// the table a statement changes. Name is the parser-folded name: lower case
// for the MySQL family, where these names are case-insensitive, and the
// server's folding for PostgreSQL, where only a quoted name keeps its case.
type SchemaObject struct {
	Kind SchemaObjectKind
	Name string
}

// ObjectChanges is what exactly one statement does to the named objects of
// the table it changes. A rename reads as the old name dropped and the new
// one added. A statement that changes an object in place, such as a column
// type change or a table option, lists it under Modified. A statement that
// names no object of a table, such as CREATE TYPE, reports no changes.
//
// Referenced lists the columns an added index or constraint covers, since
// dropping one of them destroys that index or constraint too. Where an index
// part or a check is an expression rather than a column, the table itself is
// referenced: the statement does not say which columns the expression reads.
//
// Validated lists the constraints a statement validates in a step of its own,
// such as PostgreSQL's VALIDATE CONSTRAINT. The statement also lists its table
// under Modified, since the step changes the table in place.
type ObjectChanges struct {
	Added      []SchemaObject
	Dropped    []SchemaObject
	Modified   []SchemaObject
	Referenced []SchemaObject
	Validated  []SchemaObject
}

func (c *ObjectChanges) add(kind SchemaObjectKind, name string) {
	if name != "" {
		c.Added = append(c.Added, SchemaObject{Kind: kind, Name: name})
	}
}

func (c *ObjectChanges) drop(kind SchemaObjectKind, name string) {
	if name != "" {
		c.Dropped = append(c.Dropped, SchemaObject{Kind: kind, Name: name})
	}
}

func (c *ObjectChanges) modify(kind SchemaObjectKind, name string) {
	if name != "" {
		c.Modified = append(c.Modified, SchemaObject{Kind: kind, Name: name})
	}
}

func (c *ObjectChanges) validate(kind SchemaObjectKind, name string) {
	if name != "" {
		c.Validated = append(c.Validated, SchemaObject{Kind: kind, Name: name})
	}
}

func (c *ObjectChanges) reference(kind SchemaObjectKind, name string) {
	if name != "" && !slices.Contains(c.Referenced, SchemaObject{Kind: kind, Name: name}) {
		c.Referenced = append(c.Referenced, SchemaObject{Kind: kind, Name: name})
	}
}

// addKey records an index or constraint a statement adds. One added without a
// name gets a name the server picks, which the statement does not say, so it
// is recorded as a reference to its whole table: any later change that
// destroys part of the table may be destroying it.
func (c *ObjectChanges) addKey(kind SchemaObjectKind, name, table string) {
	if name == "" {
		c.reference(ObjectTable, table)
		return
	}
	c.add(kind, name)
}

// mysqlPrimaryKeyName is the name MySQL gives every primary key.
const mysqlPrimaryKeyName = "primary"

// ObjectChanges implements StatementParser.
func (tidbStatementParser) ObjectChanges(stmt string) (ObjectChanges, error) {
	parsed, err := statement.New(stmt)
	if err != nil {
		return ObjectChanges{}, fmt.Errorf("parse object changes from statement %q: %w", StatementPreview(stmt), err)
	}
	if len(parsed) != 1 {
		return ObjectChanges{}, fmt.Errorf("expected one statement for object changes, got %d", len(parsed))
	}

	var changes ObjectChanges
	switch node := (*parsed[0].StmtNode).(type) {
	case *ast.CreateTableStmt:
		changes.add(ObjectTable, node.Table.Name.L)
		for _, col := range node.Cols {
			addMySQLColumn(&changes, node.Table.Name.L, col)
		}
		for _, c := range node.Constraints {
			addMySQLConstraint(&changes, node.Table.Name.L, c)
		}
	case *ast.DropTableStmt:
		for _, table := range node.Tables {
			changes.drop(ObjectTable, table.Name.L)
		}
	case *ast.CreateIndexStmt:
		changes.add(ObjectIndex, lowerName(node.IndexName))
		referenceMySQLIndexParts(&changes, node.Table.Name.L, node.IndexPartSpecifications)
	case *ast.DropIndexStmt:
		changes.drop(ObjectIndex, lowerName(node.IndexName))
	case *ast.AlterTableStmt:
		for _, spec := range node.Specs {
			alterSpecObjectChanges(&changes, node.Table.Name.L, spec)
		}
	}
	return changes, nil
}

// alterSpecObjectChanges records what one ALTER TABLE clause does to the
// table's named objects.
func alterSpecObjectChanges(changes *ObjectChanges, table string, spec *ast.AlterTableSpec) {
	switch spec.Tp {
	case ast.AlterTableAddColumns:
		for _, col := range spec.NewColumns {
			addMySQLColumn(changes, table, col)
		}
		for _, c := range spec.NewConstraints {
			addMySQLConstraint(changes, table, c)
		}
	case ast.AlterTableDropColumn:
		changes.drop(ObjectColumn, spec.OldColumnName.Name.L)
	case ast.AlterTableModifyColumn, ast.AlterTableAlterColumn:
		for _, col := range spec.NewColumns {
			changes.modify(ObjectColumn, col.Name.Name.L)
		}
	case ast.AlterTableAlterColumnVisibility:
		// A visibility clause names its column as the old column, not as a
		// new column definition.
		changes.modify(ObjectColumn, spec.OldColumnName.Name.L)
	case ast.AlterTableChangeColumn:
		renameOrModify(changes, ObjectColumn, spec.OldColumnName.Name.L, spec.NewColumns[0].Name.Name.L)
	case ast.AlterTableRenameColumn:
		renameOrModify(changes, ObjectColumn, spec.OldColumnName.Name.L, spec.NewColumnName.Name.L)
	case ast.AlterTableAddConstraint:
		addMySQLConstraint(changes, table, spec.Constraint)
	case ast.AlterTableDropIndex:
		changes.drop(ObjectIndex, lowerName(spec.Name))
	case ast.AlterTableDropPrimaryKey:
		changes.drop(ObjectIndex, mysqlPrimaryKeyName)
	case ast.AlterTableRenameIndex:
		renameOrModify(changes, ObjectIndex, spec.FromKey.L, spec.ToKey.L)
	case ast.AlterTableIndexInvisible:
		changes.modify(ObjectIndex, spec.IndexName.L)
	case ast.AlterTableDropForeignKey:
		changes.drop(ObjectConstraint, lowerName(spec.Name))
	case ast.AlterTableDropCheck, ast.AlterTableAlterCheck:
		if spec.Tp == ast.AlterTableDropCheck {
			changes.drop(ObjectConstraint, lowerName(spec.Constraint.Name))
		} else {
			changes.modify(ObjectConstraint, lowerName(spec.Constraint.Name))
		}
	case ast.AlterTableDropConstraint:
		// DROP CONSTRAINT names a CHECK, a FOREIGN KEY or a UNIQUE key, and
		// the statement alone does not say which, so it drops either kind.
		changes.drop(ObjectIndex, lowerName(spec.Constraint.Name))
		changes.drop(ObjectConstraint, lowerName(spec.Constraint.Name))
	case ast.AlterTableAddPartitions:
		for _, def := range spec.PartDefinitions {
			changes.add(ObjectPartition, def.Name.L)
		}
	case ast.AlterTableDropPartition:
		for _, name := range spec.PartitionNames {
			changes.drop(ObjectPartition, name.L)
		}
	case ast.AlterTableTruncatePartition:
		for _, name := range spec.PartitionNames {
			changes.modify(ObjectPartition, name.L)
		}
	default:
		changes.modify(ObjectTable, table)
	}
}

// addMySQLConstraint records a constraint a statement adds, and the columns
// it covers. Keys are indexes in MySQL; foreign keys and checks are
// constraints.
func addMySQLConstraint(changes *ObjectChanges, table string, c *ast.Constraint) {
	if c == nil {
		return
	}
	switch c.Tp {
	case ast.ConstraintPrimaryKey:
		changes.add(ObjectIndex, mysqlPrimaryKeyName)
	case ast.ConstraintForeignKey:
		changes.addKey(ObjectConstraint, lowerName(c.Name), table)
	case ast.ConstraintCheck:
		changes.addKey(ObjectConstraint, lowerName(c.Name), table)
		changes.reference(ObjectTable, table)
	default:
		changes.addKey(ObjectIndex, lowerName(c.Name), table)
	}
	referenceMySQLIndexParts(changes, table, c.Keys)
}

// addMySQLColumn records a column a statement adds, with the keys and
// constraints declared on it. An inline primary key is the table's primary
// key. Any other inline key or constraint has no name of its own, so it is
// recorded as a reference to the table.
func addMySQLColumn(changes *ObjectChanges, table string, col *ast.ColumnDef) {
	changes.add(ObjectColumn, col.Name.Name.L)
	for _, opt := range col.Options {
		switch opt.Tp {
		case ast.ColumnOptionPrimaryKey:
			changes.add(ObjectIndex, mysqlPrimaryKeyName)
		case ast.ColumnOptionUniqKey, ast.ColumnOptionCheck, ast.ColumnOptionReference:
			changes.reference(ObjectTable, table)
		}
	}
}

// referenceMySQLIndexParts records the columns an index or key covers, and the
// table for a part that is an expression.
func referenceMySQLIndexParts(changes *ObjectChanges, table string, parts []*ast.IndexPartSpecification) {
	for _, part := range parts {
		if part.Column == nil {
			changes.reference(ObjectTable, table)
			continue
		}
		changes.reference(ObjectColumn, part.Column.Name.L)
	}
}

// renameOrModify records a clause that may rename an object: a different new
// name drops the old one and adds the new one, and the same name changes the
// object in place.
func renameOrModify(changes *ObjectChanges, kind SchemaObjectKind, from, to string) {
	if from == to {
		changes.modify(kind, from)
		return
	}
	changes.drop(kind, from)
	changes.add(kind, to)
}

// ObjectChanges implements StatementParser.
func (postgresStatementParser) ObjectChanges(stmt string) (ObjectChanges, error) {
	result, err := pgquery.Parse(stmt)
	if err != nil {
		return ObjectChanges{}, fmt.Errorf("parse object changes from statement %q: %w", StatementPreview(stmt), err)
	}
	stmts := result.GetStmts()
	if len(stmts) != 1 {
		return ObjectChanges{}, fmt.Errorf("expected one statement for object changes, got %d", len(stmts))
	}

	var changes ObjectChanges
	switch node := stmts[0].GetStmt().GetNode().(type) {
	case *pgproto.Node_CreateStmt:
		changes.add(ObjectTable, node.CreateStmt.GetRelation().GetRelname())
		for _, elt := range node.CreateStmt.GetTableElts() {
			if col := elt.GetColumnDef(); col != nil {
				addPostgresColumn(&changes, node.CreateStmt.GetRelation().GetRelname(), col)
			}
			if c := elt.GetConstraint(); c != nil {
				addPostgresConstraint(&changes, node.CreateStmt.GetRelation().GetRelname(), c)
			}
		}
	case *pgproto.Node_IndexStmt:
		table := node.IndexStmt.GetRelation().GetRelname()
		changes.addKey(ObjectIndex, node.IndexStmt.GetIdxname(), table)
		for _, param := range node.IndexStmt.GetIndexParams() {
			referencePostgresIndexElem(&changes, table, param.GetIndexElem())
		}
		for _, param := range node.IndexStmt.GetIndexIncludingParams() {
			referencePostgresIndexElem(&changes, table, param.GetIndexElem())
		}
		if node.IndexStmt.GetWhereClause() != nil {
			// A partial index depends on the columns its predicate reads, and
			// the statement does not say which those are.
			changes.reference(ObjectTable, table)
		}
	case *pgproto.Node_DropStmt:
		kind, ok := postgresDropKind(node.DropStmt.GetRemoveType())
		if !ok {
			break
		}
		for _, object := range node.DropStmt.GetObjects() {
			changes.drop(kind, lastNameComponent(object.GetList().GetItems()))
		}
	case *pgproto.Node_AlterTableStmt:
		table := node.AlterTableStmt.GetRelation().GetRelname()
		for _, cmd := range node.AlterTableStmt.GetCmds() {
			alterCmdObjectChanges(&changes, table, cmd.GetAlterTableCmd())
		}
	case *pgproto.Node_RenameStmt:
		rename := node.RenameStmt
		switch rename.GetRenameType() {
		case pgproto.ObjectType_OBJECT_COLUMN:
			renameOrModify(&changes, ObjectColumn, rename.GetSubname(), rename.GetNewname())
		case pgproto.ObjectType_OBJECT_TABCONSTRAINT:
			renameOrModify(&changes, ObjectConstraint, rename.GetSubname(), rename.GetNewname())
		case pgproto.ObjectType_OBJECT_INDEX:
			renameOrModify(&changes, ObjectIndex, rename.GetRelation().GetRelname(), rename.GetNewname())
		case pgproto.ObjectType_OBJECT_TABLE:
			renameOrModify(&changes, ObjectTable, rename.GetRelation().GetRelname(), rename.GetNewname())
		}
	}
	return changes, nil
}

// postgresDropKind maps the object type a DROP statement removes to the kind
// of schema object it names.
func postgresDropKind(t pgproto.ObjectType) (SchemaObjectKind, bool) {
	switch t {
	case pgproto.ObjectType_OBJECT_TABLE:
		return ObjectTable, true
	case pgproto.ObjectType_OBJECT_INDEX:
		return ObjectIndex, true
	default:
		return "", false
	}
}

// addPostgresColumn records a column a statement adds, with the constraints
// declared on it. A key, check or foreign key declared without a name is
// recorded as a reference to the table, since the server names it.
func addPostgresColumn(changes *ObjectChanges, table string, col *pgproto.ColumnDef) {
	changes.add(ObjectColumn, col.GetColname())
	for _, node := range col.GetConstraints() {
		c := node.GetConstraint()
		if postgresConstraintIsObject(c.GetContype()) {
			changes.addKey(ObjectConstraint, c.GetConname(), table)
			continue
		}
		changes.add(ObjectConstraint, c.GetConname())
	}
}

// postgresConstraintIsObject reports whether a column constraint of this type
// creates a constraint or index of its own, rather than a column property
// such as NOT NULL or a default.
func postgresConstraintIsObject(t pgproto.ConstrType) bool {
	switch t {
	case pgproto.ConstrType_CONSTR_PRIMARY,
		pgproto.ConstrType_CONSTR_UNIQUE,
		pgproto.ConstrType_CONSTR_CHECK,
		pgproto.ConstrType_CONSTR_FOREIGN,
		pgproto.ConstrType_CONSTR_EXCLUSION:
		return true
	default:
		return false
	}
}

// alterCmdObjectChanges records what one ALTER TABLE command does to the
// table's named objects.
func alterCmdObjectChanges(changes *ObjectChanges, table string, cmd *pgproto.AlterTableCmd) {
	switch cmd.GetSubtype() {
	case pgproto.AlterTableType_AT_AddColumn:
		if col := cmd.GetDef().GetColumnDef(); col != nil {
			addPostgresColumn(changes, table, col)
		}
	case pgproto.AlterTableType_AT_DropColumn:
		changes.drop(ObjectColumn, cmd.GetName())
	case pgproto.AlterTableType_AT_ColumnDefault,
		pgproto.AlterTableType_AT_DropNotNull,
		pgproto.AlterTableType_AT_SetNotNull,
		pgproto.AlterTableType_AT_SetExpression,
		pgproto.AlterTableType_AT_DropExpression,
		pgproto.AlterTableType_AT_SetCompression,
		pgproto.AlterTableType_AT_AlterColumnType:
		changes.modify(ObjectColumn, cmd.GetName())
	case pgproto.AlterTableType_AT_AddConstraint:
		if c := cmd.GetDef().GetConstraint(); c != nil {
			addPostgresConstraint(changes, table, c)
		}
	case pgproto.AlterTableType_AT_DropConstraint:
		changes.drop(ObjectConstraint, cmd.GetName())
	case pgproto.AlterTableType_AT_ValidateConstraint:
		changes.validate(ObjectConstraint, cmd.GetName())
		changes.modify(ObjectTable, table)
	default:
		changes.modify(ObjectTable, table)
	}
}

// addPostgresConstraint records a table constraint a statement adds, and the
// columns it covers. A check, and an exclusion constraint with a predicate,
// read an expression, so they reference the table.
func addPostgresConstraint(changes *ObjectChanges, table string, c *pgproto.Constraint) {
	changes.addKey(ObjectConstraint, c.GetConname(), table)
	if c.GetContype() == pgproto.ConstrType_CONSTR_CHECK || c.GetWhereClause() != nil {
		changes.reference(ObjectTable, table)
	}
	for _, group := range [][]*pgproto.Node{c.GetKeys(), c.GetIncluding(), c.GetFkAttrs()} {
		for _, key := range group {
			changes.reference(ObjectColumn, key.GetString_().GetSval())
		}
	}
}

// referencePostgresIndexElem records the column an index element covers, or
// the table when the element is an expression.
func referencePostgresIndexElem(changes *ObjectChanges, table string, elem *pgproto.IndexElem) {
	if elem == nil {
		return
	}
	if elem.GetName() == "" {
		changes.reference(ObjectTable, table)
		return
	}
	changes.reference(ObjectColumn, elem.GetName())
}

// lowerName folds a MySQL identifier the way the server compares it.
func lowerName(name string) string {
	return ast.NewCIStr(name).L
}
