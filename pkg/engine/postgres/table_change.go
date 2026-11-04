package postgres

import (
	"context"
	"encoding/json"
	"fmt"
	"slices"
	"strings"

	"github.com/block/pg-sprite/pkg/schemadiff"
	"github.com/block/pg-sprite/pkg/statement"
	pganalyze "github.com/pganalyze/pg_query_go/v6"
	pgquery "github.com/wasilibs/go-pgquery"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"

	"github.com/block/schemabot/pkg/ddl"
	ternv1 "github.com/block/schemabot/pkg/proto/ternv1"
)

// comparisonSchema qualifies the statements both diffs of a comparison emit.
// The statements are only compared with each other, so any schema works as
// long as both diffs use the same one.
const comparisonSchema = "public"

// CompareSchemaFileChange reports whether a pull request's edit to one
// PostgreSQL schema file generates the same schema change on two different
// bases: the head runs no DDL the reviewer did not approve. It is the
// PostgreSQL counterpart of ddl.SameTableChange.
//
// The DDL each commit runs is its diff from its base. The change is the same
// when every statement the head's DDL runs is one the approved DDL runs, in
// the same order, and every approved statement the head skips is one the diff
// from the approved commit's base to the head's base runs, as when a stacked
// pull request below this one merged part of the edit (see
// ddl.CoveredByApproval). On the same base the head must run every approved
// statement. The diff runs on the target,
// because pg-sprite understands a file only by running it: each version is
// introspected on a scratch schema inside a transaction that is always
// rolled back (schemadiff.IntrospectDesired), so nothing persists.
//
// A file the pull request adds is absent at both bases, passed as "". Its
// DDL is the file's own statements, so it is the same when the approved
// commit and the head parse to the same statements in the same order:
// formatting and comments may differ, nothing the statements build may. That
// needs only the parse tree, so the target is not dialed.
//
// Each file must declare one table, the same one at all four commits, and no
// row security or foreign key. A file that does not, or that the target cannot
// run, is an error: it proves nothing, and the caller treats the file as
// changed.
func (e *Engine) CompareSchemaFileChange(ctx context.Context, req *ternv1.CompareSchemaFileChangeRequest) (*ternv1.CompareSchemaFileChangeResponse, error) {
	if req == nil {
		return nil, fmt.Errorf("compare PostgreSQL schema file change: request is required")
	}
	if req.GetApprovedBase() == "" && req.GetHeadBase() == "" {
		resp, err := sameAddedSchemaFile(req.GetApproved(), req.GetHead())
		if err != nil {
			return nil, fmt.Errorf("compare PostgreSQL schema file the pull request adds for database %q: %w", e.pullDatabase, err)
		}
		return resp, nil
	}
	if req.GetApprovedBase() == "" || req.GetHeadBase() == "" {
		return nil, fmt.Errorf("compare PostgreSQL schema file change for database %q: the file is absent at one base but not the other", e.pullDatabase)
	}
	if e.pullCredentials == nil || e.pullCredentials.DSN == "" {
		return nil, fmt.Errorf("compare PostgreSQL schema file change for database %q: DSN credentials are required", e.pullDatabase)
	}
	files := [...]struct {
		name    string
		content string
	}{
		{"approved commit's base", req.GetApprovedBase()},
		{"approved commit", req.GetApproved()},
		{"head's base", req.GetHeadBase()},
		{"head", req.GetHead()},
	}
	// Every file is parsed before the target is dialed, so a file that
	// cannot be compared never costs a connection.
	var desired [len(files)]statement.DesiredSchema
	for i, f := range files {
		schema, err := comparableSchema(f.content)
		if err != nil {
			return nil, fmt.Errorf("compare PostgreSQL schema file for database %q: read the file at the %s: %w", e.pullDatabase, f.name, err)
		}
		desired[i] = schema
	}
	table := desired[0].Table()
	for i, d := range desired {
		if d.Table() != table {
			return &ternv1.CompareSchemaFileChangeResponse{Reason: fmt.Sprintf("the file declares table %s at the %s but %s at the %s",
				table, files[0].name, d.Table(), files[i].name)}, nil
		}
	}
	pool, closePool, err := openTargetPool(ctx, e.pullCredentials)
	if err != nil {
		return nil, fmt.Errorf("compare PostgreSQL schema file for database %q: %w", e.pullDatabase, err)
	}
	defer closePool()

	var models [len(files)]schemadiff.Model
	for i, d := range desired {
		// The schema sets the scratch search path the file's statements
		// resolve types against, and names where a foreign key's referenced
		// tables are read from. comparableSchema admits no foreign key, and
		// both versions of the file resolve against the same path, so the
		// comparison schema serves.
		m, err := schemadiff.IntrospectDesired(ctx, pool, comparisonSchema, d)
		if err != nil {
			return nil, fmt.Errorf("compare PostgreSQL schema file for database %q: run table %s at the %s: %w", e.pullDatabase, table, files[i].name, err)
		}
		models[i] = m
	}
	reason, err := statementDifference(models)
	if err != nil {
		return nil, fmt.Errorf("compare PostgreSQL schema file for database %q table %s: %w", e.pullDatabase, table, err)
	}
	if reason != "" {
		reason = fmt.Sprintf("table %s: %s", table, reason)
	}
	return &ternv1.CompareSchemaFileChangeResponse{Same: reason == "", Reason: reason}, nil
}

// sameAddedSchemaFile compares a schema file the pull request adds at the
// approved commit and at the head by their statements' parse trees.
func sameAddedSchemaFile(approved, head string) (*ternv1.CompareSchemaFileChangeResponse, error) {
	approvedStmts, err := parsedStatements(approved)
	if err != nil {
		return nil, fmt.Errorf("read the file at the approved commit: %w", err)
	}
	headStmts, err := parsedStatements(head)
	if err != nil {
		return nil, fmt.Errorf("read the file at the head: %w", err)
	}
	if slices.Equal(approvedStmts, headStmts) {
		return &ternv1.CompareSchemaFileChangeResponse{Same: true}, nil
	}
	for i := range min(len(approvedStmts), len(headStmts)) {
		if approvedStmts[i] != headStmts[i] {
			return &ternv1.CompareSchemaFileChangeResponse{Reason: fmt.Sprintf(
				"statement %d of the file differs between the approved commit and the head", i+1)}, nil
		}
	}
	return &ternv1.CompareSchemaFileChangeResponse{Reason: fmt.Sprintf(
		"the file holds %d statements at the approved commit but %d at the head", len(approvedStmts), len(headStmts))}, nil
}

// parsedStatements parses content and encodes each statement's parse tree,
// in file order, with every source position removed.
func parsedStatements(content string) ([]string, error) {
	tree, err := pgquery.Parse(content)
	if err != nil {
		return nil, fmt.Errorf("parse: %w", err)
	}
	if len(tree.GetStmts()) == 0 {
		return nil, fmt.Errorf("the file holds no statements")
	}
	stmts := make([]string, 0, len(tree.GetStmts()))
	for i, raw := range tree.GetStmts() {
		encoded, err := encodeParseNode(raw.GetStmt())
		if err != nil {
			return nil, fmt.Errorf("encode statement %d: %w", i+1, err)
		}
		stmts = append(stmts, encoded)
	}
	return stmts, nil
}

// comparableSchema admits a schema file the comparison can run: one table
// declaration with no row security, which the ordinary planner manages, and no
// foreign key. Running a foreign key needs the namespace its referenced tables
// live in, which the request does not carry, and SchemaBot never applies a
// PostgreSQL foreign key anyway.
func comparableSchema(content string) (statement.DesiredSchema, error) {
	file, err := statement.ParseDesiredFile(content)
	if err != nil {
		return statement.DesiredSchema{}, err
	}
	if _, ok := file.RowSecurity(); ok {
		return statement.DesiredSchema{}, fmt.Errorf("table %s declares row security, which the comparison does not cover", file.Table())
	}
	schema, ok := file.Schema()
	if !ok {
		return statement.DesiredSchema{}, fmt.Errorf("the parse admitted no table declaration")
	}
	if referenced := schema.ReferencedTables(); len(referenced) > 0 {
		return statement.DesiredSchema{}, fmt.Errorf("table %s declares foreign keys to %s, which the comparison does not cover", schema.Table(), strings.Join(referenced, ", "))
	}
	return schema, nil
}

// statementDifference returns why the statements the head runs, its diff from
// its base, are not the approved commit's statements less those the head's
// base already ran, or "" when they are. models holds the table at
// the approved commit's base, the approved commit, the head's base, and the
// head, in that order.
func statementDifference(models [4]schemadiff.Model) (string, error) {
	approvedChange, err := schemadiff.Diff(comparisonSchema, models[0], models[1])
	if err != nil {
		return "", fmt.Errorf("diff the approved commit against its base: %w", err)
	}
	headChange, err := schemadiff.Diff(comparisonSchema, models[2], models[3])
	if err != nil {
		return "", fmt.Errorf("diff the head against its base: %w", err)
	}
	baseMove, err := schemadiff.Diff(comparisonSchema, models[0], models[2])
	if err != nil {
		return "", fmt.Errorf("diff the head's base against the approved commit's base: %w", err)
	}
	covered, err := changesCovered(approvedChange, headChange, baseMove)
	if err != nil {
		return "", err
	}
	if !covered {
		return fmt.Sprintf("the approved commit generates %q but the head generates %q, and the head's base already runs %q",
			changeSQL(approvedChange), changeSQL(headChange), changeSQL(baseMove)), nil
	}
	return "", nil
}

// encodeParseNode encodes a parse tree node as canonical JSON with every
// source position removed.
func encodeParseNode(node proto.Message) (string, error) {
	encoded, err := protojson.MarshalOptions{UseProtoNames: true}.Marshal(node)
	if err != nil {
		return "", err
	}
	var decoded any
	if err := json.Unmarshal(encoded, &decoded); err != nil {
		return "", err
	}
	canonical, err := json.Marshal(withoutLocations(decoded))
	if err != nil {
		return "", err
	}
	return string(canonical), nil
}

// withoutLocations drops every "location" field from a decoded parse tree.
func withoutLocations(value any) any {
	switch v := value.(type) {
	case map[string]any:
		delete(v, "location")
		for k, child := range v {
			v[k] = withoutLocations(child)
		}
	case []any:
		for i, child := range v {
			v[i] = withoutLocations(child)
		}
	}
	return value
}

// changesCovered reports whether the head's statements are covered by the
// approved ones given the base move (see ddl.CoveredByApproval).
func changesCovered(approved, head, baseMove []schemadiff.Change) (bool, error) {
	var items [3][]ddl.ChangeItem
	for i, changes := range [...][]schemadiff.Change{approved, head, baseMove} {
		for _, c := range changes {
			elements, err := statementElements(c.SQL)
			if err != nil {
				return false, fmt.Errorf("read the elements of statement %q: %w", c.SQL, err)
			}
			items[i] = append(items[i], ddl.ChangeItem{SQL: changeIdentity(c), Elements: elements})
		}
	}
	return ddl.CoveredByApproval(items[0], items[1], items[2]), nil
}

// changeIdentity renders a change as the statement it runs plus the live
// definition it was planned against. An ADD CONSTRAINT ... USING INDEX or a
// VALIDATE CONSTRAINT names only the index or constraint, so the same
// statement can take over a different definition on a different base;
// comparing by statement alone would treat the two as the same approved
// change.
func changeIdentity(c schemadiff.Change) string {
	switch {
	case c.Adopts != nil:
		return c.SQL + " adopting " + c.Adopts.Def
	case c.Validates != nil:
		return c.SQL + " validating " + c.Validates.Def
	default:
		return c.SQL
	}
}

// statementElements keys the columns, constraints, and indexes a statement
// the diff emits changes, or returns none for a statement whose elements it
// does not know.
func statementElements(sql string) ([]string, error) {
	tree, err := pgquery.Parse(sql)
	if err != nil {
		return nil, fmt.Errorf("parse: %w", err)
	}
	if len(tree.GetStmts()) != 1 {
		return nil, fmt.Errorf("expected one statement, got %d", len(tree.GetStmts()))
	}
	stmt := tree.GetStmts()[0].GetStmt()
	switch {
	case stmt.GetAlterTableStmt() != nil:
		var elements []string
		for _, node := range stmt.GetAlterTableStmt().GetCmds() {
			key := alterTableCmdElement(node.GetAlterTableCmd())
			if key == "" {
				return nil, nil
			}
			elements = append(elements, key)
		}
		return elements, nil
	case stmt.GetIndexStmt() != nil && stmt.GetIndexStmt().GetIdxname() != "":
		return []string{"index " + stmt.GetIndexStmt().GetIdxname()}, nil
	case stmt.GetDropStmt() != nil && stmt.GetDropStmt().GetRemoveType() == pganalyze.ObjectType_OBJECT_INDEX:
		var elements []string
		for _, object := range stmt.GetDropStmt().GetObjects() {
			names := object.GetList().GetItems()
			if len(names) == 0 {
				return nil, nil
			}
			elements = append(elements, "index "+names[len(names)-1].GetString_().GetSval())
		}
		return elements, nil
	default:
		return nil, nil
	}
}

// alterTableCmdElement keys the column or constraint an ALTER TABLE command
// changes, or returns "" for a command whose element it does not know.
func alterTableCmdElement(cmd *pganalyze.AlterTableCmd) string {
	switch cmd.GetSubtype() {
	case pganalyze.AlterTableType_AT_AddColumn:
		return "column " + cmd.GetDef().GetColumnDef().GetColname()
	case pganalyze.AlterTableType_AT_DropColumn, pganalyze.AlterTableType_AT_AlterColumnType,
		pganalyze.AlterTableType_AT_ColumnDefault, pganalyze.AlterTableType_AT_SetNotNull,
		pganalyze.AlterTableType_AT_DropNotNull, pganalyze.AlterTableType_AT_SetExpression,
		pganalyze.AlterTableType_AT_DropExpression:
		return "column " + cmd.GetName()
	case pganalyze.AlterTableType_AT_AddConstraint:
		if name := cmd.GetDef().GetConstraint().GetConname(); name != "" {
			return "constraint " + name
		}
		return ""
	case pganalyze.AlterTableType_AT_DropConstraint:
		return "constraint " + cmd.GetName()
	default:
		return ""
	}
}

// changeSQL lists the statements of changes, each with the definition it
// adopts or validates, for a reason string.
func changeSQL(changes []schemadiff.Change) []string {
	stmts := make([]string, 0, len(changes))
	for _, c := range changes {
		stmts = append(stmts, changeIdentity(c))
	}
	return stmts
}
