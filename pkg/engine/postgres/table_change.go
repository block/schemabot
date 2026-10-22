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
// bases. It is the PostgreSQL counterpart of ddl.SameTableChange.
//
// The change is the same when two things hold:
//
//   - Every column, constraint, index, and CREATE TABLE clause the head
//     declares is the three-way merge of the approved commit and the head's
//     base (ddl.UnmergedElement). Elements are read from PostgreSQL's own
//     parse tree, so every clause the file declares takes part, including
//     ones pg-sprite's model does not carry (storage parameters, tablespaces,
//     identity sequence options): a clause neither side declared cannot
//     reach the head unseen.
//   - Diffing each base against its commit yields the same statements. The
//     diff runs on the target, because pg-sprite understands a file only by
//     running it: each version is introspected on a scratch schema inside a
//     transaction that is always rolled back (schemadiff.IntrospectDesired),
//     so nothing persists.
//
// Column order gets no check of its own, as it does on MySQL: PostgreSQL
// appends every added column, and pg-sprite does not manage attribute order.
// The statements still list changes in the order the file declares them,
// and that order can matter (it is the order added columns land in), so a
// head that reorders columns the pull request changed generates a
// different change and keeps no approval.
//
// A file the pull request adds is absent at both bases, passed as "". Its
// change is the file's own statements, so it is the same when the approved
// commit and the head parse to the same statements in the same order:
// formatting and comments may differ, nothing the statements build may. That
// needs only the parse tree, so the target is not dialed.
//
// Each file must declare one table, the same one at all four commits, and no
// row security or foreign key. A file that does not, or that the target cannot run, is an
// error: it proves nothing, and the caller treats the file as changed.
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
	// Every file is parsed and merged before the target is dialed, so a file
	// that cannot be compared, or a head that is not the merge, never costs a
	// connection.
	var desired [len(files)]statement.DesiredSchema
	var elements [len(files)]map[string]string
	for i, f := range files {
		schema, err := comparableSchema(f.content)
		if err != nil {
			return nil, fmt.Errorf("compare PostgreSQL schema file for database %q: read the file at the %s: %w", e.pullDatabase, f.name, err)
		}
		declared, err := declaredElements(f.content)
		if err != nil {
			return nil, fmt.Errorf("compare PostgreSQL schema file for database %q: read table %s at the %s: %w", e.pullDatabase, schema.Table(), f.name, err)
		}
		desired[i], elements[i] = schema, declared
	}
	table := desired[0].Table()
	for i, d := range desired {
		if d.Table() != table {
			return &ternv1.CompareSchemaFileChangeResponse{Reason: fmt.Sprintf("the file declares table %s at the %s but %s at the %s",
				table, files[0].name, d.Table(), files[i].name)}, nil
		}
	}
	if why := ddl.UnmergedElement(elements[0], elements[1], elements[2], elements[3]); why != "" {
		return &ternv1.CompareSchemaFileChangeResponse{Reason: fmt.Sprintf("table %s: %s", table, why)}, nil
	}

	pool, closePool, err := openTargetPool(ctx, e.pullCredentials)
	if err != nil {
		return nil, fmt.Errorf("compare PostgreSQL schema file for database %q: %w", e.pullDatabase, err)
	}
	defer closePool()

	var models [len(files)]schemadiff.Model
	for i, d := range desired {
		// The schema names where a foreign key's referenced tables are read
		// from; comparableSchema admits no foreign key, so nothing reads it.
		m, err := schemadiff.IntrospectDesired(ctx, pool, "", d)
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

// statementDifference returns why diffing the approved commit's base against
// the approved commit yields different statements from diffing the head's
// base against the head, or "" when they are the same. models holds the
// table at the approved commit's base, the approved commit, the head's base,
// and the head, in that order.
func statementDifference(models [4]schemadiff.Model) (string, error) {
	approvedChange, err := schemadiff.Diff(comparisonSchema, models[0], models[1])
	if err != nil {
		return "", fmt.Errorf("diff the approved commit against its base: %w", err)
	}
	headChange, err := schemadiff.Diff(comparisonSchema, models[2], models[3])
	if err != nil {
		return "", fmt.Errorf("diff the head against its base: %w", err)
	}
	if !slices.Equal(approvedChange, headChange) {
		return fmt.Sprintf("the approved commit generates %q but the head generates %q", changeSQL(approvedChange), changeSQL(headChange)), nil
	}
	return "", nil
}

// declaredElements keys everything a schema file declares, read from
// PostgreSQL's parse tree, and maps each to its parsed definition: every
// column and constraint of the CREATE TABLE by name, every clause of the
// CREATE TABLE outside its element list by field name, and every index by
// name. A column's definition carries its inline constraints and identity
// options; an unnamed constraint or index is keyed by its definition, so
// changing one removes it and adds another. Source positions are dropped, so
// whitespace and keyword case do not count. comparableSchema has admitted
// the file, so it holds one CREATE TABLE and only CREATE INDEX otherwise.
func declaredElements(content string) (map[string]string, error) {
	tree, err := pgquery.Parse(content)
	if err != nil {
		return nil, fmt.Errorf("parse: %w", err)
	}
	elements := make(map[string]string)
	add := func(key, definition string) error {
		if _, dup := elements[key]; dup {
			return fmt.Errorf("%s is declared more than once", key)
		}
		elements[key] = definition
		return nil
	}
	addNode := func(key string, node proto.Message) error {
		definition, err := encodeParseNode(node)
		if err != nil {
			return fmt.Errorf("encode %s: %w", key, err)
		}
		if key == "" {
			key = definition
		}
		return add(key, definition)
	}
	for _, raw := range tree.GetStmts() {
		switch {
		case raw.GetStmt().GetCreateStmt() != nil:
			create := proto.Clone(raw.GetStmt().GetCreateStmt()).(*pganalyze.CreateStmt)
			for _, elt := range create.GetTableElts() {
				var err error
				switch {
				case elt.GetColumnDef() != nil:
					err = addNode("column "+elt.GetColumnDef().GetColname(), elt.GetColumnDef())
				case elt.GetConstraint().GetConname() != "":
					err = addNode("constraint "+elt.GetConstraint().GetConname(), elt.GetConstraint())
				default:
					err = addNode("", elt)
				}
				if err != nil {
					return nil, err
				}
			}
			create.TableElts = nil
			encoded, err := encodeParseNode(create)
			if err != nil {
				return nil, fmt.Errorf("encode the CREATE TABLE clauses: %w", err)
			}
			var clauses map[string]json.RawMessage
			if err := json.Unmarshal([]byte(encoded), &clauses); err != nil {
				return nil, fmt.Errorf("decode the CREATE TABLE clauses: %w", err)
			}
			for field, value := range clauses {
				if err := add("table clause "+field, string(value)); err != nil {
					return nil, err
				}
			}
		case raw.GetStmt().GetIndexStmt() != nil:
			index := raw.GetStmt().GetIndexStmt()
			key := ""
			if index.GetIdxname() != "" {
				key = "index " + index.GetIdxname()
			}
			if err := addNode(key, index); err != nil {
				return nil, err
			}
		default:
			return nil, fmt.Errorf("the file holds a statement other than CREATE TABLE and CREATE INDEX")
		}
	}
	return elements, nil
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

// changeSQL lists the statements of changes, for a reason string.
func changeSQL(changes []schemadiff.Change) []string {
	stmts := make([]string, 0, len(changes))
	for _, c := range changes {
		stmts = append(stmts, c.SQL)
	}
	return stmts
}
