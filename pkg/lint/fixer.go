// Package lint provides schema linting and fixing using Spirit's parser.
package lint

import (
	"fmt"
	"maps"
	"slices"
	"strings"

	"github.com/block/spirit/pkg/parser/ast"
	"github.com/block/spirit/pkg/parser/format"
	"github.com/block/spirit/pkg/parser/mysql"
	"github.com/block/spirit/pkg/statement"
)

// Fixer generates auto-fixes for lint violations using AST manipulation.
// Parses SQL, walks the AST to apply fixes, and restores a fixed file to
// canonical form. A file with nothing to fix is returned as written.
type Fixer struct {
	config FixerConfig
}

// FixerConfig holds configuration for the fixer.
type FixerConfig struct {
	// DefaultDecimalPrecision is used when converting FLOAT to DECIMAL.
	// Default: (10, 2)
	DefaultDecimalPrecision int
	DefaultDecimalScale     int
}

// DefaultFixerConfig returns the default fixer configuration.
func DefaultFixerConfig() FixerConfig {
	return FixerConfig{
		DefaultDecimalPrecision: 10,
		DefaultDecimalScale:     2,
	}
}

// NewFixer creates a new Fixer with default configuration.
func NewFixer() *Fixer {
	return &Fixer{config: DefaultFixerConfig()}
}

// NewFixerWithConfig creates a new Fixer with custom configuration.
func NewFixerWithConfig(cfg FixerConfig) *Fixer {
	return &Fixer{config: cfg}
}

// FixResult contains the result of fixing a single file.
type FixResult struct {
	Filename    string   // Original filename
	OriginalSQL string   // Original SQL content
	FixedSQL    string   // Fixed SQL content (empty if no fix was applied)
	Changed     bool     // Whether a fix was applied; an unchanged file is left as written
	Fixes       []string // Description of fixes applied
}

// FixFilesResult contains the result of fixing multiple files.
type FixFilesResult struct {
	Files      []FixResult // Results for each file
	TotalFixed int         // Number of issues fixed
	// RemainingIssues are the findings the files still carry once the fixes
	// have been applied, at whatever severity the linter reports them. Most
	// of what the fixer leaves behind is a warning rather than an error, so
	// reporting only errors would let the command claim a file is clean when
	// the linter still has something to say about it.
	RemainingIssues []Result
}

// FixFiles applies auto-fixes to a set of SQL schema files.
// Returns fixed files, any issues that couldn't be automatically fixed, and any error.
// Files are processed in filename order, so the order of the results, the order
// of the files in the remaining issues, and which file an error names are the
// same on every run over the same input. Within one file, the remaining issues
// keep the order Spirit's linters report them in, which Spirit leaves undefined.
func (f *Fixer) FixFiles(files map[string]string) (*FixFilesResult, error) {
	result := &FixFilesResult{}
	filenames := slices.Sorted(maps.Keys(files))
	fixedContent := make(map[string]string, len(files))

	for _, filename := range filenames {
		content := files[filename]
		fileResult := FixResult{
			Filename:    filename,
			OriginalSQL: content,
		}

		// Parse and fix using AST manipulation. A file counts as changed only
		// when a fix was applied to it, never because its restored spelling
		// differs from what was written.
		fixed, fixes, err := f.fixFile(content)
		if err != nil {
			return nil, fmt.Errorf("fix %s: %w", filename, err)
		}
		if len(fixes) > 0 {
			fileResult.FixedSQL = fixed
			fileResult.Changed = true
			fileResult.Fixes = fixes
			result.TotalFixed += len(fixes)
			fixedContent[filename] = fixed
		}

		result.Files = append(result.Files, fileResult)
	}

	// Lint after fixing to report whatever the files still carry.
	linter := New()
	for _, filename := range filenames {
		// Lint the fixed content where a fix was applied.
		content, ok := fixedContent[filename]
		if !ok {
			content = files[filename]
		}
		lintResults, err := linter.LintSchema(map[string]string{filename: content})
		if err != nil {
			return nil, fmt.Errorf("failed to lint %s: %w", filename, err)
		}
		result.RemainingIssues = append(result.RemainingIssues, lintResults...)
	}

	return result, nil
}

// fixFile applies the fixes to every CREATE TABLE in a schema file and
// returns the file with those tables rewritten. A file is read one statement
// at a time, the way the lint pass reads it, so a file that declares several
// tables has every one of them fixed rather than being skipped for holding
// more than one statement.
//
// Only the tables a fix changed are rewritten. Every other statement, and the
// text between statements, is copied through exactly as written, so a comment
// above an untouched table survives and a file with nothing to fix keeps its
// bytes. A rewritten table is replaced by the parser's canonical rendering of
// it, which is one line and carries no comments of its own.
//
// A file the parser rejects is returned unchanged and without fixes. The lint
// pass that follows parses it again and reports the parse failure, so refusing
// here would only report it twice.
func (f *Fixer) fixFile(sql string) (string, []string, error) {
	stmts, err := statement.NewWithOptions(sql, statement.Options{AllowMixedStatementTypes: true})
	if err != nil {
		return sql, nil, nil
	}

	var (
		fixes []string
		out   strings.Builder
		// cursor is how far through sql the rewritten output has consumed, so
		// the gaps between statements are copied rather than regenerated.
		cursor int
	)

	for _, stmt := range stmts {
		original := (*stmt.StmtNode).OriginalText()
		offset := strings.Index(sql[cursor:], original)
		if offset < 0 {
			// The statement's source text could not be located in the file, so
			// its bounds are unknown and no part of the file can be rewritten
			// without risking the loss of another statement.
			return sql, nil, nil
		}
		out.WriteString(sql[cursor : cursor+offset])
		cursor += offset + len(original)

		replacement, stmtFixes, err := f.fixStatement(stmt, original)
		if err != nil {
			return "", nil, err
		}
		fixes = append(fixes, stmtFixes...)
		out.WriteString(replacement)
	}
	out.WriteString(sql[cursor:])

	if len(fixes) == 0 {
		return sql, nil, nil
	}
	return out.String(), fixes, nil
}

// fixStatement applies the fixes to one statement and returns the text that
// replaces it. A statement that is not a CREATE TABLE, and a table with
// nothing to fix, is returned as written. A fixed table that cannot be
// rendered back to SQL is an error: reporting it as unfixed would tell the
// operator the file is clean.
func (f *Fixer) fixStatement(stmt *statement.AbstractStatement, original string) (string, []string, error) {
	if !stmt.IsCreateTable() {
		return original, nil, nil
	}
	ct, err := stmt.ParseCreateTable()
	if err != nil {
		return original, nil, nil
	}

	fixes := f.fixTable(ct)
	if len(fixes) == 0 {
		return original, nil, nil
	}

	var sb strings.Builder
	rCtx := format.NewRestoreCtx(format.DefaultRestoreFlags, &sb)
	if err := ct.Raw.Restore(rCtx); err != nil {
		return "", nil, fmt.Errorf("render table %s after applying %s: %w", ct.TableName, strings.Join(fixes, ", "), err)
	}
	// The rendering carries no terminator of its own, and a file that declares
	// more than one table needs one between them.
	return sb.String() + ";", fixes, nil
}

// fixTable applies every fix to a parsed CREATE TABLE in place and returns one
// description per kind of fix applied. Each fix visits every column, so a
// table with three columns of the same problem has all three fixed.
func (f *Fixer) fixTable(ct *statement.CreateTable) []string {
	var fixes []string

	// Fix 1: INT AUTO_INCREMENT -> BIGINT AUTO_INCREMENT
	for _, col := range ct.Raw.Cols {
		if f.fixColumnPrimaryKeyType(col) {
			fixes = append(fixes, "INT → BIGINT for primary key")
		}
	}

	// Fix 2: charset latin1/utf8 -> utf8mb4
	if f.fixTableCharset(ct.Raw) {
		fixes = append(fixes, "charset → utf8mb4")
	}

	// Fix 3: FLOAT/DOUBLE -> DECIMAL
	if applyToColumns(ct.Raw.Cols, f.fixColumnFloatType) {
		fixes = append(fixes, "FLOAT/DOUBLE → DECIMAL") // Only report once
	}

	// Fix 4: TIMESTAMP/DATETIME without DEFAULT
	if applyToColumns(ct.Raw.Cols, f.fixColumnMissingDateDefault) {
		fixes = append(fixes, "Added DEFAULT for TIMESTAMP/DATETIME") // Only report once
	}

	return fixes
}

// applyToColumns applies fix to every column and reports whether any column
// changed. It visits them all: a predicate helper that stops at the first
// match would leave the rest of a table's columns unfixed while the fix was
// still reported as applied.
func applyToColumns(cols []*ast.ColumnDef, fix func(*ast.ColumnDef) bool) bool {
	fixed := false
	for _, col := range cols {
		if fix(col) {
			fixed = true
		}
	}
	return fixed
}

// fixColumnPrimaryKeyType changes INT to BIGINT for AUTO_INCREMENT columns.
// Returns true if a fix was applied.
func (f *Fixer) fixColumnPrimaryKeyType(col *ast.ColumnDef) bool {
	// Check if column has AUTO_INCREMENT
	hasAutoInc := false
	for _, opt := range col.Options {
		if opt.Tp == ast.ColumnOptionAutoIncrement {
			hasAutoInc = true
			break
		}
	}

	if !hasAutoInc {
		return false
	}

	// Check if it's INT (TypeLong) or smaller integer types
	tp := col.Tp.GetType()
	if tp == mysql.TypeLong || tp == mysql.TypeShort || tp == mysql.TypeInt24 || tp == mysql.TypeTiny {
		col.Tp.SetType(mysql.TypeLonglong) // BIGINT
		return true
	}

	return false
}

// fixTableCharset changes latin1/utf8 charset to utf8mb4.
// Returns true if a fix was applied.
func (f *Fixer) fixTableCharset(stmt *ast.CreateTableStmt) bool {
	fixed := false
	for _, opt := range stmt.Options {
		if opt.Tp == ast.TableOptionCharset {
			charset := strings.ToLower(opt.StrValue)
			if charset == "latin1" || charset == "utf8" {
				opt.StrValue = "utf8mb4"
				fixed = true
			}
		}
		if opt.Tp == ast.TableOptionCollate {
			collate := strings.ToLower(opt.StrValue)
			if strings.HasPrefix(collate, "latin1_") || strings.HasPrefix(collate, "utf8_") {
				opt.StrValue = "utf8mb4_unicode_ci"
				fixed = true
			}
		}
	}
	return fixed
}

// fixColumnFloatType changes FLOAT/DOUBLE to DECIMAL.
// Returns true if a fix was applied.
func (f *Fixer) fixColumnFloatType(col *ast.ColumnDef) bool {
	tp := col.Tp.GetType()
	if tp != mysql.TypeFloat && tp != mysql.TypeDouble {
		return false
	}

	// Preserve precision if specified, otherwise use defaults
	flen := col.Tp.GetFlen()
	decimal := col.Tp.GetDecimal()

	if flen <= 0 {
		flen = f.config.DefaultDecimalPrecision
	}
	if decimal < 0 {
		decimal = f.config.DefaultDecimalScale
	}

	col.Tp.SetType(mysql.TypeNewDecimal)
	col.Tp.SetFlen(flen)
	col.Tp.SetDecimal(decimal)
	return true
}

// fixColumnMissingDateDefault gives a TIMESTAMP/DATETIME column that declares
// no DEFAULT one: CURRENT_TIMESTAMP when the column is NOT NULL, NULL
// otherwise. Returns true if a fix was applied.
//
// A column that already declares a DEFAULT is left alone, including one whose
// default is a zero date. Replacing a zero date changes what new rows get,
// which is the schema owner's decision and not a mechanical fix, so the
// zero_date finding is reported rather than resolved here.
func (f *Fixer) fixColumnMissingDateDefault(col *ast.ColumnDef) bool {
	tp := col.Tp.GetType()
	if tp != mysql.TypeTimestamp && tp != mysql.TypeDatetime {
		return false
	}

	// A column that already declares a DEFAULT, zero date or not, is the
	// owner's to change.
	hasDefault := false
	isNotNull := false
	for _, opt := range col.Options {
		if opt.Tp == ast.ColumnOptionDefaultValue {
			hasDefault = true
		}
		if opt.Tp == ast.ColumnOptionNotNull {
			isNotNull = true
		}
	}

	if hasDefault {
		return false
	}

	// Add appropriate DEFAULT
	if isNotNull {
		// NOT NULL columns get DEFAULT CURRENT_TIMESTAMP
		col.Options = append(col.Options, &ast.ColumnOption{
			Tp:   ast.ColumnOptionDefaultValue,
			Expr: &ast.FuncCallExpr{FnName: ast.NewCIStr("CURRENT_TIMESTAMP")},
		})
	} else {
		// Nullable columns get DEFAULT NULL
		col.Options = append(col.Options, &ast.ColumnOption{
			Tp:   ast.ColumnOptionDefaultValue,
			Expr: ast.NewValueExpr(nil, "", ""),
		})
	}
	return true
}
