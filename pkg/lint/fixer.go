// Package lint provides schema linting and fixing using Spirit's parser.
package lint

import (
	"fmt"
	"slices"
	"strings"

	"github.com/block/spirit/pkg/statement"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/format"
	"github.com/pingcap/tidb/pkg/parser/mysql"
)

// Fixer generates auto-fixes for lint violations using AST manipulation.
// Parses SQL, walks the AST to apply fixes, and restores to canonical form.
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
	FixedSQL    string   // Fixed SQL content (empty if no changes)
	Changed     bool     // Whether any changes were made
	Fixes       []string // Description of fixes applied
}

// FixFilesResult contains the result of fixing multiple files.
type FixFilesResult struct {
	Files           []FixResult // Results for each file
	TotalFixed      int         // Number of issues fixed
	UnfixableIssues []Result    // Issues that couldn't be auto-fixed
}

// FixFiles applies auto-fixes to a set of SQL schema files.
// Returns fixed files, any issues that couldn't be automatically fixed, and any error.
func (f *Fixer) FixFiles(files map[string]string) (*FixFilesResult, error) {
	result := &FixFilesResult{}

	for filename, content := range files {
		fileResult := FixResult{
			Filename:    filename,
			OriginalSQL: content,
		}

		// Parse and fix using AST manipulation
		fixed, fixes := f.fixCreateTable(content)
		if fixed != content {
			fileResult.FixedSQL = fixed
			fileResult.Changed = true
			fileResult.Fixes = fixes
			result.TotalFixed += len(fixes)
		}

		result.Files = append(result.Files, fileResult)
	}

	// Run lint after fixes to report any remaining unfixable issues
	linter := New()
	for filename, content := range files {
		// Use fixed content if available
		for _, fr := range result.Files {
			if fr.Filename == filename && fr.Changed {
				content = fr.FixedSQL
				break
			}
		}
		lintResults, err := linter.LintSchema(map[string]string{filename: content})
		if err != nil {
			return nil, fmt.Errorf("failed to lint %s: %w", filename, err)
		}
		for _, lr := range lintResults {
			// Report non-style issues that we couldn't auto-fix
			if lr.Severity == "error" {
				result.UnfixableIssues = append(result.UnfixableIssues, lr)
			}
		}
	}

	return result, nil
}

// fixCreateTable parses a CREATE TABLE statement, applies all fixes via AST
// manipulation, and returns the canonical fixed SQL.
func (f *Fixer) fixCreateTable(sql string) (string, []string) {
	ct, err := statement.ParseCreateTable(sql)
	if err != nil {
		// Not a valid CREATE TABLE, return unchanged
		return sql, nil
	}

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
	if slices.ContainsFunc(ct.Raw.Cols, f.fixColumnFloatType) {
		fixes = append(fixes, "FLOAT/DOUBLE → DECIMAL") // Only report once
	}

	// Fix 4: TIMESTAMP/DATETIME without DEFAULT
	if slices.ContainsFunc(ct.Raw.Cols, f.fixColumnZeroDate) {
		fixes = append(fixes, "Added DEFAULT for TIMESTAMP/DATETIME") // Only report once
	}

	// Restore to canonical form
	var sb strings.Builder
	rCtx := format.NewRestoreCtx(format.DefaultRestoreFlags, &sb)
	if err := ct.Raw.Restore(rCtx); err != nil {
		// Restore failed, return unchanged
		return sql, nil
	}

	canonical := sb.String()

	// If only canonicalization changed (no semantic fixes), report it
	if len(fixes) == 0 && canonical != sql {
		fixes = append(fixes, "Normalized to canonical format")
	}

	if canonical != sql {
		return canonical, fixes
	}
	return sql, nil
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

// fixColumnZeroDate adds DEFAULT for TIMESTAMP/DATETIME columns without one.
// Returns true if a fix was applied.
func (f *Fixer) fixColumnZeroDate(col *ast.ColumnDef) bool {
	tp := col.Tp.GetType()
	if tp != mysql.TypeTimestamp && tp != mysql.TypeDatetime {
		return false
	}

	// Check if DEFAULT already exists
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
