// Package templates provides CLI output formatting for SchemaBot commands.
package templates

import (
	"fmt"
	"strings"

	"golang.org/x/text/cases"
	"golang.org/x/text/language"

	"github.com/block/schemabot/pkg/apitypes"
	"github.com/block/schemabot/pkg/ddl"
	"github.com/block/schemabot/pkg/ui"
)

const minBoxWidth = 45

// PlanHeaderData contains data for rendering the plan header.
type PlanHeaderData struct {
	Database    string
	SchemaName  string
	Environment string
	IsMySQL     bool
	IsApply     bool
}

// WritePlanHeader writes the common plan header to stdout.
func WritePlanHeader(data PlanHeaderData) {
	dbType := "Vitess"
	if data.IsMySQL {
		dbType = "MySQL"
	}

	action := "Plan"
	if data.IsApply {
		action = "Apply"
	}
	title := fmt.Sprintf("%s Schema Change %s", dbType, action)

	// Compute box width from longest line
	lines := []string{
		title,
		fmt.Sprintf("Database: %s", data.Database),
	}
	if data.Environment != "" {
		lines = append(lines, fmt.Sprintf("Environment: %s", data.Environment))
	}
	// Show schema name (directory) for MySQL. Vitess uses keyspace headers instead.
	showSchemaName := data.IsMySQL && data.SchemaName != ""
	if showSchemaName {
		lines = append(lines, fmt.Sprintf("Schema name: %s", data.SchemaName))
	}
	boxWidth := minBoxWidth
	for _, line := range lines {
		if w := len(line) + 4; w > boxWidth { // +4 for "│  " prefix and "│" suffix
			boxWidth = w
		}
	}

	// Box drawing
	fmt.Printf("╭%s╮\n", strings.Repeat("─", boxWidth))
	fmt.Printf("│  %-*s│\n", boxWidth-2, title)
	fmt.Printf("│%s│\n", strings.Repeat(" ", boxWidth))
	fmt.Printf("│  %-*s│\n", boxWidth-2, fmt.Sprintf("Database: %s", data.Database))
	if data.Environment != "" {
		fmt.Printf("│  %-*s│\n", boxWidth-2, fmt.Sprintf("Environment: %s", data.Environment))
	}
	if showSchemaName {
		fmt.Printf("│  %-*s│\n", boxWidth-2, fmt.Sprintf("Schema name: %s", data.SchemaName))
	}
	fmt.Printf("╰%s╯\n", strings.Repeat("─", boxWidth))
	fmt.Println()
}

// DDLChange represents a single DDL change with its type.
type DDLChange struct {
	ChangeType string // "CREATE", "ALTER", "DROP"
	TableName  string
	DDL        string
}

// NamespaceChange groups DDL and VSchema changes for a single namespace (keyspace/schema).
type NamespaceChange struct {
	Namespace      string
	Changes        []DDLChange
	VSchemaChanged bool
	VSchemaDiff    string
}

// WriteNamespaceChanges writes per-namespace DDL and VSchema sections.
// For MySQL with a single namespace matching the database, the namespace header is omitted.
// For Vitess, each keyspace gets a header with optional VSchema diff.
func WriteNamespaceChanges(namespaces []NamespaceChange, isMySQL bool, database string) {
	singleNamespace := len(namespaces) == 1 && isMySQL && namespaces[0].Namespace == database

	for _, ns := range namespaces {
		if len(ns.Changes) == 0 && !ns.VSchemaChanged {
			continue
		}

		if !singleNamespace {
			label := "Keyspace"
			if isMySQL {
				label = "Schema Name"
			}
			fmt.Printf("\n  %s: %s\n\n", label, ns.Namespace)
		}

		if ns.VSchemaChanged && !isMySQL {
			fmt.Println("  VSchema:")
			if ns.VSchemaDiff != "" {
				for line := range strings.SplitSeq(ns.VSchemaDiff, "\n") {
					fmt.Printf("    %s\n", colorizeDiffLine(line))
				}
				fmt.Println()
			}
		}

		if len(ns.Changes) > 0 {
			WriteSQLChanges(ns.Changes)
		}
	}
}

// colorizeDiffLine applies ANSI colors to a diff line: green for additions,
// red for removals, cyan for hunk headers, dim for file headers.
func colorizeDiffLine(line string) string {
	if strings.HasPrefix(line, "+") && !strings.HasPrefix(line, "+++") {
		return "\033[32m" + line + "\033[0m" // green
	}
	if strings.HasPrefix(line, "-") && !strings.HasPrefix(line, "---") {
		return "\033[31m" + line + "\033[0m" // red
	}
	if strings.HasPrefix(line, "@@") {
		return "\033[36m" + line + "\033[0m" // cyan
	}
	if strings.HasPrefix(line, "---") || strings.HasPrefix(line, "+++") {
		return "\033[2m" + line + "\033[0m" // dim
	}
	return line
}

// changeSymbol returns the Terraform-style symbol for a change type.
func changeSymbol(changeType string) string {
	switch strings.ToUpper(changeType) {
	case "CHANGE_TYPE_CREATE", "CREATE":
		return "+"
	case "CHANGE_TYPE_ALTER", "ALTER":
		return "~"
	case "CHANGE_TYPE_DROP", "DROP":
		return "-"
	default:
		return "?"
	}
}

// WriteSQLChanges writes the SQL changes section with Terraform-style symbols.
// It combines multiple ALTER statements for the same table into a single statement.
func WriteSQLChanges(changes []DDLChange) {
	combined := combineAlterStatements(changes)
	for i, change := range combined {
		symbol := changeSymbol(change.ChangeType)
		formatted := FormatSQL(ddl.FormatDDL(change.DDL))
		fmt.Printf("  %s %s\n", symbol, formatted)
		// Add blank line between statements
		if i < len(combined)-1 {
			fmt.Println()
		}
	}
	fmt.Println()
}

// combineAlterStatements combines multiple ALTER statements for the same table
// into a single ALTER statement. CREATE and DROP statements are kept separate.
func combineAlterStatements(changes []DDLChange) []DDLChange {
	// Group ALTERs by table name, preserve order for non-ALTERs
	type tableAlters struct {
		tableName string
		clauses   []string // ALTER clauses without "ALTER TABLE `name`" prefix
	}

	var result []DDLChange
	altersByTable := make(map[string]*tableAlters)
	var tableOrder []string // Track order of first occurrence

	for _, change := range changes {
		changeType := strings.ToUpper(change.ChangeType)
		if changeType != "ALTER" && changeType != "CHANGE_TYPE_ALTER" {
			// Non-ALTER statements go through as-is
			result = append(result, change)
			continue
		}

		// Extract the ALTER clause (everything after "ALTER TABLE `tablename` ")
		clause := extractAlterClause(change.DDL)
		if clause == "" {
			// Couldn't parse, keep original
			result = append(result, change)
			continue
		}

		if _, exists := altersByTable[change.TableName]; !exists {
			altersByTable[change.TableName] = &tableAlters{
				tableName: change.TableName,
				clauses:   []string{},
			}
			tableOrder = append(tableOrder, change.TableName)
		}
		altersByTable[change.TableName].clauses = append(altersByTable[change.TableName].clauses, clause)
	}

	// Build combined ALTER statements in order
	for _, tableName := range tableOrder {
		alters := altersByTable[tableName]
		if len(alters.clauses) == 1 {
			// Single ALTER, reconstruct
			result = append(result, DDLChange{
				ChangeType: "ALTER",
				TableName:  tableName,
				DDL:        fmt.Sprintf("ALTER TABLE `%s` %s", tableName, alters.clauses[0]),
			})
		} else {
			// Multiple ALTERs, combine with commas
			combined := fmt.Sprintf("ALTER TABLE `%s` %s", tableName, strings.Join(alters.clauses, ", "))
			result = append(result, DDLChange{
				ChangeType: "ALTER",
				TableName:  tableName,
				DDL:        combined,
			})
		}
	}

	return result
}

// extractAlterClause extracts the clause portion from an ALTER TABLE statement.
// "ALTER TABLE `orders` ADD INDEX `idx`(`col`)" -> "ADD INDEX `idx`(`col`)"
func extractAlterClause(ddl string) string {
	upper := strings.ToUpper(ddl)
	idx := strings.Index(upper, "ALTER TABLE ")
	if idx == -1 {
		return ""
	}

	// Skip "ALTER TABLE "
	rest := ddl[idx+12:]

	// Find end of table name (backtick-quoted or unquoted)
	if len(rest) > 0 && rest[0] == '`' {
		// Backtick-quoted
		closeBacktick := strings.Index(rest[1:], "`")
		if closeBacktick == -1 {
			return ""
		}
		// Skip past closing backtick and space
		clauseStart := closeBacktick + 2 + 1 // +1 for opening backtick, +1 for closing, +1 for space
		if clauseStart < len(rest) {
			return strings.TrimSpace(rest[clauseStart:])
		}
	} else {
		// Unquoted - find first space
		spaceIdx := strings.Index(rest, " ")
		if spaceIdx != -1 && spaceIdx+1 < len(rest) {
			return strings.TrimSpace(rest[spaceIdx+1:])
		}
	}

	return ""
}

// WritePlanSummary writes the Terraform-style summary line.
func WritePlanSummary(changes []DDLChange) {
	creates := 0
	alters := 0
	drops := 0

	for _, c := range changes {
		switch strings.ToUpper(c.ChangeType) {
		case "CHANGE_TYPE_CREATE", "CREATE":
			creates++
		case "CHANGE_TYPE_ALTER", "ALTER":
			alters++
		case "CHANGE_TYPE_DROP", "DROP":
			drops++
		}
	}

	var parts []string
	if creates > 0 {
		word := "table"
		if creates > 1 {
			word = "tables"
		}
		parts = append(parts, fmt.Sprintf("%d %s to create", creates, word))
	}
	if alters > 0 {
		word := "table"
		if alters > 1 {
			word = "tables"
		}
		parts = append(parts, fmt.Sprintf("%d %s to alter", alters, word))
	}
	if drops > 0 {
		word := "table"
		if drops > 1 {
			word = "tables"
		}
		parts = append(parts, fmt.Sprintf("%d %s to drop", drops, word))
	}

	if len(parts) > 0 {
		fmt.Printf("📋 Plan: %s\n", strings.Join(parts, ", "))
	}
	fmt.Println() // Blank line after summary for separation
}

// WriteOptions writes the options section if any flags are set.
func WriteOptions(deferCutover bool) {
	var options []string
	if deferCutover {
		options = append(options, "⏸️ Defer Cutover")
	}
	if len(options) > 0 {
		fmt.Printf("Options: %s\n", strings.Join(options, " | "))
	}
}

// LintWarning represents a lint warning.
// LintWarning is a type alias for the shared lint warning type.
type LintWarning = apitypes.LintWarning

// WriteLintWarnings writes lint warnings if any.
func WriteLintWarnings(warnings []LintWarning) {
	if len(warnings) == 0 {
		return
	}
	fmt.Println("⚠️  Lint Warnings:")
	for _, w := range warnings {
		if w.Table != "" {
			fmt.Printf("  - [%s] %s\n", w.Table, w.Message)
		} else {
			fmt.Printf("  - %s\n", w.Message)
		}
	}
	fmt.Println()
}

// WriteEnvironmentHeader writes an environment section header.
func WriteEnvironmentHeader(env string) {
	// Use ANSI bold: \033[1m = bold on, \033[0m = reset
	fmt.Printf("\033[1m%s\033[0m\n\n", cases.Title(language.English).String(env))
}

// WriteNoChanges writes the no changes message.
func WriteNoChanges() {
	fmt.Println("✓ No schema changes detected.")
	fmt.Println()
}

// WriteErrors writes error messages.
func WriteErrors(errors []string) {
	fmt.Println("Errors:")
	for _, e := range errors {
		fmt.Printf("  • %s\n", e)
	}
	fmt.Println()
}

// UnsafeChange is a type alias for the shared unsafe change type.
type UnsafeChange = apitypes.UnsafeChange

// WriteUnsafeChangesWarning writes a warning about unsafe changes (for plan output).
func WriteUnsafeChangesWarning(changes []UnsafeChange) {
	if len(changes) == 0 {
		return
	}
	fmt.Println("⛔ Unsafe Changes Detected:")
	writeUnsafeChangesList(changes)
	fmt.Println()
}

// WriteUnsafeChangesBlocked writes the unsafe changes list and instruction to re-run with --allow-unsafe.
func WriteUnsafeChangesBlocked(changes []UnsafeChange, database, environment, schemaDir string) {
	if len(changes) > 0 {
		fmt.Println("⛔ Unsafe Changes Detected:")
		writeUnsafeChangesList(changes)
		fmt.Println()
	}
	fmt.Println("🚨 To proceed with these destructive changes, re-run with --allow-unsafe:")
	fmt.Println()
	fmt.Printf("  schemabot apply -s %s -e %s --allow-unsafe\n", schemaDir, environment)
	fmt.Println()
}

// WriteUnsafeWarningAllowed writes a warning when --allow-unsafe is used.
func WriteUnsafeWarningAllowed(changes []UnsafeChange) {
	if len(changes) == 0 {
		return
	}
	fmt.Println()
	fmt.Println("🚨 Unsafe Changes (--allow-unsafe enabled)")
	fmt.Println()
	fmt.Println("The following changes will permanently delete data:")
	writeUnsafeChangesList(changes)
	fmt.Println()
}

// writeUnsafeChangesList writes the list of unsafe changes, splitting multi-reason entries.
func writeUnsafeChangesList(changes []UnsafeChange) {
	for _, c := range changes {
		reason := ui.CleanLintReason(c.Reason)
		if reason != "" {
			// Split multiple reasons (joined by "; " in the engine)
			reasons := strings.Split(reason, "; ")
			if len(reasons) > 1 {
				fmt.Printf("  • %s:\n", c.Table)
				for _, r := range reasons {
					fmt.Printf("      - %s\n", r)
				}
			} else {
				fmt.Printf("  • %s: %s\n", c.Table, reason)
			}
		} else {
			fmt.Printf("  • %s: %s\n", c.Table, c.ChangeType)
		}
	}
}
