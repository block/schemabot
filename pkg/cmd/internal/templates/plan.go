// Package templates provides CLI output formatting for SchemaBot commands.
package templates

import (
	"fmt"
	"sort"
	"strings"

	"golang.org/x/text/cases"
	"golang.org/x/text/language"

	"github.com/block/schemabot/pkg/apitypes"
	"github.com/block/schemabot/pkg/cmd/cliname"
	"github.com/block/schemabot/pkg/glyph"
	"github.com/block/schemabot/pkg/schema"
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

	// Sort a copy so callers aren't affected by reordering. This keeps output
	// stable and groups similarly named namespaces together, but collapsing
	// still only applies to consecutive namespaces with identical changes.
	sorted := make([]NamespaceChange, len(namespaces))
	copy(sorted, namespaces)
	sort.Slice(sorted, func(i, j int) bool {
		return sorted[i].Namespace < sorted[j].Namespace
	})
	namespaces = sorted

	// Group consecutive keyspaces with identical DDL changes for collapsing.
	type nsGroup struct {
		namespaces []NamespaceChange
	}
	var groups []nsGroup
	for _, ns := range namespaces {
		if len(ns.Changes) == 0 && !ns.VSchemaChanged {
			continue
		}
		// Try to merge with previous group if DDL is identical
		if len(groups) > 0 && !ns.VSchemaChanged {
			prev := &groups[len(groups)-1]
			if !prev.namespaces[0].VSchemaChanged && ddlChangesEqual(prev.namespaces[0].Changes, ns.Changes) {
				prev.namespaces = append(prev.namespaces, ns)
				continue
			}
		}
		groups = append(groups, nsGroup{namespaces: []NamespaceChange{ns}})
	}

	const collapseThreshold = 6
	const maxShown = 3

	for _, g := range groups {
		if !singleNamespace && len(g.namespaces) >= collapseThreshold {
			// Show first few keyspace headers, then collapse the rest
			for i, ns := range g.namespaces {
				if i >= maxShown {
					fmt.Printf("\n  %s... and %d more keyspaces with identical changes%s\n",
						ANSIDim, len(g.namespaces)-maxShown, ANSIReset)
					break
				}
				fmt.Print(FormatKeyspaceHeader(ns.Namespace))
			}
			// Show DDL once
			WriteSQLChanges(g.namespaces[0].Changes)
		} else {
			for _, ns := range g.namespaces {
				if !singleNamespace {
					fmt.Print(FormatKeyspaceHeader(ns.Namespace))
				}
				if ns.VSchemaChanged && !isMySQL {
					fmt.Println(indentTable + "~ VSchema:")
					if ns.VSchemaDiff != "" {
						fmt.Print(FormatVSchemaDiff(ns.VSchemaDiff, indentContent))
						fmt.Println()
					}
				}
				if len(ns.Changes) > 0 {
					WriteSQLChanges(ns.Changes)
				}
			}
		}
	}
}

// ddlChangesEqual returns true if two slices of DDL changes have identical content.
func ddlChangesEqual(a, b []DDLChange) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i].ChangeType != b[i].ChangeType || a[i].DDL != b[i].DDL || a[i].TableName != b[i].TableName {
			return false
		}
	}
	return true
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

// WriteSQLChanges writes the SQL changes section matching the progress view format:
// table name on its own line with change symbol, DDL indented below.
func WriteSQLChanges(changes []DDLChange) {
	combined := combineAlterStatements(changes)
	for _, change := range combined {
		// Table name line: "     ~ tablename:"
		fmt.Printf(indentTable+"%s%s\n", progressSymbol(change.ChangeType), change.TableName)
		// DDL indented below. The plan view reconstructs statements under
		// MySQL grammar (combineAlterStatements), so it renders MySQL-pinned.
		fmt.Print(formatProgressDDLForDialect(schema.DialectMySQL, change.DDL))
		fmt.Println()
	}
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

// VSchemaChange represents a VSchema diff for a keyspace.
type VSchemaChange struct {
	Keyspace string
	Diff     string
}

// WritePlanSummaryWithVSchema writes a single plan summary line including VSchema changes.
func WritePlanSummaryWithVSchema(ddlChanges []DDLChange, vschemaChanges []VSchemaChange) {
	creates := 0
	alters := 0
	drops := 0
	for _, c := range ddlChanges {
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
	if len(vschemaChanges) > 0 {
		word := "VSchema change"
		if len(vschemaChanges) > 1 {
			word = "VSchema changes"
		}
		parts = append(parts, fmt.Sprintf("%d %s", len(vschemaChanges), word))
	}

	if len(parts) > 0 {
		fmt.Printf("📋 **Plan**: %s\n", strings.Join(parts, ", "))
		fmt.Println()
	}
}

// WriteOptions writes the options section if any flags are set.
func WriteOptions(deferCutover bool, skipRevert bool) {
	var options []string
	if deferCutover {
		options = append(options, "⏸️ Defer Cutover")
	}
	if skipRevert {
		options = append(options, "⏩ Skip Revert")
	}
	if len(options) > 0 {
		fmt.Printf("Options: %s\n", strings.Join(options, " | "))
	}
}

// WriteLintViolations writes advisory lint findings, mirroring the unsafe
// changes list shape: a count in the header and "table: message" items.
func WriteLintViolations(warnings []apitypes.LintViolationResponse) {
	if len(warnings) == 0 {
		return
	}
	fmt.Printf("\U0001f4a1 Lint Warnings (%d):\n", len(warnings))
	for _, w := range warnings {
		if w.Table != "" {
			fmt.Printf("  • %s: %s\n", w.Table, w.Message)
		} else {
			fmt.Printf("  • %s\n", w.Message)
		}
	}
	fmt.Println()
}

// WriteEnvironmentHeader writes an environment section header.
func WriteEnvironmentHeader(env string) {
	// Use ANSI bold: \033[1m = bold on, \033[0m = reset
	fmt.Printf("\033[1m%s\033[0m\n", cases.Title(language.English).String(env))
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

// WriteIgnoredNamespaces disclosure: the plan was built from a deliberately
// partial desired state, so a reader can distinguish "this namespace has no
// changes" from "this namespace was withheld by config". Unmatched entries are
// configured exclusions that removed nothing (typo, case mismatch, or stale
// entry) — the namespaces they name are fully reconciled.
func WriteIgnoredNamespaces(ignored, unmatched []string) {
	if len(ignored) > 0 {
		fmt.Printf(glyph.Info+"  Namespaces excluded by ignore_namespaces: %s\n", strings.Join(ignored, ", "))
	}
	for _, entry := range unmatched {
		fmt.Printf(glyph.Attention+"  ignore_namespaces entry %q matched no namespace and excluded nothing\n", entry)
	}
	if len(ignored) > 0 || len(unmatched) > 0 {
		fmt.Println()
	}
}

// UnsafeChange is a type alias for the shared unsafe change type.
type UnsafeChange = apitypes.UnsafeChange

// WriteUnsafeChangesWarning writes a warning about unsafe changes (for plan output).
// At plan time nothing has been refused yet — the changes await consent, so the
// heading carries Attention, matching the PR plan comment and list-plans.
func WriteUnsafeChangesWarning(changes []UnsafeChange) {
	if len(changes) == 0 {
		return
	}
	fmt.Println(glyph.Attention + " Unsafe Changes Detected:")
	writeUnsafeChangesList(changes)
	fmt.Println()
}

// WriteUnsafeChangesBlocked writes the unsafe changes list and instruction to re-run with --allow-unsafe.
// The apply was refused, so Refused attaches to the refusal itself — the heading
// names the blocked apply, not the unsafeness of the changes.
func WriteUnsafeChangesBlocked(changes []UnsafeChange, database, environment, schemaDir string) {
	if len(changes) > 0 {
		fmt.Printf(glyph.Refused+" Apply blocked: %d unsafe change(s) detected\n", countUnsafeFindings(changes))
		writeUnsafeChangesList(changes)
		fmt.Println()
	}
	fmt.Println(glyph.Escalation + " To proceed with these destructive changes, re-run with --allow-unsafe:")
	fmt.Println()
	fmt.Printf("  %s apply -s %s -e %s --allow-unsafe\n", cliname.Name(), schemaDir, environment)
	fmt.Println()
}

// WriteUnsafeWarningAllowed writes a warning when --allow-unsafe is used.
func WriteUnsafeWarningAllowed(changes []UnsafeChange) {
	if len(changes) == 0 {
		return
	}
	fmt.Println()
	fmt.Println(glyph.Escalation + " Unsafe Changes (--allow-unsafe enabled)")
	fmt.Println()
	fmt.Println("The following unsafe changes will be applied:")
	writeUnsafeChangesList(changes)
	fmt.Println()
}

// writeUnsafeChangesList writes the unsafe changes one numbered line per
// finding, the same list shape as the PR plan comment, so a heading's count
// always equals the number of lines below it and a finding can be referenced
// by its number.
func writeUnsafeChangesList(changes []UnsafeChange) {
	n := 0
	for _, c := range changes {
		reasons := ui.LintReasons(c.Reason)
		if len(reasons) == 0 {
			n++
			fmt.Printf("  %d. %s: %s\n", n, c.Table, c.ChangeType)
			continue
		}
		for _, r := range reasons {
			n++
			fmt.Printf("  %d. %s: %s\n", n, c.Table, r)
		}
	}
}

// countUnsafeFindings sums the individual findings across changes so the
// apply-blocked heading counts exactly what the list below shows; a change
// with no parseable reason still counts once.
func countUnsafeFindings(changes []UnsafeChange) int {
	n := 0
	for _, c := range changes {
		if reasons := ui.LintReasons(c.Reason); len(reasons) > 0 {
			n += len(reasons)
		} else {
			n++
		}
	}
	return n
}
