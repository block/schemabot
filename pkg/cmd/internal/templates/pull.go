package templates

import (
	"fmt"
	"sort"
	"strconv"
	"strings"

	"github.com/block/schemabot/pkg/apitypes"
	"github.com/block/schemabot/pkg/ui"
)

// WritePullSchema renders a pulled live schema for human reading: a summary
// box followed by each namespace's DDL. Everything after the box is valid SQL
// (statements terminated with ";", annotations and artifact bodies as "--"
// comments), so the output can be read in the terminal or redirected into a
// .sql file as-is.
func WritePullSchema(resp *apitypes.PullSchemaResponse) {
	rows := []BoxRow{
		{Label: "Database", Value: resp.Database},
		{Label: "Type", Value: resp.Type},
		{Label: "Environment", Value: resp.Environment},
	}
	if len(resp.Namespaces) > 1 {
		rows = append(rows, BoxRow{Label: "Namespaces", Value: strconv.Itoa(len(resp.Namespaces))})
	}
	rows = append(rows, BoxRow{Label: "Tables", Value: strconv.Itoa(int(resp.TableCount))})
	WriteBox(rows, "", nil)

	for _, name := range sortedKeys(resp.Namespaces) {
		ns := resp.Namespaces[name]
		fmt.Println()
		fmt.Printf("-- Namespace `%s` — %d %s\n", name, len(ns.Tables), ui.Pluralize("table", len(ns.Tables)))
		writePulledLint(ns.Lint)

		for _, table := range sortedKeys(ns.Tables) {
			fmt.Println()
			fmt.Println(terminateStatement(ns.Tables[table]))
		}
		for _, artifact := range sortedKeys(ns.Artifacts) {
			fmt.Println()
			fmt.Printf("-- Artifact `%s`\n", artifact)
			writeCommented("-- ", ns.Artifacts[artifact])
		}
	}
}

// writePulledLint renders a namespace's lint audit as SQL comments. A nil
// slice means lint was not requested and prints nothing; an empty slice is an
// explicit clean audit and says so.
func writePulledLint(violations []*apitypes.LintViolationResponse) {
	if violations == nil {
		return
	}
	if len(violations) == 0 {
		fmt.Println("-- Lint: no violations")
		return
	}
	fmt.Printf("-- Lint: %d %s\n", len(violations), ui.Pluralize("violation", len(violations)))
	for _, v := range violations {
		line := ""
		if v.Severity != "" {
			line += "[" + v.Severity + "] "
		}
		if v.Table != "" {
			line += v.Table + ": "
		}
		writeCommented("--   ", line+v.Message)
	}
}

// writeCommented prints content with every line prefixed as a SQL comment, so
// multi-line values (artifact bodies, lint messages) never break the
// executable-SQL contract of the surrounding output.
func writeCommented(prefix, content string) {
	content = strings.ReplaceAll(content, "\r\n", "\n")
	content = strings.TrimRight(content, "\n")
	for line := range strings.SplitSeq(content, "\n") {
		fmt.Println(prefix + line)
	}
}

// terminateStatement trims trailing whitespace and guarantees a ";" so each
// pulled DDL statement is executable when the output is saved as a .sql file.
// Engines differ on whether the pulled DDL already carries the terminator.
func terminateStatement(ddl string) string {
	ddl = strings.TrimRight(ddl, " \t\n")
	if !strings.HasSuffix(ddl, ";") {
		ddl += ";"
	}
	return ddl
}

func sortedKeys[V any](m map[string]V) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}
