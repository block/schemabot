package templates

import (
	"bytes"
	"encoding/json"
	"fmt"
	"sort"
	"strconv"
	"strings"

	"github.com/block/schemabot/pkg/apitypes"
	"github.com/block/schemabot/pkg/ui"
)

// WritePullSchema renders a pulled live schema for human reading: a summary
// box followed by each namespace's DDL. The DDL renders as valid SQL
// (statements terminated with ";", annotations as "--" comments) so a
// DDL-only pull can be redirected into a .sql file as-is. Artifact bodies
// (e.g. a VSchema) print raw under their comment header so they can be
// copy-pasted straight out of the terminal.
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
		fmt.Println(annotation(fmt.Sprintf("-- Namespace %s — %d %s",
			emphasis("`"+name+"`"), len(ns.Tables), ui.Pluralize("table", len(ns.Tables)))))
		writePulledLint(ns.Lint)

		for _, table := range sortedKeys(ns.Tables) {
			fmt.Println()
			stmt := terminateStatement(ns.Tables[table])
			if ui.Colors {
				stmt = FormatSQL(stmt)
			}
			fmt.Println(stmt)
		}
		for _, artifact := range sortedKeys(ns.Artifacts) {
			fmt.Println()
			fmt.Println(annotation(fmt.Sprintf("-- Artifact %s", emphasis("`"+artifact+"`"))))
			body, isJSON := formatArtifact(ns.Artifacts[artifact])
			if ui.Colors && isJSON {
				body = colorizeJSON(body)
			}
			fmt.Println(body)
		}
	}
}

// annotation renders a "--" comment line dimmed on interactive terminals, so
// the schema content stands out from its scaffolding. Plain everywhere else.
func annotation(s string) string {
	return styled(ANSIDim, s)
}

// styled wraps s in an ANSI code when stdout is an interactive terminal, and
// returns it untouched otherwise. colorWrap owns the escape shape; this adds
// the color gate the pull rendering needs, because redirecting a pull to a
// .sql file must produce the plain text byte for byte.
func styled(code, s string) string {
	if !ui.Colors {
		return s
	}
	return colorWrap(code)(s)
}

// emphasis bolds a name inside an annotation line, resuming the dim style
// afterwards. Only meaningful within annotation(); plain when colors are off.
func emphasis(s string) string {
	if !ui.Colors {
		return s
	}
	return ANSIReset + ANSIBold + s + ANSIReset + ANSIDim
}

// writePulledLint renders a namespace's lint audit as SQL comments. A nil
// slice means lint was not requested and prints nothing; an empty slice is an
// explicit clean audit and says so.
func writePulledLint(violations []*apitypes.LintViolationResponse) {
	if violations == nil {
		return
	}
	if len(violations) == 0 {
		fmt.Println(annotation("-- Lint: no violations"))
		return
	}
	fmt.Println(annotation(fmt.Sprintf("-- Lint: %d %s", len(violations), ui.Pluralize("violation", len(violations)))))
	for _, v := range violations {
		line := ""
		if v.Severity != "" {
			line += severityTag(v.Severity) + " "
		}
		if v.Table != "" {
			line += v.Table + ": "
		}
		writeCommented("--   ", line+v.Message)
	}
}

// severityTag renders a lint severity as its bracketed tag, colored on
// interactive terminals so warnings and errors are scannable at a glance.
func severityTag(severity string) string {
	tag := "[" + severity + "]"
	switch strings.ToLower(severity) {
	case "warning":
		return styled(ANSIYellow, tag)
	case "error":
		return styled(ANSIRed, tag)
	default:
		return tag
	}
}

// formatArtifact re-indents a JSON artifact body (e.g. a VSchema, often
// stored compactly) so each key reads on its own line, and normalizes
// line endings and trailing whitespace. It reports whether the body was JSON
// so the caller can colorize it. Non-JSON content passes through with
// only the normalization — like FormatDDL, this is a best-effort display
// formatter, so falling back to the original content is acceptable.
func formatArtifact(content string) (string, bool) {
	var indented bytes.Buffer
	isJSON := json.Indent(&indented, []byte(content), "", "  ") == nil
	if isJSON {
		content = indented.String()
	}
	content = strings.ReplaceAll(content, "\r\n", "\n")
	return strings.TrimRight(content, "\n"), isJSON
}

// colorizeJSON adds jq-style coloring to an indented JSON document: object
// keys in cyan, string values in green, structural punctuation dimmed. The
// document is colored by a scan of its string literals and punctuation, not
// re-encoded, so the text content is byte-identical to the plain rendering.
func colorizeJSON(doc string) string {
	var b strings.Builder
	for i := 0; i < len(doc); {
		c := doc[i]
		switch {
		case c == '"':
			end := jsonStringEnd(doc, i)
			color := ANSIGreen
			if jsonKeyFollows(doc, end) {
				color = ANSICyan
			}
			b.WriteString(color)
			b.WriteString(doc[i:end])
			b.WriteString(ANSIReset)
			i = end
		case strings.ContainsRune("{}[],:", rune(c)):
			b.WriteString(ANSIDim)
			b.WriteByte(c)
			b.WriteString(ANSIReset)
			i++
		default:
			b.WriteByte(c)
			i++
		}
	}
	return b.String()
}

// jsonStringEnd returns the index just past the string literal whose opening
// quote is at doc[start], honoring backslash escapes.
func jsonStringEnd(doc string, start int) int {
	for i := start + 1; i < len(doc); i++ {
		switch doc[i] {
		case '\\':
			i++
		case '"':
			return i + 1
		}
	}
	return len(doc)
}

// jsonKeyFollows reports whether the string literal ending at doc[end] is an
// object key, i.e. the next non-whitespace character is a colon.
func jsonKeyFollows(doc string, end int) bool {
	for i := end; i < len(doc); i++ {
		switch doc[i] {
		case ' ', '\t', '\n':
			continue
		case ':':
			return true
		default:
			return false
		}
	}
	return false
}

// writeCommented prints content with every line prefixed as a SQL comment, so
// multi-line values (e.g. lint messages) never break the executable-SQL
// contract of the DDL rendering.
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
