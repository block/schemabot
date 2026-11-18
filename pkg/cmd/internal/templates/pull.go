package templates

import (
	"bytes"
	"encoding/json"
	"fmt"
	"slices"
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
// copy-pasted straight out of the terminal. A pull that asked for a detailed
// catalog also gets each table's engine estimates above its DDL.
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
	writeTargetDivergence(resp)

	for _, name := range sortedKeys(resp.Namespaces) {
		ns := resp.Namespaces[name]
		fmt.Println()
		fmt.Println(annotation(fmt.Sprintf("-- Namespace %s — %d %s",
			emphasis("`"+name+"`"), len(ns.Tables), ui.Pluralize("table", len(ns.Tables)))))
		writePulledLint(ns.Lint)

		for _, table := range sortedKeys(ns.Tables) {
			fmt.Println()
			writeTableEstimates(table, ns.TableCatalog[table])
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

// divergenceLabels phrases each difference for a reader looking at the schema
// printed below, so the wording says what the other target has instead.
var divergenceLabels = map[string]string{
	apitypes.DivergenceDiffers:       "differs",
	apitypes.DivergenceOnlyOnPrimary: "missing",
	apitypes.DivergenceOnlyOnTarget:  "extra",
}

// targetDivergenceNames names each target of a pull so no two members of the
// report read alike. A target name is opaque to SchemaBot and only unique
// within the deployment that addresses it, so an environment whose deployments
// address targets of the same name would otherwise print the same header twice
// and leave the operator unable to tell which member a divergence belongs to.
// Such a target is named by its full member identity; every other target keeps
// the bare name, which is already the thing that distinguishes it.
func targetDivergenceNames(targets []*apitypes.TargetDivergence) []string {
	deploymentsByTarget := make(map[string]map[string]struct{}, len(targets))
	for _, target := range targets {
		if deploymentsByTarget[target.Target] == nil {
			deploymentsByTarget[target.Target] = make(map[string]struct{}, 1)
		}
		deploymentsByTarget[target.Target][target.Deployment] = struct{}{}
	}
	names := make([]string, len(targets))
	for i, target := range targets {
		if len(deploymentsByTarget[target.Target]) > 1 {
			names[i] = target.Deployment + "/" + target.Target
			continue
		}
		names[i] = target.Target
	}
	return names
}

// writeTargetDivergence lists every target the environment addresses and how
// each differs from the schema printed below. It renders as "--" comments like
// the rest of the pull output, so redirecting a multi-target pull into a .sql
// file still produces valid SQL.
//
// A target with no diverged tables is still listed: "these two hold the same
// schema" is the answer an operator is usually looking for, and omitting the
// converged targets would leave it indistinguishable from not having checked.
// The primary is named on the same list for the same reason — an operator
// counting targets against what they expect the environment to hold should not
// have to add one back. An environment whose targets are expected to hold the
// same schema carries no member list at all and prints nothing.
//
// When every namespace below is the primary's, the schema below is the primary
// target's and each line says how a target differs from it. When targets hold
// different namespaces, each target that supplied part of the schema below
// names the namespaces it supplied, and a difference is from the schema below.
func writeTargetDivergence(resp *apitypes.PullSchemaResponse) {
	targets := resp.Targets
	if len(targets) == 0 {
		return
	}
	supplied := make([][]string, len(targets))
	for _, namespace := range sortedKeys(resp.Namespaces) {
		if i := apitypes.BaselineTarget(targets, namespace); i >= 0 {
			supplied[i] = append(supplied[i], namespace)
		}
	}
	split := slices.ContainsFunc(supplied[1:], func(namespaces []string) bool { return len(namespaces) > 0 }) ||
		reportsNonPrimaryBaseline(targets)
	baseline := "the primary target"
	if split {
		baseline = "the schema below"
	}

	names := targetDivergenceNames(targets)
	for i, target := range targets {
		name := emphasis("`" + names[i] + "`")
		var parts []string
		switch {
		case target.Primary && !split:
			parts = append(parts, "primary target, whose schema is below")
		case len(supplied[i]) > 0:
			prefix := ""
			if target.Primary {
				prefix = "primary target, "
			}
			parts = append(parts, fmt.Sprintf("%sschema below for %s", prefix, backtickList(supplied[i])))
		case target.Primary:
			parts = append(parts, "primary target, holds none of the namespaces below")
		}
		if n := len(target.DivergedTables); n > 0 {
			parts = append(parts, fmt.Sprintf("%d %s %s from %s",
				n, ui.Pluralize("table", n), ui.PluralizeLabel("differs", "differ", n), baseline))
		}
		if len(parts) == 0 {
			if holdsAny(target, resp.Namespaces) {
				parts = append(parts, "same schema as "+baseline)
			} else {
				parts = append(parts, "holds none of the namespaces below")
			}
		}
		fmt.Println()
		fmt.Println(annotation(fmt.Sprintf("-- Target %s — %s", name, strings.Join(parts, "; "))))
		for _, table := range target.DivergedTables {
			label, ok := divergenceLabels[table.Difference]
			if !ok {
				// A difference this client does not know how to phrase is still
				// reported, since dropping it would understate the divergence.
				label = table.Difference
			}
			fmt.Println(annotation(fmt.Sprintf("--   %s.%s: %s", table.Namespace, table.Table, label)))
		}
	}
}

// reportsNonPrimaryBaseline reports whether any difference listed is against a
// namespace whose baseline is not the primary. A table filter can drop such a
// namespace from the schema below while keeping a difference inside it, and
// that difference is still from the namespace's baseline rather than from the
// primary.
func reportsNonPrimaryBaseline(targets []*apitypes.TargetDivergence) bool {
	for _, target := range targets {
		for _, table := range target.DivergedTables {
			if apitypes.BaselineTarget(targets, table.Namespace) > 0 {
				return true
			}
		}
	}
	return false
}

// holdsAny reports whether a target holds any of the pulled namespaces.
func holdsAny(target *apitypes.TargetDivergence, namespaces map[string]*apitypes.PulledNamespace) bool {
	for namespace := range namespaces {
		if target.Holds(namespace) {
			return true
		}
	}
	return false
}

// backtickList renders names as a comma-separated list of code spans.
func backtickList(names []string) string {
	quoted := make([]string, len(names))
	for i, name := range names {
		quoted[i] = "`" + name + "`"
	}
	return strings.Join(quoted, ", ")
}

// tableKindView is the catalog kind an engine reports for a view rather than
// a stored table.
const tableKindView = "view"

// writeTableEstimates renders the one part of a detailed table catalog the
// pulled DDL cannot carry: the engine's row-count and size estimates. The
// rest of the catalog — columns, indexes, foreign keys, the table comment —
// is already in the CREATE statement this line sits above, so repeating it
// would pad the output without adding a fact. A basic pull carries no
// catalog and prints nothing here.
func writeTableEstimates(table string, catalog *apitypes.TableCatalog) {
	// A view has no rows or storage of its own, so its zeroed estimates would
	// read as a fact about the data rather than an absence of one.
	if catalog == nil || catalog.Kind == tableKindView {
		return
	}
	fmt.Println(annotation(fmt.Sprintf("-- Table %s — rows ~%s, size ~%s (engine estimates)",
		emphasis("`"+table+"`"),
		ui.FormatNumber(catalog.EstimatedRowCount),
		ui.FormatBytesBinary(catalog.DataSizeBytes))))
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
