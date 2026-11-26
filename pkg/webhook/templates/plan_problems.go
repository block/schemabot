package templates

import (
	"fmt"
	"strings"

	"github.com/block/schemabot/pkg/engine"
	"github.com/block/schemabot/pkg/glyph"
)

// maxRenderedPlanProblems bounds how many refused statements one environment's
// section lists, so a file set with many refusals cannot crowd out the rest of
// the comment. The ones beyond it are counted, not dropped silently.
const maxRenderedPlanProblems = 25

// maxPlanProblemExcerptRunes bounds a refused statement's excerpt: enough to
// recognize the statement, never its whole body.
const maxPlanProblemExcerptRunes = 120

// writePlanProblems lists every statement an environment's plan refused, so
// the files' author can fix them all in one push. Each one is its location,
// what to change, and the statement's opening line in a SQL block.
func writePlanProblems(sb *strings.Builder, problems []engine.PlanProblem) {
	fmt.Fprintf(sb, "%s **SchemaBot can't plan %s in the schema files:**\n\n", glyph.Failed, statementCount(len(problems)))
	shown := problems
	if len(shown) > maxRenderedPlanProblems {
		shown = shown[:maxRenderedPlanProblems]
	}
	for _, problem := range shown {
		writePlanProblem(sb, problem)
	}
	if hidden := len(problems) - len(shown); hidden > 0 {
		fmt.Fprintf(sb, "…and %s more.\n\n", statementCount(hidden))
	}
}

func writePlanProblem(sb *strings.Builder, problem engine.PlanProblem) {
	message := markdownReason(sanitizeCommentError(problem.Message))
	if location := planProblemLocation(problem); location != "" {
		fmt.Fprintf(sb, "**%s:** %s\n", location, message)
	} else {
		sb.WriteString(message + "\n")
	}
	if excerpt := statementExcerpt(problem.Statement); excerpt != "" {
		writeFencedBlock(sb, "sql", excerpt)
	}
	sb.WriteString("\n")
}

// planProblemLocation is where a refused statement is, as its author would
// look for it: the file's path, then the line it starts on when known.
func planProblemLocation(problem engine.PlanProblem) string {
	file := flattenIdentifier(problem.File)
	switch {
	case file != "" && problem.Line > 0:
		return fmt.Sprintf("%s, line %d", inlineCode(file), problem.Line)
	case file != "":
		return inlineCode(file)
	case problem.Line > 0:
		return fmt.Sprintf("Line %d", problem.Line)
	default:
		return ""
	}
}

// statementExcerpt is a refused statement's first non-blank line, cut to a
// recognizable length, with "…" marking that more of the statement follows.
func statementExcerpt(statement string) string {
	lines := strings.Split(strings.TrimSpace(stripControlText(statement)), "\n")
	first := strings.TrimRight(lines[0], " \t\r")
	more := len(lines) > 1
	if runes := []rune(first); len(runes) > maxPlanProblemExcerptRunes {
		first = strings.TrimRight(string(runes[:maxPlanProblemExcerptRunes]), " ")
		more = true
	}
	if first == "" {
		return ""
	}
	if more {
		first += " …"
	}
	return first
}

func statementCount(n int) string {
	if n == 1 {
		return "1 statement"
	}
	return fmt.Sprintf("%d statements", n)
}
