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

// maxPlanProblemsBytes bounds the refused statements one comment lists, every
// environment's together, well inside GitHub's comment limit. The lists are
// fixed text with no truncation of their own, so without a shared bound a few
// environments with long paths and messages could push the comment past the
// limit and lose every location. The ones beyond it are counted.
const maxPlanProblemsBytes = 24 * 1024

// planProblemBudget is what remains of maxPlanProblemsBytes as a comment's
// environments list their refused statements.
type planProblemBudget struct{ remaining int }

func newPlanProblemBudget() *planProblemBudget {
	return &planProblemBudget{remaining: maxPlanProblemsBytes}
}

// RefusedStatements is what a refused plan names in the schema files: the
// statements its answer lists, and how many more it refused without listing
// them to keep that answer bounded.
type RefusedStatements struct {
	Problems []engine.PlanProblem
	Omitted  int
}

// total is how many statements the plan refused.
func (r RefusedStatements) total() int {
	return len(r.Problems) + r.Omitted
}

// writePlanProblems lists every statement an environment's plan refused, so
// the files' author can fix them all in one push. Each one is its location,
// what to change, and the statement's opening line in a SQL block. It lists at
// most maxRenderedPlanProblems, each that fits what is left of the comment's
// budget, and counts the rest. It writes nothing and reports false when not
// one fits, so the caller shows the refusal text instead of a list that names
// nothing.
func writePlanProblems(sb *strings.Builder, refused RefusedStatements, budget *planProblemBudget) bool {
	var entries strings.Builder
	listed := 0
	for _, problem := range refused.Problems {
		if listed == maxRenderedPlanProblems {
			break
		}
		var entry strings.Builder
		writePlanProblem(&entry, problem)
		if entry.Len() > budget.remaining {
			continue
		}
		budget.remaining -= entry.Len()
		entries.WriteString(entry.String())
		listed++
	}
	if listed == 0 {
		return false
	}
	fmt.Fprintf(sb, "%s **SchemaBot can't plan %s in the schema files:**\n\n", glyph.Failed, statementCount(refused.total()))
	sb.WriteString(entries.String())
	if hidden := refused.total() - listed; hidden > 0 {
		fmt.Fprintf(sb, "…and %s more.\n\n", statementCount(hidden))
	}
	return true
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
