package templates

import (
	"fmt"
	"strings"
	"unicode/utf8"

	"github.com/block/schemabot/pkg/apitypes"
	"github.com/block/schemabot/pkg/cmd/cliname"
	"github.com/block/schemabot/pkg/glyph"
	"github.com/block/schemabot/pkg/presentation"
)

// memberNamesInlineLimit caps how many member names a group heading names
// inline, as the PR comment does. A wider group leads with its coverage and
// lists the names on their own lines below.
const memberNamesInlineLimit = 8

// memberNamesListLimit caps a wide group's folded name list, so a group of
// hundreds of targets does not wall the terminal. The coverage phrase in the
// heading carries the total.
const memberNamesListLimit = 24

// memberNamesLineWidth is the widest a group heading or a line of its folded
// name list runs, indent included.
const memberNamesLineWidth = 76

// memberNamesIndent indents a wide group's folded name list under its heading.
const memberNamesIndent = "  "

// RolloutNoun is what a plan calls its rollout's members: targets when each
// was planned against its own schema, deployments when they mirror the
// primary.
func RolloutNoun(rollout *apitypes.PlanRolloutResponse) presentation.Noun {
	if rollout.Independent {
		return presentation.TargetNoun
	}
	return presentation.DeploymentNoun
}

// WriteRolloutAttention writes the members an apply cannot run on as
// planned, ahead of the plans, so they are read before the plans that do not
// cover them.
func WriteRolloutAttention(noun presentation.Noun, attention []*apitypes.PlanMemberAttentionResponse) {
	if len(attention) == 0 {
		return
	}
	word, verb, pronoun := noun.Plural, "need", "them"
	if len(attention) == 1 {
		word, verb, pronoun = noun.Singular, "needs", "it"
	}
	fmt.Printf("%s%s %d %s %s attention before an apply can run on %s:%s\n", ANSIYellow, glyph.Attention, len(attention), word, verb, pronoun, ANSIReset)
	for _, a := range attention {
		fmt.Printf("  • %s — %s\n", a.Member, a.Detail)
	}
	fmt.Println()
}

// WriteRolloutGroupHeading names the members that run the plan below it. Few
// members whose names fit on the heading's line are named inline; any other
// group leads with how much of the rollout it covers — "all 64 targets",
// "40 of 64 targets" — and folds its names onto wrapped lines below, capped so
// the heading stays one screen.
//
// One blank line separates the heading from what follows. A plan that opens
// on a namespace header brings that line itself, so opensOnNamespaceHeader
// leaves it to the header rather than stacking a second one above it.
func WriteRolloutGroupHeading(noun presentation.Noun, members []string, total int, opensOnNamespaceHeader bool) {
	if heading, ok := inlineGroupHeading(noun, members); ok {
		fmt.Printf("%s%s%s\n", ANSIBold, heading, ANSIReset)
	} else {
		fmt.Printf("%s▸ %s%s\n", ANSIBold, presentation.CoveragePhrase(noun, len(members), total), ANSIReset)
		shown := members[:min(len(members), memberNamesListLimit)]
		for _, line := range wrapNames(shown, len(members)-len(shown), utf8.RuneCountInString(memberNamesIndent)) {
			fmt.Printf("%s%s%s%s\n", memberNamesIndent, ANSIDim, line, ANSIReset)
		}
	}
	if !opensOnNamespaceHeader {
		fmt.Println()
	}
}

// WriteRolloutGroupNoChanges says, under a group's heading, that its members
// are already at the desired schema. Such a group renders beside groups with
// work, so it carries no ✓: the rollout is not done, and the plan's one
// summary closes the output after every group.
func WriteRolloutGroupNoChanges() {
	fmt.Println("  No schema changes detected")
	fmt.Println()
}

// inlineGroupHeading is the heading naming members on its own line, and
// whether there are few enough of them, short enough, to fit there.
func inlineGroupHeading(noun presentation.Noun, members []string) (string, bool) {
	if len(members) > memberNamesInlineLimit {
		return "", false
	}
	label := noun.Plural
	if len(members) == 1 {
		label = noun.Singular
	}
	heading := "▸ " + label + " " + strings.Join(members, ", ")
	return heading, utf8.RuneCountInString(heading) <= memberNamesLineWidth
}

// wrapNames joins names into lines that, printed after indent columns, run no
// wider than memberNamesLineWidth, closing with how many more were left out.
func wrapNames(names []string, more, indent int) []string {
	width := memberNamesLineWidth - indent
	items := append([]string(nil), names...)
	if more > 0 {
		items = append(items, fmt.Sprintf("and %d more", more))
	}
	var lines []string
	var line strings.Builder
	for i, item := range items {
		if i < len(items)-1 {
			item += ","
		}
		if line.Len() > 0 && line.Len()+1+len(item) > width {
			lines = append(lines, line.String())
			line.Reset()
		}
		if line.Len() > 0 {
			line.WriteString(" ")
		}
		line.WriteString(item)
	}
	if line.Len() > 0 {
		lines = append(lines, line.String())
	}
	return lines
}

// WriteRolloutApplyRefused writes why an apply of a whole rollout was refused
// before it started: members whose own plans the server will not run in a
// rollout-wide apply, each with why, and the next step for each. A member
// whose change its engine refuses is told to change the schema files, since no
// apply runs it. Every other member gets the narrowed apply that runs it under
// its own plan and its own consent. reruns holds one command per refused
// member that a narrowed apply runs, without the binary name. Past
// memberNamesInlineLimit the rest are counted rather than spelled out.
func WriteRolloutApplyRefused(noun presentation.Noun, refused []*apitypes.PlanMemberRefusalResponse, reruns []string) {
	fmt.Printf("%s Apply blocked: an apply of the whole rollout cannot run the plan of %d %s\n\n", glyph.Refused, len(refused), nounLabel(noun, len(refused)))
	shown := min(len(refused), memberNamesInlineLimit)
	for _, r := range refused[:shown] {
		fmt.Printf("  • %s — %s\n", r.Member, r.Detail)
	}
	if rest := len(refused) - shown; rest > 0 {
		fmt.Printf("  and %d more\n", rest)
	}
	fmt.Println()
	blocked := countBlockedRefusals(refused)
	switch {
	case blocked == len(refused):
		fmt.Println("No apply can run these changes; change the schema files so each target's engine accepts them.")
		fmt.Println()
	case blocked > 0:
		fmt.Printf("No apply can run the changes of the %d %s whose engine refuses them; change the schema files so each target's engine accepts them.\n", blocked, nounLabel(noun, blocked))
		fmt.Println()
	}
	if len(reruns) == 0 {
		return
	}
	if blocked > 0 {
		fmt.Println("Apply each other " + noun.Singular + " on its own, under its own plan and its own consent:")
	} else {
		fmt.Println("Apply each " + noun.Singular + " on its own, under its own plan and its own consent,")
		fmt.Println("then apply the rollout again for the rest:")
	}
	fmt.Println()
	shownReruns := min(len(reruns), memberNamesInlineLimit)
	for _, rerun := range reruns[:shownReruns] {
		fmt.Printf("  %s %s\n", cliname.Name(), rerun)
	}
	if rest := len(reruns) - shownReruns; rest > 0 {
		fmt.Printf("  and the same for %d more %s\n", rest, noun.Plural)
	}
	fmt.Println()
}

// countBlockedRefusals counts the refused members whose change their engine
// refuses, which no apply runs.
func countBlockedRefusals(refused []*apitypes.PlanMemberRefusalResponse) int {
	n := 0
	for _, r := range refused {
		if r.Reason == apitypes.PlanMemberBlocked {
			n++
		}
	}
	return n
}

// nounLabel is noun's singular for one member and its plural otherwise.
func nounLabel(noun presentation.Noun, n int) string {
	if n == 1 {
		return noun.Singular
	}
	return noun.Plural
}
