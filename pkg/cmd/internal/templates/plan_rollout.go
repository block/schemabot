package templates

import (
	"fmt"
	"strings"

	"github.com/block/schemabot/pkg/apitypes"
	"github.com/block/schemabot/pkg/cmd/cliname"
	"github.com/block/schemabot/pkg/glyph"
	"github.com/block/schemabot/pkg/presentation"
	"github.com/block/schemabot/pkg/ui"
)

// memberNamesInlineLimit caps how many member names a group heading names
// inline, as the PR comment does. A wider group leads with its coverage and
// lists the names on their own lines below.
const memberNamesInlineLimit = 8

// memberNamesListLimit caps a wide group's folded name list, so a group of
// hundreds of targets does not wall the terminal. The coverage phrase in the
// heading carries the total.
const memberNamesListLimit = 24

// memberNamesLineWidth is where a folded name list wraps.
const memberNamesLineWidth = 76

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

// WriteRolloutDivergence introduces a rollout whose members run more than one
// plan, the way the PR comment does.
func WriteRolloutDivergence(noun presentation.Noun) {
	fmt.Printf("%s diverge — what applies where:\n\n", ui.CapitalizeFirst(noun.Plural))
}

// WriteRolloutGroupHeading names the members that run the plan below it. Few
// members are named inline; a wide group leads with how much of the rollout
// it covers — "all 64 targets", "40 of 64 targets" — and folds its names
// onto wrapped lines below, capped so the heading stays one screen.
//
// One blank line separates the heading from what follows. A plan that opens
// on a namespace header brings that line itself, so opensOnNamespaceHeader
// leaves it to the header rather than stacking a second one above it.
func WriteRolloutGroupHeading(noun presentation.Noun, members []string, total int, opensOnNamespaceHeader bool) {
	if len(members) <= memberNamesInlineLimit {
		label := noun.Plural
		if len(members) == 1 {
			label = noun.Singular
		}
		fmt.Printf("%s▸ %s %s%s\n", ANSIBold, label, strings.Join(members, ", "), ANSIReset)
	} else {
		fmt.Printf("%s▸ %s%s\n", ANSIBold, presentation.CoveragePhrase(noun, len(members), total), ANSIReset)
		shown := members[:min(len(members), memberNamesListLimit)]
		for _, line := range wrapNames(shown, len(members)-len(shown)) {
			fmt.Printf("  %s%s%s\n", ANSIDim, line, ANSIReset)
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

// wrapNames joins names into lines no wider than memberNamesLineWidth,
// closing with how many more were left out.
func wrapNames(names []string, more int) []string {
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
		if line.Len() > 0 && line.Len()+1+len(item) > memberNamesLineWidth {
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
// rollout-wide apply, each with why, and the narrowed apply that runs each
// one under its own plan and its own consent. reruns holds one command per
// refused member that a narrowed apply runs, without the binary name; a
// member whose change its engine refuses has none. Past
// memberNamesInlineLimit the rest are counted rather than spelled out.
func WriteRolloutApplyRefused(noun presentation.Noun, refused []*apitypes.PlanMemberRefusalResponse, reruns []string) {
	label := noun.Plural
	if len(refused) == 1 {
		label = noun.Singular
	}
	fmt.Printf("%s Apply blocked: an apply of the whole rollout cannot run the plan of %d %s\n\n", glyph.Refused, len(refused), label)
	shown := min(len(refused), memberNamesInlineLimit)
	for _, r := range refused[:shown] {
		fmt.Printf("  • %s — %s\n", r.Member, r.Detail)
	}
	if rest := len(refused) - shown; rest > 0 {
		fmt.Printf("  and %d more\n", rest)
	}
	fmt.Println()
	if len(reruns) == 0 {
		fmt.Println("No apply can run these changes; change the schema files so each target's engine accepts them.")
		fmt.Println()
		return
	}
	fmt.Println("Apply each " + noun.Singular + " on its own, under its own plan and its own consent,")
	fmt.Println("then apply the rollout again for the rest:")
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
