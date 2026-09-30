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
func WriteRolloutGroupHeading(noun presentation.Noun, members []string, total int) {
	if len(members) <= memberNamesInlineLimit {
		label := noun.Plural
		if len(members) == 1 {
			label = noun.Singular
		}
		fmt.Printf("%s▸ %s %s%s\n\n", ANSIBold, label, strings.Join(members, ", "), ANSIReset)
		return
	}
	fmt.Printf("%s▸ %s%s\n", ANSIBold, presentation.CoveragePhrase(noun, len(members), total), ANSIReset)
	shown := members[:min(len(members), memberNamesListLimit)]
	for _, line := range wrapNames(shown, len(members)-len(shown)) {
		fmt.Printf("  %s%s%s\n", ANSIDim, line, ANSIReset)
	}
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

// WriteUnsafeBesideConvergedPrimary writes why an apply of a whole rollout was
// refused when its primary is already at the desired schema and other members
// carry unsafe changes, and the narrowed apply that runs each of those members
// under its own plan and its own consent. reruns holds one command per member,
// without the binary name, in the order of members; past
// memberNamesInlineLimit the rest are named rather than spelled out.
func WriteUnsafeBesideConvergedPrimary(noun presentation.Noun, primary string, members []string, changes []UnsafeChange, reruns []string) {
	fmt.Printf("%s Apply blocked: %d unsafe change(s) on %s other than the rollout primary %s, which is already at the desired schema\n",
		glyph.Refused, countUnsafeFindings(changes), noun.Plural, primary)
	writeUnsafeChangesList(changes)
	fmt.Println()
	fmt.Println("An apply of the whole rollout runs from the primary's plan, which has no")
	fmt.Println("unsafe change to consent to, so --allow-unsafe cannot run these. Apply")
	fmt.Println("each " + noun.Singular + " that carries them on its own:")
	fmt.Println()
	shown := min(len(reruns), memberNamesInlineLimit)
	for _, rerun := range reruns[:shown] {
		fmt.Printf("  %s %s\n", cliname.Name(), rerun)
	}
	if rest := members[shown:]; len(rest) > 0 {
		fmt.Println()
		fmt.Printf("and the same for %d more %s:\n", len(rest), noun.Plural)
		listed := rest[:min(len(rest), memberNamesListLimit)]
		for _, line := range wrapNames(listed, len(rest)-len(listed)) {
			fmt.Printf("  %s\n", line)
		}
	}
	fmt.Println()
}
