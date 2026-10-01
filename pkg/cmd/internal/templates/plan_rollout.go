package templates

import (
	"fmt"

	"github.com/block/schemabot/pkg/apitypes"
	"github.com/block/schemabot/pkg/cmd/cliname"
	"github.com/block/schemabot/pkg/glyph"
	"github.com/block/schemabot/pkg/presentation"
)

// memberNamesInlineLimit caps how many member names a group heading names
// inline, as the PR comment does. A wider group leads with its coverage and
// lists the names on their own lines below.
const memberNamesInlineLimit = 8

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
