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
