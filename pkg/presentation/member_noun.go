package presentation

import "fmt"

// Noun is what the members of a group are called: the shards of a keyspace,
// the targets of a rollout, or its deployments. Every surface names a group
// with the same words, so a rollout whose targets need different work reads
// the way a keyspace whose shards do.
type Noun struct{ Singular, Plural string }

var (
	ShardNoun      = Noun{Singular: "shard", Plural: "shards"}
	TargetNoun     = Noun{Singular: "target", Plural: "targets"}
	DeploymentNoun = Noun{Singular: "deployment", Plural: "deployments"}
	TableNoun      = Noun{Singular: "table", Plural: "tables"}
)

// Label is the noun for count members: "target" for one, "targets" otherwise.
func (n Noun) Label(count int) string {
	if count == 1 {
		return n.Singular
	}
	return n.Plural
}

// Count is n members by name: "1 target" or "3 targets".
func (n Noun) Count(count int) string {
	return fmt.Sprintf("%d %s", count, n.Label(count))
}

// MemberNamesInlineLimit caps how many member names, shards or targets, a
// surface names inline. Past it, naming every one reads as a wall, so the
// group reads as its coverage (CoveragePhrase) instead. The CLI and the PR
// comment both take it, so the same group is named on one and counted on the
// other only by design, never by drift.
const MemberNamesInlineLimit = 8

// CoveragePhrase states how much of the whole a group of count members
// covers: "all 32 shards" when it covers every member, "12 of 32 shards" for
// a subset, or a bare count when the total is unknown — a subset must never
// read like whole coverage.
func CoveragePhrase(noun Noun, count, total int) string {
	if count == total {
		return fmt.Sprintf("all %d %s", count, noun.Plural)
	}
	if total > 0 {
		return fmt.Sprintf("%d of %d %s", count, total, noun.Plural)
	}
	return noun.Count(count)
}

// MemberNoun is what a rollout's counts call its members: targets when a
// deployment addresses several of them, deployments otherwise.
func MemberNoun(groups []Group) Noun {
	for _, g := range groups {
		if len(g.Members) > 1 {
			return TargetNoun
		}
	}
	return DeploymentNoun
}
