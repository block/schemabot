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
)

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
	return fmt.Sprintf("%d %s", count, noun.Plural)
}
