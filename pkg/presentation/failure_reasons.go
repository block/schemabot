package presentation

import (
	"fmt"
	"slices"

	"github.com/block/schemabot/pkg/glyph"
	"github.com/block/schemabot/pkg/state"
)

// Failure is one failed member of an apply, a shard or a target, with the
// reason it failed for. Reason is the reading every surface groups on, so the
// CLI and the PR comment put the same members under the same reasons (UX-9):
// both pass the member's error through the PR comment's inline sanitizer,
// which drops transport prefixes and connection details, so two members that
// failed the same way against their own hosts share one reason. Detail is the
// fuller text a surface may show in its place, such as the terminal's
// unredacted error; it never decides the grouping.
type Failure struct {
	Name   string
	Reason string
	Detail string
	// Retrying is whether the member is retrying the failure on its own
	// rather than stopped by it.
	Retrying bool
}

// FailedMember reports whether a member in opState failed on its own work and
// so belongs in a failure summary: it failed, or is retrying after a failure.
// A member stopped at the cutover reads failed, but the failure it names is
// another member's, so it is left out.
func FailedMember(opState string, stoppedAtCutover bool) bool {
	if stoppedAtCutover {
		return false
	}
	return state.IsState(opState, state.ApplyOperation.Failed, state.ApplyOperation.FailedRetryable)
}

// FailureReason is one distinct reason some members failed for, with those
// members' names in the order they failed. Detail is the members' fuller text
// when every one of them recorded the same, and "" when they differ, since no
// single member's detail then speaks for the rest.
type FailureReason struct {
	Reason string
	Detail string
	Names  []string
}

// Shown is the text a surface prints for the reason: the members' shared
// detail when they have one, the grouped reason otherwise, and
// NoReasonRecorded when the members recorded neither.
func (r FailureReason) Shown() string {
	if r.Detail != "" {
		return r.Detail
	}
	if r.Reason != "" {
		return r.Reason
	}
	return NoReasonRecorded
}

// GroupFailureReasons groups failed members by their reason, so a change that
// fails the same way on many shards reads as one reason with a count instead
// of the same text repeated on every shard. The reason that failed the most
// members comes first; reasons with the same count keep the order their first
// member appears in. An empty reason is a group of its own.
func GroupFailureReasons(failures []Failure) []FailureReason {
	var groups []FailureReason
	index := make(map[string]int, len(failures))
	for _, f := range failures {
		i, ok := index[f.Reason]
		if !ok {
			i = len(groups)
			index[f.Reason] = i
			groups = append(groups, FailureReason{Reason: f.Reason, Detail: f.Detail})
		} else if groups[i].Detail != f.Detail {
			groups[i].Detail = ""
		}
		groups[i].Names = append(groups[i].Names, f.Name)
	}
	slices.SortStableFunc(groups, func(a, b FailureReason) int {
		return len(b.Names) - len(a.Names)
	})
	return groups
}

// FailureReasonLimit bounds the reasons a failure summary lists, so members
// that each fail for their own reason cannot wall the surface.
const FailureReasonLimit = 5

// NoReasonRecorded stands in for a failed member's reason when it recorded
// none.
const NoReasonRecorded = "no reason recorded"

// ReasonNoun is what a failure summary calls its reasons.
var ReasonNoun = Noun{Singular: "reason", Plural: "reasons"}

// FailureSummary is the one reading of an apply's failed members that the CLI
// and the PR comment both render: which members failed, whether they are all
// still retrying, and why, grouped by reason.
type FailureSummary struct {
	// Names are the failed members, in the order they failed.
	Names []string
	// Retrying is whether every failed member is retrying on its own, so the
	// summary reads as attention rather than as a stopped failure.
	Retrying bool
	// Reasons are the listed reasons, most common first, at most
	// FailureReasonLimit of them.
	Reasons []FailureReason
	// MoreReasons counts the reasons past the limit, and MoreNames the
	// members they cover.
	MoreReasons int
	MoreNames   int
}

// SummarizeFailures reads failed members as one summary.
func SummarizeFailures(failures []Failure) FailureSummary {
	s := FailureSummary{Retrying: len(failures) > 0}
	for _, f := range failures {
		s.Names = append(s.Names, f.Name)
		if !f.Retrying {
			s.Retrying = false
		}
	}
	reasons := GroupFailureReasons(failures)
	listed := min(len(reasons), FailureReasonLimit)
	s.Reasons = reasons[:listed]
	for _, r := range reasons[listed:] {
		s.MoreReasons++
		s.MoreNames += len(r.Names)
	}
	return s
}

// Verb leads the summary's headline, before the members it names.
func (s FailureSummary) Verb() string {
	if s.Retrying {
		return "Retrying after a failure on"
	}
	return "Failed on"
}

// Glyph is the summary's severity: attention while every failed member is
// retrying on its own, failed once any has stopped (UX-5).
func (s FailureSummary) Glyph() string {
	if s.Retrying {
		return glyph.Attention
	}
	return glyph.Failed
}

// MoreLine is the line that stands in for the reasons past the limit, such as
// "…and 2 more reasons on 3 shards", or "" when every reason is listed.
func (s FailureSummary) MoreLine(noun Noun) string {
	if s.MoreReasons == 0 {
		return ""
	}
	return fmt.Sprintf("…and %d more %s on %s", s.MoreReasons, ReasonNoun.Label(s.MoreReasons), noun.Count(s.MoreNames))
}

// FailedReach says how far a table's failure reaches across the shards it
// runs on, such as " on 3 of 16 shards", given each attached shard's task
// status and how many shards the change spans, attached or not. A failure
// leads the table's line however many shards are still copying, so the line
// says how many it is. It is "" when the failure covers every shard, or none.
func FailedReach(shardStatuses []string, totalShards int) string {
	failed := 0
	for _, s := range shardStatuses {
		if state.NormalizeTaskStatus(s) == state.Task.Failed {
			failed++
		}
	}
	total := max(totalShards, len(shardStatuses))
	if failed == 0 || failed == total {
		return ""
	}
	return fmt.Sprintf(" on %d of %d %s", failed, total, ShardNoun.Plural)
}

// ShardRef names one shard of a sharded apply by its keyspace.
type ShardRef struct {
	Keyspace string
	Shard    string
}

// QualifyShardNames is how a failure names each of shards, which span every
// keyspace of the apply. Keyspaces split on the same key ranges share shard
// names, so when a name appears in more than one keyspace every shard is
// named keyspace/shard to say which one failed; otherwise the bare shard
// names cannot be confused and read shorter.
func QualifyShardNames(shards []ShardRef) []string {
	names := make([]string, len(shards))
	owner := make(map[string]string)
	repeated := false
	for i, s := range shards {
		names[i] = s.Shard
		if other, ok := owner[s.Shard]; ok && other != s.Keyspace {
			repeated = true
		}
		owner[s.Shard] = s.Keyspace
	}
	if repeated {
		for i, s := range shards {
			names[i] = s.Keyspace + "/" + s.Shard
		}
	}
	return names
}
