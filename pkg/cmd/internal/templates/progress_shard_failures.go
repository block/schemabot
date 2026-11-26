package templates

import (
	"fmt"
	"strings"

	"github.com/block/schemabot/pkg/presentation"
	"github.com/block/schemabot/pkg/state"
)

// ShardFailureCallout is the failure callout for a sharded apply that failed
// on more than one shard: how many failed, then each distinct reason once with
// the shards it failed, as the PR comment's callout reads them
// (presentation.SummarizeFailures). It is "" when at most one shard failed,
// which the apply's error already names, or when every keyspace runs on its
// only shard. Reasons arrive terminal-safe from ParseProgressResponse.
func ShardFailureCallout(applyState string, ops []ProgressOperation, tables []TableProgress, released bool) string {
	summary, totalShards, ok := shardFailures(applyState, ops, tables, released)
	if !ok || len(summary.Names) < 2 {
		return ""
	}
	var b strings.Builder
	if len(summary.Reasons) == 1 && summary.MoreReasons == 0 {
		subject := summary.Verb() + " " + shardNameList(summary.Names, totalShards)
		if reason := summary.Reasons[0].Reason; reason != "" {
			subject += ": " + reason
		}
		fmt.Fprintf(&b, "  %s %s%s%s\n", summary.Glyph(), ANSIRed, subject, ANSIReset)
		return b.String()
	}
	// Each reason's line names its shards, so the headline only counts them.
	fmt.Fprintf(&b, "  %s %s%s %s:%s\n", summary.Glyph(), ANSIRed, summary.Verb(), presentation.CoveragePhrase(presentation.ShardNoun, len(summary.Names), totalShards), ANSIReset)
	for _, r := range summary.Reasons {
		reason := r.Reason
		if reason == "" {
			reason = presentation.NoReasonRecorded
		}
		fmt.Fprintf(&b, "    - %s: %s\n", shardNameList(r.Names, totalShards), reason)
	}
	if more := summary.MoreLine(presentation.ShardNoun); more != "" {
		fmt.Fprintf(&b, "    - %s\n", more)
	}
	return b.String()
}

// shardFailedWhileRunning reports whether a running sharded apply has a shard
// whose own work failed, so the view says when a new apply can retry it, as
// the PR comment's footer does.
func shardFailedWhileRunning(applyState string, ops []ProgressOperation, tables []TableProgress, released bool) bool {
	if !state.IsRunningApplyState(applyState) {
		return false
	}
	summary, _, ok := shardFailures(applyState, ops, tables, released)
	return ok && len(summary.Names) > 0
}

// shardFailures reads the shards of a sharded apply whose own work failed or
// is retrying after a failure as one summary (presentation.FailedMember), with
// how many shards the apply runs on. It reports false when every keyspace runs
// on its only shard.
func shardFailures(applyState string, ops []ProgressOperation, tables []TableProgress, released bool) (presentation.FailureSummary, int, bool) {
	statuses, _, _, sharded := shardStatuses(applyState, ops, tables, released)
	if !sharded {
		return presentation.FailureSummary{}, 0, false
	}
	refs := make([]presentation.ShardRef, len(statuses))
	for i, s := range statuses {
		refs[i] = presentation.ShardRef{Keyspace: s.Keyspace, Shard: s.Shard}
	}
	names := presentation.QualifyShardNames(refs)
	var failures []presentation.Failure
	for i, s := range statuses {
		if !presentation.FailedMember(s.State, s.Presentation == presentation.StateStoppedAtCutover) {
			continue
		}
		failures = append(failures, presentation.Failure{
			Name:     names[i],
			Reason:   s.Error,
			Retrying: state.IsState(s.State, state.ApplyOperation.FailedRetryable),
		})
	}
	return presentation.SummarizeFailures(failures), len(statuses), true
}

// shardNameList names a few shards, or counts them against the apply's shards
// when naming every one would wall the callout, as the PR comment does.
func shardNameList(names []string, totalShards int) string {
	if len(names) > memberNamesInlineLimit {
		return presentation.CoveragePhrase(presentation.ShardNoun, len(names), totalShards)
	}
	if len(names) == 1 {
		return presentation.ShardNoun.Singular + " " + names[0]
	}
	return presentation.ShardNoun.Plural + " " + strings.Join(names, ", ")
}

// isShardedRollup reports whether t stands for one change across a sharded
// keyspace's shards, listed under it, rather than a single table or a
// rollout's targets.
func isShardedRollup(t TableProgress) bool {
	return len(t.Shards) > 0 && !t.AcrossTargets
}

// shardTaskStatuses is each shard's task status for t.
func shardTaskStatuses(t TableProgress) []string {
	statuses := make([]string, len(t.Shards))
	for i, s := range t.Shards {
		statuses[i] = s.Status
	}
	return statuses
}
