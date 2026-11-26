package templates

import (
	"fmt"
	"strings"

	"github.com/block/schemabot/pkg/presentation"
	"github.com/block/schemabot/pkg/state"
	"github.com/block/schemabot/pkg/ui"
	webhooktemplates "github.com/block/schemabot/pkg/webhook/templates"
)

// ShardFailureCallout is the failure callout for a sharded apply: how many
// shards failed, then each distinct reason once with the shards it failed, as
// the PR comment's callout reads them (presentation.SummarizeFailures). It is
// "" when no shard failed, or when one failed under a name the apply's error
// already gives in full, or when every keyspace runs on its only shard.
func ShardFailureCallout(applyState string, ops []ProgressOperation, tables []TableProgress, released bool) string {
	summary, totalShards, qualified, ok := shardFailures(applyState, ops, tables, released)
	if !ok || len(summary.Names) == 0 || !calloutNamesMoreThanError(summary, qualified) {
		return ""
	}
	var b strings.Builder
	if len(summary.Reasons) == 1 && summary.MoreReasons == 0 {
		subject := summary.Verb() + " " + shardNameList(summary.Names, totalShards)
		if r := summary.Reasons[0]; r.Reason != "" || r.Detail != "" {
			subject += ": " + r.Shown()
		}
		fmt.Fprintf(&b, "  %s %s%s%s\n", summary.Glyph(), ANSIRed, subject, ANSIReset)
		return b.String()
	}
	// Each reason's line names its shards, so the headline only counts them.
	fmt.Fprintf(&b, "  %s %s%s %s:%s\n", summary.Glyph(), ANSIRed, summary.Verb(), presentation.CoveragePhrase(presentation.ShardNoun, len(summary.Names), totalShards), ANSIReset)
	for _, r := range summary.Reasons {
		fmt.Fprintf(&b, "    - %s: %s\n", shardNameList(r.Names, totalShards), r.Shown())
	}
	if more := summary.MoreLine(presentation.ShardNoun); more != "" {
		fmt.Fprintf(&b, "    - %s\n", more)
	}
	return b.String()
}

// calloutNamesMoreThanError reports whether the callout says something the
// apply's error does not. The error names its one shard by the bare shard
// name, so a single failure needs the callout only when shard names repeat
// across keyspaces and the callout qualifies them with their keyspace.
func calloutNamesMoreThanError(summary presentation.FailureSummary, qualified bool) bool {
	return len(summary.Names) > 1 || qualified
}

// ShardFailedWhileRunning reports whether a running sharded apply has a shard
// that failed for good, so the view says when a new apply can retry it, as the
// PR comment's footer does. A shard SchemaBot is still retrying does not count:
// no new apply is needed for it.
func ShardFailedWhileRunning(applyState string, ops []ProgressOperation, tables []TableProgress, released bool) bool {
	if !state.IsRunningApplyState(applyState) {
		return false
	}
	statuses, _, _, sharded := shardStatuses(applyState, ops, tables, released)
	if !sharded {
		return false
	}
	for _, s := range statuses {
		if presentation.FailedMember(s.State, s.Presentation == presentation.StateStoppedAtCutover) &&
			state.IsState(s.State, state.ApplyOperation.Failed) {
			return true
		}
	}
	return false
}

// shardFailures reads the shards of a sharded apply whose own work failed or
// is retrying after a failure as one summary (presentation.FailedMember), with
// how many shards the apply runs on and whether the names carry their keyspace
// (presentation.QualifyShardNames). It reports false when every keyspace runs
// on its only shard.
func shardFailures(applyState string, ops []ProgressOperation, tables []TableProgress, released bool) (summary presentation.FailureSummary, totalShards int, qualified bool, ok bool) {
	statuses, _, _, sharded := shardStatuses(applyState, ops, tables, released)
	if !sharded {
		return presentation.FailureSummary{}, 0, false, false
	}
	refs := make([]presentation.ShardRef, len(statuses))
	for i, s := range statuses {
		refs[i] = presentation.ShardRef{Keyspace: s.Keyspace, Shard: s.Shard}
	}
	names := presentation.QualifyShardNames(refs)
	// Shards are grouped on the reason the PR comment shows
	// (webhooktemplates.SanitizeInlineError), so both surfaces count the same
	// shards under the same reasons (UX-9). The terminal is an operator's own
	// view, so a reason whose shards all recorded the same error prints that
	// error in full, with only the runes a terminal would act on removed
	// (ui.TerminalSafeInline); shards that read the same only once their hosts
	// are redacted print the redacted reason, since no one shard's host speaks
	// for the rest.
	var failures []presentation.Failure
	for i, s := range statuses {
		if !presentation.FailedMember(s.State, s.Presentation == presentation.StateStoppedAtCutover) {
			continue
		}
		failures = append(failures, presentation.Failure{
			Name:     names[i],
			Reason:   webhooktemplates.SanitizeInlineError(s.Error),
			Detail:   ui.TerminalSafeInline(s.Error),
			Retrying: state.IsState(s.State, state.ApplyOperation.FailedRetryable),
		})
	}
	for i, s := range statuses {
		if names[i] != s.Shard {
			qualified = true
			break
		}
	}
	return presentation.SummarizeFailures(failures), len(statuses), qualified, true
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
