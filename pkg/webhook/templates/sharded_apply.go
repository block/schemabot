package templates

import (
	"fmt"
	"html"
	"strings"

	"github.com/block/schemabot/pkg/state"
)

// ShardedApplyData is the input to the sharded-apply comment: an apply that fans
// out across the shards of a single keyspace within one deployment. Its unit of
// work is one operation per (shard, table). Shards are grouped by their change
// signature: an apply whose shards all need the same change renders one status
// table with the DDL once, and an apply whose shards diverge — different tables,
// or the same table computing to different DDL because shards drifted — renders
// one group per distinct change set, each tying its shards' statuses to the DDL
// it runs. This is distinct from the multi-deployment comment, whose unit is the
// deployment.
type ShardedApplyData struct {
	// State is the aggregate apply state (state.Apply.*), driving the headline.
	State string

	Environment string
	Database    string
	Keyspace    string
	ApplyID     string
	RequestedBy string
	StartedAt   string
	CompletedAt string

	// Shards is the per-shard rollup in resolved order: one entry per shard with
	// its aggregate state. It drives the count histogram, each group's status
	// rows, and the first-failure callout.
	Shards []ShardStatus

	// Cells is one entry per (shard, table) operation — the unit that carries the
	// DDL and defines a shard's change signature for grouping.
	Cells []ShardCell
}

// ShardStatus is one shard's aggregate status. Emoji/Label come from the same
// per-operation projection the multi-deployment comment uses, so the vocabulary
// is identical; only the unit (shard vs deployment) differs.
type ShardStatus struct {
	Shard string
	Emoji string
	Label string
	State string
	Error string
}

// ShardCell is one (shard, table) operation: the DDL for that table on that
// shard. Cells with the same (table, DDL) set across shards group those shards
// together.
type ShardCell struct {
	Shard string
	Table string
	DDL   string
}

// ShardChange is one table's DDL within a group, shown once for the whole group.
type ShardChange struct {
	Table string
	DDL   string
}

// shardGroup is a set of shards that share an identical change signature, with
// the changes they all apply.
type shardGroup struct {
	Shards  []ShardStatus
	Changes []ShardChange
}

// RenderShardedApplyComment renders the PR comment for a sharded apply: the
// shared apply header and metadata, a per-shard count histogram, the first
// failed shard's error lifted to the top, then the shards grouped by change
// signature — a single group renders one status table with the DDL once; more
// than one renders a labelled group per distinct change set.
func RenderShardedApplyComment(data ShardedApplyData) string {
	var sb strings.Builder
	renderedAt := currentTimestamp()

	writeApplyHeader(&sb, ApplyStatusCommentData{State: data.State, Environment: data.Environment})
	writeShardedMetadata(&sb, data, renderedAt)

	writeShardCounts(&sb, data.Shards)
	writeShardFirstFailure(&sb, data.Shards)

	fmt.Fprintf(&sb, "\n#### Keyspace `%s`\n", data.Keyspace)
	groups := groupShardsBySignature(data.Shards, data.Cells)
	if len(groups) <= 1 {
		writeShardStatusTable(&sb, data.Shards)
		if len(groups) == 1 {
			writeGroupDDL(&sb, groups[0].Changes)
		}
	} else {
		sb.WriteString("\nShards diverge — grouped by change:\n")
		for _, g := range groups {
			fmt.Fprintf(&sb, "\n**%s**\n", shardList(g.Shards))
			writeShardStatusTable(&sb, g.Shards)
			writeGroupDDL(&sb, g.Changes)
		}
	}

	writeShardedFooter(&sb, data)
	if !state.IsTerminalApplyState(data.State) {
		writeLastUpdatedFooter(&sb, renderedAt)
	}
	return sb.String()
}

func writeShardedMetadata(sb *strings.Builder, data ShardedApplyData, renderedAt string) {
	parts := []string{
		fmt.Sprintf("**Database**: `%s`", data.Database),
		"**Type**: `Strata`",
		fmt.Sprintf("**Apply ID**: `%s`", data.ApplyID),
	}
	fmt.Fprintf(sb, "%s\n", strings.Join(parts, " | "))
	attributionAt := renderedAt
	if data.RequestedBy == "" {
		attributionAt = startedAtDisplay(data.StartedAt, renderedAt)
	}
	writeAppliedByOrTimestampAt(sb, data.RequestedBy, attributionAt)
}

// groupShardsBySignature buckets shards whose change set is identical. The
// signature is the ordered (table, DDL) pairs the shard applies, so shards
// needing different tables — or the same table with different DDL — fall into
// different groups. Groups and the shards within them keep resolved order; a
// uniform apply yields exactly one group.
func groupShardsBySignature(shards []ShardStatus, cells []ShardCell) []shardGroup {
	changesByShard := make(map[string][]ShardChange, len(shards))
	for _, c := range cells {
		changesByShard[c.Shard] = append(changesByShard[c.Shard], ShardChange{Table: c.Table, DDL: c.DDL})
	}

	var order []string
	bySig := make(map[string]*shardGroup)
	for _, s := range shards {
		changes := changesByShard[s.Shard]
		sig := signatureOf(changes)
		g := bySig[sig]
		if g == nil {
			g = &shardGroup{Changes: changes}
			bySig[sig] = g
			order = append(order, sig)
		}
		g.Shards = append(g.Shards, s)
	}

	groups := make([]shardGroup, 0, len(order))
	for _, sig := range order {
		groups = append(groups, *bySig[sig])
	}
	return groups
}

// signatureOf builds the change-set key for a shard from its ordered changes.
func signatureOf(changes []ShardChange) string {
	parts := make([]string, len(changes))
	for i, c := range changes {
		parts[i] = c.Table + "\x00" + c.DDL
	}
	return strings.Join(parts, "\x01")
}

// shardList renders a group's shards as "shard `x`" or "shards `x`, `y`".
func shardList(shards []ShardStatus) string {
	names := make([]string, len(shards))
	for i, s := range shards {
		names[i] = fmt.Sprintf("`%s`", s.Shard)
	}
	if len(names) == 1 {
		return "shard " + names[0]
	}
	return "shards " + strings.Join(names, ", ")
}

// writeShardCounts writes the per-status histogram across shards so rollout
// health is visible at a glance — the shard-unit analogue of the
// multi-deployment "Deployments:" line.
func writeShardCounts(sb *strings.Builder, shards []ShardStatus) {
	if len(shards) == 0 {
		return
	}
	order := make([]string, 0, len(shards))
	counts := make(map[string]int, len(shards))
	for _, s := range shards {
		label := shardCountLabel(s)
		if _, seen := counts[label]; !seen {
			order = append(order, label)
		}
		counts[label]++
	}
	parts := make([]string, 0, len(order))
	for _, label := range order {
		parts = append(parts, fmt.Sprintf("%d %s", counts[label], label))
	}
	fmt.Fprintf(sb, "\n**Shards**: %s\n", strings.Join(parts, ", "))
}

// shardCountLabel collapses a shard's full label to its leading state word
// ("halted — …" → "halted") for the histogram.
func shardCountLabel(s ShardStatus) string {
	if i := strings.Index(s.Label, " — "); i >= 0 {
		return s.Label[:i]
	}
	return s.Label
}

// isShardFailureState reports whether a shard's state carries an operator-facing
// error to surface — a terminal failure or an automatic retry after one. The
// retry case matters because SchemaBot holds the apply in failed_retryable while
// it retries, and the operator still needs to see what went wrong.
func isShardFailureState(opState string) bool {
	return opState == state.ApplyOperation.Failed || opState == state.ApplyOperation.FailedRetryable
}

// writeShardFirstFailure lifts the first failed shard's error to the top so an
// operator sees the cause without scanning the table. Renders nothing when no
// shard has failed or is retrying after a failure.
func writeShardFirstFailure(sb *strings.Builder, shards []ShardStatus) {
	for _, s := range shards {
		if !isShardFailureState(s.State) {
			continue
		}
		shard := html.EscapeString(s.Shard)
		if s.Error == "" {
			fmt.Fprintf(sb, "\n> ⚠️ **First failure:** shard <code>%s</code>\n", shard)
		} else {
			fmt.Fprintf(sb, "\n> ⚠️ **First failure:** shard <code>%s</code> — %s\n", shard, html.EscapeString(s.Error))
		}
		return
	}
}

// writeShardStatusTable renders the per-shard status table for a set of shards.
func writeShardStatusTable(sb *strings.Builder, shards []ShardStatus) {
	if len(shards) == 0 {
		return
	}
	sb.WriteString("\n| Shard | Status |\n| --- | --- |\n")
	for _, s := range shards {
		fmt.Fprintf(sb, "| `%s` | %s |\n", s.Shard, shardStatusCell(s))
	}
}

// writeGroupDDL writes a group's table changes, each DDL once.
func writeGroupDDL(sb *strings.Builder, changes []ShardChange) {
	for _, c := range changes {
		// Guard against an empty DDL, which would render a blank ```sql box. The
		// DDL should always be present; if it is missing the change data is
		// incomplete, so say so rather than show an empty code block.
		if strings.TrimSpace(c.DDL) == "" {
			fmt.Fprintf(sb, "\n`%s` — _DDL unavailable_\n", c.Table)
			continue
		}
		fmt.Fprintf(sb, "\n`%s`\n```sql\n%s\n```\n", c.Table, c.DDL)
	}
}

// shardStatusCell renders one shard's "<emoji> <label>" cell, appending the
// error for a failed shard.
func shardStatusCell(s ShardStatus) string {
	cell := html.EscapeString(s.Label)
	if s.Emoji != "" {
		cell = fmt.Sprintf("%s %s", s.Emoji, cell)
	}
	if isShardFailureState(s.State) && s.Error != "" {
		cell = fmt.Sprintf("%s — %s", cell, html.EscapeString(s.Error))
	}
	return cell
}

// writeShardedFooter renders the single next operator action, matching the
// single-deployment footer vocabulary: a failed apply is retried, an
// auto-retrying (failed_retryable) apply offers the stop-retrying command, and a
// stopped apply is resumed.
func writeShardedFooter(sb *strings.Builder, data ShardedApplyData) {
	switch {
	case state.IsState(data.State, state.Apply.Failed):
		writeFooterAction(sb, "To retry:", fmt.Sprintf("schemabot apply -e %s", data.Environment))
	case state.IsState(data.State, state.Apply.FailedRetryable):
		writeFooterAction(sb, "An error interrupted this schema change. SchemaBot retries automatically and marks it failed if retries are exhausted. To stop retrying:", fmt.Sprintf("schemabot stop %s -e %s", data.ApplyID, data.Environment))
	case state.IsState(data.State, state.Apply.Stopped):
		writeFooterAction(sb, "Paused — to resume from where it stopped:", fmt.Sprintf("schemabot start %s -e %s", data.ApplyID, data.Environment))
	}
}
