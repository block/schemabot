//go:build ignore

// Generate fictional Vitess PR comments through the production webhook
// templates, for the Vitess PR workflow recording.
// scripts/prepare-pr-vitess-demo.py runs it from the repository root:
// go run ./pkg/cmd/prdemo/vitess.go
package main

import (
	"encoding/json"
	"fmt"
	"os"
	"strconv"
	"time"

	"github.com/block/schemabot/pkg/state"
	"github.com/block/schemabot/pkg/webhook/templates"
)

const (
	database    = "shop"
	keyspace    = "shop"
	table       = "orders"
	environment = "staging"
	requestedBy = "octocat"
	repository  = "example/shop"
	headSHA     = "abcdef1234567890abcdef1234567890abcdef12"
	lockOwner   = "example/shop#42"
	applyID     = "apply-7f3c91a2b8d4"
	// The change the docs' worked example makes: a column on a table sharded
	// on customer_id, so every shard runs its own copy.
	addColumn = "ALTER TABLE `orders` ADD COLUMN `note` varchar(255) DEFAULT NULL"
	// deployRequest is the PlanetScale deploy request the apply drives. It is
	// fictional, on the documentation org PlanetScale reserves for examples.
	deployRequest = "https://app.planetscale.com/example-org/shop/deploy-requests/118"
	// percentStep is how far the copy advances between progress updates.
	percentStep = 4
)

var startedAt = time.Date(2026, 9, 1, 10, 0, 0, 0, time.UTC)

// recordedShards are the four shards of a keyspace split on customer_id, with
// the row count each one holds. The counts differ so the recording reads as a
// real keyspace rather than an evenly divided one, and they are ordered the
// way Vitess names the ranges.
var recordedShards = []struct {
	name string
	rows int64
}{
	{"-40", 1_742_000},
	{"40-80", 1_905_000},
	{"80-c0", 1_688_000},
	{"c0-", 1_865_000},
}

// shardLead spreads the shards out so they do not advance in lockstep: each
// one runs a little ahead of or behind the table's overall percent, which is
// what a real keyspace looks like mid-copy.
var shardLead = []int{6, -4, 2, -7}

func plan(locked bool) string {
	data := templates.PlanCommentData{
		Database:     database,
		SchemaName:   database,
		Environment:  environment,
		HeadSHA:      headSHA,
		Repository:   repository,
		RequestedBy:  requestedBy,
		DatabaseType: "vitess",
		Changes:      []templates.KeyspaceChangeData{{Keyspace: keyspace, Statements: []string{addColumn}}},
	}
	if locked {
		data.IsLocked = true
		data.LockOwner = lockOwner
		data.LockAcquired = startedAt.Format("2006-01-02 15:04:05 UTC")
	}
	return templates.RenderPlanComment(data)
}

// rowsTotal is every shard's rows added up: what the table's own progress
// counts, since each shard copies its own slice of it.
func rowsTotal() int64 {
	var total int64
	for _, s := range recordedShards {
		total += s.rows
	}
	return total
}

// clampPercent keeps a shard's spread-out percent inside the bar's range.
func clampPercent(p int) int {
	if p < 0 {
		return 0
	}
	if p > 100 {
		return 100
	}
	return p
}

// shards is the per-shard breakdown when the table's copy is percent of the
// way through, each shard carrying its own share of the rows.
func shards(percent int, status string) []templates.ShardProgressData {
	out := make([]templates.ShardProgressData, 0, len(recordedShards))
	for i, s := range recordedShards {
		shardPercent := percent
		shardStatus := status
		if status == state.Task.Running {
			shardPercent = clampPercent(percent + shardLead[i])
			if shardPercent == 100 {
				shardStatus = state.Task.Completed
			}
		}
		out = append(out, templates.ShardProgressData{
			Shard:           s.name,
			Status:          shardStatus,
			PercentComplete: shardPercent,
			RowsCopied:      s.rows * int64(shardPercent) / 100,
			RowsTotal:       s.rows,
		})
	}
	return out
}

// apply is the progress comment for one apply state, with the table's shards
// at percent. It carries the deploy request link on every state, since that is
// the operator's entry point into PlanetScale's own view of the same change.
func apply(applyState, taskState string, percent int) templates.ApplyStatusCommentData {
	total := rowsTotal()
	return templates.ApplyStatusCommentData{
		Database:         database,
		Environment:      environment,
		RequestedBy:      requestedBy,
		State:            applyState,
		Engine:           "vitess",
		ApplyID:          applyID,
		StartedAt:        startedAt.Format(time.RFC3339),
		DeployRequestURL: deployRequest,
		Tables: []templates.TableProgressData{{
			Namespace:       keyspace,
			TableName:       table,
			DDL:             addColumn,
			Status:          taskState,
			ChangeType:      "alter",
			Started:         true,
			PercentComplete: percent,
			RowsCopied:      total * int64(percent) / 100,
			RowsTotal:       total,
			PlannedShards:   len(recordedShards),
			Shards:          shards(percent, taskState),
		}},
	}
}

func main() {
	templates.TimestampFunc = func() string { return startedAt.Format("2006-01-02 15:04:05 UTC") }
	templates.NowFunc = func() time.Time { return startedAt.Add(time.Minute) }

	progress := map[string]string{}
	for p := 0; p <= 100; p += percentStep {
		data := apply(state.Apply.Running, state.Task.Running, p)
		data.Step, data.StepsTotal = 1, 1
		data.Statement = addColumn
		progress[strconv.Itoa(p)] = templates.RenderApplyStatusComment(data)
	}

	// Cutover: every shard has copied its rows and Vitess is swapping them in.
	cutover := apply(state.Apply.CuttingOver, state.Task.CuttingOver, 100)

	// The revert window: the change is live, the original tables are still
	// there, and the comment counts down to when that stops being true.
	revert := apply(state.Apply.RevertWindow, state.Task.RevertWindow, 100)
	revert.RevertExpiresAt = startedAt.Add(36 * time.Minute).Format(time.RFC3339)

	complete := apply(state.Apply.Completed, state.Task.Completed, 100)
	complete.CompletedAt = startedAt.Add(36 * time.Minute).Format(time.RFC3339)

	summary := complete

	out := map[string]any{
		"plan":     plan(false),
		"apply":    plan(true),
		"running":  progress,
		"cutover":  templates.RenderApplyStatusComment(cutover),
		"revert":   templates.RenderApplyStatusComment(revert),
		"complete": templates.RenderApplyStatusComment(complete),
		"summary":  templates.RenderApplySummaryComment(summary),
	}
	enc := json.NewEncoder(os.Stdout)
	enc.SetIndent("", " ")
	if err := enc.Encode(out); err != nil {
		fmt.Fprintln(os.Stderr, "encode Vitess demo comments:", err)
		os.Exit(1)
	}
}
