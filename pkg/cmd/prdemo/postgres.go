//go:build ignore

// Generate fictional PostgreSQL PR comments through the production webhook
// templates, for the PostgreSQL PR workflow recording.
// scripts/prepare-pr-postgres-demo.py runs it from the repository root:
// go run ./pkg/cmd/prdemo/postgres.go
package main

import (
	"encoding/json"
	"fmt"
	"os"
	"strconv"
	"time"

	"github.com/block/schemabot/pkg/apitypes"
	"github.com/block/schemabot/pkg/engine/postgres/indexphase"
	"github.com/block/schemabot/pkg/state"
	"github.com/block/schemabot/pkg/webhook/templates"
)

const (
	database    = "shop"
	namespace   = "public"
	environment = "staging"
	requestedBy = "octocat"
	repository  = "example/shop"
	headSHA     = "abcdef1234567890abcdef1234567890abcdef12"
	lockOwner   = "example/shop#42"
	applyID     = "apply-a1b2c3d4e5f6"
	// The plan shows the concurrent form pg-sprite chose for the file's
	// plain CREATE INDEX; the build runs that statement against the table.
	addIndex    = "CREATE INDEX CONCURRENTLY idx_email_created ON users USING btree (email, created_at)"
	runningStmt = "CREATE INDEX CONCURRENTLY idx_email_created ON public.users USING btree (email, created_at)"
	blocksTotal = 48_000
	tuplesTotal = 1_200_000
	// percentStep is how far the build advances between progress updates.
	percentStep = 4
)

var startedAt = time.Date(2026, 9, 1, 10, 0, 0, 0, time.UTC)

func plan(locked bool) string {
	data := templates.PlanCommentData{
		Database:     database,
		Environment:  environment,
		HeadSHA:      headSHA,
		Repository:   repository,
		RequestedBy:  requestedBy,
		DatabaseType: "postgres",
		Changes:      []templates.KeyspaceChangeData{{Keyspace: namespace, Statements: []string{addIndex}}},
	}
	if locked {
		data.IsLocked = true
		data.LockOwner = lockOwner
		data.LockAcquired = startedAt.Format("2006-01-02 15:04:05 UTC")
	}
	return templates.RenderPlanComment(data)
}

func apply(applyState, taskState string) templates.ApplyStatusCommentData {
	return templates.ApplyStatusCommentData{
		Database:    database,
		Environment: environment,
		RequestedBy: requestedBy,
		State:       applyState,
		Engine:      "postgres",
		ApplyID:     applyID,
		StartedAt:   startedAt.Format(time.RFC3339),
		Tables:      []templates.TableProgressData{{Namespace: namespace, TableName: "users", DDL: addIndex, Status: taskState}},
	}
}

// running renders the progress comment step percent of the way through the
// recording's build. The build walks the phases PostgreSQL counts in
// execution order, so the bar shows the whole-build percent the engine
// reports and the detail line shows the current phase's counters. A running
// build never reads 100%: its last counted phase ends below it.
func running(step int) string {
	data := apply(state.Apply.Running, state.Task.Running)
	data.Step, data.StepsTotal = 1, 1
	data.Statement = runningStmt
	phase, inBand := buildPhaseAt(step)
	data.BuildWork = apitypes.BuildWork{Operation: "concurrent-index-build", ServerPhase: phase.Name}
	switch phase.Counter {
	case indexphase.Tuples:
		data.BuildWork.TuplesDone, data.BuildWork.TuplesTotal = int64(inBand*tuplesTotal), tuplesTotal
	default:
		data.BuildWork.BlocksDone, data.BuildWork.BlocksTotal = int64(inBand*blocksTotal), blocksTotal
	}
	data.Tables[0].PercentComplete = phase.Start + int(inBand*float64(phase.End-phase.Start))
	return templates.RenderApplyStatusComment(data)
}

// buildPhaseAt is the counted build phase step percent of the way through the
// recording, and how far through that phase's band the build is.
func buildPhaseAt(step int) (indexphase.Phase, float64) {
	var counted []indexphase.Phase
	for _, phase := range indexphase.Phases {
		if phase.End > phase.Start && phase.Name != indexphase.BuildingIndex {
			counted = append(counted, phase)
		}
	}
	last := counted[len(counted)-1]
	whole := float64(step) * float64(last.End) / 100
	for _, phase := range counted {
		if whole <= float64(phase.End) {
			return phase, (whole - float64(phase.Start)) / float64(phase.End-phase.Start)
		}
	}
	return last, 1
}

func main() {
	templates.TimestampFunc = func() string { return startedAt.Format("2006-01-02 15:04:05 UTC") }
	templates.NowFunc = func() time.Time { return startedAt.Add(time.Minute) }
	progress := map[string]string{}
	for p := 0; p <= 100; p += percentStep {
		progress[strconv.Itoa(p)] = running(p)
	}
	summary := apply(state.Apply.Completed, state.Task.Completed)
	summary.CompletedAt = startedAt.Add(6 * time.Minute).Format(time.RFC3339)
	out := map[string]any{
		"plan":     plan(false),
		"apply":    plan(true),
		"running":  progress,
		"complete": templates.RenderApplyStatusComment(apply(state.Apply.Completed, state.Task.Completed)),
		"summary":  templates.RenderApplySummaryComment(summary),
	}
	enc := json.NewEncoder(os.Stdout)
	enc.SetIndent("", " ")
	if err := enc.Encode(out); err != nil {
		fmt.Fprintln(os.Stderr, "encode PostgreSQL demo comments:", err)
		os.Exit(1)
	}
}
