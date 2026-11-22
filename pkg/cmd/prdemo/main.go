//go:build ignore

// Generate fictional PR comments through the production webhook
// templates, for the PR workflow recordings. scripts/prepare-pr-multi-target-demo.py
// and scripts/prepare-pr-postgres-demo.py run it from the repository root:
// go run ./pkg/cmd/prdemo/main.go [postgres]
package main

import (
	"encoding/json"
	"fmt"
	"math"
	"os"
	"time"

	"github.com/block/schemabot/pkg/apitypes"
	"github.com/block/schemabot/pkg/engine/postgres/indexphase"
	"github.com/block/schemabot/pkg/presentation"
	"github.com/block/schemabot/pkg/state"
	"github.com/block/schemabot/pkg/webhook/templates"
)

const (
	database    = "shop"
	environment = "production"
	deployment  = "prod"
	requestedBy = "octocat"
	repository  = "example/shop"
	headSHA     = "abcdef1234567890abcdef1234567890abcdef12"
	lockOwner   = "example/shop#42"
	addIndex    = "ALTER TABLE `orders` ADD INDEX `idx_status`(`status`)"
)

var (
	startedAt = time.Date(2026, 9, 1, 10, 0, 0, 0, time.UTC)
	targets   = []string{"shop-001", "shop-002", "shop-003", "shop-004"}
	// Each target holds a different share of the orders, so their copies
	// finish at different times.
	rows = map[string]int64{"shop-001": 6_000_000, "shop-002": 9_000_000, "shop-003": 7_500_000, "shop-004": 4_500_000}
)

func member(target string) string { return deployment + "/" + target }

func changes() []templates.KeyspaceChangeData {
	return []templates.KeyspaceChangeData{{Keyspace: database, Statements: []string{addIndex + ";"}}}
}

func bytesOf(target string) *int64 {
	b := rows[target] * 500
	return &b
}

// rollout is what the plan of every target reports: one plan, run by all four.
func rollout() *templates.DeploymentDriftData {
	drift := &templates.DeploymentDriftData{Computed: true, Clean: true, Independent: true}
	members := make([]string, 0, len(targets))
	for i, target := range targets {
		drift.Deployments = append(drift.Deployments, templates.DeploymentDriftEntry{Deployment: deployment, Target: target, Primary: i == 0, Class: "planned"})
		drift.TableSizes = append(drift.TableSizes, templates.TargetTableSize{Target: member(target), Keyspace: database, Size: templates.TableSizeData{Table: "orders", EstimatedBytes: bytesOf(target)}})
		members = append(members, member(target))
	}
	drift.Plans = []templates.DeploymentPlanGroup{{Members: members, Primary: true, Changes: changes()}}
	return drift
}

func planComment(target string, locked bool) string {
	data := templates.PlanCommentData{
		Database:     database,
		SchemaName:   database,
		Environment:  environment,
		HeadSHA:      headSHA,
		Repository:   repository,
		RequestedBy:  requestedBy,
		IsMySQL:      true,
		DatabaseType: "mysql",
		Changes:      changes(),
		Target:       target,
	}
	if target == "" {
		data.DeploymentDrift = rollout()
	}
	if locked {
		data.IsLocked = true
		data.LockOwner = lockOwner
		data.LockAcquired = startedAt.Format("2006-01-02 15:04:05 UTC")
	}
	return templates.RenderPlanComment(data)
}

// cuttingOver marks a target whose copy is done and whose cutover is running.
const cuttingOver = -1

func tableProgress(target string, percent int, stopped bool) templates.TableProgressData {
	total := rows[target]
	table := templates.TableProgressData{TableName: "orders", DDL: addIndex, RowsTotal: total}
	switch {
	case stopped && percent >= 0 && percent < 100:
		table.Status, table.PercentComplete = state.Task.Stopped, percent
		table.RowsCopied = total * int64(percent) / 100
	case percent == cuttingOver:
		table.Status, table.RowsCopied, table.PercentComplete = state.Task.CuttingOver, total, 100
	case percent >= 100:
		table.Status, table.RowsCopied, table.PercentComplete = state.Task.Completed, total, 100
	default:
		table.Status, table.PercentComplete = state.Task.Running, percent
		table.RowsCopied = total * int64(percent) / 100
		// Every target copies at the same rate, so the ETA follows the rows left.
		table.ETASeconds = (total - table.RowsCopied) / 25_000
	}
	return table
}

func operationState(percent int, stopped bool) string {
	switch {
	case stopped && percent >= 0 && percent < 100:
		return state.ApplyOperation.Stopped
	case percent == cuttingOver:
		return state.ApplyOperation.CuttingOver
	case percent >= 100:
		return state.ApplyOperation.Completed
	default:
		return state.ApplyOperation.Running
	}
}

func detail(target, opState string, tables []templates.TableProgressData) *templates.ApplyStatusCommentData {
	return &templates.ApplyStatusCommentData{
		Database:    target,
		Environment: environment,
		State:       opState,
		Engine:      "Spirit",
		ApplyID:     "apply-example-91",
		StartedAt:   startedAt.Format(time.RFC3339),
		Tables:      tables,
	}
}

// rolloutComment renders the progress comment of the parallel rollout, or its
// summary once every target has completed and summary is set.
func rolloutComment(percents []int, elapsed time.Duration, summary bool) string {
	return rolloutCommentStopped(percents, elapsed, summary, false)
}

// rolloutCommentStopped renders the rollout like rolloutComment; when stopped
// is set, every target still copying is stopped at its percent.
func rolloutCommentStopped(percents []int, elapsed time.Duration, summary, stopped bool) string {
	var ops []presentation.Operation
	var details []*templates.ApplyStatusCommentData
	done := true
	for i, target := range targets {
		opState := operationState(percents[i], stopped)
		done = done && opState == state.ApplyOperation.Completed
		ops = append(ops, presentation.Operation{Deployment: deployment, Target: target, State: opState, Parallel: true})
		details = append(details, detail(target, opState, []templates.TableProgressData{tableProgress(target, percents[i], stopped)}))
	}
	data := templates.MultiDeploymentApplyData{
		Model:       presentation.Derive(ops),
		ApplyID:     "apply-example-91",
		Environment: environment,
		StartedAt:   startedAt.Format(time.RFC3339),
		Details:     details,
	}
	if done || stopped {
		data.CompletedAt = startedAt.Add(elapsed).Format(time.RFC3339)
	}
	if summary {
		return templates.RenderMultiDeploymentApplySummaryComment(data)
	}
	return templates.RenderMultiDeploymentApplyComment(data)
}

// narrowedComment renders the progress comment of the apply narrowed to one
// target, or its summary once it has completed and summary is set. It runs one
// operation, so it takes the single-deployment layout.
func narrowedComment(target string, percent int, elapsed time.Duration, summary bool) string {
	table := tableProgress(target, percent, false)
	data := templates.ApplyStatusCommentData{
		Database:    database,
		Environment: environment,
		State:       operationState(percent, false),
		Engine:      "Spirit",
		ApplyID:     "apply-example-92",
		StartedAt:   startedAt.Format(time.RFC3339),
		Tables:      []templates.TableProgressData{table},
	}
	if data.State == state.Apply.Completed {
		data.CompletedAt = startedAt.Add(elapsed).Format(time.RFC3339)
	}
	if summary {
		return templates.RenderApplySummaryComment(data)
	}
	return templates.RenderApplyStatusComment(data)
}

// stopScenario is the rollout an operator stops part-way and starts again. The
// stop leaves the progress comment in place at Stopped and posts a stopped
// summary; the start folds that comment and tracks the resumed copy in a fresh
// one at the bottom of the PR.
type stopScenario struct {
	Command   string   `json:"command"`
	Plan      string   `json:"plan"`
	Apply     string   `json:"apply"`
	Progress  []string `json:"progress"`
	Stop      string   `json:"stop"`
	StopAck   string   `json:"stopAck"`
	Stopped   string   `json:"stopped"`
	StoppedAt string   `json:"stoppedSummary"`
	Start     string   `json:"start"`
	StartAck  string   `json:"startAck"`
	Frozen    string   `json:"frozen"`
	Resumed   []string `json:"resumed"`
	Complete  string   `json:"complete"`
	Summary   string   `json:"summary"`
}

type scenario struct {
	Command  string   `json:"command"`
	Plan     string   `json:"plan"`
	Apply    string   `json:"apply"`
	Progress []string `json:"progress"`
	Complete string   `json:"complete"`
	Summary  string   `json:"summary"`
}

func main() {
	// Every target copies in parallel and cuts over as soon as its own copy is
	// done, so the smallest finishes first.
	steps := [][]int{
		{6, 4, 5, 8},
		{22, 15, 18, 29},
		{40, 27, 32, 53},
		{58, 39, 47, 77},
		{75, 50, 61, 100},
		{93, 62, 75, 100},
		{cuttingOver, 74, 89, 100},
		{100, 86, cuttingOver, 100},
		{100, 98, 100, 100},
		{100, cuttingOver, 100, 100},
	}
	complete := []int{100, 100, 100, 100}
	fanOut := scenario{
		Command:  "schemabot apply -e production",
		Plan:     planComment("", false),
		Apply:    planComment("", true),
		Complete: rolloutComment(complete, 6*time.Minute, false),
		Summary:  rolloutComment(complete, 6*time.Minute, true),
	}
	for _, percents := range steps {
		fanOut.Progress = append(fanOut.Progress, rolloutComment(percents, 0, false))
	}

	const narrowedTo = "shop-002"
	narrowed := scenario{
		Command:  "schemabot apply -e production --target " + narrowedTo,
		Plan:     planComment("", false),
		Apply:    planComment(narrowedTo, true),
		Complete: narrowedComment(narrowedTo, 100, 6*time.Minute, false),
		Summary:  narrowedComment(narrowedTo, 100, 6*time.Minute, true),
	}
	for _, percent := range []int{4, 15, 27, 39, 50, 62, 74, 86, 98, cuttingOver} {
		narrowed.Progress = append(narrowed.Progress, narrowedComment(narrowedTo, percent, 0, false))
	}

	out := map[string]any{"multiTarget": fanOut, "target": narrowed, "stop": stopDemo()}
	// scripts/prepare-pr-postgres-demo.py asks for the PostgreSQL recording alone.
	if len(os.Args) > 1 && os.Args[1] == "postgres" {
		out = map[string]any{"postgres": postgresDemo()}
	}
	enc := json.NewEncoder(os.Stdout)
	enc.SetIndent("", " ")
	if err := enc.Encode(out); err != nil {
		fmt.Fprintln(os.Stderr, "encode PR demo comments:", err)
		os.Exit(1)
	}
}

func stopDemo() stopScenario {
	const applyID = "apply-example-91"
	// shop-004 holds the fewest rows, so it has finished when the stop lands.
	running := [][]int{{22, 15, 18, 29}, {31, 21, 25, 51}, {38, 25, 31, 75}, {45, 30, 37, 100}}
	stoppedAt := running[len(running)-1]
	resumed := [][]int{
		{45, 30, 37, 100},
		{58, 39, 47, 100},
		{71, 48, 58, 100},
		{84, 57, 69, 100},
		{97, 66, 80, 100},
		{cuttingOver, 75, 91, 100},
		{100, 84, cuttingOver, 100},
		{100, 93, 100, 100},
		{100, cuttingOver, 100, 100},
	}
	complete := []int{100, 100, 100, 100}
	stopped := rolloutCommentStopped(stoppedAt, 2*time.Minute, false, true)
	demo := stopScenario{
		Command: "schemabot apply -e production",
		Plan:    planComment("", false),
		Apply:   planComment("", true),
		Stop:    "schemabot stop " + applyID + " -e production",
		// Stop and start act on every task of the apply: the targets still
		// copying stop, and the one that already finished is skipped.
		StopAck: templates.RenderStopCommandAccepted(templates.StopCommandAcceptedData{
			ApplyID: applyID, Environment: environment, RequestedBy: requestedBy, StoppedCount: 3, SkippedCount: 1,
		}),
		Stopped:   stopped,
		StoppedAt: rolloutCommentStopped(stoppedAt, 2*time.Minute, true, true),
		Start:     "schemabot start " + applyID + " -e production",
		StartAck: templates.RenderStartCommandAccepted(templates.StartCommandAcceptedData{
			ApplyID: applyID, Environment: environment, RequestedBy: requestedBy, StartedCount: 3, SkippedCount: 1,
		}),
		Frozen: templates.RenderResumeSupersededProgressComment(templates.SupersededProgressData{
			Repo: repository, PR: 42, NewCommentID: 1002, PreviousBody: stopped,
		}),
		Complete: rolloutComment(complete, 7*time.Minute, false),
		Summary:  rolloutComment(complete, 7*time.Minute, true),
	}
	for _, percents := range running {
		demo.Progress = append(demo.Progress, rolloutComment(percents, 0, false))
	}
	for _, percents := range resumed {
		demo.Resumed = append(demo.Resumed, rolloutComment(percents, 0, false))
	}
	return demo
}

// The PostgreSQL recording: one PR adds a column and an index to users. The
// schema file declares a plain CREATE INDEX; pg-sprite plans the concurrent
// form, and the apply runs each statement as its own task.
const (
	pgEnvironment = "staging"
	pgNamespace   = "public"
	pgApplyID     = "apply-example-42"
	pgAddColumn   = "ALTER TABLE users ADD COLUMN last_seen_at timestamptz"
	pgAddIndex    = "CREATE INDEX CONCURRENTLY idx_users_last_seen_at ON users USING btree (last_seen_at)"
	// The executor reports the statement it is running schema-qualified.
	pgAddColumnRunning = "ALTER TABLE public.users ADD COLUMN last_seen_at timestamptz"
	pgAddIndexRunning  = "CREATE INDEX CONCURRENTLY idx_users_last_seen_at ON public.users USING btree (last_seen_at)"
	pgTableBlocks      = 48_000
	pgTableTuples      = 2_400_000
)

func pgPlanComment(locked bool) string {
	data := templates.PlanCommentData{
		Database:     database,
		SchemaName:   pgNamespace,
		Environment:  pgEnvironment,
		HeadSHA:      headSHA,
		Repository:   repository,
		RequestedBy:  requestedBy,
		DatabaseType: "postgres",
		Changes:      []templates.KeyspaceChangeData{{Keyspace: pgNamespace, Statements: []string{pgAddColumn + ";", pgAddIndex + ";"}}},
	}
	if locked {
		data.IsLocked = true
		data.LockOwner = lockOwner
		data.LockAcquired = startedAt.Format("2006-01-02 15:04:05 UTC")
	}
	return templates.RenderPlanComment(data)
}

// buildPoll is one progress poll of the concurrent index build: the server
// phase PostgreSQL reports and the counters it publishes for that phase.
type buildPoll struct {
	phase string
	work  apitypes.BuildWork
}

// percent places the poll in the whole build with the same phase bands the
// engine uses, so the bar moves the way a real build's bar does.
func (p buildPoll) percent() int {
	phase, ok := indexphase.Lookup(p.phase)
	if !ok {
		panic("unknown index build phase " + p.phase)
	}
	fraction := 0.0
	switch {
	case p.work.BlocksTotal > 0 && (phase.Counter == indexphase.Blocks || phase.Counter == indexphase.BlocksOrTuples):
		fraction = float64(p.work.BlocksDone) / float64(p.work.BlocksTotal)
	case p.work.TuplesTotal > 0 && phase.Counter == indexphase.Tuples:
		fraction = float64(p.work.TuplesDone) / float64(p.work.TuplesTotal)
	}
	return phase.Start + int(math.Round(fraction*float64(phase.End-phase.Start)))
}

func blocks(phase string, done int64) buildPoll {
	return buildPoll{phase: phase, work: apitypes.BuildWork{BlocksDone: done, BlocksTotal: pgTableBlocks}}
}

func pgTables(column, index string, indexPercent int) []templates.TableProgressData {
	return []templates.TableProgressData{
		{Namespace: pgNamespace, TableName: "users", DDL: pgAddColumn, Status: column, PercentComplete: percentFor(column, 0)},
		{Namespace: pgNamespace, TableName: "users", DDL: pgAddIndex, Status: index, PercentComplete: percentFor(index, indexPercent)},
	}
}

func percentFor(status string, running int) int {
	switch status {
	case state.Task.Completed:
		return 100
	case state.Task.Running:
		return running
	default:
		return 0
	}
}

func pgApplyData(applyState string, tables []templates.TableProgressData) templates.ApplyStatusCommentData {
	return templates.ApplyStatusCommentData{
		Database:    database,
		Environment: pgEnvironment,
		RequestedBy: requestedBy,
		State:       applyState,
		Engine:      "postgres",
		ApplyID:     pgApplyID,
		StartedAt:   startedAt.Format(time.RFC3339),
		Tables:      tables,
	}
}

func postgresDemo() scenario {
	polls := []buildPoll{
		{phase: "building index: initializing"},
		blocks("building index: scanning table", 6_200),
		blocks("building index: scanning table", 15_800),
		blocks("building index: scanning table", 26_400),
		blocks("building index: scanning table", 37_100),
		blocks("building index: scanning table", 46_900),
		{phase: "building index: sorting live tuples"},
		{phase: "building index: loading tuples in tree", work: apitypes.BuildWork{TuplesDone: 1_350_000, TuplesTotal: pgTableTuples}},
		blocks("index validation: scanning index", 31_000),
		blocks("index validation: scanning table", 29_500),
		{phase: "waiting for old snapshots", work: apitypes.BuildWork{LockersDone: 0, LockersTotal: 1}},
	}

	demo := scenario{
		Command: "schemabot apply -e " + pgEnvironment,
		Plan:    pgPlanComment(false),
		Apply:   pgPlanComment(true),
	}

	// The column add is metadata-only and finishes on the first poll.
	adding := pgApplyData(state.Apply.Running, pgTables(state.Task.Running, state.Task.Pending, 0))
	adding.Step, adding.StepsTotal, adding.Statement = 1, 1, pgAddColumnRunning
	demo.Progress = append(demo.Progress, templates.RenderApplyStatusComment(adding))

	for _, poll := range polls {
		data := pgApplyData(state.Apply.Running, pgTables(state.Task.Completed, state.Task.Running, poll.percent()))
		data.Step, data.StepsTotal, data.Statement = 1, 1, pgAddIndexRunning
		data.BuildWork = poll.work
		data.BuildWork.Operation = "concurrent-index-build"
		data.BuildWork.ServerPhase = poll.phase
		demo.Progress = append(demo.Progress, templates.RenderApplyStatusComment(data))
	}

	done := pgApplyData(state.Apply.Completed, pgTables(state.Task.Completed, state.Task.Completed, 100))
	done.CompletedAt = startedAt.Add(4 * time.Minute).Format(time.RFC3339)
	demo.Complete = templates.RenderApplyStatusComment(done)
	demo.Summary = templates.RenderApplySummaryComment(done)
	return demo
}
