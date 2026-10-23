//go:build ignore

// Generate fictional multi-target PR comments through the production webhook
// templates, for the PR workflow recordings. scripts/prepare-pr-demo.py runs it
// from the repository root: go run ./pkg/cmd/prdemo/main.go
package main

import (
	"encoding/json"
	"fmt"
	"os"
	"time"

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

func tableProgress(target string, percent int) templates.TableProgressData {
	total := rows[target]
	table := templates.TableProgressData{TableName: "orders", DDL: addIndex, RowsTotal: total}
	switch {
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

func operationState(percent int) string {
	switch {
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
		RequestedBy: requestedBy,
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
	var ops []presentation.Operation
	var details []*templates.ApplyStatusCommentData
	done := true
	for i, target := range targets {
		opState := operationState(percents[i])
		done = done && opState == state.ApplyOperation.Completed
		ops = append(ops, presentation.Operation{Deployment: deployment, Target: target, State: opState, Parallel: true})
		details = append(details, detail(target, opState, []templates.TableProgressData{tableProgress(target, percents[i])}))
	}
	data := templates.MultiDeploymentApplyData{
		Model:       presentation.Derive(ops),
		ApplyID:     "apply-example-91",
		Environment: environment,
		RequestedBy: requestedBy,
		StartedAt:   startedAt.Format(time.RFC3339),
		Details:     details,
	}
	if done {
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
	table := tableProgress(target, percent)
	data := templates.ApplyStatusCommentData{
		Database:    database,
		Environment: environment,
		RequestedBy: requestedBy,
		State:       operationState(percent),
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

	out := map[string]scenario{"multiTarget": fanOut, "target": narrowed}
	enc := json.NewEncoder(os.Stdout)
	enc.SetIndent("", " ")
	if err := enc.Encode(out); err != nil {
		fmt.Fprintln(os.Stderr, "encode PR demo comments:", err)
		os.Exit(1)
	}
}
