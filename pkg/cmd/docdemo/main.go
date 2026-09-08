//go:build ignore

// Generate fictional CLI scenarios through the production output templates.
// Run from the repository root: go run ./pkg/cmd/docdemo/main.go > assets/src/cli-demo.json
package main

import (
	"encoding/json"
	"fmt"
	"io"
	"os"
	"time"

	t "github.com/block/schemabot/pkg/cmd/internal/templates"
	"github.com/block/schemabot/pkg/state"
)

type Frame struct {
	Seconds float64 `json:"seconds"`
	Command string  `json:"command"`
	Caption string  `json:"caption"`
	Output  string  `json:"output"`
}
type Demo struct {
	Name   string  `json:"name"`
	Title  string  `json:"title"`
	Frames []Frame `json:"frames"`
}

func capture(fn func()) string {
	old := os.Stdout
	r, w, err := os.Pipe()
	if err != nil {
		panic(err)
	}
	os.Stdout = w
	done := make(chan []byte)
	go func() { b, _ := io.ReadAll(r); done <- b }()
	fn()
	w.Close()
	os.Stdout = old
	b := <-done
	r.Close()
	return string(b)
}

const ddl = "ALTER TABLE `orders` ADD INDEX `idx_status` (`status`)"

func progress(p int, phase string, throttled bool) string {
	return capture(func() {
		table := t.TableProgress{TableName: "orders", Namespace: "shop", ChangeType: "alter", DDL: ddl, Status: phase, RowsCopied: int64(p) * 100000, RowsTotal: 10000000, PercentComplete: p, ETASeconds: int64(100-p) * 12}
		if phase == state.Apply.Running {
			table.ProgressDetail = fmt.Sprintf("%d/10000000 %d.00%% copyRows", table.RowsCopied, p)
		}
		table.Throttled = throttled
		if throttled {
			table.ThrottleReason = "Replication lag exceeds the configured limit"
		}
		t.WriteProgress(t.ProgressData{ApplyID: "apply-example-73", Database: "shop", Environment: "staging", State: phase, Engine: "Spirit", Tables: []t.TableProgress{table}})
	})
}
func main() {
	t.SetPreviewMode()
	plan := capture(func() {
		t.WritePlanHeader(t.PlanHeaderData{Database: "shop", SchemaName: "schema", IsMySQL: true})
		changes := []t.DDLChange{{TableName: "orders", ChangeType: "alter", DDL: ddl}}
		t.WriteEnvironmentHeader("staging")
		t.WriteNamespaceChanges([]t.NamespaceChange{{Namespace: "shop", Changes: changes}}, true, "shop")
		t.WritePlanSummary(changes)
	})
	demos := []Demo{{Name: "cli-plan-apply", Title: "Review the plan. Follow the change.", Frames: []Frame{{4, "schemabot plan -s ./schema -e staging", "See the DDL before running it", plan}, {2, "schemabot apply -s ./schema -e staging", "After you confirm, follow the apply", progress(10, state.Apply.Running, false)}, {1, "", "", progress(35, state.Apply.Running, false)}, {1, "", "", progress(65, state.Apply.Running, false)}, {1, "", "", progress(90, state.Apply.Running, false)}, {4, "", "The change is complete", progress(100, state.Apply.Completed, false)}}}}
	now := time.Date(2026, 1, 15, 14, 30, 0, 0, time.UTC)
	status := capture(func() {
		t.WriteStatusList(t.StatusListData{ActiveCount: 1, Limit: 4, StateCounts: map[string]int{state.Apply.Running: 1, state.Apply.Completed: 2, state.Apply.Failed: 1}, Applies: []t.ActiveApplyData{
			{ApplyID: "apply-example-73", Database: "shop", Environment: "staging", State: state.Apply.Running, Caller: "github:alex@acme/store#42", StartedAt: now.Add(-10 * time.Minute).Format(time.RFC3339)},
			{ApplyID: "apply-example-72", Database: "billing", Environment: "staging", State: state.Apply.Completed, Caller: "github:sam@acme/billing#18", StartedAt: now.Add(-time.Hour).Format(time.RFC3339)},
			{ApplyID: "apply-example-71", Database: "catalog", Environment: "staging", State: state.Apply.Failed, Caller: "github:jamie@acme/store#40", StartedAt: now.Add(-2 * time.Hour).Format(time.RFC3339)},
			{ApplyID: "apply-example-70", Database: "accounts", Environment: "staging", State: state.Apply.Completed, Caller: "github:alex@acme/accounts#12", StartedAt: now.Add(-3 * time.Hour).Format(time.RFC3339)},
		}})
	})
	plans := capture(func() {
		t.WritePlansList(t.PlansListData{Limit: 3, Plans: []t.PlanSummaryData{
			{PlanID: "plan-example-42", Database: "shop", Environment: "staging", Changes: "1 alter", Source: "acme/store#42", CreatedAt: now.Add(-10 * time.Minute)},
			{PlanID: "plan-example-41", Database: "billing", Environment: "staging", Changes: "1 create", Source: "acme/billing#18", CreatedAt: now.Add(-time.Hour)},
			{PlanID: "plan-example-40", Database: "catalog", Environment: "staging", Changes: "1 alter", Source: "acme/store#40", CreatedAt: now.Add(-2 * time.Hour)},
		}})
	})
	demos = append(demos, Demo{Name: "cli-fleet", Title: "From the fleet to one change.", Frames: []Frame{{5, "schemabot status -e staging", "See changes across databases", status}, {4, "schemabot status apply-example-73", "Inspect one running change", progress(60, state.Apply.Running, false)}, {5, "schemabot list-plans -e staging", "Find the plans behind the work", plans}}})
	frames := []Frame{{3, "schemabot progress apply-example-73", "Follow an apply started with --defer-cutover", progress(20, state.Apply.Running, false)}}
	for _, p := range []int{35, 50, 60} {
		frames = append(frames, Frame{0.7, "", "", progress(p, state.Apply.Running, false)})
	}
	frames = append(frames, Frame{3, "", "See why copying has slowed", progress(60, state.Apply.Running, true)})
	for _, p := range []int{70, 80, 90, 100} {
		frames = append(frames, Frame{0.7, "", "Copying resumes as conditions improve", progress(p, state.Apply.Running, false)})
	}
	frames = append(frames, Frame{3, "", "Copy complete; wait for the final swap", progress(100, state.Apply.WaitingForCutover, false)}, Frame{2, "schemabot cutover -e staging apply-example-73", "Request the final swap when ready", progress(100, state.Apply.CuttingOver, false)}, Frame{4, "schemabot status apply-example-73", "Verify that the apply completed", progress(100, state.Apply.Completed, false)})
	demos = append(demos, Demo{Name: "cli-cutover", Title: "Know when to wait. Choose when to swap.", Frames: frames})
	enc := json.NewEncoder(os.Stdout)
	enc.SetIndent("", "  ")
	if err := enc.Encode(demos); err != nil {
		panic(err)
	}
}
