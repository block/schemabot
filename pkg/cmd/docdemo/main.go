//go:build ignore

// Generate fictional CLI scenarios through the production output templates.
// Run from the repository root: go run ./pkg/cmd/docdemo/main.go > assets/src/cli-demo.json
package main

import (
	"context"
	"encoding/json"
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os/exec"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"sync"

	"github.com/block/schemabot/pkg/apitypes"
	"github.com/block/schemabot/pkg/cmd/commands"
	"github.com/block/schemabot/pkg/schema"
	tea "github.com/charmbracelet/bubbletea"
	"github.com/charmbracelet/lipgloss"
	"github.com/charmbracelet/x/ansi"
	"github.com/muesli/termenv"
	"os"
	"time"

	t "github.com/block/schemabot/pkg/cmd/internal/templates"
	"github.com/block/schemabot/pkg/state"
	"github.com/block/schemabot/pkg/ui"
)

type Frame struct {
	Seconds float64 `json:"seconds"`
	Command string  `json:"command"`
	Caption string  `json:"caption"`
	Output  string  `json:"output"`
}
type Demo struct {
	Height int     `json:"height,omitempty"`
	Bar    string  `json:"bar,omitempty"`
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

// live renders the production interactive watcher against a loopback-only API
// fixture. Init's first fetch seeds the model; subsequent requests have the apply
// ID supplied by that response. No private model fields or UI strings are copied.
func live(p int, phase string, throttled bool, key string) string {
	response := apitypes.ProgressResponse{
		ApplyID: "apply-example-73", Database: "shop", DatabaseType: "mysql", Environment: "staging", Engine: "Spirit", State: phase,
		Tables: []*apitypes.TableProgressResponse{{TableName: "orders", Keyspace: "shop", ChangeType: "alter", DDL: ddl,
			Status: phase, RowsCopied: int64(p) * 100000, RowsTotal: 10000000, PercentComplete: int32(p), ETASeconds: int64(100-p) * 12,
			Throttled: throttled, ThrottleReason: "commit-latency 120ms >= 100ms"}},
	}
	var cutovers, stops int
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		if r.Method == http.MethodGet && strings.HasPrefix(r.URL.Path, "/api/progress/apply/") {
			if err := json.NewEncoder(w).Encode(response); err != nil {
				panic(err)
			}
			return
		}
		if r.Method == http.MethodPost && (r.URL.Path == "/api/cutover" || r.URL.Path == "/api/stop") {
			var request apitypes.ControlRequest
			if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
				panic(err)
			}
			if request.ApplyID != "apply-example-73" || request.Environment != "staging" {
				panic("wrong cutover target")
			}
			if r.URL.Path == "/api/cutover" {
				cutovers++
			} else {
				stops++
			}
			if err := json.NewEncoder(w).Encode(apitypes.ControlResponse{Accepted: true}); err != nil {
				panic(err)
			}
			return
		}
		panic("unexpected fixture request: " + r.Method + " " + r.URL.Path)
	}))
	defer server.Close()
	model := commands.NewWatchModel(server.URL, "shop", "staging", true)
	batch := model.Init()().(tea.BatchMsg)
	updated, _ := model.Update(batch[0]())
	if key == "enter" {
		var cmd tea.Cmd
		updated, cmd = updated.Update(tea.KeyMsg{Type: tea.KeyEnter})
		if cmd == nil {
			panic("Enter did not request cutover")
		}
		updated, _ = updated.Update(cmd())
		if cutovers != 1 {
			panic("expected exactly one cutover request")
		}
		_, escape := updated.Update(tea.KeyMsg{Type: tea.KeyEsc})
		if escape != nil {
			panic("cutover unexpectedly allowed detach")
		}
	}
	if key == "s" {
		var cmd tea.Cmd
		updated, cmd = updated.Update(tea.KeyMsg{Type: tea.KeyRunes, Runes: []rune{'s'}})
		if cmd == nil {
			panic("stop key did not request stop")
		}
		updated, _ = updated.Update(cmd())
		if stops != 1 {
			panic("expected one stop request")
		}
	}
	if phase == state.Apply.Running && key == "" {
		_, enter := updated.Update(tea.KeyMsg{Type: tea.KeyEnter})
		if enter != nil {
			panic("Enter triggered a control before cutover was ready")
		}
		_, escape := updated.Update(tea.KeyMsg{Type: tea.KeyEsc})
		if escape == nil {
			panic("running watch cannot detach")
		}
		if _, ok := escape().(tea.QuitMsg); !ok {
			panic("Escape did not quit the watcher")
		}
		_, stop := updated.Update(tea.KeyMsg{Type: tea.KeyRunes, Runes: []rune{'s'}})
		if stop == nil {
			panic("stop key did not request stop")
		}
		stop()
		if stops != 1 {
			panic("stop request missing")
		}
	}
	view := updated.View()
	if strings.Contains(view, "Error:") || strings.Contains(view, "Loading...") {
		panic(view)
	}
	if phase == state.Apply.WaitingForCutover && key == "" && !strings.Contains(view, "Press Enter to proceed with cutover") {
		panic("cutover prompt missing")
	}
	if phase == state.Apply.Running && key == "" && !strings.Contains(view, "ESC detach") {
		panic("running controls missing")
	}
	return view
}

// vitess renders PlanetScale deploy requests and shard progress with the same
// watcher used by the CLI. Enter is exercised through its real control handler.
func vitess(phase string, percentages []int, enter bool) string {
	response := apitypes.ProgressResponse{
		ApplyID: "apply-example-84", Database: "shop", DatabaseType: "vitess", Environment: "staging", Engine: "PlanetScale", State: phase,
		Metadata: map[string]string{"deploy_request_url": "https://app.planetscale.com/acme/shop/deploy-requests/42"},
	}
	table := &apitypes.TableProgressResponse{TableName: "orders", Keyspace: "commerce", ChangeType: "alter", DDL: ddl, Status: phase}
	for i, p := range percentages {
		status := state.Task.Running
		eta := int64(100-p) * 3
		if p == 100 {
			status = state.Task.Completed
			eta = 0
		}
		table.Shards = append(table.Shards, &apitypes.ShardProgressResponse{
			Shard: []string{"-40", "40-80", "80-c0", "c0-"}[i], Status: status,
			RowsCopied: int64(p) * 10000, RowsTotal: 1000000, PercentComplete: int32(p), ETASeconds: eta,
		})
		table.RowsCopied += int64(p) * 10000
		table.RowsTotal += 1000000
		if eta > table.ETASeconds {
			table.ETASeconds = eta
		}
	}
	if table.RowsTotal > 0 {
		table.PercentComplete = int32(table.RowsCopied * 100 / table.RowsTotal)
	}
	response.Tables = []*apitypes.TableProgressResponse{table}
	expected := "/api/start"
	if phase == state.Apply.Running {
		expected = "/api/stop"
	}
	if phase == state.Apply.RevertWindow {
		expected = "/api/skip-revert"
	}
	requests := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		if r.Method == http.MethodGet && strings.HasPrefix(r.URL.Path, "/api/progress/apply/") {
			if err := json.NewEncoder(w).Encode(response); err != nil {
				panic(err)
			}
			return
		}
		if r.Method != http.MethodPost || r.URL.Path != expected {
			panic("unexpected Vitess control: " + r.Method + " " + r.URL.Path)
		}
		var request apitypes.ControlRequest
		if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
			panic(err)
		}
		if request.ApplyID != response.ApplyID || request.Environment != response.Environment {
			panic("wrong Vitess control target")
		}
		requests++
		if err := json.NewEncoder(w).Encode(apitypes.ControlResponse{Accepted: true}); err != nil {
			panic(err)
		}
	}))
	defer server.Close()
	model := commands.NewWatchModel(server.URL, "shop", "staging", true)
	batch := model.Init()().(tea.BatchMsg)
	updated, _ := model.Update(batch[0]())
	if enter {
		var cmd tea.Cmd
		updated, cmd = updated.Update(tea.KeyMsg{Type: tea.KeyEnter})
		if cmd == nil {
			panic("Vitess Enter control missing")
		}
		updated, _ = updated.Update(cmd())
		if requests != 1 {
			panic("expected one Vitess control request")
		}
	}
	if phase == state.Apply.Running {
		cancelled, cmd := updated.Update(tea.KeyMsg{Type: tea.KeyRunes, Runes: []rune{'c'}})
		if cmd == nil {
			panic("c did not request cancellation")
		}
		cancelled, _ = cancelled.Update(cmd())
		if requests != 1 || !strings.Contains(cancelled.View(), "Cancelling...") {
			panic("PlanetScale cancellation handler failed")
		}
	}
	view := updated.View()
	if strings.Contains(view, "Error:") || strings.Contains(view, "Loading...") {
		panic(view)
	}
	if !enter && phase == state.Apply.WaitingForDeploy && !strings.Contains(view, "Press Enter to deploy") {
		panic("deploy prompt missing")
	}
	if !enter && phase == state.Apply.RevertWindow && !strings.Contains(view, "Press Enter to skip revert") {
		panic("revert prompt missing")
	}
	if phase == state.Apply.Running {
		if !strings.Contains(view, "c cancel") {
			panic("PlanetScale cancel control missing")
		}
		for _, shard := range []string{"-40", "40-80", "80-c0", "c0-"} {
			if !strings.Contains(view, shard) {
				panic("shard missing: " + shard)
			}
		}
	}
	return view
}

func vitessDemo() Demo {
	renderPlan := func(apply bool) string {
		return capture(func() {
			t.WritePlanHeader(t.PlanHeaderData{Database: "shop", SchemaName: "schema", IsApply: apply})
			t.WriteEnvironmentHeader("staging")
			changes := []t.DDLChange{{TableName: "orders", ChangeType: "alter", DDL: ddl}}
			t.WriteNamespaceChanges([]t.NamespaceChange{{Namespace: "commerce", Changes: changes}}, false, "shop", schema.DialectMySQL)
			t.WritePlanSummary(changes)
		})
	}
	plan := renderPlan(false)
	confirm := renderPlan(true) + applyPrompt()
	frames := []Frame{
		{3, "schemabot plan -s ./schema -e staging", "Review the change for the commerce keyspace", plan},
		{2, "schemabot apply -s ./schema -e staging --defer-deploy", "Review the plan before creating the deploy request", confirm + "▌"},
		{0.8, "", "Type yes to confirm", confirm + "yes"},
		{4, "", "Deploy request ready after apply --defer-deploy", vitess(state.Apply.WaitingForDeploy, nil, false)},
		{1.5, "", "Press Enter to deploy", vitess(state.Apply.WaitingForDeploy, nil, true)},
	}
	for _, p := range [][]int{{25, 15, 10, 5}, {65, 45, 35, 20}, {100, 80, 65, 45}, {100, 100, 90, 70}, {100, 100, 100, 95}, {100, 100, 100, 100}} {
		frames = append(frames, Frame{1.4, "", "Each shard reports its own progress, rows, and ETA", vitess(state.Apply.Running, p, false)})
	}
	frames = append(frames,
		Frame{5, "", "Deployed; the revert window remains open", vitess(state.Apply.RevertWindow, nil, false)},
		Frame{1.5, "", "Press Enter when ready to close the revert window", vitess(state.Apply.RevertWindow, nil, true)},
		Frame{3, "", "The watcher confirms completion", vitess(state.Apply.Completed, nil, false)},
	)
	return Demo{Name: "cli-vitess", Title: "One deploy request. Progress across every shard.", Height: 740, Frames: frames}
}

// Read the confirmation text from the actual apply command so this fixture
// fails when the command stops using that prompt rather than silently drifting.
func applyPrompt() string {
	_, file, _, _ := runtime.Caller(0)
	source, err := parser.ParseFile(token.NewFileSet(), filepath.Join(filepath.Dir(file), "../commands/apply.go"), nil, 0)
	if err != nil {
		panic(err)
	}
	var prompt string
	ast.Inspect(source, func(node ast.Node) bool {
		call, ok := node.(*ast.CallExpr)
		if !ok {
			return true
		}
		name, ok := call.Fun.(*ast.Ident)
		if !ok || name.Name != "confirmAction" {
			return true
		}
		literal, ok := call.Args[0].(*ast.BasicLit)
		if !ok {
			panic("apply prompt is no longer a literal")
		}
		prompt, err = strconv.Unquote(literal.Value)
		if err != nil {
			panic(err)
		}
		return false
	})
	if prompt == "" {
		panic("apply confirmation not found")
	}
	return prompt
}

// followLogView uses the actual logs command, API client, and colored formatter.
// Cancel after the initial fetch so each selected tail view is deterministic.
func followLogView(count int) string {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	// Messages come from the Spirit runner and replica throttler, also checked
	// against the disposable MySQL CLI run. Only demo IDs and times are synthetic.
	messages := []struct{ level, message string }{
		{"info", "Apply queued: apply-example-73"},
		{"info", "[orders] Starting spirit migration"},
		{"info", "[orders] acquired advisory lock"},
		{"info", "[orders] preserved AUTO_INCREMENT value"},
		{"info", "[orders] create BinlogSyncer"},
		{"info", "[orders] begin to sync binlog from position"},
		{"info", "[orders] Connected to server"},
		{"info", "[orders] scaled write workers up"},
		{"info", "[orders] scaled read workers up"},
		{"warn", "[orders] replication delayed, throttling in progress"},
		{"info", "[orders] approaching the end of the table, synchronously updating statistics"},
		{"info", "[orders] copy rows complete"},
		{"info", "[orders] Running ANALYZE TABLE"},
		{"info", "[orders] starting checksum operation, this will require a table lock"},
		{"warn", "[orders] table lock(s) acquired"},
		{"info", "[orders] table unlocked, starting checksum"},
		{"info", "[orders] checksum passed"},
		{"warn", "[orders] Attempting final cut over operation"},
		{"warn", "[orders] final cut over operation complete"},
		{"info", "[_orders_old] successfully dropped old table"},
		{"info", "[orders] apply complete"},
		{"info", "[orders] releasing advisory locks"},
	}
	entries := make([]*apitypes.LogEntry, 0, count)
	for i, entry := range messages[:count] {
		entries = append(entries, &apitypes.LogEntry{ID: int64(i + 1), ApplyID: "apply-example-73", Level: entry.level, Message: entry.message, CreatedAt: time.Date(2026, 1, 15, 14, 20+i/2, i%2*15, 0, time.UTC)})
	}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet || r.URL.Path != "/api/logs" || r.URL.Query().Get("apply_id") != "apply-example-73" || r.URL.Query().Get("limit") != "50" {
			panic("unexpected logs request")
		}
		w.Header().Set("Content-Type", "application/json")
		if err := json.NewEncoder(w).Encode(apitypes.LogsResponse{ApplyID: "apply-example-73", Logs: entries}); err != nil {
			panic(err)
		}
		cancel()
	}))
	defer server.Close()
	output := capture(func() {
		cmd := commands.LogsCmd{ApplyIDArg: "apply-example-73", Limit: 50, Follow: true}
		if err := cmd.Run(ctx, &commands.Globals{Endpoint: server.URL}); err != nil {
			panic(err)
		}
	})
	// Keep the most recent terminal lines in view as the tail scrolls.
	lines := strings.Split(strings.TrimRight(output, "\n"), "\n")
	if len(lines) > 14 {
		lines = lines[len(lines)-14:]
	}
	return strings.Join(lines, "\n") + "\n"
}

func fleetInventory() string {
	response := apitypes.DatabaseListResponse{}
	for _, name := range []string{"accounts", "analytics", "billing", "catalog", "inventory", "notifications", "search", "shop"} {
		response.Databases = append(response.Databases, &apitypes.DatabaseResponse{Database: name, Type: "mysql", Environments: []*apitypes.DatabaseEnvironmentResponse{{Environment: "staging"}, {Environment: "production"}}})
	}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet || r.URL.Path != "/api/databases" {
			panic("unexpected inventory request")
		}
		w.Header().Set("Content-Type", "application/json")
		if err := json.NewEncoder(w).Encode(response); err != nil {
			panic(err)
		}
	}))
	defer server.Close()
	return capture(func() {
		if err := (&commands.DatabasesCmd{}).Run(&commands.Globals{Endpoint: server.URL}); err != nil {
			panic(err)
		}
	})
}

func busyFleet(now time.Time) []string {
	data := t.StatusListData{Sectioned: true, ActiveCount: 3, Limit: 12, HasMore: true, StateCounts: map[string]int{state.Apply.Completed: 483, state.Apply.Failed: 9, state.Apply.Stopped: 5}}
	names := []string{"shop", "billing", "catalog", "accounts", "inventory", "search", "analytics", "notifications"}
	for i := 0; i < 15; i++ {
		apply := t.ActiveApplyData{ApplyID: fmt.Sprintf("apply-example-%02d", 73-i), Database: names[i%len(names)], Environment: "staging", State: state.Apply.Running, Caller: fmt.Sprintf("github:alex@acme/store#%d", 42-i), StartedAt: now.Add(-time.Duration(10+i*25) * time.Minute).Format(time.RFC3339)}
		if i < 3 {
			data.Active = append(data.Active, apply)
			continue
		}
		apply.State = state.Apply.Completed
		if i == 5 {
			apply.State = state.Apply.Failed
		}
		if i == 8 {
			apply.State = state.Apply.Stopped
		}
		data.Applies = append(data.Applies, apply)
	}
	// The capture pipe is not a TTY; this frame represents an interactive
	// terminal, where SOURCE is a short labeled link.
	previousHyperlinks := ui.Hyperlinks
	ui.Hyperlinks = true
	output := capture(func() { t.WriteStatusList(data) })
	ui.Hyperlinks = previousHyperlinks
	if !strings.Contains(output, "3 active schema changes") || !strings.Contains(output, "497 total:") || !strings.Contains(output, "12 most recent") {
		panic("fleet summary missing")
	}
	return strings.Split(strings.TrimRight(output, "\n"), "\n")
}

// The multi-target scenarios apply one index to a production environment whose
// deployment lists four targets. The apply command runs against a loopback
// fixture that answers as the server does for a targets list: a plan of every
// member, or a plan narrowed to one, and one operation per target.
const (
	rolloutDeployment = "prod"
	rolloutApplyID    = "apply-example-91"
	narrowedApplyID   = "apply-example-92"
)

var rolloutTargets = []string{"shop-001", "shop-002", "shop-003", "shop-004"}

// rolloutRows is each target's own table size; the targets hold different
// data, so their copies finish at different times.
var rolloutRows = map[string]int64{"shop-001": 6_000_000, "shop-002": 9_000_000, "shop-003": 7_500_000, "shop-004": 4_500_000}

func rolloutMember(target string) string { return rolloutDeployment + "/" + target }

func rolloutPlan(target string) apitypes.PlanResponse {
	changes := []*apitypes.SchemaChangeResponse{{Namespace: "shop", TableChanges: []*apitypes.TableChangeResponse{{TableName: "orders", ChangeType: "alter", DDL: ddl}}}}
	plan := apitypes.PlanResponse{PlanID: "plan-example-rollout", Database: "shop", DatabaseType: "mysql", Environment: "production", Engine: "Spirit",
		Deployment: rolloutDeployment, Target: rolloutTargets[0], Changes: changes}
	if target != "" {
		plan.PlanID = "plan-example-" + target
		plan.Target = target
		plan.NarrowedTo = rolloutMember(target)
		return plan
	}
	members := make([]string, 0, len(rolloutTargets))
	for _, target := range rolloutTargets {
		members = append(members, rolloutMember(target))
	}
	rollout := &apitypes.PlanRolloutResponse{Members: len(members), Independent: true, MultiTarget: true,
		Groups: []*apitypes.PlanMemberGroupResponse{{Members: members, Primary: true, Changes: changes}}}
	for _, target := range rolloutTargets {
		bytes := rolloutRows[target] * 500
		rollout.TableSizes = append(rollout.TableSizes, &apitypes.PlanMemberTableSizeResponse{Member: rolloutMember(target), Namespace: "shop", Table: "orders", EstimatedBytes: &bytes})
	}
	plan.Rollout = rollout
	return plan
}

// cuttingOver marks a target whose copy has finished and is swapping tables.
const cuttingOver = -1

// rolloutProgress is the apply's progress with each listed target at its own
// percentage. A target at 100 has cut over and completed; the apply completes
// once every target has.
func rolloutProgress(applyID string, targets []string, percents []int) apitypes.ProgressResponse {
	response := apitypes.ProgressResponse{ApplyID: applyID, Database: "shop", DatabaseType: "mysql", Environment: "production", Engine: "Spirit", State: state.Apply.Completed}
	for i, target := range targets {
		p := percents[i]
		opState, taskState := state.Apply.Running, state.Task.Running
		switch {
		case p == cuttingOver:
			p = 100
			opState, taskState = state.Apply.CuttingOver, state.Task.CuttingOver
			response.State = state.Apply.Running
		case p >= 100:
			opState, taskState = state.Apply.Completed, state.Task.Completed
		default:
			response.State = state.Apply.Running
		}
		total := rolloutRows[target]
		var eta int64
		if p < 100 {
			eta = int64(100-p) * total / 1_000_000 / 2
		}
		response.Operations = append(response.Operations, &apitypes.ProgressOperationResponse{Deployment: rolloutDeployment, Target: target, State: opState, CutoverPolicy: "parallel"})
		response.Tables = append(response.Tables, &apitypes.TableProgressResponse{TableName: "orders", DDL: ddl, Deployment: rolloutDeployment, Target: target, Keyspace: "shop", ChangeType: "alter",
			Status: taskState, RowsCopied: total * int64(p) / 100, RowsTotal: total, PercentComplete: int32(p), ETASeconds: eta})
	}
	return response
}

// runningTargets lists the targets of a rollout whose work has not finished.
func runningTargets(response apitypes.ProgressResponse) []string {
	var running []string
	for _, table := range response.Tables {
		if !state.IsState(table.Status, state.Task.Completed) {
			running = append(running, table.Target)
		}
	}
	return running
}

// rolloutStopped is the rollout once a stop has reached it: every target that
// was still copying stopped at its own checkpoint, and a target that had
// finished stays finished.
func rolloutStopped(percents []int) apitypes.ProgressResponse {
	response := rolloutProgress(rolloutApplyID, rolloutTargets, percents)
	response.State = state.Apply.Stopped
	for i, op := range response.Operations {
		if state.IsState(op.State, state.Apply.Completed) {
			continue
		}
		op.State = state.Apply.Stopped
		response.Tables[i].Status = state.Task.Stopped
		response.Tables[i].ETASeconds = 0
	}
	return response
}

// rolloutStart runs the real start command against a loopback fixture holding
// the stopped rollout. The fixture checks the start request and scripts the
// watcher start opens through polls to completion. It returns what start
// printed before the watcher took over.
func rolloutStart(stoppedAt []int) string {
	stopped := rolloutStopped(stoppedAt)
	resumed := len(runningTargets(stopped))
	polls := [][]int{{60, 45, 52, 100}, {100, 100, 100, 100}}
	var requests []string
	var fixtureMu sync.Mutex
	poll := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fixtureMu.Lock()
		defer fixtureMu.Unlock()
		requests = append(requests, r.Method+" "+r.URL.Path)
		w.Header().Set("Content-Type", "application/json")
		var response any
		switch r.Method + " " + r.URL.Path {
		case "GET /api/progress/apply/" + rolloutApplyID:
			switch {
			case len(requests) == 1:
				response = stopped
			case poll < len(polls):
				response = rolloutProgress(rolloutApplyID, rolloutTargets, polls[poll])
				poll++
			default:
				panic("start watcher polled after completion")
			}
		case "POST /api/start":
			var request apitypes.ControlRequest
			if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
				panic(err)
			}
			if request.ApplyID != rolloutApplyID || request.Environment != "production" {
				panic("wrong start request")
			}
			response = apitypes.StartResponse{Accepted: true, StartedCount: int64(resumed), SkippedCount: int64(len(rolloutTargets) - resumed)}
		default:
			panic("unexpected start request: " + r.Method + " " + r.URL.Path)
		}
		if err := json.NewEncoder(w).Encode(response); err != nil {
			panic(err)
		}
	}))
	defer server.Close()
	output := runInTerminalWithInput("", "start-fixture", server.URL)
	fixtureMu.Lock()
	sequence := strings.Join(requests, ",")
	fixtureMu.Unlock()
	want := "GET /api/progress/apply/" + rolloutApplyID + ",POST /api/start" + strings.Repeat(",GET /api/progress/apply/"+rolloutApplyID, len(polls))
	if sequence != want {
		panic("wrong start request sequence: " + sequence)
	}
	if !strings.Contains(output, "Apply complete!") {
		panic("start watcher did not reach completion: " + output)
	}
	const resuming = "Resuming from checkpoint..."
	before, after, found := strings.Cut(output, resuming)
	if !found {
		panic("start did not resume: " + output)
	}
	reset, _, _ := strings.Cut(after, "\n")
	return before + resuming + reset + "\n"
}

// watchView renders the production watcher's view of one progress response.
func watchView(response apitypes.ProgressResponse) string {
	return watchViewPressing(response, false)
}

// watchViewPressing renders the production watcher's view of one progress
// response, after pressing s when stop is set. The fixture checks that the
// key sends one stop request for the apply being watched.
func watchViewPressing(response apitypes.ProgressResponse, stop bool) string {
	var stops int
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		var body any = response
		switch {
		case r.Method == http.MethodGet && strings.HasPrefix(r.URL.Path, "/api/progress/apply/"):
		case stop && r.Method == http.MethodPost && r.URL.Path == "/api/stop":
			var request apitypes.ControlRequest
			if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
				panic(err)
			}
			if request.ApplyID != response.ApplyID || request.Environment != response.Environment {
				panic("wrong stop request")
			}
			stops++
			body = apitypes.StopResponse{Accepted: true, StoppedCount: int64(len(runningTargets(response))), SkippedCount: int64(len(response.Tables) - len(runningTargets(response)))}
		default:
			panic("unexpected watch request: " + r.Method + " " + r.URL.Path)
		}
		if err := json.NewEncoder(w).Encode(body); err != nil {
			panic(err)
		}
	}))
	defer server.Close()
	model := commands.NewWatchModel(server.URL, response.Database, response.Environment, true)
	batch := model.Init()().(tea.BatchMsg)
	updated, _ := model.Update(batch[0]())
	if stop {
		var cmd tea.Cmd
		updated, cmd = updated.Update(tea.KeyMsg{Type: tea.KeyRunes, Runes: []rune{'s'}})
		if cmd == nil {
			panic("stop key did not request stop")
		}
		updated, _ = updated.Update(cmd())
		if stops != 1 {
			panic("expected one stop request")
		}
	}
	view := updated.View()
	if strings.Contains(view, "Error:") || strings.Contains(view, "Loading...") {
		panic(view)
	}
	return view
}

// rolloutApply runs the real apply command, confirmed with yes, against a
// loopback fixture for the multi-target environment. An empty target applies
// the whole rollout. The fixture checks every request the command sends and
// scripts the automatic watcher through polls to completion. It returns the
// command's output up to the watcher's announcement.
func rolloutApply(target string) string {
	applyID := rolloutApplyID
	if target != "" {
		applyID = narrowedApplyID
	}
	var polls [][]int
	targets := rolloutTargets
	if target == "" {
		polls = [][]int{{10, 8, 9, 12}, {60, 40, 50, 80}, {100, 100, 100, 100}}
	} else {
		targets = []string{target}
		polls = [][]int{{10}, {60}, {100}}
	}
	plan := rolloutPlan(target)
	var requests []string
	var fixtureMu sync.Mutex
	poll := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fixtureMu.Lock()
		defer fixtureMu.Unlock()
		requests = append(requests, r.Method+" "+r.URL.Path)
		w.Header().Set("Content-Type", "application/json")
		var response any
		switch r.Method + " " + r.URL.Path {
		case "GET /api/status":
			query := r.URL.Query()
			wantDeployment := ""
			if target != "" {
				wantDeployment = rolloutDeployment
			}
			if query.Get("environment") != "production" || query.Get("active") != "true" || query.Get("deployment") != wantDeployment {
				panic("wrong active schema change preflight: " + r.URL.RawQuery)
			}
			response = apitypes.StatusResponse{}
		case "POST /api/plan":
			var request apitypes.PlanRequest
			if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
				panic(err)
			}
			if request.Database != "shop" || request.Environment != "production" || request.Target != target || !request.RendersRollout {
				panic("wrong rollout plan request")
			}
			if files := request.SchemaFiles["shop"]; files == nil || !strings.Contains(files.Files["orders.sql"], "`idx_status`") {
				panic("plan request did not carry the schema files")
			}
			response = plan
		case "GET /api/locks/shop/mysql", "POST /api/locks/acquire":
			response = map[string]any{"lock": nil}
		case "POST /api/apply":
			var request apitypes.ApplyRequest
			if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
				panic(err)
			}
			if request.PlanID != plan.PlanID || request.Environment != "production" || request.Target != plan.NarrowedTo || !request.RendersRollout || request.Options["defer_cutover"] != "" {
				panic("wrong rollout apply request")
			}
			response = apitypes.ApplyResponse{Accepted: true, ApplyID: applyID}
		case "GET /api/progress/apply/" + applyID:
			if poll >= len(polls) {
				panic("automatic watcher polled after completion")
			}
			response = rolloutProgress(applyID, targets, polls[poll])
			poll++
		default:
			panic("unexpected rollout request: " + r.Method + " " + r.URL.Path)
		}
		if err := json.NewEncoder(w).Encode(response); err != nil {
			panic(err)
		}
	}))
	defer server.Close()

	dir, err := os.MkdirTemp("", "docdemo-rollout-")
	if err != nil {
		panic(err)
	}
	defer func() {
		if err := os.RemoveAll(dir); err != nil {
			panic(err)
		}
	}()
	schemaDir := filepath.Join(dir, "schema")
	if err := os.MkdirAll(filepath.Join(schemaDir, "shop"), 0o755); err != nil {
		panic(err)
	}
	if err := os.WriteFile(filepath.Join(schemaDir, "schemabot.yaml"), []byte("database: shop\ntype: mysql\n"), 0o644); err != nil {
		panic(err)
	}
	orders := "CREATE TABLE `orders` (\n  `id` bigint unsigned NOT NULL AUTO_INCREMENT,\n  `status` varchar(32) NOT NULL,\n  PRIMARY KEY (`id`),\n  KEY `idx_status` (`status`)\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;\n"
	if err := os.WriteFile(filepath.Join(schemaDir, "shop", "orders.sql"), []byte(orders), 0o644); err != nil {
		panic(err)
	}
	args := []string{"apply-fixture", server.URL, schemaDir}
	if target != "" {
		args = append(args, target)
	}
	output := runInTerminal(args...)

	fixtureMu.Lock()
	sequence := strings.Join(requests, ",")
	fixtureMu.Unlock()
	progress := strings.Repeat(",GET /api/progress/apply/"+applyID, len(polls))
	want := "GET /api/status,POST /api/plan,GET /api/locks/shop/mysql,POST /api/locks/acquire,POST /api/apply" + progress
	if target != "" {
		want = "POST /api/plan,GET /api/status,GET /api/locks/shop/mysql,POST /api/locks/acquire,POST /api/apply" + progress
	}
	if sequence != want {
		panic("wrong rollout request sequence: " + sequence)
	}
	if !strings.Contains(output, "Apply complete!") {
		panic("automatic rollout watcher did not reach completion: " + output)
	}
	submitted, _, found := strings.Cut(output, "Watching progress...\n")
	if !found || !strings.Contains(submitted, "Apply started: "+applyID) {
		panic("rollout apply did not start: " + output)
	}
	return submitted + "Watching progress...\n"
}

// rolloutDemo follows an apply of the whole environment: every target copies at
// once under the default parallel cutover policy, and each cuts over as soon
// as its own copy is ready.
func rolloutDemo() Demo {
	command := "schemabot apply -s ./schema -e production"
	output := rolloutApply("")
	preview, submitted := splitAtConfirmation(output)
	if !strings.Contains(ansi.Strip(preview), "shop-004") {
		panic("rollout plan does not name every target: " + preview)
	}
	frames := []Frame{
		{4, command, "One plan covers every target of the environment", preview + "▌"},
		{0.3, "", "Type yes to apply it to all four", preview + "y▌"},
		{0.3, "", "", preview + "ye▌"},
		{0.8, "", "", preview + "yes"},
		{1.5, "", "One apply fans out to every target", submitted},
	}
	steps := [][]int{{5, 4, 5, 8}, {20, 14, 17, 30}, {35, 25, 30, 52}, {50, 36, 44, 75}, {65, 46, 57, cuttingOver}, {80, 57, 70, 100}, {95, 68, 83, 100}, {cuttingOver, 79, 96, 100}, {100, 89, cuttingOver, 100}, {100, 97, 100, 100}}
	for i, p := range steps {
		caption := "Every target copies at once"
		if i >= 4 {
			caption = "Each target cuts over as soon as its own copy is ready"
		}
		frames = append(frames, Frame{0.9, "", caption, watchView(rolloutProgress(rolloutApplyID, rolloutTargets, p))})
	}
	final := watchView(rolloutProgress(rolloutApplyID, rolloutTargets, []int{100, 100, 100, 100}))
	if !strings.Contains(final, "Apply complete!") {
		panic("rollout completion missing: " + final)
	}
	frames = append(frames, Frame{4, "", "The index lands on every target", final})
	return Demo{Name: "cli-multi-target", Title: "One apply. Every target.", Height: 820, Bar: "shop · production", Frames: frames}
}

// targetDemo applies the change to one member of the rollout with --target.
func targetDemo() Demo {
	command := "schemabot apply -s ./schema -e production --target shop-002"
	output := rolloutApply("shop-002")
	preview, submitted := splitAtConfirmation(output)
	if !strings.Contains(preview, "Target: "+rolloutMember("shop-002")+" (this plan covers only this rollout member)") {
		panic("narrowed plan header missing: " + preview)
	}
	frames := []Frame{
		{4, command, "The plan covers only shop-002", preview + "▌"},
		{0.3, "", "Type yes to apply it to that target alone", preview + "y▌"},
		{0.3, "", "", preview + "ye▌"},
		{0.8, "", "", preview + "yes"},
		{1.5, "", "The apply runs on shop-002 only", submitted},
	}
	for p := 10; p < 100; p += 10 {
		frames = append(frames, Frame{0.4, "", "Follow the one target to completion", watchView(rolloutProgress(narrowedApplyID, []string{"shop-002"}, []int{p}))})
	}
	final := watchView(rolloutProgress(narrowedApplyID, []string{"shop-002"}, []int{100}))
	if !strings.Contains(final, "Apply complete!") {
		panic("narrowed completion missing: " + final)
	}
	frames = append(frames, Frame{4, "", "shop-002 has the index; the other targets are unchanged", final})
	return Demo{Name: "cli-target", Title: "Apply to one target first.", Height: 740, Bar: "shop · production", Frames: frames}
}

// rolloutStopDemo stops a rollout partway through its copies and starts it
// again: one stop halts every target still copying, a finished target stays
// finished, and start resumes each target from its own checkpoint.
func rolloutStopDemo() Demo {
	stoppedAt := []int{45, 30, 37, 100}
	frames := []Frame{
		{1.5, "", "Every target copies in parallel", watchView(rolloutProgress(rolloutApplyID, rolloutTargets, []int{38, 25, 31, 92}))},
		{1.5, "", "shop-004 finishes first", watchView(rolloutProgress(rolloutApplyID, rolloutTargets, stoppedAt))},
		{2, "", "Press s to stop the apply", watchViewPressing(rolloutProgress(rolloutApplyID, rolloutTargets, stoppedAt), true)},
	}
	stopped := watchView(rolloutStopped(stoppedAt))
	if !strings.Contains(stopped, "schemabot start") {
		panic("stopped rollout does not say how to resume: " + stopped)
	}
	frames = append(frames, Frame{3.5, "", "Every target still copying stops; shop-004 stays done", stopped})
	started := rolloutStart(stoppedAt)
	frames = append(frames, Frame{2, "schemabot start " + rolloutApplyID + " -e production", "Start resumes every stopped target", started})
	steps := [][]int{{45, 30, 37, 100}, {55, 40, 47, 100}, {65, 50, 57, 100}, {75, 60, 67, 100}, {85, 70, 77, 100}, {95, 80, 87, 100}, {cuttingOver, 90, 97, 100}, {100, 100, cuttingOver, 100}, {100, cuttingOver, 100, 100}}
	for _, p := range steps {
		frames = append(frames, Frame{0.6, "", "Each target resumes from its own checkpoint", watchView(rolloutProgress(rolloutApplyID, rolloutTargets, p))})
	}
	final := watchView(rolloutProgress(rolloutApplyID, rolloutTargets, []int{100, 100, 100, 100}))
	if !strings.Contains(final, "Apply complete!") {
		panic("resumed rollout completion missing: " + final)
	}
	frames = append(frames, Frame{4, "", "The index lands on every target", final})
	return Demo{Name: "cli-multi-target-stop", Title: "Stop every target. Resume each one.", Height: 820, Bar: "shop · production", Frames: frames}
}

// splitAtConfirmation separates the plan and prompt the apply command printed
// from what it printed once confirmed, so the renderer can type the answer.
func splitAtConfirmation(output string) (string, string) {
	prompt := applyPrompt()
	before, after, found := strings.Cut(output, prompt)
	if !found {
		panic("apply prompt missing: " + output)
	}
	return before + prompt, strings.TrimPrefix(after, "\n")
}

func main() {
	t.SetPreviewMode()
	time.Local = time.UTC
	lipgloss.SetColorProfile(termenv.TrueColor)
	// A fictional explicit token prevents the fixture from reading cached logins.
	if err := os.Setenv("SCHEMABOT_TOKEN", "fictional-doc-fixture"); err != nil {
		panic(err)
	}
	if len(os.Args) == 3 && os.Args[1] == "rollback-fixture" {
		endpoint, err := url.Parse(os.Args[2])
		if err != nil {
			panic(err)
		}
		if endpoint.Scheme != "http" || endpoint.Hostname() != "127.0.0.1" {
			panic("rollback fixture requires a loopback HTTP endpoint")
		}
		cmd := commands.RollbackCmd{ApplyID: "apply-example-85", Environment: "staging", Watch: true}
		if err := cmd.Run(context.Background(), &commands.Globals{Endpoint: endpoint.String()}); err != nil {
			panic(err)
		}
		return
	}
	if len(os.Args) == 3 && os.Args[1] == "start-fixture" {
		endpoint, err := url.Parse(os.Args[2])
		if err != nil {
			panic(err)
		}
		if endpoint.Scheme != "http" || endpoint.Hostname() != "127.0.0.1" {
			panic("start fixture requires a loopback HTTP endpoint")
		}
		cmd := commands.StartCmd{ControlFlags: commands.ControlFlags{ApplyID: rolloutApplyID, Environment: "production"}, Watch: true}
		if err := cmd.Run(&commands.Globals{Endpoint: endpoint.String()}); err != nil {
			panic(err)
		}
		return
	}
	if (len(os.Args) == 4 || len(os.Args) == 5) && os.Args[1] == "apply-fixture" {
		endpoint, err := url.Parse(os.Args[2])
		if err != nil {
			panic(err)
		}
		if endpoint.Scheme != "http" || endpoint.Hostname() != "127.0.0.1" {
			panic("apply fixture requires a loopback HTTP endpoint")
		}
		cmd := commands.ApplyCmd{SchemaDir: os.Args[3], Environment: "production", Watch: true, Output: commands.OutputFormatInteractive, LogHeartbeat: 10 * time.Second}
		if len(os.Args) == 5 {
			cmd.Target = os.Args[4]
		}
		if err := cmd.Run(context.Background(), &commands.Globals{Endpoint: endpoint.String()}); err != nil {
			panic(err)
		}
		return
	}
	renderPlan := func(apply bool) string {
		return capture(func() {
			t.WritePlanHeader(t.PlanHeaderData{Database: "shop", SchemaName: "schema", IsMySQL: true, IsApply: apply})
			changes := []t.DDLChange{{TableName: "orders", ChangeType: "alter", DDL: ddl}}
			t.WriteEnvironmentHeader("staging")
			t.WriteNamespaceChanges([]t.NamespaceChange{{Namespace: "shop", Changes: changes}}, true, "shop", schema.DialectMySQL)
			t.WritePlanSummary(changes)
		})
	}
	plan := renderPlan(false)
	confirm := renderPlan(true) + applyPrompt()
	demos := []Demo{{Name: "cli-plan-apply", Title: "Review the plan. Follow the change.", Frames: []Frame{
		{4, "schemabot plan -s ./schema -e staging", "See the DDL before running it", plan},
		{2, "schemabot apply -s ./schema -e staging", "Review the plan and type yes to confirm", confirm + "▌"},
		{0.3, "", "", confirm + "y▌"},
		{0.3, "", "", confirm + "ye▌"},
		{0.8, "", "", confirm + "yes"},
		{2, "", "After confirming, follow the apply", live(10, state.Apply.Running, false, "")},
		{1, "", "", live(35, state.Apply.Running, false, "")},
		{1, "", "", live(65, state.Apply.Running, false, "")},
		{1, "", "", live(90, state.Apply.Running, false, "")},
		{4, "", "The change is complete", live(100, state.Apply.Completed, false, "")}}}}
	now := time.Date(2026, 1, 15, 14, 30, 0, 0, time.UTC)
	frames := []Frame{{2, "schemabot progress apply-example-73", "Follow an apply started with --defer-cutover", live(20, state.Apply.Running, false, "")}}
	for p := 25; p <= 60; p += 5 {
		frames = append(frames, Frame{0.18, "", "", live(p, state.Apply.Running, false, "")})
	}
	for p := 65; p <= 100; p += 5 {
		frames = append(frames, Frame{0.18, "", "Follow the row copy to completion", live(p, state.Apply.Running, false, "")})
	}
	frames = append(frames, Frame{3, "", "Copy complete; wait for the final swap", live(100, state.Apply.WaitingForCutover, false, "")}, Frame{2, "", "Press Enter to request the final swap", live(100, state.Apply.WaitingForCutover, false, "enter")}, Frame{3, "", "The watcher confirms completion", live(100, state.Apply.Completed, false, "")})
	demos = append(demos, Demo{Name: "cli-cutover", Title: "Choose when to cut over.", Frames: frames})
	// This scenario illustrates a terminal that supports labeled links.
	previousHyperlinks := ui.Hyperlinks
	ui.Hyperlinks = true
	throttleFrames := []Frame{{2, "schemabot progress apply-example-73", "Follow the row copy", live(40, state.Apply.Running, false, "")}}
	for p := 45; p <= 60; p += 5 {
		throttleFrames = append(throttleFrames, Frame{0.18, "", "", live(p, state.Apply.Running, false, "")})
	}
	throttleFrames = append(throttleFrames, Frame{5, "", "Slow commits pause copying; the docs explain the signal", live(60, state.Apply.Running, true, "")})
	for p := 65; p <= 100; p += 5 {
		throttleFrames = append(throttleFrames, Frame{0.18, "", "Copying resumes when conditions improve", live(p, state.Apply.Running, false, "")})
	}
	throttleFrames = append(throttleFrames, Frame{3, "", "The change completes", live(100, state.Apply.Completed, false, "")})
	demos = append(demos, Demo{Name: "cli-throttle", Title: "See why a change slows down.", Frames: throttleFrames})
	ui.Hyperlinks = previousHyperlinks
	stopFrames := []Frame{
		{2, "", "The change is already copying rows", live(50, state.Apply.Running, false, "")},
		{1, "", "", live(55, state.Apply.Running, false, "")},
		{2, "", "Press s to request a stop", live(55, state.Apply.Running, false, "s")},
		{3, "", "Wait for Stopped; the CLI shows how to resume", live(55, state.Apply.Stopped, false, "")},
		{2, "schemabot start apply-example-73 -e staging", "Resume the stopped change", capture(func() {
			t.WriteStartSuccess(t.StartData{Database: "shop", Environment: "staging", ApplyID: "apply-example-73", StartedCount: 1})
		})},
		{1, "", "Start opens the live watcher automatically", live(55, state.Apply.Running, false, "")},
	}
	for p := 60; p <= 100; p += 5 {
		stopFrames = append(stopFrames, Frame{0.18, "", "Copying continues from the checkpoint", live(p, state.Apply.Running, false, "")})
	}
	stopFrames = append(stopFrames, Frame{3, "", "The watcher confirms completion", live(100, state.Apply.Completed, false, "")})
	demos = append(demos, Demo{Name: "cli-stop", Title: "Stop and resume a change.", Frames: stopFrames})
	demos = append(demos, vitessDemo(), rollbackDemo(), rolloutDemo(), targetDemo(), rolloutStopDemo())
	fleetLines := busyFleet(now)
	pull := capture(func() {
		// The capture pipe is not a TTY; this frame represents an interactive terminal.
		previousColors := ui.Colors
		ui.Colors = true
		defer func() { ui.Colors = previousColors }()
		t.WritePullSchema(&apitypes.PullSchemaResponse{Database: "shop", Type: "mysql", Environment: "staging", TableCount: 1, Namespaces: map[string]*apitypes.PulledNamespace{"shop": {Lint: []*apitypes.LintViolationResponse{{Table: "orders", Column: "created_at", Severity: "warning", Linter: "zero_date", Message: `column "created_at" with type "datetime" has a zero default value`}}, Tables: map[string]string{"orders": "CREATE TABLE `orders` (\n  `id` bigint unsigned NOT NULL AUTO_INCREMENT,\n  `status` varchar(32) NOT NULL,\n  `created_at` datetime NOT NULL DEFAULT '0000-00-00 00:00:00',\n  PRIMARY KEY (`id`)\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;"}}}})
	})
	demos = append(demos, Demo{Name: "cli-ops", Title: "Know your database fleet.", Height: 960, Frames: []Frame{
		{2, "schemabot databases", "See the databases and environments in your fleet", fleetInventory()},
		{3.5, "schemabot pull -d shop -e staging --table orders --lint", "Inspect the live schema and its lint findings", pull},
		{4, "schemabot status -e staging -n 12", "Running changes first, then the latest finished ones", strings.Join(fleetLines, "\n")},
		{1, "schemabot logs apply-example-73 -f", "Follow the engine logs as they arrive", followLogView(5)},
		{0.6, "", "", followLogView(9)},
		{0.9, "", "See why copying slows down", followLogView(10)},
		{0.6, "", "Copying finishes; verification begins", followLogView(14)},
		{0.6, "", "", followLogView(17)},
		{0.6, "", "Follow the final cutover", followLogView(20)},
		{2.5, "", "The engine confirms completion", followLogView(22)},
	}})
	enc := json.NewEncoder(os.Stdout)
	enc.SetIndent("", "  ")
	if err := enc.Encode(demos); err != nil {
		panic(err)
	}
}

// rollbackDemo runs both confirmation choices through the real rollback command.
// Every HTTP request terminates at this fictional fixture; no engine is invoked.
func rollbackDemo() Demo {
	const rollbackDDL = "ALTER TABLE `orders` ADD INDEX `idx_status` (`status`);"
	plan := apitypes.PlanResponse{PlanID: "plan-example-rollback", Database: "shop", DatabaseType: "mysql", Environment: "staging", Engine: "Spirit",
		Changes: []*apitypes.SchemaChangeResponse{{Namespace: "shop", TableChanges: []*apitypes.TableChangeResponse{{TableName: "orders", ChangeType: "alter", DDL: rollbackDDL}}}}}
	var requests []string
	var fixtureMu sync.Mutex
	watchingCommand := false
	watchPolls := 0
	rollbackPhase := state.Apply.Running
	rollbackPercent := int32(25)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fixtureMu.Lock()
		defer fixtureMu.Unlock()
		requests = append(requests, r.Method+" "+r.URL.Path)
		w.Header().Set("Content-Type", "application/json")
		var response any
		switch r.Method + " " + r.URL.Path {
		case "POST /api/rollback/plan":
			var request apitypes.ControlRequest
			if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
				panic(err)
			}
			if request.ApplyID != "apply-example-85" || request.Environment != "staging" {
				panic("wrong rollback source")
			}
			response = plan
		case "GET /api/status":
			if r.URL.Query().Get("environment") != "staging" || r.URL.Query().Get("active") != "true" {
				panic("wrong rollback preflight")
			}
			response = apitypes.StatusResponse{}
		case "GET /api/locks/shop/mysql", "POST /api/locks/acquire":
			if r.Method == http.MethodPost {
				var request map[string]any
				if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
					panic(err)
				}
				if request["database"] != "shop" || request["database_type"] != "mysql" || request["owner"] == "" {
					panic("wrong rollback lock")
				}
			}
			response = map[string]any{"lock": nil}
		case "POST /api/apply":
			var request apitypes.ApplyRequest
			if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
				panic(err)
			}
			if request.PlanID != plan.PlanID || request.Environment != "staging" || request.Options["allow_unsafe"] == "true" {
				panic("wrong rollback apply")
			}
			response = apitypes.ApplyResponse{Accepted: true, ApplyID: "apply-example-86"}
		case "GET /api/progress/apply/", "GET /api/progress/apply/apply-example-86":
			phase, percent := rollbackPhase, rollbackPercent
			if watchingCommand {
				if r.URL.Path != "/api/progress/apply/apply-example-86" {
					panic("automatic watcher did not use the new apply ID")
				}
				watchPolls++
				switch watchPolls {
				case 1:
					phase, percent = state.Apply.Running, 25
				case 2:
					phase, percent = state.Apply.Running, 100
				case 3:
					phase, percent = state.Apply.Completed, 100
				default:
					panic("automatic watcher polled after completion")
				}
			}
			response = apitypes.ProgressResponse{ApplyID: "apply-example-86", Database: "shop", DatabaseType: "mysql", Environment: "staging", Engine: "Spirit", State: phase,
				Tables: []*apitypes.TableProgressResponse{{TableName: "orders", Keyspace: "shop", ChangeType: "alter", DDL: rollbackDDL, Status: phase, PercentComplete: percent, RowsCopied: int64(percent) * 100000, RowsTotal: 10000000}}}
		default:
			panic("unexpected rollback request: " + r.Method + " " + r.URL.Path)
		}
		if err := json.NewEncoder(w).Encode(response); err != nil {
			panic(err)
		}
	}))
	defer server.Close()
	run := func(answer string) string {
		input, writer, err := os.Pipe()
		if err != nil {
			panic(err)
		}
		if _, err := io.WriteString(writer, answer+"\n"); err != nil {
			panic(err)
		}
		if err := writer.Close(); err != nil {
			panic(err)
		}
		old := os.Stdin
		os.Stdin = input
		defer func() {
			os.Stdin = old
			if err := input.Close(); err != nil {
				panic(err)
			}
		}()
		return capture(func() {
			cmd := commands.RollbackCmd{ApplyID: "apply-example-85", Environment: "staging", Watch: true}
			if err := cmd.Run(context.Background(), &commands.Globals{Endpoint: server.URL}); err != nil {
				panic(err)
			}
		})
	}
	requestSequence := func() string {
		fixtureMu.Lock()
		defer fixtureMu.Unlock()
		return strings.Join(requests, ",")
	}
	declined := run("no")
	if requestSequence() != "POST /api/rollback/plan,GET /api/status" {
		panic("declined rollback performed a write")
	}
	preview, _, found := strings.Cut(declined, "\nRollback cancelled.")
	if !found || !strings.Contains(ansi.Strip(preview), "ALTER TABLE `orders` ADD INDEX `idx_status`(`status`);") || !strings.Contains(preview, t.ANSIBlue+"ALTER") {
		panic("rollback preview missing")
	}
	fixtureMu.Lock()
	requests = nil
	watchingCommand = true
	fixtureMu.Unlock()
	accepted := rollbackInteractive(server.URL)
	fixtureMu.Lock()
	watchingCommand = false
	polls := watchPolls
	fixtureMu.Unlock()
	if requestSequence() != "POST /api/rollback/plan,GET /api/status,GET /api/locks/shop/mysql,POST /api/locks/acquire,POST /api/apply,GET /api/progress/apply/apply-example-86,GET /api/progress/apply/apply-example-86,GET /api/progress/apply/apply-example-86" {
		panic("wrong rollback request sequence")
	}
	submitted, found := strings.CutPrefix(accepted, preview)
	if !found || !strings.Contains(submitted, "Rollback started: apply-example-86") {
		panic("rollback acceptance missing")
	}
	if polls != 3 || !strings.Contains(accepted, "100.00%") || !strings.Contains(accepted, "Apply complete!") {
		panic("automatic rollback watcher did not reach completion: " + accepted)
	}
	submitted, _, found = strings.Cut(submitted, "Watching progress...\n")
	if !found {
		panic("automatic watcher announcement missing")
	}
	submitted += "Watching progress...\n"
	render := func(phase string, percent int32) string {
		fixtureMu.Lock()
		rollbackPhase, rollbackPercent = phase, percent
		fixtureMu.Unlock()
		model := commands.NewWatchModel(server.URL, "shop", "staging", true)
		batch := model.Init()().(tea.BatchMsg)
		updated, _ := model.Update(batch[0]())
		return updated.View()
	}
	started := render(state.Apply.Running, 25)
	var copying []Frame
	for p := int32(30); p < 100; p += 5 {
		copying = append(copying, Frame{0.18, "", "Follow the new apply", render(state.Apply.Running, p)})
	}
	finished := render(state.Apply.Running, 100)
	if !strings.Contains(finished, "100") {
		panic("rollback progress missing: " + finished)
	}
	final := render(state.Apply.Completed, 100)
	if !strings.Contains(final, "Apply complete!") || !strings.Contains(final, "apply-example-86") {
		panic("rollback completion missing: " + final)
	}
	command := "schemabot rollback -e staging apply-example-85"
	frames := []Frame{
		{4, command, "Review the plan to restore the removed index", preview + "▌"},
		{0.25, "", "Type yes only after reviewing the plan", preview + "y▌"},
		{0.25, "", "", preview + "ye▌"},
		{1, "", "", preview + "yes"},
		{1.5, "", "Rollback starts the new apply and opens its watcher", submitted},
		{2, "", "Live progress opens automatically after confirmation", started},
	}
	frames = append(frames, copying...)
	frames = append(frames, Frame{2, "", "The copy reaches 100%; wait for confirmed completion", finished}, Frame{4, "", "The new apply completes; the index is restored", final})
	return Demo{Name: "cli-rollback", Title: "Review a rollback. Follow the new change.", Height: 740, Frames: frames}
}

// rollbackInteractive gives the actual command terminal input while capturing
// its output. The helper process runs this generator's loopback-only mode.
func rollbackInteractive(endpoint string) string {
	return runInTerminal("rollback-fixture", endpoint)
}

// runInTerminal runs this generator in one of its loopback-only fixture modes
// with terminal input that answers the confirmation prompt with yes.
func runInTerminal(args ...string) string {
	return runInTerminalWithInput("yes\n", args...)
}

// runInTerminalWithInput runs a child fixture mode in a terminal, typing input
// before the command starts reading.
func runInTerminalWithInput(input string, args ...string) string {
	executable, err := os.Executable()
	if err != nil {
		panic(err)
	}
	_, source, _, _ := runtime.Caller(0)
	helper := filepath.Join(filepath.Dir(source), "../../../scripts/cli-demo-tty.py")
	ctx, cancel := context.WithTimeout(context.Background(), 25*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, "python3", append([]string{helper, executable}, args...)...)
	cmd.Env = append(os.Environ(), "CLI_DEMO_INPUT="+input)
	cmd.Stderr = os.Stderr
	output, err := cmd.Output()
	if err != nil {
		panic(fmt.Errorf("run interactive %s: %w", args[0], err))
	}
	return string(output)
}
