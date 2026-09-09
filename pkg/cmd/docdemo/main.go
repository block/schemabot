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
			Throttled: throttled, ThrottleReason: "commit-latency 120ms >= 50ms"}},
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
	data := t.StatusListData{ActiveCount: 3, Limit: 20, HasMore: true, StateCounts: map[string]int{state.Apply.Completed: 483, state.Apply.Running: 3, state.Apply.Failed: 9, state.Apply.Stopped: 5}}
	names := []string{"shop", "billing", "catalog", "accounts", "inventory", "search", "analytics", "notifications"}
	for i := 0; i < 20; i++ {
		phase := state.Apply.Completed
		if i < 3 {
			phase = state.Apply.Running
		}
		if i == 5 {
			phase = state.Apply.Failed
		}
		if i == 8 {
			phase = state.Apply.Stopped
		}
		data.Applies = append(data.Applies, t.ActiveApplyData{ApplyID: fmt.Sprintf("apply-example-%02d", 73-i), Database: names[i%len(names)], Environment: "staging", State: phase, Caller: fmt.Sprintf("github:alex@acme/store#%d", 42-i), StartedAt: now.Add(-time.Duration(10+i*25) * time.Minute).Format(time.RFC3339)})
	}
	output := capture(func() { t.WriteStatusList(data) })
	if !strings.Contains(output, "500 total:") || !strings.Contains(output, "20 most recent") {
		panic("fleet summary missing")
	}
	return strings.Split(strings.TrimRight(output, "\n"), "\n")
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
		if err := cmd.Run(&commands.Globals{Endpoint: endpoint.String()}); err != nil {
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
		{2, "schemabot start -e staging apply-example-73", "Resume the stopped change", capture(func() {
			t.WriteStartSuccess(t.StartData{Database: "shop", Environment: "staging", ApplyID: "apply-example-73", StartedCount: 1})
		})},
		{1, "", "Start opens the live watcher automatically", live(55, state.Apply.Running, false, "")},
	}
	for p := 60; p <= 100; p += 5 {
		stopFrames = append(stopFrames, Frame{0.18, "", "Copying continues from the checkpoint", live(p, state.Apply.Running, false, "")})
	}
	stopFrames = append(stopFrames, Frame{3, "", "The watcher confirms completion", live(100, state.Apply.Completed, false, "")})
	demos = append(demos, Demo{Name: "cli-stop", Title: "Stop and resume a change.", Frames: stopFrames})
	demos = append(demos, vitessDemo(), rollbackDemo())
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
		{4, "schemabot status -e staging", "500 changes; the latest 20 at a glance", strings.Join(fleetLines, "\n")},
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
			if request.PlanID != plan.PlanID || request.Environment != "staging" || request.Options["allow_unsafe"] != "true" {
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
			if err := cmd.Run(&commands.Globals{Endpoint: server.URL}); err != nil {
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
	executable, err := os.Executable()
	if err != nil {
		panic(err)
	}
	_, source, _, _ := runtime.Caller(0)
	helper := filepath.Join(filepath.Dir(source), "../../../scripts/cli-demo-tty.py")
	ctx, cancel := context.WithTimeout(context.Background(), 25*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, "python3", helper, executable, "rollback-fixture", endpoint)
	cmd.Stderr = os.Stderr
	output, err := cmd.Output()
	if err != nil {
		panic(fmt.Errorf("run interactive rollback fixture: %w", err))
	}
	return string(output)
}
