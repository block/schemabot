//go:build ignore

// Generate fictional CLI scenarios through the production output templates.
// Run from the repository root: go run ./pkg/cmd/docdemo/main.go > assets/src/cli-demo.json
package main

import (
	"encoding/json"
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"io"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"

	"github.com/block/schemabot/pkg/apitypes"
	"github.com/block/schemabot/pkg/cmd/commands"
	tea "github.com/charmbracelet/bubbletea"
	"github.com/charmbracelet/lipgloss"
	"github.com/muesli/termenv"
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

// live renders the production interactive watcher against a loopback-only API
// fixture. Init's first fetch seeds the model; subsequent requests have the apply
// ID supplied by that response. No private model fields or UI strings are copied.
func live(p int, phase string, throttled bool, key string) string {
	response := apitypes.ProgressResponse{
		ApplyID: "apply-example-73", Database: "shop", Environment: "staging", Engine: "Spirit", State: phase,
		Tables: []*apitypes.TableProgressResponse{{TableName: "orders", Keyspace: "shop", ChangeType: "alter", DDL: ddl,
			Status: phase, RowsCopied: int64(p) * 100000, RowsTotal: 10000000, PercentComplete: int32(p), ETASeconds: int64(100-p) * 12,
			Throttled: throttled, ThrottleReason: "Replication lag exceeds the configured limit"}},
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
	if key == "esc" {
		var cmd tea.Cmd
		updated, cmd = updated.Update(tea.KeyMsg{Type: tea.KeyEsc})
		if cmd == nil {
			panic("Escape did not detach")
		}
		if _, ok := cmd().(tea.QuitMsg); !ok {
			panic("Escape did not quit the watcher")
		}
		if cutovers != 0 {
			panic("detach triggered cutover")
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

func main() {
	t.SetPreviewMode()
	lipgloss.SetColorProfile(termenv.TrueColor)
	// A fictional explicit token prevents the fixture from reading cached logins.
	if err := os.Setenv("SCHEMABOT_TOKEN", "fictional-doc-fixture"); err != nil {
		panic(err)
	}
	renderPlan := func(apply bool) string {
		return capture(func() {
			t.WritePlanHeader(t.PlanHeaderData{Database: "shop", SchemaName: "schema", IsMySQL: true, IsApply: apply})
			changes := []t.DDLChange{{TableName: "orders", ChangeType: "alter", DDL: ddl}}
			t.WriteEnvironmentHeader("staging")
			t.WriteNamespaceChanges([]t.NamespaceChange{{Namespace: "shop", Changes: changes}}, true, "shop")
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
	demos = append(demos, Demo{Name: "cli-fleet", Title: "From the fleet to one change.", Frames: []Frame{
		{5, "schemabot status -e staging", "See changes across databases", status},
		{4, "schemabot status apply-example-73", "Inspect one running change", progress(60, state.Apply.Running, false)},
		{3, "schemabot progress apply-example-73", "Attach to live progress; ESC detaches", live(60, state.Apply.Running, false, "")},
		{2, "", "Press ESC to detach; the apply keeps running", live(60, state.Apply.Running, false, "esc")},
		{5, "schemabot list-plans -e staging", "After pressing ESC, find recent plans", plans}}})
	frames := []Frame{{3, "schemabot progress apply-example-73", "Follow an apply started with --defer-cutover", live(20, state.Apply.Running, false, "")}}
	for _, p := range []int{35, 50, 60} {
		frames = append(frames, Frame{0.7, "", "", live(p, state.Apply.Running, false, "")})
	}
	frames = append(frames, Frame{3, "", "See why copying has slowed", live(60, state.Apply.Running, true, "")})
	for _, p := range []int{70, 80, 90, 100} {
		frames = append(frames, Frame{0.7, "", "Copying resumes as conditions improve", live(p, state.Apply.Running, false, "")})
	}
	frames = append(frames, Frame{3, "", "Copy complete; wait for the final swap", live(100, state.Apply.WaitingForCutover, false, "")}, Frame{2, "", "Press Enter to request the final swap", live(100, state.Apply.WaitingForCutover, false, "enter")}, Frame{4, "", "The watcher confirms completion", live(100, state.Apply.Completed, false, "")})
	demos = append(demos, Demo{Name: "cli-cutover", Title: "Know when to wait. Choose when to swap.", Frames: frames})
	enc := json.NewEncoder(os.Stdout)
	enc.SetIndent("", "  ")
	if err := enc.Encode(demos); err != nil {
		panic(err)
	}
}
