package commands

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/block/schemabot/pkg/localsetup"
	tea "github.com/charmbracelet/bubbletea"
	"github.com/charmbracelet/lipgloss"
	"github.com/stretchr/testify/require"
)

func wizardKey(m *initWizard, k tea.KeyType) { m.Update(tea.KeyMsg{Type: k}) }

func TestInitWizardNavigationAndValidation(t *testing.T) {
	t.Setenv("DATABASE_URL", "test-only")
	t.Setenv("SCHEMABOT_STORAGE_DSN", "test-only")
	m := newInitWizard(&InitCmd{Namespaces: []string{"public"}}, "default", io.Discard)
	m.editing = true
	wizardKey(m, tea.KeyDown)
	require.Equal(t, "postgres", m.fields[0].value)
	wizardKey(m, tea.KeyEnter)
	m.input.SetValue("Bad Name")
	wizardKey(m, tea.KeyEnter)
	require.Equal(t, 1, m.step)
	require.NotEmpty(t, m.err)
	m.input.SetValue("shop")
	wizardKey(m, tea.KeyEnter)
	wizardKey(m, tea.KeyShiftTab)
	require.Equal(t, "shop", m.input.Value())
	wizardKey(m, tea.KeyEnter)
	wizardKey(m, tea.KeyEnter)
	m.input.SetValue("postgres://secret")
	wizardKey(m, tea.KeyEnter)
	require.Equal(t, stepDSN, m.step)
	require.Contains(t, m.err, "env:VARIABLE")
	m.input.SetValue("env:DATABASE_URL")
	wizardKey(m, tea.KeyEnter)
	m.Update(initConnectionMsg{generation: m.generation})
	wizardKey(m, tea.KeyEnter)
	wizardKey(m, tea.KeyEnter)
	m.Update(initConnectionMsg{generation: m.generation})
	wizardKey(m, tea.KeyEnter)
	require.Equal(t, "public", m.input.Value())
	for m.step < len(m.fields) {
		wizardKey(m, tea.KeyEnter)
	}
	require.False(t, m.confirmed)
	require.Contains(t, strings.Join(strings.Fields(m.View()), " "), "We won’t change your application’s schema")
	wizardKey(m, tea.KeyEnter)
	require.True(t, m.confirmed)
}
func TestInitWizardReviewExistingFilesAndCancel(t *testing.T) {
	root := t.TempDir()
	path := filepath.Join(root, "users.sql")
	require.NoError(t, os.WriteFile(path, []byte("existing"), 0600))
	require.NoError(t, os.WriteFile(filepath.Join(root, "schemabot.yaml"), []byte("database: app\ntype: postgres\n"), 0600))
	m := newInitWizard(&InitCmd{SchemaDir: root}, "default", io.Discard)
	m.step = len(m.fields)
	m.loadField()
	require.Contains(t, strings.Join(strings.Fields(m.View()), " "), "verify them and keep your edits")
	wizardKey(m, tea.KeyEsc)
	require.False(t, m.confirmed)
	require.True(t, m.cancelled)
	data, err := os.ReadFile(path)
	require.NoError(t, err)
	require.Equal(t, "existing", string(data))
}
func TestInitWizardNonInteractive(t *testing.T) {
	cmd := InitCmd{NonInteractive: true}
	err := cmd.collectInputs(t.Context(), &Globals{})
	require.ErrorContains(t, err, "--database")
	require.ErrorContains(t, err, "--namespace")
}
func TestInitWizardMissingVariableAndNarrowTerminal(t *testing.T) {
	t.Setenv("DATABASE_URL", "")
	m := newInitWizard(&InitCmd{}, "default", io.Discard)
	m.step = stepDSN
	m.loadField()
	wizardKey(m, tea.KeyEnter)
	require.Equal(t, stepDSN, m.step)
	require.Contains(t, m.err, "This variable is empty")
	m.Update(tea.WindowSizeMsg{Width: 40, Height: 24})
	require.NotEmpty(t, m.View())
}
func TestInitProgressCancellationWaitsForCleanup(t *testing.T) {
	cancelled := false
	m := &initProgress{cancel: func() { cancelled = true }}
	_, cmd := m.Update(tea.KeyMsg{Type: tea.KeyCtrlC})
	require.True(t, cancelled)
	require.True(t, m.stopping)
	require.Nil(t, cmd)
	require.Nil(t, m.finished)
	_, cmd = m.Update(initFinishedMsg{})
	require.NotNil(t, cmd)
}
func TestInitCompletionQuotesNextCommand(t *testing.T) {
	output := initCompletion(&initResult{SchemaDir: "my schema", Profile: "dev's profile"}, "development", "default")
	require.Contains(t, output, "-s 'my schema'")
	require.Contains(t, output, "--profile 'dev'\"'\"'s profile'")
}

func TestInitCompletionOmitsDefaultProfile(t *testing.T) {
	output := initCompletion(&initResult{SchemaDir: "schema", Profile: "default"}, "development", "default")
	require.Contains(t, output, "schemabot plan -s 'schema' -e 'development'\n")
	require.NotContains(t, output, "--profile")
}

func TestInitWizardContextCancellationLeavesInputsUntouched(t *testing.T) {
	t.Setenv("HOME", t.TempDir())
	input, writer := io.Pipe()
	t.Cleanup(func() { require.NoError(t, input.Close()); require.NoError(t, writer.Close()) })
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	output := initSignalWriter{started: make(chan struct{}, 1)}
	original := InitCmd{Database: "shop"}
	cmd := original
	done := make(chan error, 1)
	go func() { done <- cmd.promptInputs(ctx, input, output, &Globals{}) }()
	<-output.started
	cancel()
	require.ErrorIs(t, <-done, context.Canceled)
	require.Equal(t, original.Database, cmd.Database)
	require.Empty(t, cmd.Type)
}

type initSignalWriter struct{ started chan struct{} }

func (w initSignalWriter) Write(p []byte) (int, error) {
	select {
	case w.started <- struct{}{}:
	default:
	}
	return len(p), nil
}

func TestInitWizardNarrowReviewCanScroll(t *testing.T) {
	m := newInitWizard(&InitCmd{}, "default", io.Discard)
	m.step = len(m.fields)
	m.Update(tea.WindowSizeMsg{Width: 40, Height: 20})
	before := m.View()
	require.Contains(t, before, "scroll")
	for line := range strings.SplitSeq(before, "\n") {
		require.LessOrEqual(t, lipgloss.Width(line), 40)
	}
	wizardKey(m, tea.KeyDown)
	require.NotEqual(t, before, m.View())
	require.False(t, m.confirmed)
}

func TestInitWizardDiscoverySelectsOneAndReviewsDefaults(t *testing.T) {
	m := newInitWizard(&InitCmd{}, "default", io.Discard)
	m.step = stepNamespaces
	m.generation = 1
	m.discovering = true
	m.Update(initNamespacesMsg{generation: 1, names: []string{"public"}})
	require.Equal(t, len(m.fields), m.step)
	require.Equal(t, "public", m.fields[stepNamespaces].value)
	require.Contains(t, m.View(), "Found public")
	require.False(t, m.confirmed)
	wizardKey(m, tea.KeyShiftTab)
	require.Equal(t, stepProfile, m.step)
	m.input.SetValue("my-profile")
	wizardKey(m, tea.KeyEnter)
	require.Equal(t, "my-profile", m.fields[stepProfile].value)
}
func TestInitWizardDiscoveryPickerKeepsSelectionAcrossSearch(t *testing.T) {
	m := newInitWizard(&InitCmd{}, "default", io.Discard)
	m.step = stepNamespaces
	m.generation = 1
	m.loadField()
	m.Update(initNamespacesMsg{generation: 1, names: []string{"analytics", "public", "reports"}})
	wizardKey(m, tea.KeyEnter)
	require.Contains(t, m.err, "at least one")
	m.Update(tea.KeyMsg{Type: tea.KeyRunes, Runes: []rune(" ")})
	m.input.SetValue("reports")
	m.cursor = 0
	m.Update(tea.KeyMsg{Type: tea.KeyRunes, Runes: []rune(" ")})
	wizardKey(m, tea.KeyEnter)
	require.Equal(t, "analytics, reports", m.fields[stepNamespaces].value)
	require.Equal(t, len(m.fields), m.step)
}
func TestInitWizardDiscoveryFailureAndStaleResponse(t *testing.T) {
	m := newInitWizard(&InitCmd{}, "default", io.Discard)
	m.step = stepNamespaces
	m.generation = 2
	m.Update(initNamespacesMsg{generation: 1, names: []string{"wrong_database"}})
	require.Empty(t, m.names)
	m.Update(initNamespacesMsg{generation: 2})
	require.Equal(t, stepNamespaces, m.step)
	require.Contains(t, m.err, "didn’t find")
	m.Update(initNamespacesMsg{generation: 2, err: fmt.Errorf("connection unavailable")})
	require.Equal(t, stepNamespaces, m.step)
	require.Contains(t, m.err, "connection unavailable")
}

func TestInitWizardDiscoveryNeverFallsBackToAmbientConnection(t *testing.T) {
	t.Setenv("UNSET_INIT_TARGET", "")
	m := newInitWizard(&InitCmd{DSN: "env:UNSET_INIT_TARGET"}, "default", io.Discard)
	m.ctx = t.Context()
	m.step = stepNamespaces
	m.discover = func(context.Context, localsetup.Target) ([]string, error) {
		t.Fatal("must not connect with an empty DSN")
		return nil, nil
	}
	msg := m.discoverNamespaces()().(initNamespacesMsg)
	require.Error(t, msg.err)
}

func TestInitWizardPreservesExplicitNamespaceWithComma(t *testing.T) {
	original := []string{"sales,west", "public"}
	m := newInitWizard(&InitCmd{Namespaces: original}, "default", io.Discard)
	require.Equal(t, original, m.namespaceChoices(original))
}

func TestInitWizardConfirmsConfiguredConnections(t *testing.T) {
	t.Setenv("DATABASE_URL", "postgres://demo:secret@localhost:5432/shop?sslmode=disable")
	t.Setenv("SCHEMABOT_STORAGE_DSN", "postgres://demo:secret@localhost:5432/state?sslmode=disable")
	m := newInitWizard(&InitCmd{Type: "postgres", Database: "shop", DSN: "env:DATABASE_URL", StorageDSN: "env:SCHEMABOT_STORAGE_DSN"}, "default", io.Discard)
	require.Equal(t, stepDSN, m.step)
	require.Contains(t, m.View(), "localhost:5432")
	require.Contains(t, m.View(), `Database: "shop"`)
	require.NotContains(t, m.View(), "secret")
	wizardKey(m, tea.KeyEnter)
	require.True(t, m.checkingConnection)
	require.Equal(t, stepDSN, m.step)
	m.Update(initConnectionMsg{generation: m.generation})
	require.Contains(t, m.View(), "✓ Connected")
	wizardKey(m, tea.KeyEnter)
	require.Equal(t, stepStorageDSN, m.step)
	require.Contains(t, m.View(), `Database: "state"`)
	require.False(t, m.confirmed)
}

func TestInitConnectionSummaryRedactsCredentials(t *testing.T) {
	for _, tt := range []struct{ engine, dsn string }{
		{"mysql", "demo:secret@tcp(localhost:3306)/shop"},
		{"postgres", "postgres://demo:secret@localhost:5432/shop?sslmode=disable"},
		{"postgres", "host=localhost port=5432 dbname=shop user=demo password=secret sslmode=disable"},
		{"postgres", "postgres://demo:secret@%invalid"},
	} {
		t.Run(tt.dsn, func(t *testing.T) {
			t.Setenv("WIZARD_TEST_DSN", tt.dsn)
			got := initConnectionSummary(tt.engine, "env:WIZARD_TEST_DSN")
			require.NotContains(t, got, "secret")
			require.NotContains(t, got, "demo")
			require.NotContains(t, got, "sslmode")
			if !strings.Contains(tt.dsn, "%invalid") {
				require.Contains(t, got, "localhost")
				require.Contains(t, got, `Database: "shop"`)
			}
		})
	}
	t.Setenv("WIZARD_TEST_DSN", "postgres://user:secret@localhost/shop%1B%5B2J?sslmode=disable")
	require.NotContains(t, initConnectionSummary("postgres", "env:WIZARD_TEST_DSN"), "\x1b")
	t.Setenv("WIZARD_TEST_DSN", "")
	require.Contains(t, initConnectionSummary("postgres", "env:WIZARD_TEST_DSN"), "isn’t set")
}

func TestInitConnectionFailureRetryAndEdit(t *testing.T) {
	t.Setenv("DATABASE_URL", "postgres://demo:secret@localhost/shop?sslmode=disable")
	m := newInitWizard(&InitCmd{Type: "postgres", Database: "shop"}, "default", io.Discard)
	wizardKey(m, tea.KeyEnter)
	m.Update(initConnectionMsg{generation: m.generation, err: fmt.Errorf("couldn’t connect")})
	require.Equal(t, stepDSN, m.step)
	require.False(t, m.connectionChecked)
	wizardKey(m, tea.KeyEnter)
	require.True(t, m.checkingConnection)
	m.Update(initConnectionMsg{generation: m.generation - 1})
	require.True(t, m.checkingConnection)
	m.Update(initConnectionMsg{generation: m.generation})
	require.True(t, m.connectionChecked)
	m.Update(tea.KeyMsg{Type: tea.KeyRunes, Runes: []rune("X")})
	require.False(t, m.connectionChecked)
}

func TestInitTerminalModes(t *testing.T) {
	for _, tt := range []struct {
		name                                      string
		noninteractive, json, stdin, stdout, want bool
	}{
		{"terminal", false, false, true, true, true},
		{"explicit noninteractive", true, false, true, true, false},
		{"json", false, true, true, true, false},
		{"redirected input", false, false, false, true, false},
		{"redirected output", false, false, true, false, false},
	} {
		t.Run(tt.name, func(t *testing.T) {
			cmd := InitCmd{Database: "shop", Environment: "dev", Type: "postgres", DSN: "env:APP", StorageDSN: "env:STATE", Namespaces: []string{"public"}, NonInteractive: tt.noninteractive, JSON: tt.json}
			require.NoError(t, cmd.collectInputsWithTerminalState(t.Context(), &Globals{}, tt.stdin, tt.stdout))
			require.Equal(t, tt.want, cmd.interactive)
		})
	}
	cmd := InitCmd{Type: "unknown"}
	require.ErrorContains(t, cmd.collectInputsWithTerminalState(t.Context(), &Globals{}, true, true), "must be mysql, postgres, vitess")
}

func TestInitPublishEmptyDirectoryPreservesConcurrentFiles(t *testing.T) {
	root := t.TempDir()
	stage := filepath.Join(root, "stage")
	dest := filepath.Join(root, "schema")
	require.NoError(t, os.Mkdir(stage, 0700))
	require.NoError(t, os.Mkdir(dest, 0700))
	require.NoError(t, os.WriteFile(filepath.Join(stage, "schema.sql"), []byte("verified"), 0600))
	require.NoError(t, publishInitSchema(stage, dest))
	data, err := os.ReadFile(filepath.Join(dest, "schema.sql"))
	require.NoError(t, err)
	require.Equal(t, "verified", string(data))
	require.Error(t, removeEmptyInitDir(dest))
	link := filepath.Join(root, "link")
	require.NoError(t, os.Symlink(dest, link))
	require.Error(t, removeEmptyInitDir(link))
	require.FileExists(t, filepath.Join(dest, "schema.sql"))
}

func TestInitWizardRejectsTrailingNamespaceBeforeReview(t *testing.T) {
	m := newInitWizard(&InitCmd{Namespaces: []string{"public"}}, "default", io.Discard)
	m.step = stepNamespaces
	m.loadField()
	m.input.SetValue("public,")
	wizardKey(m, tea.KeyEnter)
	require.Equal(t, stepNamespaces, m.step)
	require.NotEmpty(t, m.err)
	require.False(t, m.confirmed)
}

func TestInitExplicitNamespacesRoundTrip(t *testing.T) {
	for _, names := range [][]string{{"sales", "west"}, {"sales,west"}} {
		m := newInitWizard(&InitCmd{Namespaces: names}, "default", io.Discard)
		m.step = stepNamespaces
		m.loadField()
		require.Empty(t, m.validate())
		require.Equal(t, names, m.namespaceChoices(names))
	}
}

func TestInitCatalogNamesCannotControlTerminal(t *testing.T) {
	name := "a\x1b[2Jb"
	m := newInitWizard(&InitCmd{}, "default", io.Discard)
	m.step = stepNamespaces
	m.loadField()
	m.generation = 1
	m.Update(initNamespacesMsg{generation: 1, names: []string{name, "public"}})
	require.NotContains(t, m.namespaceView(), name)
	require.Contains(t, m.namespaceView(), `\x1b`)
	m.Update(initNamespacesMsg{generation: 1, names: []string{name}})
	require.NotContains(t, m.notice, name)
	require.NotContains(t, m.View(), name)
}

func TestInitManualNamespaceFallback(t *testing.T) {
	m := newInitWizard(&InitCmd{}, "default", io.Discard)
	m.step = stepNamespaces
	m.err = "discovery failed"
	m.Update(tea.KeyMsg{Type: tea.KeyRunes, Runes: []rune("m")})
	require.True(t, m.explicitNamespaces)
	m.input.SetValue("sales, west")
	require.Empty(t, m.validate())
	wizardKey(m, tea.KeyEnter)
	require.Equal(t, []string{"sales", "west"}, m.namespaceChoices(nil))
}

func TestInitCopyBackPreservesConnectionsAndDoesNotInitialize(t *testing.T) {
	home := t.TempDir()
	t.Setenv("HOME", home)
	root := t.TempDir()
	require.NoError(t, os.WriteFile(filepath.Join(root, "schemabot.yaml"), []byte("database: shop\ntype: postgres\n"), 0600))
	original := InitCmd{Namespaces: []string{"sales", "west"}}
	m := newInitWizard(&original, "default", io.Discard)
	values := []string{"postgres", "shop", "development", "env:APP", "", "", "env:STATE", "sales, west", root, "chosen"}
	for i, v := range values {
		m.fields[i].value = v
	}
	g := Globals{}
	require.ErrorIs(t, m.copyToCommand(&original, &g), ErrSilent)
	require.Empty(t, original.DSN)
	m.confirmed = true
	require.NoError(t, m.copyToCommand(&original, &g))
	require.Equal(t, "postgres", original.Type)
	require.Equal(t, "shop", original.Database)
	require.Equal(t, "development", original.Environment)
	require.Equal(t, "env:APP", original.DSN)
	require.Equal(t, "env:STATE", original.StorageDSN)
	require.Equal(t, []string{"sales", "west"}, original.Namespaces)
	require.Equal(t, root, original.SchemaDir)
	require.Equal(t, "chosen", g.Profile)
	require.True(t, original.ReuseSchema)
	require.NoDirExists(t, filepath.Join(home, ".schemabot"))
}

func TestInitRejectsUnrelatedFilesBeforeReview(t *testing.T) {
	root := t.TempDir()
	require.NoError(t, os.WriteFile(filepath.Join(root, "README.md"), []byte("keep"), 0600))
	m := newInitWizard(&InitCmd{SchemaDir: root}, "default", io.Discard)
	m.step = len(m.fields)
	wizardKey(m, tea.KeyEnter)
	require.False(t, m.confirmed)
	require.Equal(t, stepSchemaDir, m.step)
	require.Contains(t, m.err, "schemabot.yaml")
	require.Contains(t, m.err, root)
	require.NotContains(t, m.err, ".schemabot-init-")
}

func TestInitMissingInputsJSONUsesErrorObject(t *testing.T) {
	cmd := InitCmd{JSON: true}
	output := captureStdout(func() {
		require.ErrorIs(t, cmd.collectInputsWithTerminalState(t.Context(), &Globals{}, false, false), ErrSilent)
	})
	var response struct {
		Error   struct{ Code, Message string }
		Missing []string
	}
	require.NoError(t, json.Unmarshal([]byte(output), &response))
	require.Equal(t, "missing_inputs", response.Error.Code)
	require.NotEmpty(t, response.Error.Message)
	require.Contains(t, response.Missing, "database")
}

func TestInitWizardChecksFolderBeforeReviewAndKeepsStateStepReachable(t *testing.T) {
	root := t.TempDir()
	require.NoError(t, os.WriteFile(filepath.Join(root, ".gitkeep"), nil, 0644))
	m := newInitWizard(&InitCmd{SchemaDir: root}, "default", io.Discard)
	m.step = len(m.fields)
	m.loadField()
	require.Equal(t, stepSchemaDir, m.step)
	require.Contains(t, m.err, "placeholder")
	require.NotContains(t, m.View(), "verify them and keep your edits")
	require.FileExists(t, filepath.Join(root, ".gitkeep"))
	m.step = stepNamespaces
	m.explicitNamespaces = false
	m.loadField()
	wizardKey(m, tea.KeyShiftTab)
	require.Equal(t, stepStorageDSN, m.step)
}

type initBrokenTerminal struct{ started <-chan struct{} }

func (r initBrokenTerminal) Read([]byte) (int, error) {
	<-r.started
	return 0, errors.New("terminal failed")
}

func TestInitTerminalFailureJoinsCleanup(t *testing.T) {
	started, cleaned := make(chan struct{}), make(chan struct{})
	_, err := runInitProgress(t.Context(), func(ctx context.Context, report func(string)) (*initResult, error) {
		defer close(cleaned)
		close(started)
		<-ctx.Done()
		return nil, errors.New("initialization incomplete; runtime registration is retained for retry")
	}, tea.WithInput(initBrokenTerminal{started: started}), tea.WithOutput(io.Discard), tea.WithoutRenderer())
	require.ErrorContains(t, err, "terminal failed")
	require.ErrorContains(t, err, "runtime registration is retained for retry")
	select {
	case <-cleaned:
	default:
		t.Fatal("terminal returned before cleanup")
	}
}

type initStartupFailure struct{}

func (initStartupFailure) Run() (tea.Model, error) { return nil, errors.New("terminal startup failed") }
func (initStartupFailure) Kill()                   {}
func (initStartupFailure) Send(tea.Msg)            {}

func TestInitTerminalStartupFailureDoesNotInitialize(t *testing.T) {
	called := false
	_, err := runInitProgressProgram(t.Context(), func(ctx context.Context, report func(string)) (*initResult, error) {
		called = true
		return nil, nil
	}, func(tea.Model) initProgressProgram { return initStartupFailure{} })
	require.ErrorContains(t, err, "terminal startup failed")
	require.NotErrorIs(t, err, context.Canceled)
	require.False(t, called, "setup must not run when the terminal fails before Init")
}

func TestInitCancelledBeforeWorkReturnsError(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	called := false
	result, err := runInitProgress(ctx, func(context.Context, func(string)) (*initResult, error) {
		called = true
		return &initResult{}, nil
	}, tea.WithInput(nil), tea.WithOutput(io.Discard), tea.WithoutRenderer())
	require.ErrorIs(t, err, context.Canceled)
	require.Nil(t, result)
	require.False(t, called)
}

func TestInitReviewEscapesFlagValues(t *testing.T) {
	m := newInitWizard(&InitCmd{}, "default", io.Discard)
	m.step = len(m.fields)
	for i := range m.fields {
		m.fields[i].value = "value\x1b[2J"
	}
	view := m.View()
	require.NotContains(t, view, "\x1b[2J")
	require.Contains(t, view, `\x1b[2J`)
}

// A Vitess database adds the PlanetScale organization and token steps between
// the application and state connections. The token step shows the token's name
// only, the state connection is checked as MySQL, and the review lists the
// PlanetScale details alongside the connections.
func TestInitWizardVitessStepsAndReview(t *testing.T) {
	t.Setenv("DATABASE_URL", "user:secret@tcp(vtgate:3306)/shop")
	t.Setenv("PLANETSCALE_TOKEN", "tok-name:tok-secret")
	t.Setenv("SCHEMABOT_STORAGE_DSN", "user:secret@tcp(localhost:3306)/state")
	m := newInitWizard(&InitCmd{Namespaces: []string{"commerce"}, APIURL: "http://localscale"}, "default", io.Discard)
	wizardKey(m, tea.KeyUp)
	require.Equal(t, "vitess", m.engine())
	wizardKey(m, tea.KeyEnter)
	require.Equal(t, stepName, m.step)
	require.Contains(t, m.View(), "PlanetScale database name")
	m.input.SetValue("shop")
	wizardKey(m, tea.KeyEnter)
	require.Equal(t, stepDSN, m.step)
	require.Contains(t, m.View(), "vtgate:3306")
	wizardKey(m, tea.KeyEnter)
	m.Update(initConnectionMsg{generation: m.generation})
	wizardKey(m, tea.KeyEnter)
	require.Equal(t, stepOrganization, m.step)
	m.input.SetValue("Acme")
	wizardKey(m, tea.KeyEnter)
	require.NotEmpty(t, m.err)
	m.input.SetValue("acme")
	wizardKey(m, tea.KeyEnter)
	require.Equal(t, stepAPIToken, m.step)
	require.Contains(t, m.View(), `Token: "tok-name"`)
	require.NotContains(t, m.View(), "tok-secret")
	var checked localsetup.Target
	m.checkAPI = func(_ context.Context, target localsetup.Target) error { checked = target; return nil }
	result := m.checkConnection()().(initConnectionMsg)
	require.NoError(t, result.err)
	require.Equal(t, localsetup.Target{Engine: "vitess", Database: "shop", Organization: "acme", Token: "tok-name:tok-secret", APIURL: "http://localscale"}, checked)
	m.Update(result)
	require.True(t, m.connectionChecked)
	wizardKey(m, tea.KeyEnter)
	require.Equal(t, stepStorageDSN, m.step)
	require.Contains(t, m.View(), "MySQL database of their own")
	var storageEngine string
	m.check = func(_ context.Context, engine, _ string) error { storageEngine = engine; return nil }
	m.Update(m.checkConnection()())
	require.Equal(t, "mysql", storageEngine)
	wizardKey(m, tea.KeyEnter)
	require.Equal(t, len(m.fields), m.step)
	view := m.View()
	require.Contains(t, view, "PlanetScale organization: acme")
	require.Contains(t, view, "PlanetScale API: env:PLANETSCALE_TOKEN")
	wizardKey(m, tea.KeyEnter)
	require.True(t, m.confirmed)
	cmd, g := InitCmd{}, Globals{}
	require.NoError(t, m.copyToCommand(&cmd, &g))
	require.Equal(t, "vitess", cmd.Type)
	require.Equal(t, "acme", cmd.Organization)
	require.Equal(t, "env:PLANETSCALE_TOKEN", cmd.APIToken)
	require.Equal(t, "env:SCHEMABOT_STORAGE_DSN", cmd.StorageDSN)
}

// Other engines move straight from the application connection to the state
// connection, forward and back, and never copy PlanetScale values.
func TestInitWizardHidesPlanetScaleStepsForOtherEngines(t *testing.T) {
	t.Setenv("DATABASE_URL", "test-only")
	t.Setenv("SCHEMABOT_STORAGE_DSN", "test-only")
	m := newInitWizard(&InitCmd{Type: "mysql", Database: "shop", Namespaces: []string{"shop"}}, "default", io.Discard)
	require.Equal(t, stepDSN, m.step)
	wizardKey(m, tea.KeyEnter)
	m.Update(initConnectionMsg{generation: m.generation})
	wizardKey(m, tea.KeyEnter)
	require.Equal(t, stepStorageDSN, m.step)
	wizardKey(m, tea.KeyShiftTab)
	require.Equal(t, stepDSN, m.step)
	m.confirmed = true
	cmd := InitCmd{}
	require.NoError(t, m.copyToCommand(&cmd, &Globals{}))
	require.Equal(t, "mysql", cmd.Type)
	require.Empty(t, cmd.Organization)
	require.Empty(t, cmd.APIToken)
}

func TestInitVitessMissingInputs(t *testing.T) {
	cmd := InitCmd{Type: "vitess", NonInteractive: true}
	err := cmd.collectInputs(t.Context(), &Globals{})
	require.ErrorContains(t, err, "--organization")
	require.ErrorContains(t, err, "--api-token")
	mysql := InitCmd{Type: "mysql", NonInteractive: true}
	err = mysql.collectInputs(t.Context(), &Globals{})
	require.Error(t, err)
	require.NotContains(t, err.Error(), "organization")
}

func TestInitVitessDiscoveryUsesPlanetScaleTarget(t *testing.T) {
	t.Setenv("VT_DSN", "user:secret@tcp(vtgate:3306)/shop")
	t.Setenv("PS_TOKEN", "n:v")
	m := newInitWizard(&InitCmd{Type: "vitess", Database: "shop", Organization: "acme", DSN: "env:VT_DSN", APIToken: "env:PS_TOKEN", APIURL: "http://localscale"}, "default", io.Discard)
	m.ctx = t.Context()
	var got localsetup.Target
	m.discover = func(_ context.Context, target localsetup.Target) ([]string, error) {
		got = target
		return []string{"commerce"}, nil
	}
	msg := m.discoverNamespaces()().(initNamespacesMsg)
	require.NoError(t, msg.err)
	require.Equal(t, localsetup.Target{Engine: "vitess", DSN: "user:secret@tcp(vtgate:3306)/shop", Database: "shop", Organization: "acme", Token: "n:v", APIURL: "http://localscale"}, got)
	t.Setenv("PS_TOKEN", "")
	msg = m.discoverNamespaces()().(initNamespacesMsg)
	require.ErrorContains(t, msg.err, "PlanetScale token")
}

func TestInitStorageDialect(t *testing.T) {
	require.Equal(t, "mysql", initStorageDialect("mysql"))
	require.Equal(t, "postgres", initStorageDialect("postgres"))
	require.Equal(t, "mysql", initStorageDialect("vitess"))
}
