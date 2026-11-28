package main

import (
	"bytes"
	"errors"
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"io"
	"os"
	"testing"

	"github.com/alecthomas/kong"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/block/schemabot/pkg/cmd/client"
	"github.com/block/schemabot/pkg/cmd/commands"
	"github.com/block/schemabot/pkg/cmd/internal/templates"
)

func TestRollbackRequiresEnvironmentFlag(t *testing.T) {
	var cli CLI
	parser, err := kong.New(&cli,
		kong.Name("schemabot"),
		kong.Writers(io.Discard, io.Discard),
		kong.Vars{"cli_name": "schemabot"},
	)
	require.NoError(t, err)

	_, err = parser.Parse([]string{"rollback", "apply_abc123", "--auto-approve"})

	require.Error(t, err)
	assert.Contains(t, err.Error(), "-e")
}

func TestStorageResyncIdentitySequencesIsInvocable(t *testing.T) {
	var cli CLI
	parser, err := kong.New(&cli,
		kong.Name("schemabot"),
		kong.Writers(io.Discard, io.Discard),
		kong.Vars{"cli_name": "schemabot"},
	)
	require.NoError(t, err)

	_, err = parser.Parse([]string{"storage", "resync-identity-sequences", "--dsn", "postgres://user@localhost:5432/db"})
	require.NoError(t, err)
}

// A storage schema command routed through the API needs an endpoint, and with
// a local runtime selected that endpoint is one the CLI has to start first.
// The direct path needs none: it exists for when no server is up, so starting
// a runtime for it would be exactly the work the operator reached for --dsn to
// avoid. The maintenance commands never call the API at all.
func TestUsesLocalRuntime_StorageSubcommands(t *testing.T) {
	tests := []struct {
		name string
		args []string
		want bool
	}{
		{name: "plan through the API", args: []string{"storage", "plan", "--release", "v1.4.0"}, want: true},
		{name: "plan of a data plane's storage", args: []string{"storage", "plan", "--release", "v1.4.0", "--deployment", "west", "-e", "production"}, want: true},
		{name: "apply through the API", args: []string{"storage", "apply"}, want: true},
		{name: "plan against a DSN", args: []string{"storage", "plan", "--release", "v1.4.0", "--dsn", "postgres://user@localhost:5432/db"}, want: false},
		{name: "apply against a server config", args: []string{"storage", "apply", "--config", "/etc/schemabot/config.yaml"}, want: false},
		{name: "a maintenance command", args: []string{"storage", "resync-identity-sequences", "--dsn", "postgres://user@localhost:5432/db"}, want: false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			var cli CLI
			parser, err := kong.New(&cli,
				kong.Name("schemabot"),
				kong.Writers(io.Discard, io.Discard),
				kong.Vars{"cli_name": "schemabot"},
			)
			require.NoError(t, err)

			ctx, err := parser.Parse(tc.args)
			require.NoError(t, err)
			assert.Equal(t, tc.want, usesLocalRuntime(ctx.Command(), &cli))
		})
	}

	// The commands that already resolved an endpoint keep doing so.
	var cli CLI
	parser, err := kong.New(&cli,
		kong.Name("schemabot"),
		kong.Writers(io.Discard, io.Discard),
		kong.Vars{"cli_name": "schemabot"},
	)
	require.NoError(t, err)
	ctx, err := parser.Parse([]string{"status", "apply_abc123"})
	require.NoError(t, err)
	assert.True(t, usesLocalRuntime(ctx.Command(), &cli))
}

func TestInitWithoutFlagsReachesWizard(t *testing.T) {
	var cli CLI
	parser, err := kong.New(&cli, kong.Writers(io.Discard, io.Discard), kong.Vars{"cli_name": "schemabot"})
	require.NoError(t, err)
	_, err = parser.Parse([]string{"init"})
	require.NoError(t, err)
}

// main prints the error a command returns, and that error can quote text the
// server relayed from an engine or a database. Escape sequences in it are
// removed where it is printed, while the line break of a multi-line error stays.
// The label and the color around the line are main's own and stay.
func TestErrorLineDropsTerminalEscapes(t *testing.T) {
	err := errors.New("cutover failed: lock wait timeout\x1b[2J\rcompleted\n  retry the apply")

	assert.Equal(t, "Error: cutover failed: lock wait timeout[2Jcompleted\n  retry the apply\n",
		errorLine("", "Error", err))
	assert.Equal(t, "\x1b[31mError: cutover failed: lock wait timeout[2Jcompleted\n  retry the apply\x1b[0m\n",
		errorLine(templates.ANSIRed, "Error", err))
	assert.Equal(t, "\x1b[33mWarning: cutover failed: lock wait timeout[2Jcompleted\n  retry the apply\x1b[0m\n",
		errorLine(templates.ANSIYellow, "Warning", err))
}

// errorLine only protects the terminal if main uses it every time it reports
// an error, and main cannot be run by a test because it exits the process. So
// this reads main.go and requires every write to stderr to be
// fmt.Fprint(os.Stderr, errorLine(...)): a line added later that prints an
// error as it arrived fails here.
func TestMainWritesToStderrOnlyThroughErrorLine(t *testing.T) {
	fileSet := token.NewFileSet()
	file, err := parser.ParseFile(fileSet, "main.go", nil, 0)
	require.NoError(t, err, "parse main.go")

	isStderr := func(expr ast.Expr) bool {
		selector, ok := expr.(*ast.SelectorExpr)
		if !ok || selector.Sel.Name != "Stderr" {
			return false
		}
		pkg, ok := selector.X.(*ast.Ident)
		return ok && pkg.Name == "os"
	}

	var stderrUses, errorLineWrites int
	ast.Inspect(file, func(node ast.Node) bool {
		if expr, ok := node.(ast.Expr); ok && isStderr(expr) {
			stderrUses++
		}
		call, ok := node.(*ast.CallExpr)
		if !ok || len(call.Args) == 0 || !isStderr(call.Args[0]) {
			return true
		}
		where := fileSet.Position(call.Pos())
		fn, ok := call.Fun.(*ast.SelectorExpr)
		if !assert.True(t, ok, "%s: stderr is written by something other than fmt.Fprint", where) {
			return true
		}
		pkg, ok := fn.X.(*ast.Ident)
		if !assert.True(t, ok && pkg.Name == "fmt" && fn.Sel.Name == "Fprint",
			"%s: stderr is written by something other than fmt.Fprint", where) {
			return true
		}
		if !assert.Len(t, call.Args, 2, "%s: fmt.Fprint(os.Stderr, ...) takes the one line errorLine built", where) {
			return true
		}
		line, ok := call.Args[1].(*ast.CallExpr)
		if !assert.True(t, ok, "%s: the line written to stderr is not built by errorLine", where) {
			return true
		}
		builder, ok := line.Fun.(*ast.Ident)
		if assert.True(t, ok && builder.Name == "errorLine", "%s: the line written to stderr is not built by errorLine", where) {
			errorLineWrites++
		}
		return true
	})

	assert.Positive(t, errorLineWrites, "main.go reports errors on stderr")
	assert.Equal(t, errorLineWrites, stderrUses, "os.Stderr is used somewhere other than as the destination of an errorLine write")
}

// The freeze commands call the schema API, so a selected local runtime starts
// for them, including the bare `freeze` that creates a freeze.
func TestUsesLocalRuntime_FreezeCommands(t *testing.T) {
	for _, args := range [][]string{
		{"freeze", "-e", "production", "--reason", "SEV-1234"},
		{"freeze", "list"},
		{"freeze", "approve", "ovr-123"},
		{"unfreeze", "freeze-abc"},
	} {
		var cli CLI
		parser, err := kong.New(&cli,
			kong.Name("schemabot"),
			kong.Writers(io.Discard, io.Discard),
			kong.Vars{"cli_name": "schemabot"},
		)
		require.NoError(t, err)
		ctx, err := parser.Parse(args)
		require.NoError(t, err, args)
		assert.True(t, usesLocalRuntime(ctx.Command(), &cli), args)
	}
}

// A command asked for JSON output that fails writes its error to stdout as a
// JSON object, so a script parses the failure instead of meeting text where it
// expects JSON. The code is the API's when the server gave one, the command's
// own for an absence it decided, and absent otherwise; the message is cleaned
// of terminal escapes as errorLine cleans it.
func TestWriteJSONErrorNamesTheCodeAndCleansTheMessage(t *testing.T) {
	for name, tc := range map[string]struct {
		err  error
		want string
	}{
		"absence the command decided": {
			err:  &commands.NotFoundError{Message: `no schema change found for apply "apply-1" on this server`},
			want: `{"error":{"code":"not_found","message":"no schema change found for apply \"apply-1\" on this server"}}`,
		},
		"server error code": {
			err:  fmt.Errorf("get logs: %w", &client.APIError{Status: 404, ErrorCode: "not_found", Message: "apply not found"}),
			want: `{"error":{"code":"not_found","message":"get logs: apply not found"}}`,
		},
		"plain error": {
			err:  errors.New("dial tcp: connection refused\x1b[2J"),
			want: `{"error":{"message":"dial tcp: connection refused[2J"}}`,
		},
	} {
		t.Run(name, func(t *testing.T) {
			var out bytes.Buffer
			writeJSONError(&out, tc.err)
			assert.JSONEq(t, tc.want, out.String())
		})
	}
}

// Only a command run with --json reports its failure as JSON.
func TestJSONOutputRequestedReadsTheCommandsJSONFlag(t *testing.T) {
	for name, tc := range map[string]struct {
		args []string
		want bool
	}{
		"status with --json":    {args: []string{"status", "apply-1", "--json"}, want: true},
		"status without --json": {args: []string{"status", "apply-1"}, want: false},
		"logs with --json":      {args: []string{"logs", "apply-1", "--json"}, want: true},
	} {
		t.Run(name, func(t *testing.T) {
			var cli CLI
			parser, err := kong.New(&cli, kong.Writers(io.Discard, io.Discard), kong.Vars{"cli_name": "schemabot"})
			require.NoError(t, err)
			ctx, err := parser.Parse(tc.args)
			require.NoError(t, err)
			assert.Equal(t, tc.want, jsonOutputRequested(ctx))
		})
	}
}

// A failure main reports for a --json command, including one from startup
// before the command runs, such as a bearer token that cannot be resolved, is
// a single JSON document on stdout. A command that already wrote its response
// with the failure in it, such as a refused storage apply or a checks backfill
// that needs attention, returns its error as already reported, and main adds
// nothing after that response.
func TestReportFailureWritesOneJSONDocumentForAJSONCommand(t *testing.T) {
	t.Run("startup failure", func(t *testing.T) {
		var out bytes.Buffer
		reportFailure(&out, true, templates.ANSIRed, errors.New("no token configured for profile \"missing\"; run schemabot login"))
		assert.JSONEq(t, `{"error":{"message":"no token configured for profile \"missing\"; run schemabot login"}}`, out.String())
	})
	t.Run("already reported", func(t *testing.T) {
		var out bytes.Buffer
		stderr := captureMainStderr(t, func() {
			reportFailure(&out, true, "", fmt.Errorf("%w: %w", commands.ErrSilent, errors.New("refusing to converge storage schema")))
		})
		assert.Empty(t, out.String(), "the command's own response is the whole of stdout")
		assert.Empty(t, stderr, "a silent error was already shown in full")
	})
	t.Run("carried in the response", func(t *testing.T) {
		var out bytes.Buffer
		stderr := captureMainStderr(t, func() {
			reportFailure(&out, true, "", inJSONResponseError{errors.New("refusing to converge storage schema")})
		})
		assert.Empty(t, out.String(), "the command's own response is the whole of stdout")
		assert.Equal(t, "Error: refusing to converge storage schema\n", stderr, "the person at the terminal still reads why")
	})
}

// inJSONResponseError stands in for an error a command returns after writing
// it inside its --json response.
type inJSONResponseError struct{ err error }

func (e inJSONResponseError) Error() string        { return e.err.Error() }
func (e inJSONResponseError) Unwrap() []error      { return []error{commands.ErrSilent, e.err} }
func (e inJSONResponseError) InJSONResponse() bool { return true }

// captureMainStderr returns what fn writes to os.Stderr.
func captureMainStderr(t *testing.T, fn func()) string {
	t.Helper()
	read, write, err := os.Pipe()
	require.NoError(t, err)
	original := os.Stderr
	os.Stderr = write
	fn()
	os.Stderr = original
	require.NoError(t, write.Close())
	captured, err := io.ReadAll(read)
	require.NoError(t, err)
	require.NoError(t, read.Close())
	return string(captured)
}
