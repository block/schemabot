package commands

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"

	"github.com/alecthomas/kong"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/block/schemabot/pkg/api"
	"github.com/block/schemabot/pkg/apitypes"
)

// A target is either the API path or a direct connection, never a blend of the
// two. Every refusal below is a combination that would otherwise have read a
// database the operator did not name — the failure mode these flags exist to
// prevent.
func TestStorageSchemaTargetFlags_Validate(t *testing.T) {
	tests := []struct {
		name    string
		flags   storageSchemaTargetFlags
		wantErr string
	}{
		{
			name:  "no flags reads the storage of the server the CLI is pointed at",
			flags: storageSchemaTargetFlags{},
		},
		{
			name:  "deployment with its environment",
			flags: storageSchemaTargetFlags{Deployment: "west", Environment: "production"},
		},
		{
			name:  "a DSN on its own",
			flags: storageSchemaTargetFlags{DSN: "root@tcp(127.0.0.1:3306)/schemabot"},
		},
		{
			name:  "a config file on its own",
			flags: storageSchemaTargetFlags{Config: "/etc/schemabot/config.yaml"},
		},
		{
			name:    "a deployment without an environment names no single endpoint",
			flags:   storageSchemaTargetFlags{Deployment: "west"},
			wantErr: "needs -e <environment>",
		},
		{
			name:    "an environment without a deployment selects nothing",
			flags:   storageSchemaTargetFlags{Environment: "production"},
			wantErr: "without --deployment",
		},
		{
			name:    "a deployment cannot be read through a direct DSN",
			flags:   storageSchemaTargetFlags{Deployment: "west", Environment: "production", DSN: "root@tcp(127.0.0.1:3306)/schemabot"},
			wantErr: "--deployment cannot be combined with a direct connection",
		},
		{
			name:    "an environment cannot be combined with a direct DSN",
			flags:   storageSchemaTargetFlags{Environment: "production", Config: "/etc/schemabot/config.yaml"},
			wantErr: "-e cannot be combined with a direct connection",
		},
		{
			name:    "a dialect assertion has nothing to apply to through the API",
			flags:   storageSchemaTargetFlags{Dialect: "postgres"},
			wantErr: "--dialect only applies to a direct connection",
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			err := tc.flags.validate()
			if tc.wantErr == "" {
				require.NoError(t, err)
				return
			}
			require.Error(t, err)
			assert.Contains(t, err.Error(), tc.wantErr)
		})
	}
}

// A plan names the schema it compares the database against: one selector is
// required, and naming two is refused by the parser. The plan would say which
// schema it used either way; requiring the flag is what makes the operator
// decide before they read the plan rather than after.
func TestStoragePlanCmd_RequiresADesiredSchema(t *testing.T) {
	cmd := &StoragePlanCmd{
		storageSchemaTargetFlags: storageSchemaTargetFlags{DSN: "root@tcp(127.0.0.1:3306)/schemabot"},
	}
	err := cmd.Run(t.Context(), &Globals{})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "missing flags: --release=STRING or --schema-dir=STRING")

	parse := func(args ...string) error {
		var cli struct {
			Plan StoragePlanCmd `cmd:"" name:"plan"`
		}
		parser, err := kong.New(&cli, kong.Name("schemabot"))
		require.NoError(t, err)
		_, err = parser.Parse(append([]string{"plan"}, args...))
		return err
	}
	require.NoError(t, parse("--release", "v1.4.0"))
	require.NoError(t, parse("--schema-dir", "."))

	err = parse("--release", "v1.4.0", "--schema-dir", ".")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "--release and --schema-dir can't be used together")
}

// Passing a DSN source is what selects the direct path, so a command can tell
// which path it is on without inspecting what happened to resolve.
func TestStorageSchemaTargetFlags_Direct(t *testing.T) {
	assert.False(t, (&storageSchemaTargetFlags{}).direct())
	assert.False(t, (&storageSchemaTargetFlags{Deployment: "west", Environment: "production"}).direct())
	assert.True(t, (&storageSchemaTargetFlags{DSN: "root@tcp(127.0.0.1:3306)/schemabot"}).direct())
	assert.True(t, (&storageSchemaTargetFlags{Config: "/etc/schemabot/config.yaml"}).direct())

	// A malformed direct source stays on the direct path, where it is refused
	// by name. Reading it as "no DSN" would quietly report the storage of the
	// server the CLI happens to point at instead of the one addressed.
	assert.True(t, (&storageSchemaTargetFlags{DSN: "   "}).direct())
	_, err := resolveStorageTarget("   ", "", "")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "--dsn contains only whitespace")
}

// A refused destructive statement is not counted as applied, and is not a
// failure either: the surplus state stays in place on purpose.
func TestStorageSchemaConvergenceOutcome(t *testing.T) {
	require.NoError(t, storageSchemaConvergenceOutcome(
		&apitypes.StorageSchemaReport{Database: "schemabot", Dialect: "mysql", Converged: true}))
	require.NoError(t, storageSchemaConvergenceOutcome(&apitypes.StorageSchemaReport{
		Database:    "schemabot",
		Dialect:     "mysql",
		Destructive: []apitypes.StorageSchemaStatement{{Table: "checks", DDL: "DROP TABLE `checks_old`", Reason: "drops a table"}},
	}))

	// Manual remediation aborts the whole convergence, so an unattended run has
	// to fail rather than exit 0 having converged nothing.
	err := storageSchemaConvergenceOutcome(&apitypes.StorageSchemaReport{
		Database: "schemabot",
		Host:     "db-1.example",
		Dialect:  "postgres",
		Manual: []apitypes.StorageSchemaStatement{{
			Table:  "checks",
			DDL:    `ALTER TABLE "checks" ADD COLUMN "head_sha" varchar(64) NOT NULL`,
			Reason: "column is NOT NULL without a DEFAULT",
		}},
	})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "schemabot on db-1.example (postgres) was not converged")
	assert.Contains(t, err.Error(), "1 change(s) need manual remediation")
}

// storageSchemaTestServer answers the two storage schema routes with the
// reports a test hands it, and records the routes the command called. What was
// called is half of what these tests assert: a command that skips the apply is
// indistinguishable from one that ran it, if only the output is read.
func storageSchemaTestServer(t *testing.T, plan *apitypes.StorageSchemaReport, applied, remaining *apitypes.StorageSchemaReport) (endpoint string, routes *[]string) {
	t.Helper()
	called := make([]string, 0, 4)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		called = append(called, r.Method+" "+r.URL.Path)
		w.Header().Set("Content-Type", "application/json")
		switch r.URL.Path {
		case "/api/storage/schema/plan":
			assert.NoError(t, json.NewEncoder(w).Encode(apitypes.StorageSchemaPlanResponse{Report: plan}))
		case "/api/storage/schema/apply":
			assert.NoError(t, json.NewEncoder(w).Encode(apitypes.StorageSchemaApplyResponse{Planned: applied, Remaining: remaining}))
		default:
			http.Error(w, "unexpected request", http.StatusBadRequest)
		}
	}))
	t.Cleanup(server.Close)
	return server.URL, &called
}

// answerPrompt feeds a confirmation prompt its answer, so an interactive run
// can be driven from a test.
func answerPrompt(t *testing.T, answer string) {
	t.Helper()
	path := filepath.Join(t.TempDir(), "confirmation")
	require.NoError(t, os.WriteFile(path, []byte(answer+"\n"), 0o600))
	input, err := os.Open(path)
	require.NoError(t, err)
	original := os.Stdin
	os.Stdin = input
	t.Cleanup(func() {
		os.Stdin = original
		require.NoError(t, input.Close())
	})
}

// storageSchemaCheckoutDir writes a checkout of a release's schema files, which
// is what --schema-dir reads, so a diff needs no network to name its desired
// side.
func storageSchemaCheckoutDir(t *testing.T) string {
	t.Helper()
	dir := t.TempDir()
	require.NoError(t, os.WriteFile(filepath.Join(dir, "applies.sql"),
		[]byte("CREATE TABLE `applies` (`id` BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY)"), 0o600))
	return dir
}

// The exit status is the machine-readable half of a diff's answer. A converged
// storage exits 0 and an outstanding one exits 2, and nothing is printed under
// the report to explain the status: 2 is not a failure, so an "Error:" line
// below a report that reads correctly would misrepresent it.
func TestStoragePlanCmd_ExitStatusSaysWhetherWorkIsOutstanding(t *testing.T) {
	dir := storageSchemaCheckoutDir(t)

	outstanding := &apitypes.StorageSchemaReport{
		Dialect:      "mysql",
		Database:     "schemabot",
		Host:         "db-1.example",
		SchemaSource: "the schema files in " + dir,
		Outstanding: []apitypes.StorageSchemaStatement{
			{Table: "applies", Operation: "alter_table", DDL: "ALTER TABLE `applies` ADD COLUMN `caller` varchar(255) NOT NULL DEFAULT ''"},
		},
	}
	endpoint, routes := storageSchemaTestServer(t, outstanding, nil, nil)

	var err error
	out := captureStdout(func() {
		cmd := StoragePlanCmd{storageSchemaSourceFlags: storageSchemaSourceFlags{SchemaDir: dir}}
		err = cmd.Run(t.Context(), &Globals{Endpoint: endpoint})
	})
	require.Error(t, err)
	assert.Equal(t, ExitStorageSchemaOutstanding, ExitCodeFor(err))
	assert.ErrorIs(t, err, ErrSilent, "the report is the answer; an error line under it would read as a failed read")
	assert.Contains(t, stripAnsi(out), "~ applies")
	assert.Equal(t, []string{"POST /api/storage/schema/plan"}, *routes)

	converged := &apitypes.StorageSchemaReport{
		Dialect:      "mysql",
		Database:     "schemabot",
		Host:         "db-1.example",
		SchemaSource: "the schema files in " + dir,
		Converged:    true,
	}
	cleanEndpoint, _ := storageSchemaTestServer(t, converged, nil, nil)
	out = captureStdout(func() {
		cmd := StoragePlanCmd{storageSchemaSourceFlags: storageSchemaSourceFlags{SchemaDir: dir}}
		err = cmd.Run(t.Context(), &Globals{Endpoint: cleanEndpoint})
	})
	require.NoError(t, err, "a converged storage is status 0, which is what a pre-deploy gate reads")
	assert.Contains(t, out, "No schema changes detected")
}

// An interactive apply converges what --auto-approve would, including when the
// preview found the catalog already matching. The bootstrap also clears the
// schema change engine's leftover tables, which a catalog diff cannot see, so
// stopping at a converged preview would leave them on the database and make the
// command an operator reaches for mid-incident do less than the unattended one.
func TestStorageApplyCmd_ConvergesAConvergedCatalogToo(t *testing.T) {
	converged := &apitypes.StorageSchemaReport{
		Dialect:      "postgres",
		Database:     "schemabot",
		Host:         "db-1.example",
		SchemaSource: "the schema embedded in v1.4.0",
		Converged:    true,
	}
	endpoint, routes := storageSchemaTestServer(t, converged, converged, converged)
	answerPrompt(t, "yes")

	var err error
	out := captureStdout(func() {
		cmd := StorageApplyCmd{}
		err = cmd.Run(t.Context(), &Globals{Endpoint: endpoint})
	})
	require.NoError(t, err)
	assert.Contains(t, out, "already matches", "the prompt asks for the run it is about to do, not for statements there are none of")
	assert.Equal(t, []string{"POST /api/storage/schema/plan", "POST /api/storage/schema/apply"}, *routes,
		"the convergence runs; a converged catalog is not a reason to skip the bootstrap")
	assert.Contains(t, out, "Nothing is outstanding.")
}

// A declined confirmation converges nothing at all. The preview is read-only,
// so the command has to leave the database exactly as it found it.
func TestStorageApplyCmd_DeclinedConfirmationRunsNothing(t *testing.T) {
	plan := &apitypes.StorageSchemaReport{
		Dialect:      "mysql",
		Database:     "schemabot",
		Host:         "db-1.example",
		SchemaSource: "the schema embedded in v1.4.0",
		Outstanding: []apitypes.StorageSchemaStatement{
			{Table: "applies", Operation: "alter_table", DDL: "ALTER TABLE `applies` ADD COLUMN `caller` varchar(255) NOT NULL DEFAULT ''"},
		},
	}
	endpoint, routes := storageSchemaTestServer(t, plan, nil, nil)
	answerPrompt(t, "no")

	var err error
	out := captureStdout(func() {
		cmd := StorageApplyCmd{}
		err = cmd.Run(t.Context(), &Globals{Endpoint: endpoint})
	})
	require.NoError(t, err)
	assert.Contains(t, out, "Apply cancelled.")
	assert.Equal(t, []string{"POST /api/storage/schema/plan"}, *routes)
}

// A preview carrying a manual-remediation entry refuses before the prompt.
// Nothing would converge anyway — the bootstrap refuses the whole drift set
// while one is outstanding — so asking for consent to a run that cannot happen
// would be asking the operator to approve nothing.
func TestStorageApplyCmd_ManualRemediationRefusesBeforeThePrompt(t *testing.T) {
	plan := &apitypes.StorageSchemaReport{
		Dialect:      "postgres",
		Database:     "schemabot",
		Host:         "db-1.example",
		SchemaSource: "the schema embedded in v1.4.0",
		Manual: []apitypes.StorageSchemaStatement{{
			Table:     "checks",
			Operation: "add_column",
			DDL:       `ALTER TABLE "checks" ADD COLUMN "head_sha" varchar(64) NOT NULL`,
			Reason:    "definition is NOT NULL without a DEFAULT",
		}},
	}
	endpoint, routes := storageSchemaTestServer(t, plan, nil, nil)

	var err error
	out := captureStdout(func() {
		cmd := StorageApplyCmd{}
		err = cmd.Run(t.Context(), &Globals{Endpoint: endpoint})
	})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "need manual remediation first")
	assert.NotContains(t, out, "Only 'yes' will be accepted")
	assert.Equal(t, []string{"POST /api/storage/schema/plan"}, *routes)
}

// Both halves of a direct convergence name the release that ran it, the same
// way the preview named it. Stamping only the version would leave the result
// header carrying the placeholder the preview had already expanded, so one run
// would describe its own schema two ways.
func TestAttributeStorageSchemaConvergence(t *testing.T) {
	planned := &api.StorageSchemaReport{Outstanding: []api.StorageSchemaStatement{{Table: "applies"}}}
	remaining := &api.StorageSchemaReport{}

	attributeStorageSchemaConvergence("v1.4.0", planned, remaining)

	assert.Equal(t, "v1.4.0", planned.Version)
	assert.Equal(t, "the schema embedded in v1.4.0", planned.SchemaSource)
	assert.Equal(t, "v1.4.0", remaining.Version)
	assert.Equal(t, "the schema embedded in v1.4.0", remaining.SchemaSource,
		"the half an operator reads last describes the same schema as the half they read first")
}

// An error names the storage that was being read. A failure that said only
// that a read failed would leave an operator unable to tell whether they had
// reached the deployment they asked for.
func TestStorageSchemaTargetSuffix(t *testing.T) {
	assert.Empty(t, storageSchemaTargetSuffix("", ""))
	assert.Equal(t, " for deployment west in production", storageSchemaTargetSuffix("west", "production"))
}
