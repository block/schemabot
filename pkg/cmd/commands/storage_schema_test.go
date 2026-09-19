package commands

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
	"time"

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

// Destructive storage statements are permitted with --allow-unsafe, the flag
// the rest of the CLI already uses for destructive changes, and the convergence
// is the only command that takes it. The plan discloses those statements and
// names the flag; seeing them as statements that will run means running the
// apply, which is where the normal plan/apply flow puts the same question.
func TestStorageSchemaCommands_PermitDestructiveStatementsWithAllowUnsafe(t *testing.T) {
	parse := func(command string, target any, args ...string) error {
		parser, err := kong.New(target, kong.Name("schemabot"))
		require.NoError(t, err)
		_, err = parser.Parse(append([]string{command}, args...))
		return err
	}

	var applyCLI struct {
		Apply StorageApplyCmd `cmd:"" name:"apply"`
	}
	require.NoError(t, parse("apply", &applyCLI, "--dsn", "postgres://localhost/schemabot", "--allow-unsafe"))
	assert.True(t, applyCLI.Apply.AllowUnsafe, "the flag has to reach the convergence that acts on it")

	var planCLI struct {
		Plan StoragePlanCmd `cmd:"" name:"plan"`
	}
	err := parse("plan", &planCLI, "--schema-dir", ".", "--allow-unsafe")
	require.Error(t, err, "the plan runs nothing, so there is no consent for it to take")
	assert.Contains(t, err.Error(), "unknown flag --allow-unsafe")
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
	var discarded apitypes.StorageSchemaApplyRequest
	return storageSchemaTestServerRecording(t, plan, applied, remaining, &discarded)
}

// storageSchemaTestServerRecording is storageSchemaTestServer for a test that
// also asserts on what the convergence request carried, rather than only on
// which routes were called.
func storageSchemaTestServerRecording(t *testing.T, plan *apitypes.StorageSchemaReport, applied, remaining *apitypes.StorageSchemaReport, lastApplyRequest *apitypes.StorageSchemaApplyRequest) (endpoint string, routes *[]string) {
	t.Helper()
	called := make([]string, 0, 4)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		called = append(called, r.Method+" "+r.URL.Path)
		w.Header().Set("Content-Type", "application/json")
		switch r.URL.Path {
		case "/api/storage/schema/plan":
			assert.NoError(t, json.NewEncoder(w).Encode(apitypes.StorageSchemaPlanResponse{Report: plan}))
		case "/api/storage/schema/apply":
			assert.NoError(t, json.NewDecoder(r.Body).Decode(lastApplyRequest))
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

// An operator's --timeout reaches the target as the budget it bounds the
// convergence with, and an unset flag names the operator default on the wire
// rather than sending nothing. Both ends then agree on the ceiling by
// construction: a target handed no budget runs the convergence under the one a
// booting pod gets, which is not the wait the command told the operator it
// would hold for. This is what lets a convergence outlive the boot budget: the
// ceiling is the caller's to name, on every path.
func TestStorageApplyCmd_CarriesTheOperatorBudgetToTheTarget(t *testing.T) {
	converged := &apitypes.StorageSchemaReport{
		Dialect:      "mysql",
		Database:     "schemabot",
		Host:         "db-1.example",
		SchemaSource: "the schema embedded in v1.4.0",
		Converged:    true,
	}

	var chosen apitypes.StorageSchemaApplyRequest
	endpoint, _ := storageSchemaTestServerRecording(t, converged, converged, converged, &chosen)
	captureStdout(func() {
		cmd := StorageApplyCmd{AutoApprove: true, Timeout: 20 * time.Minute}
		require.NoError(t, cmd.Run(t.Context(), &Globals{Endpoint: endpoint}))
	})
	assert.Equal(t, int64(1200), chosen.TimeoutSeconds)

	var unset apitypes.StorageSchemaApplyRequest
	defaultEndpoint, _ := storageSchemaTestServerRecording(t, converged, converged, converged, &unset)
	captureStdout(func() {
		cmd := StorageApplyCmd{AutoApprove: true}
		require.NoError(t, cmd.Run(t.Context(), &Globals{Endpoint: defaultEndpoint}))
	})
	assert.Equal(t, int64(apitypes.DefaultStorageApplyTimeout/time.Second), unset.TimeoutSeconds,
		"an unset flag names the operator default on the wire so the target runs under the wait the client holds for")
}

// A budget beyond what the target will accept is refused at the CLI, before a
// convergence starts, rather than after one has run under a ceiling the
// operator did not choose.
func TestStorageApplyCmd_RefusesABudgetAboveTheMaximum(t *testing.T) {
	cmd := StorageApplyCmd{AutoApprove: true, Timeout: apitypes.MaxStorageApplyTimeout + time.Minute}
	err := cmd.Run(t.Context(), &Globals{Endpoint: "http://127.0.0.1:1"})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "exceeds the maximum")
}

// A budget the wire's whole seconds cannot carry is refused rather than
// truncated. Truncation would turn the shortest budget an operator can name
// into the zero that means "no preference", and hand back the hour-long
// default — the opposite of what they asked for, with nothing saying so.
func TestStorageApplyCmd_RefusesABudgetTheWireCannotCarry(t *testing.T) {
	cmd := StorageApplyCmd{AutoApprove: true, Timeout: 500 * time.Millisecond}
	err := cmd.Run(t.Context(), &Globals{Endpoint: "http://127.0.0.1:1"})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "shorter than the one second the request carries")
}

// A budget carrying a fraction of a second is refused rather than rounded.
// The request carries whole seconds, so the remainder is dropped on the way
// out — and a budget just over the maximum loses exactly the part that put it
// over, arriving as one the target accepts. Refusing the fraction is what
// keeps the maximum a refusal rather than a clamp.
func TestStorageApplyCmd_RefusesAFractionalBudget(t *testing.T) {
	for name, tc := range map[string]struct {
		timeout time.Duration
		ran     string
	}{
		"a fraction the target would accept": {timeout: 1500 * time.Millisecond, ran: "1s"},
		"a fraction that hides the maximum":  {timeout: time.Hour + 500*time.Millisecond, ran: "1h0m0s"},
		"a fraction on a many-second budget": {timeout: 90*time.Second + time.Millisecond, ran: "1m30s"},
	} {
		t.Run(name, func(t *testing.T) {
			cmd := StorageApplyCmd{AutoApprove: true, Timeout: tc.timeout}
			err := cmd.Run(t.Context(), &Globals{Endpoint: "http://127.0.0.1:1"})
			require.Error(t, err)
			assert.Contains(t, err.Error(), "not the whole number of seconds the request carries")
			assert.Contains(t, err.Error(), tc.ran, "the refusal names the budget the truncation would have run")
		})
	}
}

// A negative budget is refused whatever its magnitude. One smaller than a
// whole second truncates to the zero that means "no preference", so a bound
// applied only after the truncation would answer an invalid flag with the
// hour-long default instead of an error.
func TestStorageApplyCmd_RefusesANegativeBudget(t *testing.T) {
	for name, timeout := range map[string]time.Duration{
		"smaller than the wire's resolution": -500 * time.Millisecond,
		"a whole number of seconds":          -30 * time.Second,
	} {
		t.Run(name, func(t *testing.T) {
			cmd := StorageApplyCmd{AutoApprove: true, Timeout: timeout}
			err := cmd.Run(t.Context(), &Globals{Endpoint: "http://127.0.0.1:1"})
			require.Error(t, err)
			assert.Contains(t, err.Error(), "must be positive")
		})
	}
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

// A destructive statement nothing has permitted stops the convergence before it
// runs, the way `apply` stops a destructive schema change: the plan is shown,
// the statement is named, --allow-unsafe is named as the way through, and
// nothing converges. The gate is in front of --auto-approve too, because
// skipping the prompt is not consenting to destroy storage state.
func TestStorageApplyCmd_DestructiveStatementsBlockTheConvergence(t *testing.T) {
	plan := &apitypes.StorageSchemaReport{
		Dialect:      "mysql",
		Database:     "schemabot",
		Host:         "db-1.example",
		SchemaSource: "the schema embedded in v1.4.0",
		Outstanding: []apitypes.StorageSchemaStatement{
			{Table: "applies", Operation: "alter_table", DDL: "ALTER TABLE `applies` ADD COLUMN `caller` varchar(255) NOT NULL DEFAULT ''"},
		},
		Destructive: []apitypes.StorageSchemaStatement{
			{Table: "stale_state", Operation: "drop_table", DDL: "DROP TABLE `stale_state`", Reason: "DROP TABLE destroys data"},
		},
	}

	for _, tc := range []struct {
		name        string
		autoApprove bool
	}{
		{name: "attended"},
		{name: "unattended", autoApprove: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			endpoint, routes := storageSchemaTestServer(t, plan, nil, nil)
			// An answer is staged for the attended run to prove the prompt is
			// never reached: a gate that asked first and blocked afterwards
			// would consume this and still pass an output assertion.
			answerPrompt(t, "yes")

			var err error
			out := captureStdout(func() {
				cmd := StorageApplyCmd{AutoApprove: tc.autoApprove}
				err = cmd.Run(t.Context(), &Globals{Endpoint: endpoint})
			})

			require.Error(t, err)
			assert.ErrorIs(t, err, ErrSilent, "the plan carries the refusal, so an error line under it would repeat it")
			assert.Equal(t, []string{"POST /api/storage/schema/plan"}, *routes,
				"the convergence must not run; the safe remainder is not a reason to proceed past a refused DROP")
			assert.Contains(t, out, "Apply blocked: 1 unsafe change(s) detected",
				"the same heading a blocked schema change apply prints")
			assert.Contains(t, out, "schemabot storage apply --allow-unsafe",
				"the refusal names the command that permits what it refused")
			assert.Contains(t, out, "1. stale_state: DROP TABLE destroys data")
			assert.NotContains(t, out, "Only 'yes' will be accepted",
				"consent to a convergence is not consent to destroy state, so the gate is in front of the prompt")
		})
	}
}

// A manual-remediation entry is the refusal an operator is told about, even
// when destructive statements are outstanding too. It blocks the whole drift
// set, so the destructive ones are not yet reachable, and offering consent for
// them would name a flag that permits a DROP and converges nothing.
func TestStorageApplyCmd_ManualRemediationOutranksTheDestructiveRefusal(t *testing.T) {
	plan := &apitypes.StorageSchemaReport{
		Dialect:      "mysql",
		Database:     "schemabot",
		Host:         "db-1.example",
		SchemaSource: "the schema embedded in v1.4.0",
		Destructive: []apitypes.StorageSchemaStatement{
			{Table: "stale_state", Operation: "drop_table", DDL: "DROP TABLE `stale_state`", Reason: "DROP TABLE destroys data"},
		},
		Manual: []apitypes.StorageSchemaStatement{{
			Table:     "checks",
			Operation: "add_column",
			DDL:       "ALTER TABLE `checks` ADD COLUMN `head_sha` varchar(64) NOT NULL",
			Reason:    "definition is NOT NULL without a DEFAULT",
		}},
	}
	for _, unattended := range []bool{false, true} {
		name := "attended"
		if unattended {
			name = "unattended"
		}
		t.Run(name, func(t *testing.T) {
			endpoint, routes := storageSchemaTestServer(t, plan, nil, nil)

			var err error
			out := captureStdout(func() {
				cmd := StorageApplyCmd{AutoApprove: unattended}
				err = cmd.Run(t.Context(), &Globals{Endpoint: endpoint})
			})

			require.Error(t, err)
			assert.Contains(t, err.Error(), "need manual remediation first",
				"the refusal an operator can act on is named, not left to an exit status")
			assert.Contains(t, out, "Needs manual remediation",
				"the error says these are listed above, so they are listed on both paths")
			assert.Contains(t, out, "checks: definition is NOT NULL without a DEFAULT",
				"the entry names the table and what has to be resolved by hand")
			assert.NotContains(t, out, "Apply blocked",
				"a manual entry makes the destructive statement unreachable, so refusing it separately would name the wrong remedy")
			assert.Equal(t, []string{"POST /api/storage/schema/plan"}, *routes, "nothing converges either way")
		})
	}
}

// The command a refusal offers addresses the same storage database the refusal
// is about, so it carries the target flags forward. It never carries the DSN:
// that is a credential, and the refusal is printed to a terminal.
func TestStorageApplyCmd_RerunCommandAddressesTheSameTarget(t *testing.T) {
	tests := []struct {
		name  string
		cmd   StorageApplyCmd
		rerun string
	}{
		{
			name:  "the server's own storage",
			rerun: "storage apply --allow-unsafe",
		},
		{
			name:  "a data plane's storage",
			cmd:   StorageApplyCmd{storageSchemaTargetFlags: storageSchemaTargetFlags{Deployment: "shard-a", Environment: "production"}},
			rerun: "storage apply --deployment shard-a -e production --allow-unsafe",
		},
		{
			name:  "resolved from a config file",
			cmd:   StorageApplyCmd{storageSchemaTargetFlags: storageSchemaTargetFlags{Config: "/etc/schemabot/config.yaml"}},
			rerun: "storage apply --config /etc/schemabot/config.yaml --allow-unsafe",
		},
		{
			name: "a DSN is named, not repeated",
			cmd: StorageApplyCmd{storageSchemaTargetFlags: storageSchemaTargetFlags{
				DSN:     "postgres://schemabot:hunter2@db-1.example:5432/schemabot",
				Dialect: "postgres",
			}},
			rerun: "storage apply --dsn <the same DSN> --dialect postgres --allow-unsafe",
		},
		{
			name: "a chosen budget is carried over",
			cmd: StorageApplyCmd{
				storageSchemaTargetFlags: storageSchemaTargetFlags{Deployment: "shard-a", Environment: "production"},
				Timeout:                  10 * time.Minute,
			},
			rerun: "storage apply --deployment shard-a -e production --timeout 10m0s --allow-unsafe",
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			rerun := tc.cmd.rerunWithAllowUnsafe()
			assert.Equal(t, tc.rerun, rerun)
			assert.NotContains(t, rerun, "hunter2", "a suggested command must not print the storage credentials")
		})
	}
}

// Destructive statements the target has permitted are not gated: the report
// says they will run, and blocking them would narrow a standing storage policy
// the CLI has no business narrowing (AV-9).
func TestStorageApplyCmd_PermittedDestructiveStatementsConverge(t *testing.T) {
	permitted := &apitypes.StorageSchemaReport{
		Dialect:      "mysql",
		Database:     "schemabot",
		Host:         "db-1.example",
		SchemaSource: "the schema embedded in v1.4.0",
		Destructive: []apitypes.StorageSchemaStatement{
			{Table: "stale_state", Operation: "drop_table", DDL: "DROP TABLE `stale_state`", Reason: "DROP TABLE destroys data"},
		},
		DestructiveAllowed: true,
	}
	converged := &apitypes.StorageSchemaReport{
		Dialect:      "mysql",
		Database:     "schemabot",
		Host:         "db-1.example",
		SchemaSource: "the schema embedded in v1.4.0",
		Converged:    true,
	}
	endpoint, routes := storageSchemaTestServer(t, permitted, permitted, converged)

	var err error
	out := captureStdout(func() {
		cmd := StorageApplyCmd{AllowUnsafe: true, AutoApprove: true}
		err = cmd.Run(t.Context(), &Globals{Endpoint: endpoint})
	})
	require.NoError(t, err)
	assert.Equal(t, []string{"POST /api/storage/schema/plan", "POST /api/storage/schema/apply"}, *routes)
	assert.Contains(t, out, "Nothing is outstanding.")
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
