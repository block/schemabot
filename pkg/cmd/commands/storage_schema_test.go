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
		{
			// The refused statements were computed against the named release,
			// so a command that dropped the selector would permit destructive
			// changes while converging a different release's schema.
			name: "a named release travels with the permission",
			cmd: StorageApplyCmd{storageSchemaSourceFlags: storageSchemaSourceFlags{
				Release: "v1.5.0",
				Repo:    "example/mirror",
			}},
			rerun: "storage apply --release v1.5.0 --release-repo example/mirror --allow-unsafe",
		},
		{
			name: "a named checkout travels with the permission",
			cmd: StorageApplyCmd{storageSchemaSourceFlags: storageSchemaSourceFlags{
				SchemaDir: "./pkg/schema/mysql",
			}},
			rerun: "storage apply --schema-dir ./pkg/schema/mysql --allow-unsafe",
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

// Converging a release the answering binary does not carry is confirmed at a
// terminal, so the selectors are refused together with --auto-approve. The
// refusal is of the command form, before anything is read: an unattended run
// must not reach a database at all to find out it will not be allowed to
// converge (AV-9).
func TestBlockUnattendedNamedSchema(t *testing.T) {
	require.NoError(t, blockUnattendedNamedSchema("", true),
		"an unattended convergence of the answering binary's own schema is the pre-deploy step this command exists for")
	require.NoError(t, blockUnattendedNamedSchema("--release", false),
		"a named release is fine with a person watching; the prompt is the consent")

	for _, selector := range []string{"--release", "--schema-dir"} {
		err := blockUnattendedNamedSchema(selector, true)
		require.Error(t, err)
		assert.Contains(t, err.Error(), selector+" cannot be combined with --auto-approve")
		assert.Contains(t, err.Error(), "confirmed at a terminal")
	}
}

// The refusal reaches no database and no server. An operator's pre-deploy job
// that names a release learns that from the flags alone, and a storage
// database that is unreachable or mid-incident is not touched to tell them.
func TestStorageApplyCmd_UnattendedNamedReleaseRunsNothing(t *testing.T) {
	endpoint, routes := storageSchemaTestServer(t, &apitypes.StorageSchemaReport{
		Dialect: "mysql", Database: "schemabot", Host: "db-1.example",
	}, nil, nil)

	cmd := StorageApplyCmd{
		storageSchemaSourceFlags: storageSchemaSourceFlags{SchemaDir: storageSchemaCheckoutDir(t)},
		AutoApprove:              true,
	}
	err := cmd.Run(t.Context(), &Globals{Endpoint: endpoint, Version: "v1.4.0"})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "cannot be combined with --auto-approve")
	assert.Empty(t, *routes, "the flags are refused before anything is read")
}

// A named release's files travel with the convergence, because the answering
// binary does not carry them — and they travel with the attribution, so the
// reports name the release whose files ran rather than the binary that ran
// them.
func TestStorageApplyCmd_SendsTheNamedSchemaToConverge(t *testing.T) {
	dir := storageSchemaCheckoutDir(t)
	report := &apitypes.StorageSchemaReport{
		Dialect: "mysql", Database: "schemabot", Host: "db-1.example",
		Version:      "v1.4.0",
		SchemaSource: "the schema files in " + dir,
		Outstanding: []apitypes.StorageSchemaStatement{
			{Table: "applies", Operation: "create_table", DDL: "CREATE TABLE `applies` (`id` BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY)"},
		},
	}
	converged := &apitypes.StorageSchemaReport{
		Dialect: "mysql", Database: "schemabot", Host: "db-1.example",
		Version: "v1.4.0", SchemaSource: "the schema files in " + dir,
	}

	var applied apitypes.StorageSchemaApplyRequest
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch r.URL.Path {
		case "/api/storage/schema/plan":
			assert.NoError(t, json.NewEncoder(w).Encode(apitypes.StorageSchemaPlanResponse{Report: report}))
		case "/api/storage/schema/apply":
			assert.NoError(t, json.NewDecoder(r.Body).Decode(&applied))
			assert.NoError(t, json.NewEncoder(w).Encode(apitypes.StorageSchemaApplyResponse{
				Planned: report, Remaining: converged,
			}))
		default:
			http.Error(w, "unexpected request", http.StatusBadRequest)
		}
	}))
	t.Cleanup(server.Close)

	answerPrompt(t, "yes")
	cmd := StorageApplyCmd{storageSchemaSourceFlags: storageSchemaSourceFlags{SchemaDir: dir}}
	out := captureStdout(func() {
		require.NoError(t, cmd.Run(t.Context(), &Globals{Endpoint: server.URL, Version: "v1.4.0"}))
	})

	assert.Equal(t, "the schema files in "+dir, applied.SchemaSource,
		"the convergence is attributed to the release whose files ran")
	require.Contains(t, applied.SchemaFiles, "applies.sql",
		"the answering binary does not carry the named release's files, so they travel with the request")
	assert.Contains(t, applied.SchemaFiles["applies.sql"], "CREATE TABLE `applies`")
	assert.Contains(t, out, "This is not the schema v1.4.0 converges on boot",
		"the operator is told the deployed release will converge the difference back")
}

// A named schema has to be confirmed at a terminal, so --json cannot be paired
// with --auto-approve to get a clean stream. The two therefore have to coexist
// on one run: the person is asked on stderr and the program reading stdout
// gets the response and nothing else. Printing the human plan or the prompt to
// stdout would make `storage apply --schema-dir … --json` unparseable, which
// is the only machine-readable form a cross-release convergence has.
func TestStorageApplyCmd_NamedSchemaKeepsJSONParseable(t *testing.T) {
	dir := storageSchemaCheckoutDir(t)
	report := &apitypes.StorageSchemaReport{
		Dialect: "mysql", Database: "schemabot", Host: "db-1.example",
		Version:      "v1.4.0",
		SchemaSource: "the schema files in " + dir,
		Outstanding: []apitypes.StorageSchemaStatement{
			{Table: "applies", Operation: "create_table", DDL: "CREATE TABLE `applies` (`id` BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY)"},
		},
	}
	converged := &apitypes.StorageSchemaReport{
		Dialect: "mysql", Database: "schemabot", Host: "db-1.example",
		Version: "v1.4.0", SchemaSource: "the schema files in " + dir,
	}

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch r.URL.Path {
		case "/api/storage/schema/plan":
			assert.NoError(t, json.NewEncoder(w).Encode(apitypes.StorageSchemaPlanResponse{Report: report}))
		case "/api/storage/schema/apply":
			assert.NoError(t, json.NewEncoder(w).Encode(apitypes.StorageSchemaApplyResponse{
				Planned: report, Remaining: converged,
			}))
		default:
			http.Error(w, "unexpected request", http.StatusBadRequest)
		}
	}))
	t.Cleanup(server.Close)

	answerPrompt(t, "yes")
	cmd := StorageApplyCmd{
		storageSchemaSourceFlags: storageSchemaSourceFlags{SchemaDir: dir},
		JSON:                     true,
	}
	var out string
	stderr := captureStderr(t, func() {
		out = captureStdout(func() {
			require.NoError(t, cmd.Run(t.Context(), &Globals{Endpoint: server.URL, Version: "v1.4.0"}))
		})
	})

	var decoded apitypes.StorageSchemaApplyResponse
	require.NoError(t, json.Unmarshal([]byte(out), &decoded),
		"stdout has to parse as the convergence response on its own: %q", out)
	require.NotNil(t, decoded.Planned)
	assert.Equal(t, "the schema files in "+dir, decoded.Planned.SchemaSource)

	assert.Contains(t, stderr, "This is not the schema v1.4.0 converges on boot",
		"the operator is still told what converging another release's schema costs")
	assert.Contains(t, stderr, "Only 'yes' will be accepted")
}

// A convergence resolves its release as a convergence, not as a diff. The two
// read the same files over the same path, but only one of them runs them: a
// release read over plaintext is a report to distrust for a plan and a hazard
// to the storage database for an apply, so the apply is refused and nothing
// reaches the target.
func TestStorageApplyCmd_RefusesAReleaseReadOverPlaintext(t *testing.T) {
	t.Setenv("GITHUB_API_URL", "http://ghe.example")
	t.Setenv("GITHUB_TOKEN", "")
	t.Setenv("GH_TOKEN", "")

	var routes []string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		routes = append(routes, r.URL.Path)
		w.Header().Set("Content-Type", "application/json")
		assert.NoError(t, json.NewEncoder(w).Encode(apitypes.StorageSchemaPlanResponse{Report: &apitypes.StorageSchemaReport{
			Dialect: "mysql", Database: "schemabot", Host: "db-1.example", Version: "v1.4.0",
		}}))
	}))
	t.Cleanup(server.Close)

	cmd := StorageApplyCmd{storageSchemaSourceFlags: storageSchemaSourceFlags{Release: "v1.5.0"}}
	err := cmd.Run(t.Context(), &Globals{Endpoint: server.URL, Version: "v1.4.0"})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "refusing to converge a release read from http://ghe.example over plaintext")
	assert.NotContains(t, routes, "/api/storage/schema/apply", "nothing converges on a refused release")
}

// The notice under a cross-release convergence states the consequences an
// operator cannot see in the plan. The deployed release refuses to drop what
// it does not declare, so what was applied here survives its pods and each of
// their boots logs a refused destructive change for it — a fleet warning about
// work that was done on purpose, which needs saying before it is read as an
// incident. A release from before indexes were protected is the exception.
// What the notice actually predicts about a surplus index is pinned against a
// real convergence and boot by
// TestApplyStorageSchemaMySQL_PreAppliedIndexSurvivesTheRunningRelease.
func TestStorageSchemaConfirmation_CrossReleaseNotice(t *testing.T) {
	report := &apitypes.StorageSchemaReport{
		Dialect: "mysql", Database: "schemabot", Host: "db-1.example",
		Version:      "v1.4.0",
		SchemaSource: "the schema files of release v1.5.0 in block/schemabot",
	}

	named := storageSchemaConfirmation(report, true)
	assert.Contains(t, named, "Converging schemabot on db-1.example (mysql) to the schema files of release v1.5.0 in block/schemabot")
	assert.Contains(t, named, "This is not the schema v1.4.0 converges on boot")
	assert.Contains(t, named, "refuses to drop what it does not declare")
	assert.Contains(t, named, "logs a refused destructive change")
	assert.Contains(t, named, "indexes were protected is the exception")
	assert.Contains(t, named, "storage plan")
	assert.Contains(t, named, "Only 'yes' will be accepted")

	own := storageSchemaConfirmation(report, false)
	assert.NotContains(t, own, "This is not the schema",
		"a convergence of the answering binary's own schema has no cross-release consequence to state")
	assert.Contains(t, own, "Only 'yes' will be accepted")

	// A report with no version still states the consequences; the release it
	// names is the one that answered, which is all the operator needs to know
	// they are ahead of it.
	unversioned := storageSchemaConfirmation(&apitypes.StorageSchemaReport{
		Dialect: "mysql", Database: "schemabot", SchemaSource: "the schema files in ./pkg/schema/mysql",
	}, true)
	assert.Contains(t, unversioned, "the release answering this command")
	assert.NotContains(t, unversioned, "schema  converges", "an empty version must not leave a gap in the sentence")
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

// The storage target is resolved once per run. Every resolution of a config
// using storage.dsn_from is a fresh read of secret references, so a second one
// can answer differently than the first — and the run that resolved twice would
// be the one that converged a database its own preview never looked at.
func TestStorageSchemaTargetFlags_ResolveTheTargetOnce(t *testing.T) {
	flags := storageSchemaTargetFlags{DSN: "postgres://schemabot@db-1.example:5432/schemabot"}
	first, err := flags.target()
	require.NoError(t, err)
	assert.Equal(t, "--dsn flag", first.source)
	require.NotNil(t, flags.resolvedTarget)

	// The selectors are emptied, so a second resolution has nothing to resolve
	// from: an answer at all is the memo answering.
	flags.DSN = ""
	second, err := flags.target()
	require.NoError(t, err)
	assert.Same(t, first, second, "the target is resolved once; a second read is the same database, not another lookup")
}

// The resolution travels with the flags that named it. An apply hands its
// target flags to its own preview, and it is that hand-off — a struct copy —
// that has to carry the resolved database rather than the selectors that would
// resolve one again.
func TestStorageSchemaTargetFlags_CopyCarriesTheResolution(t *testing.T) {
	flags := storageSchemaTargetFlags{DSN: "schemabot@tcp(db-1.example:3306)/schemabot"}
	resolved, err := flags.target()
	require.NoError(t, err)

	preview := StoragePlanCmd{storageSchemaTargetFlags: flags}
	preview.DSN = ""
	carried, err := preview.target()
	require.NoError(t, err)
	assert.Same(t, resolved, carried, "the preview addresses the database the apply resolved")
}

// A refused --json convergence is reported as JSON. It is the case a program
// most needs to read, and nothing ran, so every statement the plan found is
// still outstanding and both halves of the response are the same report. A
// human plan printed instead would be unparseable text where a caller expects
// an object, and the destructive gate alone would print nothing at all.
func TestStorageApplyCmd_RefusalIsReportedAsJSON(t *testing.T) {
	tests := []struct {
		name  string
		plan  *apitypes.StorageSchemaReport
		human string
	}{
		{
			name:  "manual remediation",
			human: "Needs manual remediation",
			plan: &apitypes.StorageSchemaReport{
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
			},
		},
		{
			name:  "destructive statement",
			human: "Apply blocked",
			plan: &apitypes.StorageSchemaReport{
				Dialect:      "mysql",
				Database:     "schemabot",
				Host:         "db-1.example",
				SchemaSource: "the schema embedded in v1.4.0",
				Destructive: []apitypes.StorageSchemaStatement{
					{Table: "stale_state", Operation: "drop_table", DDL: "DROP TABLE `stale_state`", Reason: "DROP TABLE destroys data"},
				},
			},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			endpoint, routes := storageSchemaTestServer(t, tc.plan, nil, nil)

			var err error
			out := captureStdout(func() {
				cmd := StorageApplyCmd{AutoApprove: true, JSON: true}
				err = cmd.Run(t.Context(), &Globals{Endpoint: endpoint})
			})

			require.Error(t, err)
			assert.Equal(t, []string{"POST /api/storage/schema/plan"}, *routes, "a refusal converges nothing")
			assert.NotContains(t, out, tc.human, "a program reads this surface, so the refusal is the response shape")

			var response apitypes.StorageSchemaApplyResponse
			require.NoError(t, json.Unmarshal([]byte(out), &response), "the whole of stdout is the response")
			require.NotNil(t, response.Planned)
			require.NotNil(t, response.Remaining)
			assert.Equal(t, tc.plan.Manual, response.Remaining.Manual)
			assert.Equal(t, tc.plan.Destructive, response.Remaining.Destructive)
			assert.Equal(t, response.Planned, response.Remaining, "nothing ran, so what was planned is what is still outstanding")
		})
	}
}
