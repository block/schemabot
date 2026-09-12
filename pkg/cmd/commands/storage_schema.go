package commands

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"os"
	"strings"

	"github.com/block/schemabot/pkg/api"
	"github.com/block/schemabot/pkg/apitypes"
	cmdclient "github.com/block/schemabot/pkg/cmd/client"
	"github.com/block/schemabot/pkg/schema"
)

// Storage schema commands answer, and then close, the one question a deploy
// that did not converge leaves open: which storage DDL is still outstanding on
// this instance's own storage database.
//
// Both read the live database, always. A release names the schema to compare
// against (--release, --schema-dir), and it never stands in for the live side:
// what a release would converge to and what the storage actually converged to
// differ exactly when a deploy has failed, which is the only time anyone runs
// these. So the desired side is a release's files — a published tag's or a
// checkout's — and the report names which.
//
// There are two ways to reach a storage database, and which one applies is the
// operator's to state rather than the CLI's to discover:
//
//	through the API      the server reads its own storage, or asks a data
//	                     plane to read its own over the connection that
//	                     already exists between them
//	directly, by DSN     this workstation opens the storage database itself,
//	                     for when the server is down — including when it is
//	                     down because its schema bootstrap is failing
//
// Nothing falls back from one to the other. A deployment that cannot be
// reached through the API is an error naming the deployment, never a report
// about a different database that happened to be reachable.

// storageSchemaTargetFlags selects which storage database the command acts on.
// The two paths are mutually exclusive and the flags say which is in use, so a
// command never has to infer the operator's intent from what happened to
// resolve.
type storageSchemaTargetFlags struct {
	Deployment  string `help:"Data plane whose storage to read, as named under tern_deployments in the server config; omit for the storage of the server the CLI is pointed at"`
	Environment string `short:"e" help:"Environment of the deployment's endpoint; required with --deployment"`
	DSN         string `help:"Connect to the storage database directly with this DSN instead of going through the API; for when the server is down"`
	Config      string `help:"Server config file to resolve the storage DSN from, connecting directly instead of going through the API"`
	Dialect     string `help:"Storage database family of --dsn (mysql or postgres) when its form does not say"`
}

// direct reports whether the operator asked for a direct connection. Passing
// either flag is the whole signal: both exist only on that path.
//
// Presence is what routes, not content. A --dsn of whitespace is a malformed
// direct connection, and resolveStorageTarget refuses it by name; reading it
// as "no DSN" would send the command through the API instead and report a
// different database than the operator addressed (AZ-5).
//
// Only a flag routes, never the environment. $SCHEMABOT_CONFIG_FILE stands in
// for --config once a direct connection is already chosen, but it cannot choose
// one: an operator who runs this with no target flags asked for the server's
// storage, and an exported variable in their shell must not turn that into a
// direct connection to whatever database the config names.
func (f *storageSchemaTargetFlags) direct() bool {
	return f.DSN != "" || f.Config != ""
}

// validate refuses flag combinations that mix the two paths, instead of
// silently honoring one and dropping the other. Dropping a --deployment would
// report the wrong database under the right name, which is worse than any
// error message.
func (f *storageSchemaTargetFlags) validate() error {
	if !f.direct() {
		if strings.TrimSpace(f.Dialect) != "" {
			return fmt.Errorf("--dialect only applies to a direct connection: through the API the server reports its own storage dialect; pass --dsn or --config to connect directly")
		}
		if f.Deployment == "" && f.Environment != "" {
			return fmt.Errorf("-e %s was given without --deployment: an environment selects which of a deployment's endpoints to reach, so name the deployment too, or omit both to read the storage of the server the CLI is pointed at", f.Environment)
		}
		if f.Deployment != "" && f.Environment == "" {
			return fmt.Errorf("--deployment %s needs -e <environment>: a deployment serves one endpoint per environment, so there is no single storage to read without one", f.Deployment)
		}
		return nil
	}
	if f.Deployment != "" {
		return fmt.Errorf("--deployment cannot be combined with a direct connection: a DSN addresses one storage database, and reaching a data plane's storage goes through its own endpoint; drop --dsn/--config to route through the API, or drop --deployment to read the database the DSN names")
	}
	if f.Environment != "" {
		return fmt.Errorf("-e cannot be combined with a direct connection: a DSN already names one database, so there is no endpoint to select")
	}
	return nil
}

// StoragePlanCmd reports the storage DDL outstanding between a live storage
// database and the schema files of the release the operator names.
//
// It is strictly read-only — it reads the catalog, computes a diff, and takes
// no lock — so it is safe to run at any time, including against production
// while an apply is in flight or an incident is open.
//
// The exit status is the machine-readable half of the answer: 0 when the
// storage needs nothing, 2 when statements are outstanding, and 1 when the
// read itself failed. A pre-deploy gate needs those three apart, because
// "converged" and "unreachable" call for opposite decisions.
//
// The release it compares against is always named — --release or --schema-dir —
// because the question a deploy asks is whether the storage is ready for the
// release about to roll, and that is a different question from whether it
// matches the release now running.
type StoragePlanCmd struct {
	storageSchemaTargetFlags `embed:""`
	storageSchemaSourceFlags `embed:""`
	AllowDestructive         bool `help:"Report destructive statements as ones that would run, matching what an apply with the same flag would do" name:"allow-destructive"`
	JSON                     bool `help:"Output as JSON"`
}

func (cmd *StoragePlanCmd) Run(ctx context.Context, g *Globals) error {
	if err := cmd.validate(); err != nil {
		return err
	}
	if err := cmd.validateSource(); err != nil {
		return err
	}
	report, err := cmd.read(ctx, g)
	if err != nil {
		return err
	}
	if cmd.JSON {
		encoder := json.NewEncoder(os.Stdout)
		encoder.SetIndent("", "  ")
		if err := encoder.Encode(apitypes.StorageSchemaPlanResponse{Report: report}); err != nil {
			return fmt.Errorf("encode storage schema report: %w", err)
		}
	} else if err := outputStorageSchemaPlan(report, false, storageSchemaPlanHints(report)); err != nil {
		return err
	}
	if report.Converged {
		return nil
	}
	return exitStorageSchemaOutstanding()
}

// read fetches the report over whichever path the flags selected.
func (cmd *StoragePlanCmd) read(ctx context.Context, g *Globals) (*apitypes.StorageSchemaReport, error) {
	if cmd.direct() {
		return cmd.readDirect(ctx, g)
	}
	return cmd.readThroughAPI(ctx, g)
}

// readDirect opens the storage database from this workstation and diffs it
// here, for when the server is down — including when it is down because its own
// schema bootstrap is failing.
func (cmd *StoragePlanCmd) readDirect(ctx context.Context, g *Globals) (*apitypes.StorageSchemaReport, error) {
	target, err := resolveStorageTarget(cmd.DSN, cmd.Config, cmd.Dialect)
	if err != nil {
		return nil, err
	}
	// The dialect is already resolved on this path, so a release fetch costs no
	// extra round trip.
	desired, err := cmd.resolve(ctx, func() (schema.Dialect, error) { return target.dialect, nil })
	if err != nil {
		return nil, err
	}
	logger := storageSchemaLogger(g)
	logger.Info("reading storage schema directly",
		"source", target.source, "dialect", target.dialect, "schema_source", desired.Describe())
	report, err := api.PlanStorageSchema(ctx, target.dsn, desired, logger,
		target.ensureSchemaOptions(cmd.AllowDestructive)...)
	if err != nil {
		return nil, fmt.Errorf("diff storage schema on the database from %s: %w", target.source, err)
	}
	// This CLI answered, so the version is this binary's — and so is the
	// schema, unless the operator named another release's.
	report.AttributeTo(g.Version)
	return report.APIType(), nil
}

// readThroughAPI asks the server, which reads its own storage or has a data
// plane read its own. A desired schema the operator named travels with the
// request, because the answering binary does not carry another release's files.
func (cmd *StoragePlanCmd) readThroughAPI(ctx context.Context, g *Globals) (*apitypes.StorageSchemaReport, error) {
	endpoint, err := g.Resolve()
	if err != nil {
		return nil, err
	}
	request := apitypes.StorageSchemaPlanRequest{
		Deployment:       cmd.Deployment,
		Environment:      cmd.Environment,
		AllowDestructive: cmd.AllowDestructive,
	}
	desired, err := cmd.resolve(ctx, func() (schema.Dialect, error) {
		return cmd.dialectThroughAPI(ctx, endpoint)
	})
	if err != nil {
		return nil, err
	}
	if desired != nil {
		request.SchemaFiles = desired.Files
		request.SchemaSource = desired.Description
	}

	response, err := cmdclient.StorageSchemaPlan(ctx, endpoint, request)
	if err != nil {
		return nil, fmt.Errorf("diff storage schema%s: %w", storageSchemaTargetSuffix(cmd.Deployment, cmd.Environment), err)
	}
	if response.Report == nil {
		return nil, fmt.Errorf("storage schema diff%s returned no report", storageSchemaTargetSuffix(cmd.Deployment, cmd.Environment))
	}
	return response.Report, nil
}

// dialectThroughAPI asks the target which storage family it runs, so a release
// fetch reads the right one of its schema directories.
//
// It is the diff itself, asked with no desired schema: a read-only call that
// the target answers about its own storage, which is the only authority on the
// question. Asking costs a round trip and is why only --release pays for it —
// but asking beats making the operator state a dialect their control plane
// already knows, and beats guessing one and fetching DDL of the wrong family.
func (cmd *StoragePlanCmd) dialectThroughAPI(ctx context.Context, endpoint string) (schema.Dialect, error) {
	response, err := cmdclient.StorageSchemaPlan(ctx, endpoint, apitypes.StorageSchemaPlanRequest{
		Deployment:  cmd.Deployment,
		Environment: cmd.Environment,
	})
	if err != nil {
		return "", fmt.Errorf("ask which storage family%s runs: %w", storageSchemaTargetSuffix(cmd.Deployment, cmd.Environment), err)
	}
	if response.Report == nil || response.Report.Dialect == "" {
		return "", fmt.Errorf("the report for the storage%s named no dialect, so there is no way to tell which of a release's schema files apply to it", storageSchemaTargetSuffix(cmd.Deployment, cmd.Environment))
	}
	return schema.Dialect(response.Report.Dialect), nil
}

// StorageApplyCmd converges a SchemaBot instance's storage database by running
// the schema bootstrap that instance would run on its next boot.
//
// It is the bootstrap, not a second implementation of it: the same differ, the
// same refusal of destructive statements, and the same advisory lock — so two
// operators running this at once serialize exactly the way two booting pods
// do, and a pre-deploy convergence step is this command with nothing added.
// It converges to the schema of the binary that runs it, and there is no flag
// to point it at another release's — see storageSchemaSourceRefusal.
type StorageApplyCmd struct {
	storageSchemaTargetFlags `embed:""`
	AllowDestructive         bool `help:"Permit the destructive statements the convergence would otherwise refuse; it widens the target's standing storage policy and never narrows it" name:"allow-destructive"`
	AutoApprove              bool `short:"y" help:"Skip confirmation prompt" name:"auto-approve"`
	JSON                     bool `help:"Output as JSON"`
	// The diff's file selectors are accepted here only to be refused with the
	// reason and the alternative. An operator who has just run the diff against
	// a release reaches for the same flags on the apply, and Kong's bare
	// "unknown flag" would leave them guessing at whether the convergence
	// silently used a different schema.
	SchemaDir string `hidden:"" name:"schema-dir"`
	Release   string `hidden:""`
}

func (cmd *StorageApplyCmd) Run(ctx context.Context, g *Globals) error {
	if err := cmd.validate(); err != nil {
		return err
	}
	if err := storageSchemaSourceRefusal(cmd.SchemaDir, cmd.Release); err != nil {
		return err
	}

	// Confirm against a fresh read rather than against a description of the
	// command: an operator approving DDL on SchemaBot's own storage should see
	// the statements, and the read is free of side effects. With --auto-approve
	// the convergence's own planned report says what ran, so there is nothing
	// this preview would add.
	if !cmd.AutoApprove {
		// No selector on the preview asks the target about its own embedded
		// schema, which is the schema this convergence is about to run.
		preview := &StoragePlanCmd{
			storageSchemaTargetFlags: cmd.storageSchemaTargetFlags,
			AllowDestructive:         cmd.AllowDestructive,
		}
		report, err := preview.read(ctx, g)
		if err != nil {
			return err
		}
		if err := outputStorageSchemaPlan(report, true, nil); err != nil {
			return err
		}
		if report.Converged {
			// Nothing to converge and nothing to approve. Returning success is
			// the honest answer: the storage already matches.
			return nil
		}
		if len(report.Manual) > 0 {
			return fmt.Errorf("refusing to converge storage schema on %s: %d change(s) need manual remediation first (listed above)", storageSchemaDatabaseLabel(report), len(report.Manual))
		}
		confirmed, err := confirmAction(
			fmt.Sprintf("\nDo you want to apply these changes to %s? Only 'yes' will be accepted: ", storageSchemaDatabaseLabel(report)),
			"\nApply cancelled.",
		)
		if err != nil {
			return err
		}
		if !confirmed {
			return nil
		}
	}

	planned, remaining, err := cmd.converge(ctx, g)
	if err != nil {
		return err
	}
	if cmd.JSON {
		encoder := json.NewEncoder(os.Stdout)
		encoder.SetIndent("", "  ")
		if err := encoder.Encode(apitypes.StorageSchemaApplyResponse{Planned: planned, Remaining: remaining}); err != nil {
			return fmt.Errorf("encode storage schema convergence: %w", err)
		}
	} else if err := outputStorageSchemaConvergence(planned, remaining, cmd.AutoApprove); err != nil {
		return err
	}
	return storageSchemaConvergenceOutcome(remaining)
}

// storageSchemaConvergenceOutcome is whether a convergence counts as having
// happened, decided from what it left behind.
//
// A manual-remediation entry means nothing ran at all: the bootstrap refuses
// the whole drift set while one is outstanding. The report has already listed
// them, and this is what makes the command fail — an unattended run must not
// exit 0 having converged nothing, whichever output mode it was asked for.
//
// Refused destructive statements are the other case, and they are not a
// failure: the surplus state is left in place on purpose (AV-9), everything
// else converged, and a rollback is expected to sit there.
func storageSchemaConvergenceOutcome(remaining *apitypes.StorageSchemaReport) error {
	if len(remaining.Manual) == 0 {
		return nil
	}
	return fmt.Errorf("storage schema on %s was not converged: %d change(s) need manual remediation before anything else runs",
		storageSchemaDatabaseLabel(remaining), len(remaining.Manual))
}

// converge runs the convergence over whichever path the flags selected.
func (cmd *StorageApplyCmd) converge(ctx context.Context, g *Globals) (planned, remaining *apitypes.StorageSchemaReport, err error) {
	if cmd.direct() {
		target, err := resolveStorageTarget(cmd.DSN, cmd.Config, cmd.Dialect)
		if err != nil {
			return nil, nil, err
		}
		logger := storageSchemaLogger(g)
		logger.Info("converging storage schema directly",
			"source", target.source,
			"dialect", target.dialect,
			"allow_destructive", target.allowDestructive || cmd.AllowDestructive,
			"config_allows_destructive", target.allowDestructive)
		plannedReport, remainingReport, err := api.ApplyStorageSchema(ctx, target.dsn, logger,
			target.ensureSchemaOptions(cmd.AllowDestructive)...)
		if err != nil {
			return nil, nil, fmt.Errorf("converge storage schema on the database from %s: %w", target.source, err)
		}
		plannedReport.Version = g.Version
		remainingReport.Version = g.Version
		return plannedReport.APIType(), remainingReport.APIType(), nil
	}

	endpoint, err := g.Resolve()
	if err != nil {
		return nil, nil, err
	}
	response, err := cmdclient.StorageSchemaApply(ctx, endpoint, apitypes.StorageSchemaApplyRequest{
		Deployment:       cmd.Deployment,
		Environment:      cmd.Environment,
		AllowDestructive: cmd.AllowDestructive,
	})
	if err != nil {
		return nil, nil, fmt.Errorf("converge storage schema%s: %w", storageSchemaTargetSuffix(cmd.Deployment, cmd.Environment), err)
	}
	if response.Planned == nil || response.Remaining == nil {
		// Both halves are required to say what happened; without the pair there
		// is no way to tell a convergence that finished from one that left
		// statements behind.
		return nil, nil, fmt.Errorf("storage schema convergence%s returned an incomplete result; check the target's logs for whether it converged", storageSchemaTargetSuffix(cmd.Deployment, cmd.Environment))
	}
	return response.Planned, response.Remaining, nil
}

// storageSchemaLogger builds the diagnostics logger for a direct connection. A
// text handler on stderr, deliberately: this is a one-shot command read at a
// terminal, and keeping diagnostics off stdout leaves the report itself
// pipeable.
func storageSchemaLogger(g *Globals) *slog.Logger {
	return slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{
		Level: logLevel(),
	})).With("schemabot_version", g.Version)
}

// storageSchemaTargetSuffix names the target in an error, so a failure says
// which storage was being read rather than only that a read failed.
func storageSchemaTargetSuffix(deployment, environment string) string {
	if deployment == "" {
		return ""
	}
	return fmt.Sprintf(" for deployment %s in %s", deployment, environment)
}
