package commands

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"os"
	"strings"

	"github.com/block/schemabot/pkg/api"
	"github.com/block/schemabot/pkg/apitypes"
	cmdclient "github.com/block/schemabot/pkg/cmd/client"
	"github.com/block/schemabot/pkg/cmd/cliname"
)

// Storage schema commands answer, and then close, the one question a deploy
// that did not converge leaves open: which storage DDL is still outstanding on
// this instance's own storage database.
//
// Both read the live database. Neither takes a version: a release tag says what
// that release would converge to, not what the storage converged to, and the
// two answers differ exactly when a deploy has failed — which is the only time
// anyone runs these.
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
	Config      string `help:"Server config file to resolve the storage DSN from, connecting directly instead of going through the API; defaults to $SCHEMABOT_CONFIG_FILE when --dsn is not set"`
	Dialect     string `help:"Storage database family of --dsn (mysql or postgres) when its form does not say"`
}

// direct reports whether the operator asked for a direct connection. Naming a
// DSN source is the whole signal: both flags exist only on that path.
func (f *storageSchemaTargetFlags) direct() bool {
	return strings.TrimSpace(f.DSN) != "" || strings.TrimSpace(f.Config) != ""
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

// StorageDiffCmd reports the storage DDL outstanding between a SchemaBot
// instance's embedded schema files and its live storage database.
//
// It is strictly read-only — it reads the catalog, computes a diff, and takes
// no lock — so it is safe to run at any time, including against production
// while an apply is in flight or an incident is open.
//
// The exit status is the machine-readable half of the answer: 0 when the
// storage needs nothing, 2 when statements are outstanding, and 1 when the
// read itself failed. A pre-deploy gate needs those three apart, because
// "converged" and "unreachable" call for opposite decisions.
type StorageDiffCmd struct {
	storageSchemaTargetFlags `embed:""`
	AllowDestructive         bool `help:"Report destructive statements as ones that would run, matching what an apply with the same flag would do" name:"allow-destructive"`
	JSON                     bool `help:"Output as JSON"`
}

func (cmd *StorageDiffCmd) Run(ctx context.Context, g *Globals) error {
	if err := cmd.validate(); err != nil {
		return err
	}
	report, err := cmd.read(ctx, g)
	if err != nil {
		return err
	}
	if cmd.JSON {
		encoder := json.NewEncoder(os.Stdout)
		encoder.SetIndent("", "  ")
		if err := encoder.Encode(apitypes.StorageSchemaDiffResponse{Report: report}); err != nil {
			return fmt.Errorf("encode storage schema report: %w", err)
		}
	} else if err := renderStorageSchemaReport(os.Stdout, report, storageSchemaDiffHints(cmd)); err != nil {
		return err
	}
	if report.Converged {
		return nil
	}
	return exitStorageSchemaOutstanding()
}

// read fetches the report over whichever path the flags selected.
func (cmd *StorageDiffCmd) read(ctx context.Context, g *Globals) (*apitypes.StorageSchemaReport, error) {
	if cmd.direct() {
		target, err := resolveStorageTarget(cmd.DSN, cmd.Config, cmd.Dialect)
		if err != nil {
			return nil, err
		}
		logger := storageSchemaLogger(g)
		logger.Info("reading storage schema directly",
			"source", target.source, "dialect", target.dialect)
		report, err := api.DiffStorageSchema(ctx, target.dsn, logger,
			api.WithDialect(target.dialect),
			api.WithAllowDestructiveSchemaChanges(cmd.AllowDestructive))
		if err != nil {
			return nil, fmt.Errorf("diff storage schema on the database from %s: %w", target.source, err)
		}
		// The version is this CLI's, because on this path the embedded schema
		// files that produced the diff are this binary's own.
		report.Version = g.Version
		return report.APIType(), nil
	}

	endpoint, err := g.Resolve()
	if err != nil {
		return nil, err
	}
	response, err := cmdclient.StorageSchemaDiff(ctx, endpoint, cmd.Deployment, cmd.Environment, cmd.AllowDestructive)
	if err != nil {
		return nil, fmt.Errorf("diff storage schema%s: %w", storageSchemaTargetSuffix(cmd.Deployment, cmd.Environment), err)
	}
	if response.Report == nil {
		return nil, fmt.Errorf("storage schema diff%s returned no report", storageSchemaTargetSuffix(cmd.Deployment, cmd.Environment))
	}
	return response.Report, nil
}

// StorageApplyCmd converges a SchemaBot instance's storage database by running
// the schema bootstrap that instance would run on its next boot.
//
// It is the bootstrap, not a second implementation of it: the same differ, the
// same refusal of destructive statements, and the same advisory lock — so two
// operators running this at once serialize exactly the way two booting pods
// do, and a pre-deploy convergence step is this command with nothing added.
type StorageApplyCmd struct {
	storageSchemaTargetFlags `embed:""`
	AllowDestructive         bool `help:"Permit the destructive statements the convergence would otherwise refuse; it widens the target's standing storage policy and never narrows it" name:"allow-destructive"`
	AutoApprove              bool `short:"y" help:"Skip confirmation prompt" name:"auto-approve"`
	JSON                     bool `help:"Output as JSON"`
}

func (cmd *StorageApplyCmd) Run(ctx context.Context, g *Globals) error {
	if err := cmd.validate(); err != nil {
		return err
	}

	// Confirm against a fresh read rather than against a description of the
	// command: an operator approving DDL on SchemaBot's own storage should see
	// the statements, and the read is free of side effects. With --auto-approve
	// the convergence's own planned report says what ran, so there is nothing
	// this preview would add.
	if !cmd.AutoApprove {
		preview := &StorageDiffCmd{
			storageSchemaTargetFlags: cmd.storageSchemaTargetFlags,
			AllowDestructive:         cmd.AllowDestructive,
		}
		report, err := preview.read(ctx, g)
		if err != nil {
			return err
		}
		if err := renderStorageSchemaReport(os.Stdout, report, nil); err != nil {
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
			fmt.Sprintf("\nRun these statements against %s? Only 'yes' will be accepted: ", storageSchemaDatabaseLabel(report)),
			"\nConvergence aborted.",
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
		return nil
	}
	return renderStorageSchemaConvergence(os.Stdout, planned, remaining)
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
			"source", target.source, "dialect", target.dialect, "allow_destructive", cmd.AllowDestructive)
		plannedReport, remainingReport, err := api.ApplyStorageSchema(ctx, target.dsn, logger,
			api.WithDialect(target.dialect),
			api.WithAllowDestructiveSchemaChanges(cmd.AllowDestructive))
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

// storageSchemaDatabaseLabel names the database a report is about, as an
// operator would say it: the database, its dialect, and the deployment it
// belongs to when the report came from one.
func storageSchemaDatabaseLabel(report *apitypes.StorageSchemaReport) string {
	label := report.Database
	if label == "" {
		label = "the storage database"
	}
	if report.Dialect != "" {
		label += fmt.Sprintf(" (%s)", report.Dialect)
	}
	if report.Deployment != "" {
		label += fmt.Sprintf(" on deployment %s in %s", report.Deployment, report.Environment)
	}
	return label
}

// renderStorageSchemaReport prints a report as the statements themselves,
// runnable as pasted.
//
// The layout is chosen for what an operator does with it at three in the
// morning: the statements are bare and one per line, with their classification
// in a heading above rather than a prefix beside, so a whole section can be
// selected and pasted into a client without stripping anything. hints are
// printed under the report when there is a next step to name.
func renderStorageSchemaReport(w io.Writer, report *apitypes.StorageSchemaReport, hints []string) error {
	if _, err := fmt.Fprintf(w, "%s\n", storageSchemaHeadline(report)); err != nil {
		return err
	}
	if report.Converged {
		return nil
	}
	sections := []struct {
		title      string
		statements []apitypes.StorageSchemaStatement
	}{
		{storageSchemaOutstandingTitle, report.Outstanding},
		{storageSchemaDestructiveTitle(report), report.Destructive},
		{"Needs manual remediation before anything converges", report.Manual},
	}
	for _, section := range sections {
		if len(section.statements) == 0 {
			continue
		}
		if _, err := fmt.Fprintf(w, "\n%s (%d):\n\n", section.title, len(section.statements)); err != nil {
			return err
		}
		for _, statement := range section.statements {
			if statement.Reason != "" {
				if _, err := fmt.Fprintf(w, "-- %s: %s\n", statement.Table, statement.Reason); err != nil {
					return err
				}
			}
			if _, err := fmt.Fprintf(w, "%s\n", storageSchemaRunnableDDL(statement.DDL)); err != nil {
				return err
			}
		}
	}
	for _, hint := range hints {
		if _, err := fmt.Fprintf(w, "\n%s\n", hint); err != nil {
			return err
		}
	}
	return nil
}

// storageSchemaHeadline is the one line an operator reads first: which database
// was read, and whether it needs anything.
func storageSchemaHeadline(report *apitypes.StorageSchemaReport) string {
	version := ""
	if report.Version != "" {
		version = fmt.Sprintf(", against the schema embedded in %s", report.Version)
	}
	if report.Converged {
		return fmt.Sprintf("%s is converged%s.", storageSchemaDatabaseLabel(report), version)
	}
	counts := make([]string, 0, 3)
	if n := len(report.Outstanding); n > 0 {
		counts = append(counts, fmt.Sprintf("%d outstanding", n))
	}
	if n := len(report.Destructive); n > 0 {
		counts = append(counts, fmt.Sprintf("%d destructive", n))
	}
	if n := len(report.Manual); n > 0 {
		counts = append(counts, fmt.Sprintf("%d needing manual remediation", n))
	}
	total := len(report.Outstanding) + len(report.Destructive) + len(report.Manual)
	return fmt.Sprintf("%s needs %d %s: %s%s.",
		storageSchemaDatabaseLabel(report), total, pluralStatements(total), strings.Join(counts, ", "), version)
}

// storageSchemaOutstandingTitle says what the statements in the section are:
// whatever else an operator does with them, these are what the next boot of the
// target's own binary will run.
const storageSchemaOutstandingTitle = "Outstanding, and run automatically on the next boot or apply"

// storageSchemaDestructiveTitle distinguishes destructive statements that would
// run from ones that are refused. The difference is the whole disposition of
// the section, so it goes in the heading rather than in a footnote: a refused
// statement is surplus state left in place on purpose, not a pending change.
func storageSchemaDestructiveTitle(report *apitypes.StorageSchemaReport) string {
	if report.DestructiveAllowed {
		return "Destructive, and permitted to run because destructive changes are allowed"
	}
	return "Destructive, and refused; surplus state stays in place"
}

// storageSchemaDiffHints names the next step for the report a diff just
// printed, in the command form the operator invoked the CLI as.
func storageSchemaDiffHints(cmd *StorageDiffCmd) []string {
	if cmd.direct() {
		return []string{fmt.Sprintf("Converge it with: %s storage apply %s", cliname.Name(), storageSchemaDirectFlagHint(cmd.DSN, cmd.Config))}
	}
	if cmd.Deployment != "" {
		return []string{fmt.Sprintf("Converge it with: %s storage apply --deployment %s -e %s", cliname.Name(), cmd.Deployment, cmd.Environment)}
	}
	return []string{fmt.Sprintf("Converge it with: %s storage apply", cliname.Name())}
}

// storageSchemaDirectFlagHint renders the flag that selected the direct path,
// without echoing a DSN that may carry credentials.
func storageSchemaDirectFlagHint(dsn, config string) string {
	if strings.TrimSpace(config) != "" {
		return "--config " + config
	}
	if dsn != "" {
		return "--dsn <the same DSN>"
	}
	return ""
}

// renderStorageSchemaConvergence prints what a convergence ran and what it left
// behind. Both halves are printed even on a clean run, because "nothing is
// left" is the fact an operator is looking for and an absent section does not
// state it.
func renderStorageSchemaConvergence(w io.Writer, planned, remaining *apitypes.StorageSchemaReport) error {
	applied := len(planned.Outstanding)
	if _, err := fmt.Fprintf(w, "Ran %d %s against %s.\n", applied, pluralStatements(applied), storageSchemaDatabaseLabel(planned)); err != nil {
		return err
	}
	if remaining.Converged {
		_, err := fmt.Fprintf(w, "%s is converged.\n", storageSchemaDatabaseLabel(remaining))
		return err
	}
	if _, err := fmt.Fprintln(w); err != nil {
		return err
	}
	return renderStorageSchemaReport(w, remaining, []string{
		"These were not applied. A destructive statement is refused unless --allow-destructive is passed; a manual entry has to be resolved by hand before anything else converges.",
	})
}

// storageSchemaRunnableDDL terminates a statement so a pasted section runs as
// one script. The differ emits statements without a terminator, and a section
// pasted into a client without them runs as a single malformed statement.
func storageSchemaRunnableDDL(ddl string) string {
	trimmed := strings.TrimRight(ddl, "; \t\r\n")
	if trimmed == "" {
		return ddl
	}
	return strings.TrimSpace(trimmed) + ";"
}

func pluralStatements(n int) string {
	if n == 1 {
		return "statement"
	}
	return "statements"
}
