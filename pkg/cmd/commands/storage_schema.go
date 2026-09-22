package commands

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"os"
	"strings"
	"time"

	"github.com/block/schemabot/pkg/api"
	"github.com/block/schemabot/pkg/apitypes"
	cmdclient "github.com/block/schemabot/pkg/cmd/client"
	"github.com/block/schemabot/pkg/schema"
	"github.com/block/schemabot/pkg/ui"
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
	// resolvedTarget is the storage database these flags name, once it has been
	// resolved. It sits on the flags rather than on a command because a command
	// that hands the target off hands these flags off with it, so the copy
	// carries the resolution and cannot resolve a second one.
	resolvedTarget *storageTarget `kong:"-"`
}

// target is the storage database these flags address, resolved at most once.
//
// One resolution per run is the point. A config using storage.dsn_from resolves
// its DSN from secret references, so every resolution is a call to someone
// else's system, with its own audit trail and its own chance to answer
// differently than it did a moment ago. A convergence that resolved again could
// run against a database its own preview never looked at.
func (f *storageSchemaTargetFlags) target() (*storageTarget, error) {
	if f.resolvedTarget != nil {
		return f.resolvedTarget, nil
	}
	target, err := resolveStorageTarget(f.DSN, f.Config, f.Dialect)
	if err != nil {
		return nil, err
	}
	f.resolvedTarget = target
	return target, nil
}

// direct reports whether the operator asked for a direct connection. Passing
// either flag is the whole signal: both exist only on that path.
//
// Presence is what routes, not content. A --dsn of whitespace is a malformed
// direct connection, and resolveStorageTarget refuses it by name; reading it
// as "no DSN" would send the command through the API instead and report a
// different database than the operator addressed (AZ-5).
//
// Only a flag routes, never the environment. $SCHEMABOT_CONFIG_FILE is the
// fallback --config source for the storage maintenance commands, which are
// direct-only and have no other path to take; on these two it is never read,
// because a direct connection is already chosen by then or not at all. An
// operator who runs these with no target flags asked for the server's storage,
// and an exported variable in their shell must not turn that into a direct
// connection to whatever database the config names.
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
	JSON                     bool `help:"Output as JSON"`
	// AllowUnsafe is deliberately not a flag on the plan. The normal
	// plan/apply flow has no way to preview an apply's --allow-unsafe either:
	// the plan discloses the destructive statements and names the flag, and the
	// way to see them as statements that will run is to run the apply and
	// decline its prompt. The apply's own preview is that path, and sets this.
	//
	// A plan can still report them as running without it, because a target's
	// standing storage policy can allow destructive changes with no flag on the
	// line at all.
	AllowUnsafe bool `kong:"-"`
	// resolved is a schema the caller has already read from the selectors,
	// used instead of reading them again. The apply's preview sets it so the
	// plan an operator approves is computed from the same bytes the
	// convergence then runs.
	resolved *api.StorageSchemaSource `kong:"-"`
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
	} else if err := outputStorageSchemaPlan(report, false, "", storageSchemaPlanHints(report)); err != nil {
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
	target, err := cmd.target()
	if err != nil {
		return nil, err
	}
	// The dialect is already resolved on this path, so a release fetch costs no
	// extra round trip.
	desired, err := cmd.desiredSchema(ctx, func() (schema.Dialect, error) { return target.dialect, nil })
	if err != nil {
		return nil, err
	}
	logger := storageSchemaLogger(g)
	logger.Info("reading storage schema directly",
		"source", target.source, "dialect", target.dialect, "schema_source", desired.Describe())
	report, err := api.PlanStorageSchema(ctx, target.dsn, desired, logger,
		target.ensureSchemaOptions(cmd.AllowUnsafe)...)
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
		AllowDestructive: cmd.AllowUnsafe,
	}
	desired, err := cmd.desiredSchema(ctx, func() (schema.Dialect, error) {
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

// desiredSchema is the schema this plan compares against: one the caller
// already read, or the selectors read now.
func (cmd *StoragePlanCmd) desiredSchema(ctx context.Context, dialect func() (schema.Dialect, error)) (*api.StorageSchemaSource, error) {
	if cmd.resolved != nil {
		return cmd.resolved, nil
	}
	return cmd.resolve(ctx, dialect, false)
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
//
// Which schema it converges to is the operator's to name. Named nothing, it
// converges the answering binary's own embedded files, which is what that
// binary's next boot converges. Named a release, it converges that release's
// files — the reason the command exists, because the storage a release needs
// has to be there before the first pod of that release starts, and the pod
// that would converge it is the one that cannot start until it is. Naming a
// release is confirmed at a terminal and never runs unattended; see
// blockUnattendedNamedSchema and storageSchemaConfirmation.
type StorageApplyCmd struct {
	storageSchemaTargetFlags `embed:""`
	storageSchemaSourceFlags `embed:""`
	AllowUnsafe              bool `help:"Permit the destructive statements the convergence would otherwise refuse; it widens the target's standing storage policy and never narrows it" name:"allow-unsafe"`
	AutoApprove              bool `short:"y" help:"Skip confirmation prompt; refused with --release or --schema-dir, which are always confirmed at a terminal" name:"auto-approve"`
	JSON                     bool `help:"Output as JSON"`
	// Timeout is how a convergence outlives the budget a boot runs under. A
	// booting pod gives up after minutes because a pod converging is a pod not
	// yet serving; this command has somebody watching it, so it does not have
	// to. Left unset, the operator default is named on its behalf, so the
	// target always runs the budget this command is waiting for.
	Timeout time.Duration `help:"Bound the whole convergence — the lock wait, the diff under it, and the DDL. Defaults to the operator budget, which is already far above a booting pod's. Raising it holds the storage bootstrap lock for that long, and pods booting in the window will not come up" name:"timeout"`
}

func (cmd *StorageApplyCmd) Run(ctx context.Context, g *Globals) error {
	if err := cmd.validate(); err != nil {
		return err
	}
	if err := cmd.validateSourceCombination(); err != nil {
		return err
	}
	// At the door, before anything is read: this refuses a command form, not
	// something a plan could have found, so there is nothing to learn from the
	// database first.
	if err := blockUnattendedNamedSchema(cmd.namedSource(), cmd.AutoApprove); err != nil {
		return err
	}
	// Checked before the preview rather than on the way to the convergence: a
	// budget the target will refuse is a flag the operator has to fix either
	// way, and finding out after a diff of a production storage database has
	// already run is finding out later for no reason.
	if _, err := cmd.convergenceBudget(); err != nil {
		return err
	}

	// Every convergence plans first, attended or not, the way `apply` does.
	// The plan is a fresh read rather than a description of the command: an
	// operator approving DDL on SchemaBot's own storage should see the
	// statements, and the read is free of side effects. Unattended, it is what
	// the destructive gate below decides from.
	//
	// It is built before anything is read so that its target flags are the only
	// ones anything here uses: the schema a named release resolves for, the
	// plan, and the convergence all take their storage database from this one
	// value, which resolves it once and remembers it (see
	// storageSchemaTargetFlags.target).
	preview := &StoragePlanCmd{
		storageSchemaTargetFlags: cmd.storageSchemaTargetFlags,
		storageSchemaSourceFlags: cmd.storageSchemaSourceFlags,
		AllowUnsafe:              cmd.AllowUnsafe,
	}
	flags := &preview.storageSchemaTargetFlags

	// The schema is resolved once and used for the preview, the confirmation
	// and the convergence. Resolving it again for the convergence would open a
	// window where a moved tag or an edited directory made the operator
	// approve one file set and converge another.
	desired, err := cmd.resolveDesired(ctx, g, flags)
	if err != nil {
		return err
	}
	preview.resolved = desired

	report, err := preview.read(ctx, g)
	if err != nil {
		return err
	}
	// With --auto-approve the convergence's own planned report says what ran,
	// so the plan is printed after the fact instead — unless the gate stops the
	// run, which prints it itself. Under --json it goes to the terminal rather
	// than into the stream being parsed: the statements are what the person at
	// the prompt is being asked to approve, so the one thing this must never do
	// is skip them.
	if !cmd.AutoApprove {
		if err := writeToTerminal(cmd.JSON, func() error {
			return outputStorageSchemaPlan(report, true, cmd.rerunWithAllowUnsafe(), nil)
		}); err != nil {
			return err
		}
	}

	// Both refusals run on the attended and the unattended path alike, so an
	// operator and a pre-deploy job are told the same thing. Manual first: it
	// gates the whole drift set, which makes a destructive statement behind it
	// unreachable rather than merely refused.
	//
	// A --json run is read by a program, so the gates print no plan on it and
	// the refusal is reported as the response shape instead.
	gatePrintsPlan := cmd.AutoApprove && !cmd.JSON
	if err := blockManualStorageApply(report, gatePrintsPlan, cmd.rerunWithAllowUnsafe()); err != nil {
		return cmd.reportRefusal(report, err)
	}
	if err := blockDestructiveStorageApply(report, gatePrintsPlan, cmd.rerunWithAllowUnsafe()); err != nil {
		return cmd.reportRefusal(report, err)
	}

	if !cmd.AutoApprove {
		prompts := io.Writer(os.Stdout)
		if cmd.JSON {
			prompts = os.Stderr
		}
		confirmed, err := confirmActionOn(
			prompts,
			storageSchemaConfirmation(report, cmd.namedSource() != ""),
			"\nApply cancelled.",
		)
		if err != nil {
			return err
		}
		if !confirmed {
			// A decline is an outcome a program has to be able to read, and it
			// leaves the database exactly as the plan found it — the same shape
			// a refusal answers with, for the same reason.
			if cmd.JSON {
				return encodeStorageSchemaConvergence(report, report)
			}
			return nil
		}
	}

	planned, remaining, err := cmd.converge(ctx, g, flags, desired)
	if err != nil {
		return err
	}
	if cmd.JSON {
		if err := encodeStorageSchemaConvergence(planned, remaining); err != nil {
			return err
		}
	} else if err := outputStorageSchemaConvergence(planned, remaining, cmd.AutoApprove, cmd.rerunWithAllowUnsafe()); err != nil {
		return err
	}
	return storageSchemaConvergenceOutcome(remaining)
}

// reportRefusal hands a refused convergence back in the shape the caller asked
// for, and returns the refusal either way.
//
// A refusal under --json is the case a program most needs to read: nothing ran,
// so every statement the plan found is still outstanding, which is why the
// planned and remaining halves are the same report. Without this the run
// printed a human plan on a surface a program parses, or on the destructive
// gate nothing at all, and a caller could not tell a refusal from a crash.
func (cmd *StorageApplyCmd) reportRefusal(report *apitypes.StorageSchemaReport, refusal error) error {
	if !cmd.JSON {
		return refusal
	}
	// An encode failure is the answer to why there is no report on stdout, so it
	// is the error to return: the refusal it would have carried is in the
	// statements the plan already holds, and the command fails on either.
	if err := encodeStorageSchemaConvergence(report, report); err != nil {
		return err
	}
	return refusal
}

func encodeStorageSchemaConvergence(planned, remaining *apitypes.StorageSchemaReport) error {
	encoder := json.NewEncoder(os.Stdout)
	encoder.SetIndent("", "  ")
	if err := encoder.Encode(apitypes.StorageSchemaApplyResponse{Planned: planned, Remaining: remaining}); err != nil {
		return fmt.Errorf("encode storage schema convergence: %w", err)
	}
	return nil
}

// blockUnattendedNamedSchema refuses to converge a named release's schema
// without a person watching.
//
// Naming a schema is the one thing about this command that a boot cannot check
// for itself. Every other decision here is the bootstrap's, made the same way
// on the same files whoever asked; this one substitutes files the answering
// binary never carried, so the fleet's own boots will not agree with what was
// just converged until the release that carries them is deployed. That
// disagreement is safe and expected, and it is also the kind of thing an
// operator must be looking at when it starts — so the confirmation is the
// consent, and --auto-approve is not a way to give it (AV-9).
//
// Refusing the combination is stricter than refusing at the prompt: a
// pre-deploy job cannot converge a release's schema at all. That is the right
// default for a surface whose whole value is that the fleet and the storage
// agree, and an unattended convergence of the answering binary's own schema —
// the pre-deploy step this command was built for — is untouched by it.
func blockUnattendedNamedSchema(selector string, autoApprove bool) error {
	if selector == "" || !autoApprove {
		return nil
	}
	return fmt.Errorf("%s cannot be combined with --auto-approve: converging storage to a schema this binary does not carry is confirmed at a terminal, because until that release is deployed the running one will not converge the same schema. Run it without --auto-approve and answer the prompt, or drop %s to converge what this binary's own next boot would", selector, selector)
}

// rerunWithAllowUnsafe is the command that permits what this one refused, for
// an operator to copy off a refusal.
//
// It carries the target flags forward because the command has to address the
// same storage database the refusal is about. Dropping them would suggest a
// convergence of the storage of the server the CLI happens to point at, which
// during a rollback is a different database than the one being looked at.
//
// It carries the schema selector forward for a stronger version of the same
// reason. The refused statements were computed against the schema the operator
// named, so a command that dropped the selector would permit destructive
// changes while converging a different schema than the one the refusal was
// about — and on a cross-release convergence that is the schema of a different
// release.
//
// A DSN is named rather than repeated: it carries the storage database's
// credentials, and this is printed to a terminal and scrolled back through.
func (cmd *StorageApplyCmd) rerunWithAllowUnsafe() string {
	parts := []string{"storage", "apply"}
	switch {
	case strings.TrimSpace(cmd.DSN) != "":
		parts = append(parts, "--dsn <the same DSN>")
	case cmd.Config != "":
		parts = append(parts, "--config", cmd.Config)
	case cmd.Deployment != "":
		parts = append(parts, "--deployment", cmd.Deployment, "-e", cmd.Environment)
	}
	if cmd.Dialect != "" {
		parts = append(parts, "--dialect", cmd.Dialect)
	}
	// A budget the operator chose is part of the run being offered back. An
	// operator who shortened it to fail fast, and copies a command that drops
	// the flag, gets the target's default instead of the ceiling they picked.
	if cmd.Timeout > 0 {
		parts = append(parts, "--timeout", cmd.Timeout.String())
	}
	switch {
	case strings.TrimSpace(cmd.Release) != "":
		parts = append(parts, "--release", cmd.Release)
		if strings.TrimSpace(cmd.Repo) != "" {
			parts = append(parts, "--release-repo", cmd.Repo)
		}
	case strings.TrimSpace(cmd.SchemaDir) != "":
		parts = append(parts, "--schema-dir", cmd.SchemaDir)
	}
	return strings.Join(append(parts, "--allow-unsafe"), " ")
}

// blockManualStorageApply stops a convergence that cannot run at all.
//
// A manual entry gates the whole drift set: the bootstrap refuses every
// statement in the report until an operator resolves it by hand. So it is the
// refusal to report even when destructive statements are present too — those
// are not reachable until this one is resolved — which is why it runs before
// the destructive gate rather than after it.
//
// withPlan prints the plan for an unattended run, which has not printed one
// yet. The error says the entries are listed above, and on that path nothing
// has listed them.
func blockManualStorageApply(report *apitypes.StorageSchemaReport, withPlan bool, rerun string) error {
	if len(report.Manual) == 0 {
		return nil
	}
	if withPlan {
		if err := outputStorageSchemaPlan(report, true, rerun, nil); err != nil {
			return err
		}
	}
	return fmt.Errorf("refusing to converge storage schema on %s: %d change(s) need manual remediation first (listed above)",
		storageSchemaDatabaseLabel(report), len(report.Manual))
}

// blockDestructiveStorageApply stops a convergence that would have to destroy
// storage state nothing has permitted, before it runs anything.
//
// This is the gate `apply` puts in front of a destructive schema change, in the
// same place and behind the same flag: the plan is on screen, the statements
// are named, and --allow-unsafe is the way through. It is in front of the
// confirmation rather than after it because --auto-approve skips a prompt, and
// consenting to a convergence is not consenting to destroy state.
//
// A deployment that already allows destructive storage changes has permitted
// them, so there is nothing here to ask: the report says so and the statements
// run. The gate narrows no standing policy (AV-9), and where it does stop a run
// it runs strictly less than the convergence would have — the bootstrap refuses
// the same statements on its own and converges the safe remainder. What changes
// is when the operator finds out: before a DROP against SchemaBot's own storage
// is decided, rather than in a report of what was already done.
//
// withPlan prints the plan for an unattended run, which has not printed one
// yet. Naming refused statements without showing them would send the operator
// back to `storage plan` to find out what was refused.
func blockDestructiveStorageApply(report *apitypes.StorageSchemaReport, withPlan bool, rerun string) error {
	if report.DestructiveAllowed || len(report.Destructive) == 0 {
		return nil
	}
	if withPlan {
		if err := outputStorageSchemaPlan(report, true, rerun, nil); err != nil {
			return err
		}
	}
	// The plan above carries the refusal, the statements, and the flag, so this
	// asks only for the exit status — an "Error:" line restating it would be
	// the third time the same refusal is on screen.
	return ErrSilent
}

// storageSchemaConfirmation asks for the convergence the operator is about to
// run, which is not always a set of statements.
//
// A preview that found nothing outstanding does not end the command. The
// bootstrap converges more than the catalog: it also clears the schema change
// engine's leftover tables, which outlive an interrupted convergence and are
// invisible to a diff of the catalog (see api.ApplyStorageSchema). Stopping
// here would leave them on the database and make an interactive apply do less
// than the same command with --auto-approve — and the one an operator reaches
// for mid-incident is the interactive one.
//
// namedSchema says the operator named a schema rather than taking the
// answering binary's own, which changes what they are consenting to and gets
// the notice that says how.
func storageSchemaConfirmation(report *apitypes.StorageSchemaReport, namedSchema bool) string {
	label := storageSchemaDatabaseLabel(report)
	notice := ""
	if namedSchema {
		notice = crossReleaseStorageNotice(report)
	}
	if report.Converged {
		return fmt.Sprintf("\n%sThe catalog of %s already matches. Run the bootstrap anyway, to clear any engine state a catalog diff cannot see? Only 'yes' will be accepted: ", notice, label)
	}
	return fmt.Sprintf("\n%sDo you want to apply these changes to %s? Only 'yes' will be accepted: ", notice, label)
}

// crossReleaseStorageNotice states the consequences of converging a schema the
// deployed release does not carry, which an operator has no other way to find
// out.
//
// A boot of the deployed release diffs this schema against its own, so
// everything converged here that the deployed release does not declare is a
// removal its bootstrap has to decide about. What it decides is the whole
// content of this notice, and it is not one answer: it depends on whether the
// deployment permits destructive storage changes, and on the dialect. The plan
// carries both, and shows what the storage needs rather than what will undo
// it, so neither is visible to an operator reading it.
func crossReleaseStorageNotice(report *apitypes.StorageSchemaReport) string {
	running := "the release answering this command"
	if version := strings.TrimSpace(report.Version); version != "" {
		running = version
	}
	return fmt.Sprintf("Converging %s to %s.\n\n%s\n\n", storageSchemaDatabaseLabel(report), report.SchemaSource,
		crossReleaseConsequence(report, running))
}

// crossReleaseConsequence is what the deployed release does to the state this
// convergence leaves behind, for the deployment and dialect the report
// describes.
//
// It reads BootConvergesDestructively and never DestructiveAllowed. The
// subject is what happens after this command exits, when the only thing still
// converging is a pod starting, and a pod converges the deployment's standing
// policy. The effective policy is this command's alone: an operator who passed
// --allow-unsafe widened what their own convergence runs and moved nothing
// about what the fleet's boots do, so reading it here would report a
// deployment's behavior from a flag the deployment never saw.
func crossReleaseConsequence(report *apitypes.StorageSchemaReport, running string) string {
	// A deployment whose boots converge destructively drops the surplus this
	// leaves rather than refusing it. The convergence is still worth running as
	// part of a deploy; what it is not is something to do in advance, which is
	// the reason an operator reaches for it.
	if report.BootConvergesDestructively {
		return fmt.Sprintf(`  This is not the schema %s converges on boot, and this deployment permits
  destructive storage changes — so it will not leave what is applied here in
  place. The next pod to boot %s drops the tables, columns and indexes its own
  schema does not declare, which is every one of them until that release is
  deployed. Converge as part of the deploy rather than ahead of it.`, running, running)
	}
	// PostgreSQL's bootstrap is additive-only: it walks what its own schema
	// declares and never computes a removal, so surplus state is not refused so
	// much as never considered. There is no refusal to log, and no release with
	// a gap to warn about.
	if schema.Dialect(report.Dialect) == schema.DialectPostgres {
		return fmt.Sprintf(`  This is not the schema %s converges on boot. Until that release is deployed,
  every pod that boots %s converges only what its own schema adds, so the
  tables, columns and indexes applied here survive untouched — and the boot
  says nothing about them, because it never considers removing them.`, running, running)
	}
	return fmt.Sprintf(`  This is not the schema %s converges on boot. Until that release is deployed,
  every pod that boots %s refuses to drop what it does not declare, so the
  tables, columns and indexes applied here survive — and every one of those
  boots logs a refused destructive change for them. A release from before
  indexes were protected is the exception: its boots converge a surplus index
  away. Re-run `+"`storage plan`"+` just before the deploy to confirm what you
  applied is still there.`, running, running)
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
// else converged, and a rollback is expected to sit there. An operator reaching
// this having permitted none of them is what blockDestructiveStorageApply
// stops, so the only way here is a target that gained surplus state between the
// plan and the convergence — where leaving it in place is still the answer.
func storageSchemaConvergenceOutcome(remaining *apitypes.StorageSchemaReport) error {
	if len(remaining.Manual) == 0 {
		return nil
	}
	return fmt.Errorf("storage schema on %s was not converged: %d change(s) need manual remediation before anything else runs",
		storageSchemaDatabaseLabel(remaining), len(remaining.Manual))
}

// convergenceBudget resolves the ceiling this run bounds the convergence with,
// refusing a value the target would refuse. The direct path bounds the
// convergence with it here; the API path sends it and the target does the same.
func (cmd *StorageApplyCmd) convergenceBudget() (time.Duration, error) {
	// Every refusal below is made against the duration the operator typed,
	// because the request carries whole seconds and the truncation to them
	// destroys the evidence. A budget under a second truncates to the zero
	// meaning "no preference" and comes back as the hour-long default; a
	// negative one too small to survive the truncation does the same; and a
	// fractional one loses its remainder, which turns a budget just over the
	// maximum into one exactly at it. Refusing a value the wire cannot carry
	// is also what makes the maximum enforceable at all: once the budget is a
	// whole number of seconds, bounding the seconds and bounding the duration
	// are the same bound, and ResolveStorageApplyTimeout applies it for both
	// ends of the wire.
	switch {
	case cmd.Timeout < 0:
		return 0, fmt.Errorf("--timeout: a convergence budget of %s must be positive, or unset for the default of %s", cmd.Timeout, apitypes.DefaultStorageApplyTimeout)
	case cmd.Timeout > 0 && cmd.Timeout < time.Second:
		return 0, fmt.Errorf("--timeout: a convergence budget of %s is shorter than the one second the request carries; name a whole number of seconds", cmd.Timeout)
	case cmd.Timeout%time.Second != 0:
		return 0, fmt.Errorf("--timeout: a convergence budget of %s is not the whole number of seconds the request carries; naming it would run under %s instead", cmd.Timeout, cmd.Timeout.Truncate(time.Second))
	}
	budget, err := apitypes.ResolveStorageApplyTimeout(cmd.timeoutSeconds(), apitypes.DefaultStorageApplyTimeout)
	if err != nil {
		return 0, fmt.Errorf("--timeout: %w", err)
	}
	return budget, nil
}

// timeoutSeconds is the flag as the request carries it. An unset flag is zero
// here, and the client names the operator default in its place before sending,
// so the request the target sees always carries the budget this command waits
// for.
func (cmd *StorageApplyCmd) timeoutSeconds() int64 {
	return int64(cmd.Timeout / time.Second)
}

// resolveDesired reads the schema this convergence will run, once, before
// anything is planned.
//
// Naming nothing resolves to nil, which is the answering binary's own embedded
// schema — and resolves without asking anything, because there are no files to
// fetch and no dialect to discover.
//
// A named release needs the target's storage dialect, since a release keeps one
// schema directory per family. Where that costs a round trip it is the same
// round trip the plan would have made; hoisting it here buys the guarantee that
// the file set is read once (see StoragePlanCmd.resolved).
func (cmd *StorageApplyCmd) resolveDesired(ctx context.Context, g *Globals, flags *storageSchemaTargetFlags) (*api.StorageSchemaSource, error) {
	if cmd.namedSource() == "" {
		return nil, nil
	}
	// One call, so that "these files are about to run" is stated once and
	// cannot be true on one path and false on the other.
	return cmd.resolve(ctx, dialectOfTarget(ctx, g, flags), true)
}

// dialectOfTarget asks whichever side owns the storage which family it runs.
// It is a function rather than a value because only --release needs the answer,
// and on the API path the answer costs a round trip.
func dialectOfTarget(ctx context.Context, g *Globals, flags *storageSchemaTargetFlags) func() (schema.Dialect, error) {
	return func() (schema.Dialect, error) {
		if flags.direct() {
			target, err := flags.target()
			if err != nil {
				return "", err
			}
			return target.dialect, nil
		}
		endpoint, err := g.Resolve()
		if err != nil {
			return "", err
		}
		probe := &StoragePlanCmd{storageSchemaTargetFlags: *flags}
		return probe.dialectThroughAPI(ctx, endpoint)
	}
}

// converge runs the convergence over whichever path the flags selected, against
// the schema already resolved for the plan the operator approved.
func (cmd *StorageApplyCmd) converge(ctx context.Context, g *Globals, flags *storageSchemaTargetFlags, desired *api.StorageSchemaSource) (planned, remaining *apitypes.StorageSchemaReport, err error) {
	if flags.direct() {
		target, err := flags.target()
		if err != nil {
			return nil, nil, err
		}
		logger := storageSchemaLogger(g)
		logger.Info("converging storage schema directly",
			"source", target.source,
			"dialect", target.dialect,
			"schema_source", desired.Describe(),
			"allow_destructive", target.allowDestructive || cmd.AllowUnsafe,
			"config_allows_destructive", target.allowDestructive)
		budget, err := cmd.convergenceBudget()
		if err != nil {
			return nil, nil, err
		}
		logger.Info("converging with an operator budget", "convergence_timeout", budget)
		// Progress goes to stderr, not stdout: it describes the run, and the
		// run's result is what stdout carries — under --json that is a
		// document a caller parses, and a progress line in the middle of it is
		// not a progress line, it is a parse error.
		progress := newStorageProgressPrinter(os.Stderr, ui.SupportsColors(os.Stderr))
		plannedReport, remainingReport, err := api.ApplyStorageSchema(ctx, target.dsn, desired, logger,
			append(target.ensureSchemaOptions(cmd.AllowUnsafe),
				api.WithConvergenceTimeout(budget),
				api.WithConvergenceProgress(progress.observe))...)
		if err != nil {
			return nil, nil, fmt.Errorf("converge storage schema on the database from %s: %w", target.source, err)
		}
		attributeStorageSchemaConvergence(g.Version, plannedReport, remainingReport)
		return plannedReport.APIType(), remainingReport.APIType(), nil
	}

	endpoint, err := g.Resolve()
	if err != nil {
		return nil, nil, err
	}
	request := apitypes.StorageSchemaApplyRequest{
		Deployment:       flags.Deployment,
		Environment:      flags.Environment,
		AllowDestructive: cmd.AllowUnsafe,
		TimeoutSeconds:   cmd.timeoutSeconds(),
	}
	// A named schema travels with the request, because the answering binary
	// does not carry another release's files. Sent as the same pair the plan
	// sends, so the convergence is attributed to the release whose files ran.
	if desired != nil {
		request.SchemaFiles = desired.Files
		request.SchemaSource = desired.Description
	}
	response, err := cmdclient.StorageSchemaApply(ctx, endpoint, request)
	if err != nil {
		return nil, nil, fmt.Errorf("converge storage schema%s: %w", storageSchemaTargetSuffix(flags.Deployment, flags.Environment), err)
	}
	if response.Planned == nil || response.Remaining == nil {
		// Both halves are required to say what happened; without the pair there
		// is no way to tell a convergence that finished from one that left
		// statements behind.
		return nil, nil, fmt.Errorf("storage schema convergence%s returned an incomplete result; check the target's logs for whether it converged", storageSchemaTargetSuffix(flags.Deployment, flags.Environment))
	}
	return response.Planned, response.Remaining, nil
}

// attributeStorageSchemaConvergence says which release a convergence ran, on
// both halves of it.
//
// A convergence of the answering binary's own schema names no release of its
// own, so the attribution is what turns "the embedded schema" into this
// binary's version; a convergence of files the operator named keeps their
// attribution, which is what AttributeTo already does. Both halves take it, and
// by the same call the preview took: a run whose plan header named a release
// and whose result header named a placeholder would read as two runs against
// two schemas.
func attributeStorageSchemaConvergence(version string, planned, remaining *api.StorageSchemaReport) {
	planned.AttributeTo(version)
	remaining.AttributeTo(version)
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
