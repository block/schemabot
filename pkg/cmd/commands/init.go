package commands

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"log/slog"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"time"

	"github.com/block/schemabot/pkg/apitypes"

	"github.com/block/schemabot/pkg/api"
	"github.com/block/schemabot/pkg/cmd/client"
	"github.com/block/schemabot/pkg/cmd/cliname"
	"github.com/block/schemabot/pkg/localdemo"
	"github.com/block/schemabot/pkg/localruntime"
	"github.com/block/schemabot/pkg/localsetup"
)

// InitCmd accepts explicit inputs or collects missing decisions in a terminal.
// Both routes use the same initialization workflow.
type InitCmd struct {
	Sample         bool         `help:"Start a disposable local MySQL or PostgreSQL database with sample tables (requires Docker)"`
	NonInteractive bool         `name:"non-interactive" help:"Never prompt; report missing inputs instead"`
	interactive    bool         `kong:"-"`
	progress       func(string) `kong:"-"`
	ReuseSchema    bool         `name:"reuse-schema" help:"Verify existing desired files without replacing them"`
	Database       string       `short:"d" help:"Name to register for this database"`
	Environment    string       `short:"e" help:"Environment to initialize"`
	Type           string       `help:"Database engine: mysql or postgres"`
	DSN            string       `help:"Target connection as env:VARIABLE or file:/absolute/path (credentials stay out of schema files)"`
	Integrated     bool         `help:"Create a separate schemabot database on the application server for SchemaBot state"`
	StorageDSN     string       `name:"storage-dsn" help:"Existing separate state database as env:VARIABLE or file:/absolute/path; startup initializes SchemaBot metadata tables"`
	SchemaDir      string       `name:"schema-dir" short:"s" default:"schema" help:"New schema directory, or unchanged files from a prior initialization"`
	Namespaces     []string     `name:"namespace" help:"Explicit namespace to import; repeat for multiple namespaces"`
	Runtime        string       `default:"local" help:"Local runtime identity"`
	JSON           bool         `name:"json" help:"Return the verified setup result as JSON"`
}

type initResult struct {
	Database    string `json:"database"`
	Environment string `json:"environment"`
	Profile     string `json:"profile"`
	SchemaDir   string `json:"schema_dir"`
	PlanID      string `json:"plan_id"`
	Tables      int32  `json:"tables"`
	Verified    bool   `json:"verified"`
	// SkippedTables are live tables schema files cannot describe. The
	// written schemabot.yaml lists each in ignore_tables, so SchemaBot leaves
	// them alone.
	SkippedTables []initSkippedTable `json:"skipped_tables,omitempty"`
}

// initSkippedTable is one table the import skipped, and why.
type initSkippedTable struct {
	Namespace string `json:"namespace"`
	Table     string `json:"table"`
	Reason    string `json:"reason"`
}

func initSkippedTables(groups []skippedTableGroup) []initSkippedTable {
	var out []initSkippedTable
	for _, group := range groups {
		for _, table := range group.Tables {
			out = append(out, initSkippedTable{Namespace: group.Namespace, Table: table.Table, Reason: table.Reason})
		}
	}
	return out
}

func (cmd *InitCmd) Run(ctx context.Context, g *Globals) error {
	if err := cmd.prepareSample(ctx, g); err != nil {
		if stopped := initStopped(err); stopped != nil {
			return stopped
		}
		if cmd.JSON && !errors.Is(err, ErrSilent) {
			return client.ExitWithJSON("initialization_error", err.Error())
		}
		return err
	}
	if err := cmd.collectInputs(ctx, g); err != nil {
		if stopped := initStopped(err); stopped != nil {
			return stopped
		}
		if cmd.JSON && !errors.Is(err, ErrSilent) {
			return client.ExitWithJSON("initialization_error", err.Error())
		}
		return err
	}
	result, err := cmd.initializeWithUI(ctx, g)
	if err != nil {
		if stopped := initStopped(err); stopped != nil {
			return stopped
		}
		if cmd.JSON {
			return client.ExitWithJSON("initialization_error", err.Error())
		}
		return err
	}
	if cmd.JSON {
		return json.NewEncoder(os.Stdout).Encode(result)
	}
	display := *result
	display.SchemaDir = cmd.SchemaDir
	cfg, err := client.LoadConfig()
	if err != nil {
		return fmt.Errorf("load CLI configuration after initialization: %w", err)
	}
	defaultProfile := cfg.DefaultProfile
	if defaultProfile == "" {
		defaultProfile = "default"
	}
	invocation := cliname.Name()
	if cliname.FromArgs(os.Args[1:]) == "" {
		invocation = initCommandName(invocation, os.Args[0])
	}
	fmt.Print(initCompletion(&display, cmd.Environment, defaultProfile, invocation))
	return nil
}

func (cmd *InitCmd) initialize(ctx context.Context, g *Globals) (*initResult, error) {
	if g.Endpoint != "" || g.Token != "" || os.Getenv("SCHEMABOT_ENDPOINT") != "" || os.Getenv("SCHEMABOT_TOKEN") != "" {
		return nil, fmt.Errorf("local initialization cannot be combined with endpoint or authentication overrides")
	}
	if cmd.Integrated && cmd.StorageDSN != "" {
		return nil, fmt.Errorf("choose --integrated or --storage-dsn, not both")
	}
	storage := api.StorageConfig{Dialect: cmd.Type, DSN: cmd.StorageDSN}
	if cmd.Integrated {
		storage.DSN = cmd.DSN
		storage.Database = "schemabot"
	}
	for _, ref := range []string{cmd.DSN, storage.DSN} {
		if !validInitConnectionReference(ref) {
			return nil, fmt.Errorf("provide target and storage connections as env:VARIABLE or file:/absolute/path references")
		}
	}
	namespaces, err := onboardPullNamespaces(cmd.Namespaces)
	if err != nil {
		return nil, err
	}
	cfg, err := client.LoadConfig()
	if err != nil {
		return nil, err
	}
	profile := initProfileName(cfg, g.Profile, cmd.Runtime)
	if existing, ok := cfg.Profiles[profile]; ok && !reflect.DeepEqual(existing, client.Profile{LocalRuntime: cmd.Runtime}) {
		return nil, fmt.Errorf("profile %q already has a different connection; choose another --profile", profile)
	}
	reuse, err := initSchemaReuse(cmd.SchemaDir)
	if err != nil {
		return nil, err
	}
	cmd.ReuseSchema = cmd.ReuseSchema || reuse
	root, err := filepath.Abs(cmd.SchemaDir)
	if err != nil {
		return nil, err
	}
	// Stage beside the destination so publication remains an atomic rename.
	if err := os.MkdirAll(filepath.Dir(root), 0755); err != nil {
		return nil, fmt.Errorf("create schema parent directory: %w", err)
	}
	stageContainer, err := os.MkdirTemp(filepath.Dir(root), ".schemabot-init-*")
	if err != nil {
		return nil, fmt.Errorf("create schema staging directory: %w", err)
	}
	defer func() {
		if err := os.RemoveAll(stageContainer); err != nil {
			slog.Warn("remove schema staging directory", "path", stageContainer, "error", err)
		}
	}()
	// Flat layouts derive their namespace from the schema root's basename.
	// Preserve it inside the private staging container for verification too.
	stage := filepath.Join(stageContainer, filepath.Base(root))
	if err := os.Mkdir(stage, 0700); err != nil {
		return nil, fmt.Errorf("create schema snapshot: %w", err)
	}
	if err := checkInitPublication(stage, renameInitSchema); err != nil {
		return nil, err
	}
	var exclusions client.PlanExclusions
	if cmd.ReuseSchema {
		exclusions, err = stageExistingInitSchema(root, stage, cmd.Database, cmd.Type, cmd.Environment, namespaces)
		if err != nil {
			return nil, fmt.Errorf("cannot reuse schema directory %q; choose an existing configured directory for --reuse-schema: %w", root, err)
		}
	}
	dir, err := localruntime.Directory(cmd.Runtime)
	if err != nil {
		return nil, err
	}
	binary, err := os.Executable()
	if err != nil {
		return nil, err
	}
	manager := localruntime.Manager{Dir: dir, Binary: binary, Version: g.Version}
	cmd.reportProgress("Setting up your database connection...")
	targetDSN, err := resolveInitConnection(cmd.DSN)
	if err != nil {
		return nil, err
	}
	if err := localsetup.CheckConnection(ctx, cmd.Type, targetDSN); err != nil {
		return nil, unregisteredInitError(ctx, fmt.Errorf("check %s: %w", cmd.DSN, err), initCancelledBeforeRegistration)
	}
	registration := localsetup.Registration{
		Database: cmd.Database, Environment: cmd.Environment, Engine: cmd.Type,
		Connection: api.EnvironmentConfig{DSN: cmd.DSN}, Storage: storage,
	}
	if cmd.Integrated {
		cmd.reportProgress("Preparing a separate schemabot database on your server...")
		if err := localsetup.PrepareIntegratedStorage(ctx, manager, registration); err != nil {
			return nil, unregisteredInitError(ctx, err, initCancelledPreparingStorage)
		}
	} else {
		stateDSN, err := resolveInitConnection(cmd.StorageDSN)
		if err != nil {
			return nil, err
		}
		if err := localsetup.CheckConnection(ctx, cmd.Type, stateDSN); err != nil {
			return nil, unregisteredInitError(ctx, fmt.Errorf("check %s: %w", cmd.StorageDSN, err), initCancelledBeforeRegistration)
		}
	}
	_, err = localsetup.Register(manager, registration)
	if err != nil {
		return nil, err
	}
	result, err := cmd.importBaseline(ctx, manager, stage, root, profile, namespaces, exclusions)
	if err != nil {
		return nil, retainedInitError(ctx, err)
	}
	return result, nil
}

func (cmd *InitCmd) importBaseline(ctx context.Context, manager localruntime.Manager, stage, root, profile string, namespaces []string, exclusions client.PlanExclusions) (*initResult, error) {
	cmd.reportProgress("Starting SchemaBot...")
	startupCtx, cancelStartup := context.WithTimeout(ctx, 30*time.Second)
	connection, err := manager.Ensure(startupCtx)
	cancelStartup()
	if err != nil {
		return nil, err
	}
	client.SetLocalAuth(connection.Token, connection.Endpoint)
	var skipped []skippedTableGroup
	cmd.reportProgress("Reading your live schema...")
	pulled, err := client.CallPullSchemaAPIWithContext(ctx, connection.Endpoint, cmd.Database, cmd.Type, cmd.Environment, client.PullSchemaOptions{Namespaces: namespaces, SkipUnrepresentableTables: true})
	if err != nil {
		return nil, fmt.Errorf("import live schema: %w", err)
	}
	if !cmd.ReuseSchema {
		plan, err := buildOnboardWritePlan(stage, pulled, client.PlanExclusions{})
		if err != nil {
			return nil, err
		}
		if err := plan.write(); err != nil {
			return nil, err
		}
		// The written config lists the tables the pull skipped in
		// ignore_tables, so the baseline is verified with them withheld.
		exclusions = plan.exclusions
		skipped = plan.skipped
	} else {
		// The reused config keeps its own ignore_tables, so each table the
		// pull skipped must already be withheld by it. A skipped table the
		// config still manages would be read as dropped by the next plan.
		// The tables are disclosed either way, so a rerun reports them as
		// the first run did.
		skipped, err = pulledSkippedTables(sortedPulledNamespaces(pulled), pulled.Namespaces)
		if err != nil {
			return nil, err
		}
		if err := requireReusedConfigWithholdsSkipped(exclusions, skipped); err != nil {
			return nil, err
		}
	}
	cmd.reportProgress("Checking that your schema files match...")
	// The baseline is verified on every rollout member's plan, the way
	// onboarding verifies it (validateOnboardPlanResult), and like onboarding
	// without the config's drop_tables, since it checks that the files mirror
	// the target.
	baseline, _, err := client.CallPlanAPIWithContext(ctx, connection.Endpoint, cmd.Database, cmd.Type, cmd.Environment, stage, "", 0, exclusions.WithoutDropTables(), false, true)
	if err != nil {
		return nil, fmt.Errorf("verify baseline: %w", err)
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if cmd.ReuseSchema && baseline != nil && len(baseline.Errors) == 0 && hasResultChanges(baseline) {
		// The connection is usable even though the desired schema is not a clean baseline.
		// Preserve the files and save the connection so the suggested plan can run.
		if _, err := client.RegisterLocalProfile(profile, cmd.Runtime); err != nil {
			return nil, fmt.Errorf("save connection for reviewing existing schema differences: %w", err)
		}
		return nil, &initSchemaDriftError{message: fmt.Sprintf("your existing schema files differ from the live database; your edits were preserved. Review them with %s plan -s %s -e %s --profile %s instead of rerunning setup:\n  %s", cliname.Name(), initShellArg(cmd.SchemaDir), initShellArg(cmd.Environment), initShellArg(profile), strings.Join(describeOnboardPlanChanges(baseline), "\n  "))}
	}
	cmd.reportProgress("Saving your schema files and connection...")
	if err := publishVerifiedInitSchema(stage, root, baseline, cmd.Database, cmd.Environment); err != nil {
		return nil, err
	}
	if _, err := client.RegisterLocalProfile(profile, cmd.Runtime); err != nil {
		return nil, fmt.Errorf("verified schema is available at %s, but the profile could not be saved; rerun with the same inputs: %w", root, err)
	}
	return &initResult{Database: cmd.Database, Environment: cmd.Environment, Profile: profile, SchemaDir: root, PlanID: baseline.PlanID, Tables: pulled.TableCount, Verified: true, SkippedTables: initSkippedTables(skipped)}, nil
}

// Only a stored, unchanged baseline authorizes publication of imported files.
func publishVerifiedInitSchema(stage, root string, baseline *apitypes.PlanResponse, database, environment string) error {
	if err := validateOnboardPlanResult(baseline, database, environment); err != nil {
		return err
	}
	if baseline.PlanID == "" {
		return fmt.Errorf("baseline verification returned no stored plan")
	}
	return publishInitSchema(stage, root)
}

// Reuse only an exact prior result. Never merge imported files into a user's
// edited desired state or silently remove files outside the imported scope.
func publishInitSchema(stage, root string) error {
	return publishInitSchemaWithRename(stage, root, renameInitSchema)
}

func publishInitSchemaWithRename(stage, root string, rename func(string, string) error) error {
	publishErr := rename(stage, root)
	if publishErr == nil {
		return nil
	}
	if !errors.Is(publishErr, fs.ErrExist) {
		return fmt.Errorf("publish schema directory without replacing existing files: %w", publishErr)
	}
	if info, err := os.Lstat(root); err == nil && info.Mode()&os.ModeSymlink != 0 {
		return fmt.Errorf("schema import cannot reuse symlinks: %s", root)
	}
	// Removing a directory with rmdir can only succeed while it is empty.
	// A concurrent file creation or symlink replacement fails closed; the
	// subsequent no-replace rename also preserves a concurrently created target.
	if entries, err := os.ReadDir(root); err == nil && len(entries) == 0 {
		if err := removeEmptyInitDir(root); err != nil {
			return fmt.Errorf("schema directory changed before publication: %w", err)
		}
		if err := rename(stage, root); err != nil {
			return fmt.Errorf("publish schema directory %q after removing empty destination: %w", root, err)
		}
		return nil
	}
	existing, err := initSchemaSnapshot(root)
	if err != nil {
		return fmt.Errorf("publish schema directory (%w): %w", publishErr, err)
	}
	proposed, err := initSchemaSnapshot(stage)
	if err != nil {
		return fmt.Errorf("compare verified import; existing files were preserved: %w", err)
	}
	if !reflect.DeepEqual(existing, proposed) {
		return fmt.Errorf("schema directory %s differs from the verified import; existing files were preserved", root)
	}
	return nil
}

const initSnapshotMaxBytes = 64 << 20
const initSnapshotMaxEntries = 10000

func initSchemaSnapshot(root string) (map[string]string, error) {
	result := make(map[string]string)
	remaining := int64(initSnapshotMaxBytes)
	entries := 0
	err := filepath.WalkDir(root, func(path string, entry fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		entries++
		if entries > initSnapshotMaxEntries {
			return fmt.Errorf("schema directory exceeds %d entries; choose a dedicated schema directory", initSnapshotMaxEntries)
		}
		if entry.Type()&os.ModeSymlink != 0 {
			return fmt.Errorf("schema import cannot reuse symlinks: %s", path)
		}
		relative, err := filepath.Rel(root, path)
		if err != nil {
			return err
		}
		if entry.IsDir() {
			return nil
		}
		if !entry.Type().IsRegular() {
			return fmt.Errorf("schema import requires regular files: %s", path)
		}
		if filepath.Ext(relative) != ".sql" && filepath.Base(relative) != "schemabot.yaml" && filepath.Base(relative) != "vschema.json" {
			return nil
		}
		info, err := entry.Info()
		if err != nil {
			return fmt.Errorf("inspect schema file: %w", err)
		}
		if info.Size() > remaining {
			return fmt.Errorf("schema directory exceeds %d bytes; choose a dedicated schema directory", initSnapshotMaxBytes)
		}
		f, err := os.Open(path)
		if err != nil {
			return fmt.Errorf("open schema file: %w", err)
		}
		data, readErr := io.ReadAll(io.LimitReader(f, remaining+1))
		closeErr := f.Close()
		if err := errors.Join(readErr, closeErr); err != nil {
			return fmt.Errorf("read schema file: %w", err)
		}
		if int64(len(data)) > remaining {
			return fmt.Errorf("schema directory exceeds %d bytes; choose a dedicated schema directory", initSnapshotMaxBytes)
		}
		remaining -= int64(len(data))
		result[relative] = string(data)
		return nil
	})
	return result, err
}

func (cmd *InitCmd) reportProgress(message string) {
	if cmd.progress != nil {
		cmd.progress(message)
	}
}

type initSchemaDriftError struct{ message string }

func (err *initSchemaDriftError) Error() string { return err.message }

func retainedInitError(ctx context.Context, err error) error {
	var drift *initSchemaDriftError
	if errors.As(err, &drift) {
		return err
	}
	if initStoppedByOperator(ctx, err) {
		return initInterrupted(ctx, fmt.Sprintf(initCancelledAfterRegistration, cliname.Name()))
	}
	return fmt.Errorf("initialization incomplete; runtime registration is retained for retry: %w", err)
}

// The notices an init the operator stopped prints before it exits with the
// interrupt status. Each says what the stop left behind at the step it landed
// on. A %s is the CLI's own name, filled in when the notice is built.
const (
	// Stopped on a screen that asks for the setup's inputs, before any of them
	// was acted on.
	initCancelledNothingChanged = "Setup cancelled; nothing was changed."
	// Stopped during a connection check, which only reads.
	initCancelledBeforeRegistration = "Setup cancelled before the runtime was registered. Run `%s init` again to retry."
	// Stopped while the integrated state database was being created. Whether
	// the creation finished is not known here, and a retry answers it: it
	// creates the database, or refuses one that exists and says how to connect
	// to it instead.
	initCancelledPreparingStorage = "Setup cancelled while preparing the schemabot database on your server; the runtime was not registered. Run `%s init` again: it finishes the setup, or says how to continue if the database was already created."
	// Stopped once the runtime was registered.
	initCancelledAfterRegistration = "Setup cancelled; runtime registration is retained for retry. Run `%s init` again with the same flags to finish it."
)

// initStoppedByOperator reports whether err is the operator stopping the
// setup: their signal, or the cancel key of the setup display. The display
// also cancels a setup it can no longer show, and that is the display failing,
// not the operator stopping it.
func initStoppedByOperator(ctx context.Context, err error) bool {
	return endedByInterrupt(ctx, err) && !errors.Is(context.Cause(ctx), errInitDisplayClosed)
}

// unregisteredInitError is the error of a setup step that ran before the
// runtime was registered: the interrupt with notice when the operator stopped
// the setup during it, and otherwise the failure the step reported.
func unregisteredInitError(ctx context.Context, failure error, notice string) error {
	if initStoppedByOperator(ctx, failure) {
		return initInterrupted(ctx, fmt.Sprintf(notice, cliname.Name()))
	}
	return failure
}

// initInterruptedError is an init the operator stopped: the interrupt status,
// and a notice saying what the stop left behind. The notice waits for
// initStopped to print it, because the setup display may still be drawing when
// the stop is detected, and a line written under it would be drawn over.
type initInterruptedError struct {
	notice string
	status error
}

func (e *initInterruptedError) Error() string { return e.status.Error() }
func (e *initInterruptedError) Unwrap() error { return e.status }

func initInterrupted(ctx context.Context, notice string) error {
	return &initInterruptedError{notice: notice, status: interrupted(ctx)}
}

// initCancelled is an init the operator ended with the cancel key of one of
// its own screens. No signal arrived, so there is no cancellation to carry,
// and the interrupt status is all it reports.
func initCancelled(notice string) error {
	return &initInterruptedError{notice: notice, status: &ExitCodeError{Code: ExitInterrupted, Err: ErrSilent}}
}

// initStopped prints the notice of an init the operator stopped and returns
// the interrupt status for it, or nil for any other error. Run calls it once
// the setup display has closed, and ahead of --json error reporting: a stopped
// init is not a failure to report as one.
func initStopped(err error) error {
	var stopped *initInterruptedError
	if !errors.As(err, &stopped) {
		return nil
	}
	fmt.Fprintln(os.Stderr, stopped.notice)
	return stopped.status
}

// A configured sample default is useful for plan/apply, but must not capture
// onboarding for a different runtime. Explicit profile selections still win.
func initProfileName(cfg *client.Config, flag, runtime string) string {
	selection := client.ResolveProfile(cfg, flag)
	existing := cfg.Profiles[selection.Name]
	if !selection.Explicit() && strings.HasPrefix(existing.LocalRuntime, localdemo.NamePrefix) && existing.LocalRuntime != runtime {
		return runtime
	}
	return selection.Name
}
