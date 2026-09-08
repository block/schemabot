package commands

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"log/slog"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"time"

	"github.com/block/schemabot/pkg/api"
	"github.com/block/schemabot/pkg/cmd/client"
	"github.com/block/schemabot/pkg/localruntime"
	"github.com/block/schemabot/pkg/localsetup"
)

// InitCmd accepts explicit inputs or collects missing decisions in a terminal.
// Both routes use the same initialization workflow.
type InitCmd struct {
	NonInteractive bool         `name:"non-interactive" help:"Never prompt; report missing inputs instead"`
	interactive    bool         `kong:"-"`
	progress       func(string) `kong:"-"`
	ReuseSchema    bool         `name:"reuse-schema" help:"Verify existing desired files without replacing them"`
	Database       string       `short:"d" help:"Name to register for this database"`
	Environment    string       `short:"e" help:"Environment to initialize"`
	Type           string       `help:"Database engine: mysql or postgres"`
	DSN            string       `help:"Target connection as env:VARIABLE (credentials stay out of schema files)"`
	StorageDSN     string       `name:"storage-dsn" help:"Existing separate state database as env:VARIABLE; startup initializes SchemaBot metadata tables"`
	SchemaDir      string       `name:"schema-dir" short:"s" default:"schema" help:"New schema directory, or unchanged files from a prior initialization"`
	Namespaces     []string     `name:"namespace" help:"Explicit namespace to import; repeat for multiple namespaces"`
	Runtime        string       `default:"local" hidden:"" help:"Local runtime identity"`
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
}

func (cmd *InitCmd) Run(ctx context.Context, g *Globals) error {
	if err := cmd.collectInputs(ctx, g); err != nil {
		if cmd.JSON && !errors.Is(err, ErrSilent) {
			return client.ExitWithJSON("initialization_error", err.Error())
		}
		return err
	}
	result, err := cmd.initializeWithUI(ctx, g)
	if err != nil {
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
	fmt.Print(initCompletion(&display, cmd.Environment))
	return nil
}

func (cmd *InitCmd) initialize(ctx context.Context, g *Globals) (*initResult, error) {
	if g.Endpoint != "" || g.Token != "" || os.Getenv("SCHEMABOT_ENDPOINT") != "" || os.Getenv("SCHEMABOT_TOKEN") != "" {
		return nil, fmt.Errorf("local initialization cannot be combined with endpoint or authentication overrides")
	}
	for _, ref := range []string{cmd.DSN, cmd.StorageDSN} {
		if !strings.HasPrefix(ref, "env:") || strings.TrimSpace(strings.TrimPrefix(ref, "env:")) == "" {
			return nil, fmt.Errorf("provide target and storage connections as env:VARIABLE references")
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
	profile := client.ResolveProfileName(cfg, g.Profile)
	if existing, ok := cfg.Profiles[profile]; ok && !reflect.DeepEqual(existing, client.Profile{LocalRuntime: cmd.Runtime}) {
		return nil, fmt.Errorf("profile %q already has a different connection; choose another --profile", profile)
	}
	if _, err := initSchemaReuse(cmd.SchemaDir); err != nil {
		return nil, err
	}
	root, err := filepath.Abs(cmd.SchemaDir)
	if err != nil {
		return nil, err
	}
	// Stage beside the destination so publication remains an atomic rename.
	if err := os.MkdirAll(filepath.Dir(root), 0700); err != nil {
		return nil, fmt.Errorf("create schema parent directory: %w", err)
	}
	stage, err := os.MkdirTemp(filepath.Dir(root), ".schemabot-init-*")
	if err != nil {
		return nil, fmt.Errorf("create schema staging directory: %w", err)
	}
	defer func() {
		if err := os.RemoveAll(stage); err != nil {
			slog.Warn("remove schema staging directory", "path", stage, "error", err)
		}
	}()
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
	_, err = localsetup.Register(manager, localsetup.Registration{
		Database: cmd.Database, Environment: cmd.Environment, Engine: cmd.Type,
		Connection: api.EnvironmentConfig{DSN: cmd.DSN},
		Storage:    api.StorageConfig{Dialect: cmd.Type, DSN: cmd.StorageDSN},
	})
	if err != nil {
		return nil, err
	}
	result, err := cmd.importBaseline(ctx, manager, stage, root, profile, namespaces)
	if err != nil {
		return nil, fmt.Errorf("initialization incomplete; runtime registration is retained for retry: %w", err)
	}
	return result, nil
}

func (cmd *InitCmd) importBaseline(ctx context.Context, manager localruntime.Manager, stage, root, profile string, namespaces []string) (*initResult, error) {
	cmd.reportProgress("Starting SchemaBot...")
	startupCtx, cancelStartup := context.WithTimeout(ctx, 30*time.Second)
	connection, err := manager.Ensure(startupCtx)
	cancelStartup()
	if err != nil {
		return nil, err
	}
	client.SetLocalAuth(connection.Token, connection.Endpoint)
	cmd.reportProgress("Reading your live schema...")
	pulled, err := client.CallPullSchemaAPIWithContext(ctx, connection.Endpoint, cmd.Database, cmd.Type, cmd.Environment, client.PullSchemaOptions{Namespaces: namespaces})
	if err != nil {
		return nil, fmt.Errorf("import live schema: %w", err)
	}
	var ignored []string
	if cmd.ReuseSchema {
		ignored, err = stageExistingInitSchema(root, stage, cmd.Database, cmd.Type, cmd.Environment, namespaces)
		if err != nil {
			return nil, err
		}
	} else {
		plan, err := buildOnboardWritePlan(stage, pulled, nil)
		if err != nil {
			return nil, err
		}
		if err := plan.write(); err != nil {
			return nil, err
		}
	}
	cmd.reportProgress("Checking that your schema files match...")
	baseline, _, err := client.CallPlanAPIWithContext(ctx, connection.Endpoint, cmd.Database, cmd.Type, cmd.Environment, stage, "", 0, ignored, false)
	if err != nil {
		return nil, fmt.Errorf("verify baseline: %w", err)
	}
	if err := validateOnboardPlanResult(baseline, cmd.Database, cmd.Environment); err != nil {
		return nil, err
	}
	if baseline.PlanID == "" {
		return nil, fmt.Errorf("baseline verification returned no stored plan")
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	cmd.reportProgress("Saving your schema files and connection...")
	if err := publishInitSchema(stage, root); err != nil {
		return nil, err
	}
	if _, err := client.RegisterLocalProfile(profile, cmd.Runtime); err != nil {
		return nil, fmt.Errorf("verified schema is available at %s, but the profile could not be saved; rerun with the same inputs: %w", root, err)
	}
	return &initResult{Database: cmd.Database, Environment: cmd.Environment, Profile: profile, SchemaDir: root, PlanID: baseline.PlanID, Tables: pulled.TableCount, Verified: true}, nil
}

// Reuse only an exact prior result. Never merge imported files into a user's
// edited desired state or silently remove files outside the imported scope.
func publishInitSchema(stage, root string) error {
	publishErr := renameInitSchema(stage, root)
	if publishErr == nil {
		return nil
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
		return renameInitSchema(stage, root)
	}
	existing, err := initSchemaSnapshot(root)
	if err != nil {
		return fmt.Errorf("publish schema directory (%w): %w", publishErr, err)
	}
	proposed, err := initSchemaSnapshot(stage)
	if err != nil {
		return err
	}
	if !reflect.DeepEqual(existing, proposed) {
		return fmt.Errorf("schema directory %s differs from the verified import; existing files were preserved", root)
	}
	return nil
}

func initSchemaSnapshot(root string) (map[string]string, error) {
	result := make(map[string]string)
	err := filepath.WalkDir(root, func(path string, entry fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if entry.Type()&os.ModeSymlink != 0 {
			return fmt.Errorf("schema import cannot reuse symlinks: %s", path)
		}
		relative, err := filepath.Rel(root, path)
		if err != nil {
			return err
		}
		if entry.IsDir() {
			result[relative+"/"] = ""
			return nil
		}
		if !entry.Type().IsRegular() {
			return fmt.Errorf("schema import requires regular files: %s", path)
		}
		data, err := os.ReadFile(path)
		if err != nil {
			return err
		}
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
