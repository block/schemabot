// Package commands implements CLI commands for SchemaBot.
package commands

import (
	"bufio"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"time"

	"gopkg.in/yaml.v3"

	"github.com/block/schemabot/pkg/apitypes"
	"github.com/block/schemabot/pkg/cmd/client"
	ghclient "github.com/block/schemabot/pkg/github"
)

// Globals holds flags shared by all commands.
type Globals struct {
	Endpoint string `help:"SchemaBot API endpoint (overrides profile)"`
	Profile  string `help:"Configuration profile"`

	// Build info (set by main.go from ldflags)
	Version string `kong:"-"`
	Commit  string `kong:"-"`
	Date    string `kong:"-"`
}

// Resolve resolves the API endpoint from the global flags.
func (g *Globals) Resolve() (string, error) {
	return resolveEndpoint(g.Endpoint, g.Profile)
}

// ControlFlags holds flags for commands that target a specific schema change.
type ControlFlags struct {
	ApplyID     string `arg:"" optional:"" help:"Apply ID to target"`
	Database    string `short:"d" help:"Database name"`
	Environment string `short:"e" help:"Target environment"`
}

// Resolve resolves the control flags using the standard resolution chain:
// endpoint → apply-id → require database+environment.
func (cf *ControlFlags) Resolve(g *Globals) (string, error) {
	return resolveControlFlags(g.Endpoint, g.Profile, &cf.ApplyID, &cf.Database, &cf.Environment)
}

// RequireApplyID returns an error if ApplyID is not set.
func (cf *ControlFlags) RequireApplyID() error {
	if cf.ApplyID == "" {
		return fmt.Errorf("apply_id is required")
	}
	return nil
}

// ErrSilent is returned when an error condition has already been displayed
// and no additional "Error:" message should be printed.
var ErrSilent = errors.New("silent error")

// CLIConfig represents the schemabot.yaml configuration file for CLI commands.
type CLIConfig struct {
	Database     string                   `yaml:"database"`
	Type         string                   `yaml:"type"`
	Deployment   string                   `yaml:"deployment,omitempty"`
	Environments ghclient.EnvironmentList `yaml:"environments,omitempty"`
	SchemaDir    string                   `yaml:"-"` // Set by LoadCLIConfig, not from YAML
}

// GetTarget returns the target for the given environment, falling back to the database name.
func (c *CLIConfig) GetTarget(env string) string {
	for _, e := range c.Environments {
		if e.Name == env && e.Target != "" {
			return e.Target
		}
	}
	return c.Database
}

// LoadCLIConfig loads configuration from schemabot.yaml in the given directory.
// The config file is required for plan and apply commands.
func LoadCLIConfig(dir string) (*CLIConfig, error) {
	if dir == "" {
		dir = "."
	}

	configPath := filepath.Join(dir, "schemabot.yaml")
	data, err := os.ReadFile(configPath)
	if err != nil {
		if os.IsNotExist(err) {
			absDir, _ := filepath.Abs(dir)
			return nil, fmt.Errorf("schemabot.yaml not found in %s\n\nUse -s to specify the schema directory:\n  schemabot plan -s ./path/to/schema\n  schemabot apply -s ./path/to/schema -e staging", absDir)
		}
		return nil, fmt.Errorf("read config file: %w", err)
	}

	var cfg CLIConfig
	if err := yaml.Unmarshal(data, &cfg); err != nil {
		return nil, fmt.Errorf("parse config file: %w", err)
	}

	if cfg.Database == "" {
		return nil, fmt.Errorf("schemabot.yaml: database is required")
	}
	// Schema files are in the same directory as schemabot.yaml
	cfg.SchemaDir = dir
	if cfg.Type == "" {
		cfg.Type = "mysql" // Default
	}

	return &cfg, nil
}

// resolveEndpoint resolves the API endpoint from explicit flag or profile config.
func resolveEndpoint(endpoint, profile string) (string, error) {
	ep, err := client.ResolveEndpointWithProfile(endpoint, profile)
	if err != nil {
		return "", fmt.Errorf("resolve endpoint: %w", err)
	}
	if ep == "" {
		return "", fmt.Errorf("no endpoint configured (run 'schemabot configure' to set up a profile)")
	}
	return ep, nil
}

// resolveApplyID resolves an apply ID to database and environment by calling the progress API.
// Returns (database, environment, error).
func resolveApplyID(endpoint, applyID string) (string, string, error) {
	result, err := client.GetProgress(endpoint, applyID)
	if err != nil {
		if client.IsNotFound(err) {
			return "", "", fmt.Errorf("no schema change found for apply ID '%s'", applyID)
		}
		return "", "", err
	}
	if result.Database == "" || result.Environment == "" {
		return "", "", fmt.Errorf("could not resolve database/environment for apply ID '%s'", applyID)
	}
	return result.Database, result.Environment, nil
}

// confirmAction prompts the user for "yes" confirmation. Returns true if confirmed.
func confirmAction(prompt, cancelMsg string) (bool, error) {
	fmt.Print(prompt)

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, os.Interrupt)
	defer signal.Stop(sigCh)

	type readResult struct {
		response string
		err      error
	}
	resultCh := make(chan readResult, 1)
	go func() {
		reader := bufio.NewReader(os.Stdin)
		response, err := reader.ReadString('\n')
		resultCh <- readResult{response, err}
	}()

	select {
	case <-sigCh:
		fmt.Println(cancelMsg)
		return false, nil
	case r := <-resultCh:
		// EOF with data is valid (e.g., echo -n yes | schemabot apply)
		if r.err != nil && !errors.Is(r.err, io.EOF) {
			return false, fmt.Errorf("failed to read response: %w", r.err)
		}
		response := strings.TrimSpace(strings.ToLower(r.response))
		if errors.Is(r.err, io.EOF) && response == "" {
			fmt.Println(cancelMsg)
			return false, nil
		}
		if response != "yes" {
			fmt.Println(cancelMsg)
			return false, nil
		}
		return true, nil
	}
}

// writeJSON writes v as pretty-printed JSON to stdout.
func writeJSON(v any) error {
	enc := json.NewEncoder(os.Stdout)
	enc.SetIndent("", "  ")
	return enc.Encode(v)
}

// resolveDBEnvFromApplyID resolves an apply-id flag to database/environment.
// If applyID is empty, this is a no-op. Otherwise it calls the progress API
// and overwrites the database and environment pointers.
func resolveDBEnvFromApplyID(ep string, applyID *string, database, environment *string) error {
	if *applyID == "" {
		return nil
	}
	db, env, err := resolveApplyID(ep, *applyID)
	if err != nil {
		return err
	}
	*database = db
	*environment = env
	return nil
}

// requireDatabaseEnvironment validates that database and environment are set.
func requireDatabaseEnvironment(database, environment string) error {
	if database == "" {
		return fmt.Errorf("--database is required (or use --apply-id)")
	}
	if environment == "" {
		return fmt.Errorf("--environment is required (or use --apply-id)")
	}
	return nil
}

// acceptedResponse is an interface for API responses that have Accepted and ErrorMessage fields.
type acceptedResponse interface {
	IsAccepted() bool
	GetErrorMessage() string
}

// Generic accepted response wrappers for the typed structs.
type controlResponseWrapper struct{ r *apitypes.ControlResponse }

func (w controlResponseWrapper) IsAccepted() bool        { return w.r.Accepted }
func (w controlResponseWrapper) GetErrorMessage() string { return w.r.ErrorMessage }

type applyResponseWrapper struct{ r *apitypes.ApplyResponse }

func (w applyResponseWrapper) IsAccepted() bool        { return w.r.Accepted }
func (w applyResponseWrapper) GetErrorMessage() string { return w.r.ErrorMessage }

type stopResponseWrapper struct{ r *apitypes.StopResponse }

func (w stopResponseWrapper) IsAccepted() bool        { return w.r.Accepted }
func (w stopResponseWrapper) GetErrorMessage() string { return w.r.ErrorMessage }

type startResponseWrapper struct{ r *apitypes.StartResponse }

func (w startResponseWrapper) IsAccepted() bool        { return w.r.Accepted }
func (w startResponseWrapper) GetErrorMessage() string { return w.r.ErrorMessage }

type volumeResponseWrapper struct{ r *apitypes.VolumeResponse }

func (w volumeResponseWrapper) IsAccepted() bool        { return w.r.Accepted }
func (w volumeResponseWrapper) GetErrorMessage() string { return w.r.ErrorMessage }

// checkAccepted checks that an API response has accepted=true.
// Returns a formatted error using the operation name if not accepted.
func checkAccepted(result acceptedResponse, operation string) error {
	if !result.IsAccepted() {
		return fmt.Errorf("%s not accepted: %v", operation, result.GetErrorMessage())
	}
	return nil
}

// autoResolveApplyID sets applyID from the progress response if not already set.
// This ensures control operations are scoped to a specific apply.
func autoResolveApplyID(applyID *string, progressResult *apitypes.ProgressResponse) {
	if *applyID == "" && progressResult.ApplyID != "" {
		*applyID = progressResult.ApplyID
	}
}

// resolveControlFlags resolves endpoint, database, and environment from the
// common control command flags (endpoint, profile, applyID, database, environment).
// Returns the resolved endpoint.
func resolveControlFlags(endpoint, profile string, applyID, database, environment *string) (string, error) {
	ep, err := resolveEndpoint(endpoint, profile)
	if err != nil {
		return "", err
	}
	if err := resolveDBEnvFromApplyID(ep, applyID, database, environment); err != nil {
		return "", err
	}
	if err := requireDatabaseEnvironment(*database, *environment); err != nil {
		return "", err
	}
	return ep, nil
}

// applyAndWatch extracts a plan ID, calls the apply API, prints status, and
// optionally watches progress. Returns the apply ID (for yield logic, etc.).
// Used by both RunApply and RunRollback.
func applyAndWatch(ep string, planResult *apitypes.PlanResponse, database, environment, caller, operation string,
	deferCutover, skipRevert bool, branch string, watch bool, format OutputFormat, logHeartbeat time.Duration, opts ...client.PlanOptions) (string, error) {

	if planResult.PlanID == "" {
		return "", fmt.Errorf("no plan_id in response")
	}

	options := make(map[string]string)
	if deferCutover {
		options["defer_cutover"] = "true"
	}
	// TUI mode: defer deploy so the user can review the deploy request diff
	// on PlanetScale before triggering. Non-interactive modes auto-deploy.
	if watch && format == OutputFormatInteractive {
		options["defer_deploy"] = "true"
	}
	if skipRevert {
		options["skip_revert"] = "true"
	}
	if branch != "" {
		options["branch"] = branch
	}

	applyResult, err := client.CallApplyAPI(ep, planResult.PlanID, database, environment, caller, options, opts...)
	if err != nil {
		return "", err
	}

	if err := checkAccepted(applyResponseWrapper{applyResult}, operation); err != nil {
		return "", err
	}

	applyID := applyResult.ApplyID

	if !watch && format == OutputFormatJSON {
		result := map[string]string{
			"apply_id":    applyID,
			"database":    database,
			"environment": environment,
		}
		enc := json.NewEncoder(os.Stdout)
		_ = enc.Encode(result)
		return applyID, nil
	}

	label := strings.ToUpper(operation[:1]) + operation[1:]
	if applyID != "" {
		fmt.Printf("\n%s started: %s\n", label, applyID)
	} else {
		fmt.Printf("\n%s started successfully.\n", label)
	}

	if !watch {
		printWatchInstructions(applyID, database, environment)
		return applyID, nil
	}

	fmt.Println("Watching progress...")
	if err := WatchApplyProgressWithFormat(ep, applyID, true, format, logHeartbeat); err != nil {
		return applyID, err
	}

	return applyID, nil
}

// printWatchInstructions prints the "To watch and manage" hint.
func printWatchInstructions(applyID, database, environment string) {
	if applyID != "" {
		fmt.Printf("To watch and manage: schemabot progress --apply-id %s\n", applyID)
	} else {
		fmt.Printf("To watch and manage: schemabot progress -d %s -e %s\n", database, environment)
	}
}
