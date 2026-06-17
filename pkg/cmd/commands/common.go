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
	"github.com/block/schemabot/pkg/state"
)

// Globals holds flags shared by all commands.
type Globals struct {
	Endpoint string `help:"SchemaBot API endpoint (overrides profile)"`
	Profile  string `help:"Configuration profile"`
	Token    string `help:"Bearer token for authenticating to an auth-enabled server (or set SCHEMABOT_TOKEN)"`

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
	Database    string `kong:"-"`
	Environment string `short:"e" help:"Target environment"`
}

// Resolve resolves the endpoint and verifies the explicit control-operation scope.
func (cf *ControlFlags) Resolve(g *Globals) (string, error) {
	return resolveControlFlags(g.Endpoint, g.Profile, cf.ApplyID, cf.Environment)
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
	Database  string `yaml:"database"`
	Type      string `yaml:"type"`
	SchemaDir string `yaml:"-"` // Set by LoadCLIConfig, not from YAML
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
	decoder := yaml.NewDecoder(strings.NewReader(string(data)))
	decoder.KnownFields(true)
	if err := decoder.Decode(&cfg); err != nil {
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

// requireControlScope validates the explicit fields that scope a control operation.
func requireControlScope(applyID, environment string) error {
	if applyID == "" {
		return fmt.Errorf("apply_id is required")
	}
	if environment == "" {
		return fmt.Errorf("--environment is required")
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

// populateControlDisplayFields fills fields used for CLI output from progress.
// The control API still derives database from apply_id; this is only display data.
func populateControlDisplayFields(database *string, progressResult *apitypes.ProgressResponse) {
	if progressResult == nil {
		return
	}
	if *database == "" {
		*database = progressResult.Database
	}
}

func formatControlTarget(applyID, database, environment string) string {
	if database != "" {
		return fmt.Sprintf("%s/%s (apply %s)", database, environment, applyID)
	}
	if environment != "" {
		return fmt.Sprintf("apply %s in %s", applyID, environment)
	}
	return fmt.Sprintf("apply %s", applyID)
}

// resolveControlFlags resolves the endpoint and validates apply_id + environment.
func resolveControlFlags(endpoint, profile, applyID, environment string) (string, error) {
	ep, err := resolveEndpoint(endpoint, profile)
	if err != nil {
		return "", err
	}
	if err := requireControlScope(applyID, environment); err != nil {
		return "", err
	}
	return ep, nil
}

// applyAndWatch extracts a plan ID, calls the apply API, prints status, and
// optionally watches progress. Used by both RunApply and RunRollback.
func applyAndWatch(ep string, planResult *apitypes.PlanResponse, database, environment, caller, operation string,
	deferCutover, deferDeploy, skipRevert bool, branch string, watch bool, format OutputFormat, logHeartbeat time.Duration) error {

	if planResult.PlanID == "" {
		return fmt.Errorf("no plan_id in response")
	}

	options := buildApplyOptions(planResult, deferCutover, deferDeploy, skipRevert, branch, watch, format)

	applyResult, err := client.CallApplyAPI(ep, planResult.PlanID, environment, caller, options)
	if err != nil {
		return err
	}

	if err := checkAccepted(applyResponseWrapper{applyResult}, operation); err != nil {
		return err
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
		return nil
	}

	label := strings.ToUpper(operation[:1]) + operation[1:]
	if applyID != "" {
		fmt.Printf("\n%s started: %s\n", label, applyID)
	} else {
		fmt.Printf("\n%s started successfully.\n", label)
	}

	if !watch {
		printWatchInstructions(applyID, database, environment)
		return nil
	}

	fmt.Println("Watching progress...")
	if err := WatchApplyProgressWithFormat(ep, applyID, environment, true, format, logHeartbeat); err != nil {
		return err
	}

	return nil
}

func buildApplyOptions(planResult *apitypes.PlanResponse, deferCutover, deferDeploy, skipRevert bool, branch string, watch bool, format OutputFormat) map[string]string {
	options := make(map[string]string)
	isPlanetScale := planResult != nil && state.IsPlanetScaleEngine(planResult.Engine)
	if deferCutover {
		options["defer_cutover"] = "true"
	}
	// TUI mode: defer deploy so the user can review the deploy request diff
	// on PlanetScale before triggering. Non-interactive modes auto-deploy.
	if isPlanetScale && (deferDeploy || (watch && format == OutputFormatInteractive)) {
		options["defer_deploy"] = "true"
	}
	if skipRevert {
		options["skip_revert"] = "true"
	}
	if branch != "" {
		options["branch"] = branch
	}
	return options
}

// printWatchInstructions prints the "To watch and manage" hint.
func printWatchInstructions(applyID, database, environment string) {
	if applyID != "" {
		fmt.Printf("To watch and manage: schemabot progress %s\n", applyID)
	} else {
		fmt.Printf("To watch and manage: schemabot status -d %s -e %s\n", database, environment)
	}
}

type applyChangeCounts struct {
	created        int
	altered        int
	dropped        int
	vschemaUpdates int
}

func countTableProgressChanges(tables []tableProgress) applyChangeCounts {
	var counts applyChangeCounts
	for _, table := range tables {
		counts.add(table.ChangeType)
	}
	return counts
}

func countProgressResponseChanges(tables []*apitypes.TableProgressResponse) applyChangeCounts {
	var counts applyChangeCounts
	for _, table := range tables {
		counts.add(table.ChangeType)
	}
	return counts
}

func (c *applyChangeCounts) add(changeType string) {
	switch strings.ToUpper(changeType) {
	case "CREATE", "CHANGE_TYPE_CREATE":
		c.created++
	case "ALTER", "CHANGE_TYPE_ALTER":
		c.altered++
	case "DROP", "CHANGE_TYPE_DROP":
		c.dropped++
	case "VSCHEMA", "VSCHEMA_UPDATE", "CHANGE_TYPE_VSCHEMA":
		c.vschemaUpdates++
	}
}

func (c applyChangeCounts) summary() string {
	var parts []string
	if c.created > 0 {
		parts = append(parts, fmt.Sprintf("%d created", c.created))
	}
	if c.altered > 0 {
		parts = append(parts, fmt.Sprintf("%d altered", c.altered))
	}
	if c.dropped > 0 {
		parts = append(parts, fmt.Sprintf("%d dropped", c.dropped))
	}
	if c.vschemaUpdates > 0 {
		word := "updates"
		if c.vschemaUpdates == 1 {
			word = "update"
		}
		parts = append(parts, fmt.Sprintf("%d VSchema %s", c.vschemaUpdates, word))
	}
	if len(parts) == 0 {
		return ""
	}
	return fmt.Sprintf("Changes: %s.", strings.Join(parts, ", "))
}
