package commands

import (
	"fmt"
	"strings"
	"time"

	"github.com/block/schemabot/pkg/apitypes"
	"github.com/block/schemabot/pkg/cmd/client"
	"github.com/block/schemabot/pkg/cmd/internal/templates"
	webhooktemplates "github.com/block/schemabot/pkg/webhook/templates"
)

// LogsCmd views apply logs for a database or specific apply.
type LogsCmd struct {
	ApplyIDArg  string `arg:"" optional:"" help:"Apply ID (positional)" name:"apply_id"`
	Database    string `short:"d" help:"Database name (required unless apply_id provided)"`
	Environment string `short:"e" help:"Target environment (required unless apply_id provided)"`
	ApplyID     string `short:"a" help:"Apply ID (e.g., apply_abc123)" name:"apply-id"`
	Limit       int    `short:"n" help:"Number of log entries to show" default:"50"`
	Follow      bool   `short:"f" help:"Follow logs in real-time"`
	Deployment  string `help:"Read logs from the selected data-plane deployment"`
	JSON        bool   `help:"Output as JSON"`
}

// Run executes the logs command.
func (cmd *LogsCmd) Run(g *Globals) error {
	// Merge positional apply_id into flag
	if cmd.ApplyIDArg != "" && cmd.ApplyID == "" {
		cmd.ApplyID = cmd.ApplyIDArg
	}

	// When apply ID is provided, database is not required
	if cmd.ApplyID == "" {
		if cmd.Database == "" {
			return fmt.Errorf("--database is required (or provide an apply_id)")
		}
		if cmd.Environment == "" {
			return fmt.Errorf("--environment is required (or provide an apply_id)")
		}
	}
	if cmd.Deployment != "" && cmd.ApplyID == "" {
		return fmt.Errorf("--deployment requires an explicit apply_id")
	}
	if cmd.Deployment != "" && cmd.Follow {
		return fmt.Errorf("--deployment is incompatible with --follow")
	}

	ep, err := resolveEndpoint(g.Endpoint, g.Profile)
	if err != nil {
		return err
	}

	if cmd.Follow {
		return followLogs(ep, cmd.Database, cmd.Environment, cmd.ApplyID)
	}
	if cmd.Deployment != "" {
		return showDeploymentLogs(ep, cmd.ApplyID, cmd.Deployment, cmd.Limit, cmd.JSON)
	}

	return showLogs(ep, cmd.Database, cmd.Environment, cmd.ApplyID, cmd.Limit, cmd.JSON)
}

func showDeploymentLogs(endpoint, applyID, deployment string, limit int, outputJSON bool) error {
	var result *apitypes.DeploymentLogsResponse
	err := withLoading("Loading data-plane logs...", !outputJSON, func() error {
		var loadErr error
		result, loadErr = client.GetDeploymentLogs(endpoint, applyID, deployment, limit)
		return loadErr
	})
	if err != nil {
		return err
	}
	if outputJSON {
		return writeJSON(result)
	}
	if len(result.Sources) == 0 {
		fmt.Println("No data-plane logs found.")
	}
	for i, source := range result.Sources {
		if len(result.Sources) > 1 {
			fmt.Printf("%s%s%s\n", templates.ANSIDim, deploymentLogSourceLabel("", source.Operations, source.ExternalID), templates.ANSIReset)
		}
		printLogs(source.Logs)
		if i+1 < len(result.Sources) {
			fmt.Println()
		}
	}
	for _, sourceErr := range result.Errors {
		fmt.Printf("Warning: %s: %s\n", deploymentLogSourceLabel(sourceErr.Target, sourceErr.Operations, sourceErr.ExternalID), sourceErr.Message)
	}
	return nil
}

func deploymentLogSourceLabel(target string, operations []*apitypes.LogOperationProvenance, externalID string) string {
	if target == "" && len(operations) > 0 {
		target = operations[0].Target
	}
	parts := []string{target}
	seenKinds := make(map[string]bool)
	for _, op := range operations {
		if op.OperationKind != "" && !seenKinds[op.OperationKind] {
			seenKinds[op.OperationKind] = true
			parts = append(parts, op.OperationKind)
		}
	}
	if len(parts) == 1 && parts[0] == "" {
		return externalID
	}
	return strings.Join(parts, " / ") + " (" + externalID + ")"
}

// showLogs displays logs once and exits.
func showLogs(endpoint, database, environment, applyID string, limit int, outputJSON bool) error {
	var logs []*client.LogEntry
	err := withLoading("Loading logs...", !outputJSON, func() error {
		var loadErr error
		logs, loadErr = client.GetLogs(endpoint, database, environment, applyID, limit)
		return loadErr
	})
	if err != nil {
		return err
	}
	if outputJSON {
		return writeJSON(&apitypes.LogsResponse{ApplyID: applyID, Logs: logs})
	}

	if len(logs) == 0 {
		fmt.Println("No logs found.")
		return nil
	}

	printLogs(logs)
	return nil
}

// followLogs continuously polls for new logs.
func followLogs(endpoint, database, environment, applyID string) error {
	switch {
	case applyID != "" && database == "":
		fmt.Printf("Following logs for %s... (Ctrl+C to stop)\n\n", applyID)
	case applyID != "":
		fmt.Printf("Following logs for %s (apply %s)... (Ctrl+C to stop)\n\n", database, applyID)
	default:
		fmt.Printf("Following logs for %s/%s... (Ctrl+C to stop)\n\n", database, environment)
	}

	var lastID int64
	for {
		logs, err := client.GetLogs(endpoint, database, environment, applyID, 100)
		if err != nil {
			fmt.Printf("Error fetching logs: %v\n", err)
			time.Sleep(2 * time.Second)
			continue
		}

		// Find new logs (those with ID > lastID)
		var newLogs []*client.LogEntry
		for _, log := range logs {
			if log.ID > lastID {
				newLogs = append(newLogs, log)
				if log.ID > lastID {
					lastID = log.ID
				}
			}
		}

		if len(newLogs) > 0 {
			printLogs(newLogs)
		}

		time.Sleep(1 * time.Second)
	}
}

// printLogs formats and prints log entries.
func printLogs(logs []*client.LogEntry) {
	for _, log := range logs {
		// Format timestamp
		ts := log.CreatedAt.Local().Format("15:04:05")

		// Format level with color
		level := formatLogLevel(log.Level)

		// Build the message
		msg := log.Message

		// Add state transition info if present
		if log.OldState != "" && log.NewState != "" {
			msg = fmt.Sprintf("%s [%s -> %s]", msg, log.OldState, log.NewState)
		}

		// Print formatted log line
		fmt.Printf("%s%s%s %s %s\n", templates.ANSIDim, ts, templates.ANSIReset, level, msg)
	}
}

// formatLogLevel returns a colored log level indicator, wrapping the shared
// tag text so the terminal output and the PR-comment log fold stay identical
// apart from color.
func formatLogLevel(level string) string {
	tag := webhooktemplates.LogLevelTag(level)
	switch strings.ToLower(level) {
	case "error":
		return "\033[31m" + tag + templates.ANSIReset // Red
	case "warn":
		return templates.ANSIYellow + tag + templates.ANSIReset
	case "info":
		return templates.ANSIGreen + tag + templates.ANSIReset
	case "debug":
		return templates.ANSIDim + tag + templates.ANSIReset
	default:
		return tag
	}
}
