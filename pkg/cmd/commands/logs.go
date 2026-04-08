package commands

import (
	"fmt"
	"strings"
	"time"

	"github.com/block/schemabot/pkg/cmd/client"
	"github.com/block/schemabot/pkg/cmd/templates"
)

// LogsCmd views apply logs for a database or specific apply.
type LogsCmd struct {
	ApplyIDArg  string `arg:"" optional:"" help:"Apply ID (positional)" name:"apply_id"`
	Database    string `short:"d" help:"Database name (required unless apply_id provided)"`
	Environment string `short:"e" help:"Target environment (required unless apply_id provided)"`
	ApplyID     string `short:"a" help:"Apply ID (e.g., apply_abc123)" name:"apply-id"`
	Limit       int    `short:"n" help:"Number of log entries to show" default:"50"`
	Follow      bool   `short:"f" help:"Follow logs in real-time"`
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

	ep, err := resolveEndpoint(g.Endpoint, g.Profile)
	if err != nil {
		return err
	}

	if cmd.Follow {
		return followLogs(ep, cmd.Database, cmd.Environment, cmd.ApplyID)
	}

	return showLogs(ep, cmd.Database, cmd.Environment, cmd.ApplyID, cmd.Limit)
}

// showLogs displays logs once and exits.
func showLogs(endpoint, database, environment, applyID string, limit int) error {
	logs, err := client.GetLogs(endpoint, database, environment, applyID, limit)
	if err != nil {
		return err
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

// formatLogLevel returns a colored log level indicator.
func formatLogLevel(level string) string {
	switch strings.ToLower(level) {
	case "error":
		return fmt.Sprintf("%s[ERR]%s", "\033[31m", templates.ANSIReset) // Red
	case "warn":
		return fmt.Sprintf("%s[WRN]%s", templates.ANSIYellow, templates.ANSIReset)
	case "info":
		return fmt.Sprintf("%s[INF]%s", templates.ANSIGreen, templates.ANSIReset)
	case "debug":
		return fmt.Sprintf("%s[DBG]%s", templates.ANSIDim, templates.ANSIReset)
	default:
		return fmt.Sprintf("[%s]", strings.ToUpper(level))
	}
}
