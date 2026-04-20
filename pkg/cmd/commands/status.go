package commands

import (
	"fmt"

	"github.com/block/schemabot/pkg/cmd/client"
	"github.com/block/schemabot/pkg/cmd/templates"
	"github.com/block/schemabot/pkg/state"
)

// StatusCmd shows schema change status.
type StatusCmd struct {
	ApplyIDArg  string `arg:"" optional:"" help:"Apply ID to show details for" name:"apply_id"`
	Database    string `short:"d" help:"Database name (show apply history)"`
	Environment string `short:"e" help:"Environment filter (with -d)"`
	JSON        bool   `help:"Output as JSON"`
}

// Run executes the status command.
func (cmd *StatusCmd) Run(g *Globals) error {
	ep, err := resolveEndpoint(g.Endpoint, g.Profile)
	if err != nil {
		return err
	}

	// Mode 1: Show details for specific apply by ID
	if cmd.ApplyIDArg != "" {
		return showApplyByID(ep, cmd.ApplyIDArg, cmd.JSON)
	}

	// Mode 2: Show history for a database
	if cmd.Database != "" {
		return showDatabaseHistory(ep, cmd.Database, cmd.Environment, cmd.JSON)
	}

	// Mode 3: List all active applies
	result, err := client.GetStatus(ep)
	if err != nil {
		return err
	}

	if cmd.JSON {
		return writeJSON(result)
	}

	// Convert to template format
	applies := make([]templates.ActiveApplyData, 0, len(result.Applies))
	for _, a := range result.Applies {
		applies = append(applies, templates.ActiveApplyData{
			ApplyID:     a.ApplyID,
			Database:    a.Database,
			Environment: a.Environment,
			State:       a.State,
			Engine:      a.Engine,
			Caller:      a.Caller,
			StartedAt:   a.StartedAt,
			CompletedAt: a.CompletedAt,
			UpdatedAt:   a.UpdatedAt,
			Volume:      a.Volume,
		})
	}

	templates.WriteStatusList(templates.StatusListData{
		ActiveCount: result.ActiveCount,
		Applies:     applies,
	})

	return nil
}

// showApplyByID shows details for a specific apply by its ID.
func showApplyByID(endpoint, applyID string, outputJSON bool) error {
	result, err := client.GetProgress(endpoint, applyID)
	if err != nil {
		if client.IsNotFound(err) {
			fmt.Printf("No schema change found for apply '%s'\n", applyID)
			return nil
		}
		return err
	}

	if outputJSON {
		return writeJSON(result)
	}

	// Use the existing progress display
	if result.State == "" || state.IsState(result.State, state.NoActiveChange) {
		fmt.Printf("No schema change found for apply '%s'\n", applyID)
		return nil
	}

	// Display using the progress template
	data := templates.ParseProgressResponse(result)
	templates.WriteProgress(data)

	return nil
}

// showDatabaseHistory shows all applies for a database.
func showDatabaseHistory(endpoint, database, environment string, outputJSON bool) error {
	result, err := client.GetDatabaseHistory(endpoint, database, environment)
	if err != nil {
		return err
	}

	if outputJSON {
		return writeJSON(result)
	}

	// Convert to template format
	applies := make([]templates.ApplyHistoryData, 0, len(result.Applies))
	for _, a := range result.Applies {
		applies = append(applies, templates.ApplyHistoryData{
			ApplyID:     a.ApplyID,
			Environment: a.Environment,
			State:       a.State,
			Engine:      a.Engine,
			Caller:      a.Caller,
			StartedAt:   a.StartedAt,
			CompletedAt: a.CompletedAt,
			Error:       a.Error,
		})
	}

	templates.WriteDatabaseHistory(templates.DatabaseHistoryData{
		Database: database,
		Applies:  applies,
	})

	return nil
}
