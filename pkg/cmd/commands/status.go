package commands

import (
	"fmt"

	"github.com/block/schemabot/pkg/apitypes"
	"github.com/block/schemabot/pkg/cmd/client"
	"github.com/block/schemabot/pkg/cmd/internal/templates"
	"github.com/block/schemabot/pkg/state"
)

// StatusCmd shows schema change status.
type StatusCmd struct {
	ApplyIDArg  string `arg:"" optional:"" help:"Apply ID to show details for" name:"apply_id"`
	Database    string `short:"d" help:"Database name (show apply history)"`
	Environment string `short:"e" help:"Environment filter"`
	Deployment  string `help:"Deployment filter"`
	Limit       int    `short:"n" help:"Maximum recent applies to show (default 20, max 1000)"`
	Failed      bool   `help:"Show only failed recent applies" name:"failed"`
	ExternalID  bool   `help:"Show external engine apply IDs" name:"external-id"`
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
		return showApplyByID(ep, cmd.ApplyIDArg, cmd.JSON, !cmd.JSON)
	}

	// Mode 2: Show history for a database
	if cmd.Database != "" {
		return showDatabaseHistory(ep, cmd.Database, cmd.Environment, cmd.JSON, !cmd.JSON)
	}

	// Mode 3: List recent applies
	var result *apitypes.StatusResponse
	err = withLoading("Loading schema change status...", !cmd.JSON, func() error {
		var loadErr error
		result, loadErr = client.GetStatus(ep, client.StatusOptions{
			Limit:       cmd.Limit,
			Environment: cmd.Environment,
			Deployment:  cmd.Deployment,
			Failed:      cmd.Failed,
		})
		return loadErr
	})
	if err != nil {
		return err
	}

	if cmd.JSON {
		return writeJSON(result)
	}

	// Convert to template format
	applies := make([]templates.ActiveApplyData, 0, len(result.Applies))
	for _, a := range result.Applies {
		applies = append(applies, activeApplyDataFromResponse(a))
	}

	templates.WriteStatusList(templates.StatusListData{
		ActiveCount:    result.ActiveCount,
		Limit:          result.Limit,
		MaxLimit:       result.MaxLimit,
		HasMore:        result.HasMore,
		FailuresOnly:   result.FailuresOnly,
		ShowExternalID: cmd.ExternalID,
		Deployment:     cmd.Deployment,
		Applies:        applies,
	})

	return nil
}

func activeApplyDataFromResponse(a *apitypes.ActiveApplyResponse) templates.ActiveApplyData {
	return templates.ActiveApplyData{
		ApplyID:             a.ApplyID,
		ExternalID:          a.ExternalID,
		ExternalOperationID: a.ExternalOperationID,
		Database:            a.Database,
		Environment:         a.Environment,
		Deployment:          a.Deployment,
		State:               a.State,
		Engine:              a.Engine,
		Caller:              a.Caller,
		ErrorMessage:        a.ErrorMessage,
		StartedAt:           a.StartedAt,
		CompletedAt:         a.CompletedAt,
		UpdatedAt:           a.UpdatedAt,
		Volume:              a.Volume,
	}
}

// showApplyByID shows details for a specific apply by its ID.
func showApplyByID(endpoint, applyID string, outputJSON, showLoader bool) error {
	var result *apitypes.ProgressResponse
	err := withLoading("Loading schema change details...", showLoader, func() error {
		var loadErr error
		result, loadErr = client.GetProgress(endpoint, applyID)
		return loadErr
	})
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
func showDatabaseHistory(endpoint, database, environment string, outputJSON, showLoader bool) error {
	var result *apitypes.DatabaseHistoryResponse
	err := withLoading("Loading schema change history...", showLoader, func() error {
		var loadErr error
		result, loadErr = client.GetDatabaseHistory(endpoint, database, environment)
		return loadErr
	})
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
