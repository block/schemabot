package commands

import (
	"fmt"
	"time"

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
	Last        string `help:"Only show applies active within this window, for example 6h or 2d; counts the latest activity on the apply or any of its deployments, so a long-running change stays listed; by default the list is bounded by --limit alone"`
	State       string `help:"Only show applies in this state, for example running, completed, or failed_retryable; cannot be combined with --failed"`
	Limit       int    `short:"n" help:"Maximum recent applies to show; without --state or --failed these are the finished ones, listed below every active apply (default 20, max 1000)"`
	Failed      bool   `help:"Show only failed recent applies" name:"failed"`
	After       string `help:"Show the page that follows this apply ID, the last one a previous page showed; repeat that page's filters"`
	All         bool   `help:"List every matching apply, page by page; with --json the pages merge into one applies array"`
	ExternalID  bool   `help:"Show external engine apply IDs" name:"external-id"`
	JSON        bool   `help:"Output as JSON"`
}

// allPagesStatusLimit is the page size --all asks for when --limit is not set.
// The server clamps it to its own cap, so a server with a lower cap still pages
// correctly, only in more requests.
const allPagesStatusLimit = 1000

// Run executes the status command.
func (cmd *StatusCmd) Run(g *Globals) error {
	ep, err := resolveEndpoint(g.Endpoint, g.Profile)
	if err != nil {
		return err
	}

	if cmd.After != "" || cmd.All {
		if cmd.ApplyIDArg != "" {
			return fmt.Errorf("--after and --all page the recent applies list; drop them to show one apply")
		}
		if cmd.Database != "" {
			return fmt.Errorf("--after and --all page the recent applies list; -d already shows every apply for the database")
		}
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
	if cmd.State != "" && cmd.Failed {
		return fmt.Errorf("--state cannot be combined with --failed")
	}
	var last time.Duration
	if cmd.Last != "" {
		window, err := parseOperatorDuration(cmd.Last)
		if err != nil {
			return fmt.Errorf("parse --last: %w", err)
		}
		last = window
	}
	opts := client.StatusOptions{
		Limit:       cmd.Limit,
		Environment: cmd.Environment,
		Deployment:  cmd.Deployment,
		State:       cmd.State,
		Failed:      cmd.Failed,
		Last:        last,
		After:       cmd.After,
	}
	if cmd.JSON {
		result, err := cmd.getStatus(ep, opts)
		if err != nil {
			return err
		}
		return writeJSON(result)
	}
	if cmd.State != "" || cmd.Failed {
		var result *apitypes.StatusResponse
		err = withLoading("Loading schema change status...", true, func() error {
			var loadErr error
			result, loadErr = cmd.getStatus(ep, opts)
			return loadErr
		})
		if err != nil {
			return err
		}
		templates.WriteStatusList(cmd.statusListData(result))
		return nil
	}
	return cmd.showSectionedStatus(ep, opts)
}

// getStatus fetches the page opts names, or every page with --all.
func (cmd *StatusCmd) getStatus(endpoint string, opts client.StatusOptions) (*apitypes.StatusResponse, error) {
	var result *apitypes.StatusResponse
	var err error
	if cmd.All {
		result, err = getAllStatusPages(endpoint, opts)
	} else {
		result, err = client.GetStatus(endpoint, opts)
	}
	if err != nil {
		return nil, err
	}
	if result.StartingAfter != opts.After {
		return nil, fmt.Errorf("the server did not continue after apply %s; it predates paging, so it cannot show the next page", opts.After)
	}
	return result, nil
}

// showSectionedStatus renders the default view: the finished applies a page at
// a time, below every active apply on the first page. A server that predates
// the finished-only filter lists everything for it, so that list is rendered
// on its own instead, in the order the server chose.
func (cmd *StatusCmd) showSectionedStatus(endpoint string, opts client.StatusOptions) error {
	finishedOpts := opts
	finishedOpts.Finished = true
	var finished, active *apitypes.StatusResponse
	err := withLoading("Loading schema change status...", true, func() error {
		var loadErr error
		finished, loadErr = cmd.getStatus(endpoint, finishedOpts)
		if loadErr != nil || finished.Active == nil || cmd.After != "" {
			return loadErr
		}
		activeOpts := opts
		activeOpts.Active = true
		activeOpts.Limit = allPagesStatusLimit
		active, loadErr = client.GetStatus(endpoint, activeOpts)
		if loadErr != nil {
			return fmt.Errorf("fetch active schema changes: %w", loadErr)
		}
		return nil
	})
	if err != nil {
		return err
	}
	data := cmd.statusListData(finished)
	if finished.Active == nil {
		templates.WriteStatusList(data)
		return nil
	}
	data.Sectioned = true
	if active != nil {
		data.Active = activeApplyDataFromResponses(active.Applies)
		data.ActiveHasMore = active.HasMore
	}
	templates.WriteStatusList(data)
	return nil
}

func (cmd *StatusCmd) statusListData(result *apitypes.StatusResponse) templates.StatusListData {
	return templates.StatusListData{
		ActiveCount:    result.ActiveCount,
		Limit:          result.Limit,
		MaxLimit:       result.MaxLimit,
		HasMore:        result.HasMore,
		FailuresOnly:   result.FailuresOnly,
		Last:           cmd.Last,
		StateFilter:    result.State,
		StateCounts:    result.StateCounts,
		ShowExternalID: cmd.ExternalID,
		Deployment:     cmd.Deployment,
		After:          cmd.After,
		Applies:        activeApplyDataFromResponses(result.Applies),
	}
}

func activeApplyDataFromResponses(responses []*apitypes.ActiveApplyResponse) []templates.ActiveApplyData {
	applies := make([]templates.ActiveApplyData, 0, len(responses))
	for _, a := range responses {
		applies = append(applies, activeApplyDataFromResponse(a))
	}
	return applies
}

// getAllStatusPages fetches the first page for opts and requests each next
// page after the last apply of the one before, until the server reports no
// more, merging every page into one response. The list is ordered by when each
// apply was created, which never changes, so the walk lists exactly once every
// apply that matches the filters throughout it. One created during the walk
// lands above the first page and is not listed, and one whose state moves into
// or out of a state filter mid-walk is listed only if the move happens before
// the walk reaches it. State counts are the first page's: every
// page counts the same filter, and the first count is the one taken before the
// walk, so they can disagree with the merged list if applies change state
// during it.
func getAllStatusPages(endpoint string, opts client.StatusOptions) (*apitypes.StatusResponse, error) {
	if opts.Limit == 0 {
		opts.Limit = allPagesStatusLimit
	}
	merged, err := client.GetStatus(endpoint, opts)
	if err != nil {
		return nil, err
	}
	page := merged
	for page.HasMore && len(page.Applies) > 0 {
		opts.After = page.Applies[len(page.Applies)-1].ApplyID
		page, err = client.GetStatus(endpoint, opts)
		if err != nil {
			return nil, fmt.Errorf("fetch the status page after apply %s: %w", opts.After, err)
		}
		if page.StartingAfter != opts.After {
			return nil, fmt.Errorf("the server did not continue after apply %s; it predates paging, so --all cannot list every apply", opts.After)
		}
		merged.Applies = append(merged.Applies, page.Applies...)
		merged.ActiveCount += page.ActiveCount
	}
	merged.HasMore = false
	return merged, nil
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
