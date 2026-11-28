package commands

import (
	"fmt"
	"time"

	"github.com/block/schemabot/pkg/apitypes"
	"github.com/block/schemabot/pkg/cmd/client"
	"github.com/block/schemabot/pkg/cmd/internal/templates"
)

// FreezeCmd groups the environment freeze commands. A bare `freeze` with
// flags creates one, since that is the command an incident commander types
// first and fastest.
//
// A failed call returns the server's error as is: it already names what to
// fix (not an approver, no such freeze, already approved), and a wrapper
// would only bury it.
type FreezeCmd struct {
	Create   FreezeCreateCmd   `cmd:"" default:"withargs" help:"Freeze an environment, or some of its deployments and databases" group:"commands"`
	List     FreezeListCmd     `cmd:"" help:"List freezes in effect, scheduled and recent" group:"commands"`
	Show     FreezeShowCmd     `cmd:"" help:"Show one freeze" group:"commands"`
	Approve  FreezeApproveCmd  `cmd:"" help:"Approve a freeze override request" group:"commands"`
	Override FreezeOverrideCmd `cmd:"" help:"Show a freeze override request" group:"commands"`
}

// FreezeCreateCmd sets an environment freeze.
type FreezeCreateCmd struct {
	Environment string   `short:"e" required:"" help:"Environment to freeze"`
	Reason      string   `required:"" help:"Why the freeze is set; shown wherever it holds a change"`
	Running     string   `help:"What happens to applies already pending or running in scope: continue (default) lets them finish; stop stops them when the freeze is set"`
	Deployment  []string `help:"Limit the freeze to this deployment (repeatable)"`
	Database    []string `help:"Limit the freeze to this database (repeatable)"`
	Planned     bool     `help:"Schedule a planned freeze; needs --end"`
	Start       string   `help:"When the freeze starts, RFC3339 (default: now)"`
	End         string   `help:"When the freeze ends, RFC3339 (default: until lifted)"`
}

// Run executes the freeze command.
func (cmd *FreezeCreateCmd) Run(g *Globals) error {
	req := apitypes.CreateFreezeRequest{
		Environment: cmd.Environment,
		Deployments: cmd.Deployment,
		Databases:   cmd.Database,
		Running:     cmd.Running,
		Reason:      cmd.Reason,
	}
	if cmd.Planned {
		req.Kind = "planned"
	}
	var err error
	if req.StartsAt, err = parseFreezeTime("--start", cmd.Start); err != nil {
		return err
	}
	if req.EndsAt, err = parseFreezeTime("--end", cmd.End); err != nil {
		return err
	}
	ep, err := g.Resolve()
	if err != nil {
		return err
	}

	var freeze *apitypes.Freeze
	err = withLoading("Creating freeze...", true, func() error {
		var createErr error
		freeze, createErr = client.CreateFreeze(ep, req)
		return createErr
	})
	if err != nil {
		return err
	}
	templates.WriteFreezeCreated(*freeze)
	return nil
}

// parseFreezeTime reads an optional RFC3339 flag value.
func parseFreezeTime(flag, value string) (*time.Time, error) {
	if value == "" {
		return nil, nil
	}
	t, err := time.Parse(time.RFC3339, value)
	if err != nil {
		return nil, fmt.Errorf("%s must be an RFC3339 time such as 2026-12-01T09:00:00Z: %w", flag, err)
	}
	return &t, nil
}

// FreezeListCmd lists freezes.
type FreezeListCmd struct {
	Environment string `short:"e" help:"Only list freezes for this environment"`
	JSON        bool   `help:"Output as JSON"`
}

// Run executes the freeze list command.
func (cmd *FreezeListCmd) Run(g *Globals) error {
	ep, err := g.Resolve()
	if err != nil {
		return err
	}
	var list *apitypes.FreezeListResponse
	err = withLoading("Loading freezes...", !cmd.JSON, func() error {
		var listErr error
		list, listErr = client.ListFreezes(ep, cmd.Environment)
		return listErr
	})
	if err != nil {
		return err
	}
	if cmd.JSON {
		return writeJSON(list)
	}
	templates.WriteFreezeList(*list)
	return nil
}

// FreezeShowCmd shows one freeze.
type FreezeShowCmd struct {
	ID   string `arg:"" name:"freeze-id" help:"Freeze ID"`
	JSON bool   `help:"Output as JSON"`
}

// Run executes the freeze show command.
func (cmd *FreezeShowCmd) Run(g *Globals) error {
	ep, err := g.Resolve()
	if err != nil {
		return err
	}
	var freeze *apitypes.Freeze
	err = withLoading("Loading freeze...", !cmd.JSON, func() error {
		var getErr error
		freeze, getErr = client.GetFreeze(ep, cmd.ID)
		return getErr
	})
	if err != nil {
		return err
	}
	if cmd.JSON {
		return writeJSON(freeze)
	}
	templates.WriteFreezeShow(*freeze)
	return nil
}

// FreezeApproveCmd approves a freeze override request.
type FreezeApproveCmd struct {
	ID string `arg:"" name:"ovr-id" help:"Override request ID"`
}

// Run executes the freeze approve command.
func (cmd *FreezeApproveCmd) Run(g *Globals) error {
	ep, err := g.Resolve()
	if err != nil {
		return err
	}
	var approved *apitypes.FreezeOverrideRequest
	err = withLoading("Approving override request...", true, func() error {
		var approveErr error
		approved, approveErr = client.ApproveFreezeOverride(ep, cmd.ID)
		return approveErr
	})
	if err != nil {
		return err
	}
	templates.WriteFreezeOverrideApproved(*approved)
	return nil
}

// FreezeOverrideCmd shows a freeze override request.
type FreezeOverrideCmd struct {
	ID   string `arg:"" name:"ovr-id" help:"Override request ID"`
	JSON bool   `help:"Output as JSON"`
}

// Run executes the freeze override command.
func (cmd *FreezeOverrideCmd) Run(g *Globals) error {
	ep, err := g.Resolve()
	if err != nil {
		return err
	}
	var request *apitypes.FreezeOverrideRequest
	err = withLoading("Loading override request...", !cmd.JSON, func() error {
		var getErr error
		request, getErr = client.GetFreezeOverride(ep, cmd.ID)
		return getErr
	})
	if err != nil {
		return err
	}
	if cmd.JSON {
		return writeJSON(request)
	}
	templates.WriteFreezeOverride(*request)
	return nil
}

// UnfreezeCmd lifts a freeze early.
type UnfreezeCmd struct {
	ID     string `arg:"" name:"freeze-id" help:"Freeze ID"`
	Reason string `help:"Why the freeze is lifted"`
}

// Run executes the unfreeze command.
func (cmd *UnfreezeCmd) Run(g *Globals) error {
	ep, err := g.Resolve()
	if err != nil {
		return err
	}
	var lifted *apitypes.Freeze
	err = withLoading("Lifting freeze...", true, func() error {
		var liftErr error
		lifted, liftErr = client.LiftFreeze(ep, cmd.ID, cmd.Reason)
		return liftErr
	})
	if err != nil {
		return err
	}
	templates.WriteFreezeLifted(*lifted)
	return nil
}
