package commands

import (
	"errors"
	"fmt"

	"github.com/block/schemabot/pkg/cmd/client"
	"github.com/block/schemabot/pkg/cmd/templates"
)

// UnlockCmd releases a database lock.
type UnlockCmd struct {
	Database string `short:"d" required:"" help:"Database name"`
	Type     string `short:"t" help:"Database type: mysql or vitess" default:"mysql"`
	Force    bool   `help:"Force release lock (bypass ownership check)"`
}

// Run executes the unlock command.
func (cmd *UnlockCmd) Run(g *Globals) error {
	ep, err := resolveEndpoint(g.Endpoint, g.Profile)
	if err != nil {
		return err
	}

	owner := client.GenerateCLIOwner()

	if cmd.Force {
		// Force release - get lock info first to show previous owner
		existingLock, err := client.GetLock(ep, cmd.Database, cmd.Type)
		if err != nil {
			return fmt.Errorf("check lock: %w", err)
		}
		if existingLock == nil {
			templates.WriteNoLockFound(cmd.Database, cmd.Type)
			return nil
		}

		if err := client.ForceReleaseLock(ep, cmd.Database, cmd.Type); err != nil {
			return fmt.Errorf("force release lock: %w", err)
		}
		templates.WriteLockForceReleased(cmd.Database, cmd.Type, existingLock.Owner)
		return nil
	}

	// Normal release - ownership required
	err = client.ReleaseLock(ep, cmd.Database, cmd.Type, owner)
	if errors.Is(err, client.ErrLockNotFound) {
		templates.WriteNoLockFound(cmd.Database, cmd.Type)
		return nil
	}
	if errors.Is(err, client.ErrLockNotOwned) {
		// Show current owner
		existingLock, getErr := client.GetLock(ep, cmd.Database, cmd.Type)
		if getErr != nil || existingLock == nil {
			return fmt.Errorf("lock is not owned by you")
		}
		templates.WriteUnlockNotOwned(cmd.Database, cmd.Type, existingLock.Owner)
		return ErrSilent
	}
	if err != nil {
		return fmt.Errorf("release lock: %w", err)
	}

	templates.WriteLockReleased(cmd.Database, cmd.Type)
	return nil
}
