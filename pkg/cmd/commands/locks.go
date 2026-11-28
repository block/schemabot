package commands

import (
	"fmt"

	"github.com/block/schemabot/pkg/cmd/client"
	"github.com/block/schemabot/pkg/cmd/internal/templates"
)

// LocksCmd lists all active database locks.
type LocksCmd struct {
	JSON bool `help:"Output as JSON"`
}

// locksJSON is the `locks --json` document, the same shape as GET /api/locks.
type locksJSON struct {
	Locks []*client.LockInfo `json:"locks"`
}

// Run executes the locks command.
func (cmd *LocksCmd) Run(g *Globals) error {
	ep, err := resolveEndpoint(g.Endpoint, g.Profile)
	if err != nil {
		return err
	}

	var locks []*client.LockInfo
	err = withLoading("Loading locks...", !cmd.JSON, func() error {
		var loadErr error
		locks, loadErr = client.ListLocks(ep)
		return loadErr
	})
	if err != nil {
		return fmt.Errorf("list locks: %w", err)
	}

	if cmd.JSON {
		// An empty list is [], never null, so a caller can range over it.
		if locks == nil {
			locks = []*client.LockInfo{}
		}
		return writeJSON(locksJSON{Locks: locks})
	}

	// Convert to template data
	lockData := make([]templates.LockData, len(locks))
	for i, lock := range locks {
		lockData[i] = templates.LockData{
			Database:     lock.Database,
			DatabaseType: lock.DatabaseType,
			Owner:        lock.Owner,
			Repository:   lock.Repository,
			PullRequest:  lock.PullRequest,
			CreatedAt:    lock.CreatedAt,
			UpdatedAt:    lock.UpdatedAt,
		}
	}

	templates.WriteLocksList(lockData)
	return nil
}
