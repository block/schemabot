package commands

import (
	"fmt"

	"github.com/block/schemabot/pkg/cmd/client"
	"github.com/block/schemabot/pkg/cmd/internal/templates"
)

// LocksCmd lists all active database locks.
type LocksCmd struct{}

// Run executes the locks command.
func (cmd *LocksCmd) Run(g *Globals) error {
	ep, err := resolveEndpoint(g.Endpoint, g.Profile)
	if err != nil {
		return err
	}

	var locks []*client.LockInfo
	err = withLoading("Loading locks...", true, func() error {
		var loadErr error
		locks, loadErr = client.ListLocks(ep)
		return loadErr
	})
	if err != nil {
		return fmt.Errorf("list locks: %w", err)
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
