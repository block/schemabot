package commands

import (
	"errors"
	"fmt"

	"github.com/block/schemabot/pkg/cmd/client"
	"github.com/block/schemabot/pkg/cmd/internal/templates"
	"github.com/block/schemabot/pkg/storage"
)

// UnlockCmd releases a database lock.
type UnlockCmd struct {
	Database string `short:"d" required:"" help:"Database name"`
	Type     string `short:"t" help:"Database type: mysql or vitess" default:"mysql"`
	Force    bool   `help:"Force release lock (bypass ownership check)"`
}

// Run executes the unlock command.
func (cmd *UnlockCmd) Run(g *Globals) error {
	// Lock records use canonical keys, and the alternate-type scan compares them byte-for-byte.
	cmd.Database = storage.CanonicalKey(cmd.Database)
	cmd.Type = storage.CanonicalKey(cmd.Type)

	ep, err := resolveEndpoint(g.Endpoint, g.Profile)
	if err != nil {
		return err
	}

	owner := client.GenerateCLIOwner()

	if cmd.Force {
		// Force release - get lock info first to show previous owner
		var existingLock *client.LockInfo
		err := withLoading("Checking database lock...", true, func() error {
			var lockErr error
			existingLock, lockErr = client.GetLock(ep, cmd.Database, cmd.Type)
			return lockErr
		})
		if err != nil {
			return fmt.Errorf("check lock: %w", err)
		}
		if existingLock == nil {
			reportNoLockFound(ep, cmd.Database, cmd.Type)
			return nil
		}

		if err := withLoading("Releasing database lock...", true, func() error {
			return client.ForceReleaseLock(ep, cmd.Database, cmd.Type)
		}); err != nil {
			return fmt.Errorf("force release lock: %w", err)
		}
		templates.WriteLockForceReleased(cmd.Database, cmd.Type, existingLock.Owner)
		return nil
	}

	// Normal release - ownership required
	err = withLoading("Releasing database lock...", true, func() error {
		return client.ReleaseLock(ep, cmd.Database, cmd.Type, owner)
	})
	if errors.Is(err, client.ErrLockNotFound) {
		reportNoLockFound(ep, cmd.Database, cmd.Type)
		return nil
	}
	if errors.Is(err, client.ErrLockNotOwned) {
		// Show current owner
		var existingLock *client.LockInfo
		getErr := withLoading("Checking database lock...", true, func() error {
			var lockErr error
			existingLock, lockErr = client.GetLock(ep, cmd.Database, cmd.Type)
			return lockErr
		})
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

// reportNoLockFound explains a missing lock. Lock lookups are keyed by
// (database, type) and the type flag defaults to mysql, so an operator
// releasing a vitess (or strata/postgres) database lock without -t searches
// the wrong namespace. When the database is locked under a different type,
// name that lock and the command that targets it instead of reporting a bare
// miss.
func reportNoLockFound(ep, database, dbType string) {
	var locks []*client.LockInfo
	err := withLoading("Checking database locks...", true, func() error {
		var listErr error
		locks, listErr = client.ListLocks(ep)
		return listErr
	})
	if err != nil {
		templates.WriteNoLockFound(database, dbType)
		templates.WriteLockTypeScanFailed(err)
		return
	}
	if other := lockUnderOtherType(locks, database, dbType); other != nil {
		templates.WriteLockExistsUnderOtherType(database, dbType, other.DatabaseType)
		return
	}
	templates.WriteNoLockFound(database, dbType)
}

// lockUnderOtherType returns a lock held on the database under a database
// type other than the requested one, or nil if none exists.
func lockUnderOtherType(locks []*client.LockInfo, database, dbType string) *client.LockInfo {
	for _, lock := range locks {
		if lock.Database == database && lock.DatabaseType != dbType {
			return lock
		}
	}
	return nil
}
