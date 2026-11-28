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
	Type     string `short:"t" help:"Database type registered with the server" default:"mysql"`
	Force    bool   `help:"Force release lock (bypass ownership check). Asks for confirmation before releasing another owner's lock"`
	// AutoApprove answers the confirmation --force asks before it releases a
	// lock another owner holds.
	AutoApprove bool `short:"y" help:"Skip confirmation prompt" name:"auto-approve"`
}

// Run executes the unlock command.
func (cmd *UnlockCmd) Run(g *Globals) error {
	// Lock records are keyed canonically, so every message below echoes the
	// stored identity of the lock rather than the flag spelling.
	database := storage.CanonicalKey(cmd.Database)
	dbType := storage.CanonicalKey(cmd.Type)

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
			existingLock, lockErr = client.GetLock(ep, database, dbType)
			return lockErr
		})
		if err != nil {
			return fmt.Errorf("check lock: %w", err)
		}
		if existingLock == nil {
			reportNoLockFound(ep, database, dbType)
			return nil
		}

		// Taking over another owner's lock is asked about before anything is
		// sent to the server. A lock this command's owner holds is released
		// without a question, as an owned release is.
		if heldByAnotherOwner(existingLock, owner) && !cmd.AutoApprove {
			if !stdinIsTerminal() {
				return errLockTakeoverNeedsApproval(database, dbType, existingLock, "it was not released", "release it")
			}
			if existingLock.ID < 1 {
				return errLockTakeoverUnidentified(database, dbType, existingLock, "it was not released", "release it")
			}
			templates.WriteLockTakeoverWarning(lockHolderData(database, dbType, existingLock), templates.UnlockTakeoverConsequence)
			confirmed, err := confirmAction(templates.UnlockTakeoverPrompt, "\nLock not changed.")
			if err != nil {
				return err
			}
			if !confirmed {
				return nil
			}
			// The answer covers the lock the prompt named. A lock released or
			// taken again while the prompt was open is not that lock.
			var current *client.LockInfo
			if err := withLoading("Checking database lock...", true, func() error {
				var lockErr error
				current, lockErr = client.GetLock(ep, database, dbType)
				return lockErr
			}); err != nil {
				return fmt.Errorf("check lock: %w", err)
			}
			if current == nil {
				reportNoLockFound(ep, database, dbType)
				return nil
			}
			if !sameLockHeld(existingLock, current) {
				return errLockChangedSincePrompt(database, dbType, current)
			}
		}

		if err := withLoading("Releasing database lock...", true, func() error {
			return client.ForceReleaseLock(ep, database, dbType)
		}); err != nil {
			return fmt.Errorf("force release lock: %w", err)
		}
		templates.WriteLockForceReleased(database, dbType, existingLock.Owner)
		return nil
	}

	// Normal release - ownership required
	err = withLoading("Releasing database lock...", true, func() error {
		return client.ReleaseLock(ep, database, dbType, owner)
	})
	if errors.Is(err, client.ErrLockNotFound) {
		reportNoLockFound(ep, database, dbType)
		return nil
	}
	if errors.Is(err, client.ErrLockNotOwned) {
		// Show current owner
		var existingLock *client.LockInfo
		getErr := withLoading("Checking database lock...", true, func() error {
			var lockErr error
			existingLock, lockErr = client.GetLock(ep, database, dbType)
			return lockErr
		})
		if getErr != nil || existingLock == nil {
			return fmt.Errorf("lock is not owned by you")
		}
		templates.WriteUnlockNotOwned(database, dbType, existingLock.Owner)
		return ErrSilent
	}
	if err != nil {
		return fmt.Errorf("release lock: %w", err)
	}

	templates.WriteLockReleased(database, dbType)
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
		templates.WriteLockExistsUnderOtherType(database, dbType, storage.CanonicalKey(other.DatabaseType))
		return
	}
	templates.WriteNoLockFound(database, dbType)
}

// lockUnderOtherType returns a lock held on the database under a database
// type other than the requested one, or nil if none exists. Both sides are
// compared canonically: locks have no expiry, so a row written before the
// server folded lock keys can still be listed in its original spelling.
func lockUnderOtherType(locks []*client.LockInfo, database, dbType string) *client.LockInfo {
	database = storage.CanonicalKey(database)
	dbType = storage.CanonicalKey(dbType)
	for _, lock := range locks {
		if storage.CanonicalKey(lock.Database) == database && storage.CanonicalKey(lock.DatabaseType) != dbType {
			return lock
		}
	}
	return nil
}
