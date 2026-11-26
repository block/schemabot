package templates

import (
	"fmt"
	"time"

	"github.com/block/schemabot/pkg/cmd/cliname"
	"github.com/block/schemabot/pkg/glyph"
	"github.com/block/schemabot/pkg/ui"
)

// LockData contains data for rendering lock information. Whoever takes a lock
// chooses its owner and repository, and the lock list reports the database and
// type of every lock the server holds, so the templates remove control runes
// from each of those where they print it.
type LockData struct {
	Database     string
	DatabaseType string
	Owner        string
	Repository   string
	PullRequest  int
	CreatedAt    time.Time
	UpdatedAt    time.Time
}

// WriteLockAcquired writes the lock acquired message.
func WriteLockAcquired(data LockData) {
	fmt.Printf("🔒 Lock acquired for %s (%s)\n", data.Database, data.DatabaseType)
}

// WriteLockReleased writes the lock released message.
func WriteLockReleased(database, dbType string) {
	fmt.Printf("🔓 Lock released for %s (%s)\n", database, dbType)
}

// WriteLockKept writes the message when --yield keeps the database lock:
// the apply has not settled, a row under a settled apply can still write, or
// the server did not release it. reason says which; the second line names the
// command that releases the lock by hand once nothing under the apply is
// left to run.
func WriteLockKept(database, dbType, reason string) {
	fmt.Printf("🔒 Lock kept for %s (%s) despite --yield: %s.\n", database, dbType, reason)
	fmt.Printf("   Release it once nothing is left to run: %s\n", cliname.Command("unlock", "-d", database, "-t", dbType))
}

// WriteUnsubmittedLockKept writes the message when the lock a command took
// for a schema change it never submitted could not be released. Nothing runs
// under the lock, so the second line says it can be released now.
func WriteUnsubmittedLockKept(database, dbType string, releaseErr error) {
	fmt.Printf("🔒 Lock kept for %s (%s): the server did not release it (%v).\n", database, dbType, releaseErr)
	fmt.Printf("   Nothing was submitted, so release it now: %s\n", cliname.Command("unlock", "-d", database, "-t", dbType))
}

// WriteUnsubmittedLockReleaseUnknown writes the message when the request to
// release the lock a command took for a schema change it never submitted
// failed after the server may have received it, so the lock may or may not
// still be held. Unlock is right either way: it releases a lock that is still
// held and reports that none is found otherwise.
func WriteUnsubmittedLockReleaseUnknown(database, dbType string) {
	fmt.Printf("🔒 Lock for %s (%s) may still be held: the release request failed, so its result is unknown.\n", database, dbType)
	fmt.Printf("   Nothing was submitted. Release it if it is still held: %s\n", cliname.Command("unlock", "-d", database, "-t", dbType))
}

// WriteAcquireUnknownLockHeldByOwner writes the message when a command's
// request to take the lock failed after the server may have received it, and
// a lookup then found a lock row held under the command's owner that is not
// the row it saw held before. Every command the same user runs on the same
// host shares that owner, so the row may be this acquire's or another
// command's; the message does not say which. The command then releases the
// row only if no apply runs under it, and the release's own message follows.
func WriteAcquireUnknownLockHeldByOwner(database, dbType string) {
	fmt.Printf("🔒 Lock for %s (%s) is held under your owner, though the acquire request failed.\n", database, dbType)
	fmt.Println("   Nothing was submitted, so it is released unless an apply runs under it.")
}

// WriteAcquireUnknownLockWithoutID writes the message when a command's request
// to take the lock failed after the server may have received it, and a lookup
// found a lock held under the command's owner but did not report which lock
// row it is. Without the row, the command cannot tell whether the acquire took
// it or release only that row, so the lock is left in place and no unlock
// command is offered.
func WriteAcquireUnknownLockWithoutID(database, dbType string) {
	fmt.Printf("🔒 Lock for %s (%s) is held under your owner, but the server did not say which lock it is, so it was left in place.\n", database, dbType)
	fmt.Printf("   Nothing was submitted. See who holds it: %s\n", cliname.Command("locks"))
}

// WriteAcquireUnknownLockHeldBefore writes the message when a command's
// request to take the lock failed after the server may have received it, and
// a lookup found the same lock the owner already held before the command ran.
// The acquire took nothing new, and that lock may guard an apply an earlier
// command started, so no unlock command is offered.
func WriteAcquireUnknownLockHeldBefore(database, dbType string) {
	fmt.Printf("🔒 Lock for %s (%s) is the one you already held, so the failed acquire took nothing.\n", database, dbType)
	fmt.Println("   Nothing was submitted. Run the command again to retry.")
}

// WriteAcquireUnknownLockHeldByAnotherOwner writes the message when a
// command's request to take the lock failed after the server may have received
// it, and a lookup found the lock held by another owner. The owner is whatever
// the caller that took the lock named itself, so its control runes are
// removed.
func WriteAcquireUnknownLockHeldByAnotherOwner(database, dbType, holder string) {
	fmt.Printf("🔒 Lock for %s (%s) is held by %s, so the failed acquire took nothing.\n", database, dbType, ui.TerminalSafeLine(holder))
	fmt.Println("   Nothing was submitted. Run the command again once that lock is released.")
}

// WriteAcquireUnknownLockFree writes the message when a command's request to
// take the lock failed after the server may have received it, and a lookup
// found no lock held. Nothing was taken or submitted, so running the command
// again is safe.
func WriteAcquireUnknownLockFree(database, dbType string) {
	fmt.Printf("🔓 Lock for %s (%s) is not held, so the failed acquire took nothing.\n", database, dbType)
	fmt.Println("   Nothing was submitted. It is safe to run the command again.")
}

// WriteAcquireUnknownLockUnchecked writes the message when a command's request
// to take the lock failed after the server may have received it, and the
// lookup that would say whether it took the lock failed too. Both lines hold
// whichever way the acquire went.
func WriteAcquireUnknownLockUnchecked(database, dbType string) {
	fmt.Printf("🔒 Lock for %s (%s) may have been taken: the acquire request failed, and the lock could not be checked.\n", database, dbType)
	fmt.Printf("   Nothing was submitted. See who holds it: %s\n", cliname.Command("locks"))
	fmt.Printf("   If you hold it and no apply of yours runs under it, release it: %s\n", cliname.Command("unlock", "-d", database, "-t", dbType))
}

// WriteUnsubmittedLockAlreadyReleased writes the message when the lock a
// command took for a schema change it never submitted was gone by the time the
// command released it. Nothing is held, so no unlock command is offered.
func WriteUnsubmittedLockAlreadyReleased(database, dbType string) {
	fmt.Printf("🔓 Lock for %s (%s) was already released, so nothing is left to unlock.\n", database, dbType)
}

// WriteUnsubmittedLockHeldByAnotherOwner writes the message when the lock a
// command took for a schema change it never submitted has since been released
// and taken under another owner. That lock is not this command's, so it is
// left held and no unlock command is offered: unlock would refuse it, and
// forcing it would take the lock from whoever holds it now.
func WriteUnsubmittedLockHeldByAnotherOwner(database, dbType string) {
	fmt.Printf("🔒 Lock for %s (%s) is now held by another owner, so it was left in place.\n", database, dbType)
	fmt.Printf("   Nothing was submitted and this command holds no lock. See who holds it: %s\n", cliname.Command("locks"))
}

// WriteLockInUse writes the message when a command's release of the lock it
// took was refused because an apply that can still change runs under that
// lock: another command of the same owner reused it, or the command's own
// apply has not settled. The apply's identifier, state, and environment come
// from the server, so their control runes are removed; the second line names
// the command that watches the apply. A server that names no apply gets the
// command that shows who holds the lock instead.
func WriteLockInUse(database, dbType, applyID, applyState, environment string) {
	if applyID == "" {
		fmt.Printf("🔒 Lock kept for %s (%s): an apply still runs under it.\n", database, dbType)
		fmt.Printf("   See who holds it: %s\n", cliname.Command("locks"))
		return
	}
	applyID = ui.TerminalSafeLine(applyID)
	fmt.Printf("🔒 Lock kept for %s (%s): apply %s (%s) runs under it.\n", database, dbType, applyID, ui.TerminalSafeLine(applyState))
	fmt.Printf("   Watch that apply: %s\n", ProgressCommand(applyID, ui.TerminalSafeLine(environment)))
}

// WriteLockReplaced writes the message when the lock a command took was
// released and taken again, possibly under the same owner, before the command
// released it. The lock held now is not the command's, so it is left in place
// and no unlock command is offered.
func WriteLockReplaced(database, dbType string) {
	fmt.Printf("🔒 Lock for %s (%s) was released and taken again since this command took it, so it was left in place.\n", database, dbType)
	fmt.Printf("   This command released nothing. See who holds it: %s\n", cliname.Command("locks"))
}

// WriteLockHeldByAnotherOwner writes the message when --yield finds the lock
// its apply ran under is now held by another owner. That lock is not this
// command's, so it is left held and no unlock command is offered.
func WriteLockHeldByAnotherOwner(database, dbType string) {
	fmt.Printf("🔒 Lock for %s (%s) is now held by another owner, so it was left in place.\n", database, dbType)
	fmt.Printf("   This command released nothing. See who holds it: %s\n", cliname.Command("locks"))
}

// WriteLockKeptWithoutID writes the message when a command keeps the lock it
// would release because the server did not report which lock row it took.
// Releasing by owner alone could remove a lock another command of the same
// owner runs an apply under, so the operator is shown how to check and
// release it themselves.
func WriteLockKeptWithoutID(database, dbType string) {
	fmt.Printf("🔒 Lock kept for %s (%s): the server did not say which lock this command took, so releasing it could remove a lock another command uses.\n", database, dbType)
	fmt.Printf("   See who holds it: %s\n", cliname.Command("locks"))
	fmt.Printf("   If no apply of yours runs under it, release it: %s\n", cliname.Command("unlock", "-d", database, "-t", dbType))
}

// WriteLockForceReleased writes the force release message. The previous owner
// is whatever the caller that took the lock named itself, so its control runes
// are removed, as for every lock owner these templates print.
func WriteLockForceReleased(database, dbType, previousOwner string) {
	fmt.Printf(glyph.Attention+"  Force released lock for %s (%s)\n", database, dbType)
	fmt.Printf("   Previous owner: %s\n", ui.TerminalSafeLine(previousOwner))
}

// LockConflictData contains data for a lock conflict error.
type LockConflictData struct {
	Database     string
	DatabaseType string
	Owner        string
	Repository   string
	PullRequest  int
	CreatedAt    time.Time
}

// WriteLockConflict writes the lock conflict error message.
func WriteLockConflict(data LockConflictData) {
	fmt.Println()
	fmt.Println(glyph.Refused + " Apply blocked: database locked")
	fmt.Println()

	// Show a table of lock info
	rows := []BoxRow{
		{"Database", fmt.Sprintf("%s (%s)", data.Database, data.DatabaseType)},
		{"Locked by", ui.TerminalSafeLine(data.Owner)},
		{"Since", formatLockTime(data.CreatedAt)},
	}
	if data.Repository != "" && data.PullRequest > 0 {
		rows = append(rows, BoxRow{"PR", fmt.Sprintf("%s#%d", ui.TerminalSafeLine(data.Repository), data.PullRequest)})
	}
	WriteBox(rows, "", nil)
	fmt.Println()

	fmt.Println("Another schema change is in progress for this database.")
	fmt.Println()
	fmt.Println("Options:")
	fmt.Println("  • Wait for the current schema change to complete")
	fmt.Printf("  • Ask the lock owner to release: %s\n", cliname.Command("unlock", "-d", data.Database))
	fmt.Printf("  • Force unlock: %s\n", cliname.Command("unlock", "-d", data.Database, "--force"))
	fmt.Println()
}

// WriteLocksList writes a formatted list of active locks.
func WriteLocksList(locks []LockData) {
	if len(locks) == 0 {
		fmt.Println("No active locks.")
		return
	}

	fmt.Printf("🔒 Active Locks (%d)\n\n", len(locks))

	for i, lock := range locks {
		fmt.Printf("  %d. %s (%s)\n", i+1, ui.TerminalSafeLine(lock.Database), ui.TerminalSafeLine(lock.DatabaseType))
		fmt.Printf("     Owner: %s\n", ui.TerminalSafeLine(lock.Owner))
		fmt.Printf("     Since: %s\n", formatLockTime(lock.CreatedAt))
		if lock.Repository != "" && lock.PullRequest > 0 {
			fmt.Printf("     PR:    %s#%d\n", ui.TerminalSafeLine(lock.Repository), lock.PullRequest)
		}
		if !lock.UpdatedAt.IsZero() && lock.UpdatedAt.After(lock.CreatedAt) {
			fmt.Printf("     Last activity: %s\n", formatLockTime(lock.UpdatedAt))
		}
		fmt.Println()
	}

	fmt.Println("To release a lock:")
	fmt.Printf("  %s\n", cliname.Command("unlock", "-d", "<database>", "-t", "<type>"))
	fmt.Printf("  %s  # override ownership\n", cliname.Command("unlock", "-d", "<database>", "-t", "<type>", "--force"))
}

// WriteNoLockFound writes the message when a lock doesn't exist.
func WriteNoLockFound(database, dbType string) {
	fmt.Printf("No lock found for %s (%s)\n", database, dbType)
}

// WriteLockExistsUnderOtherType writes the message when the database is not
// locked under the requested type but does hold a lock under another database
// type. Lock lookups are keyed by (database, type) and the type flag defaults
// to mysql, so an operator targeting a vitess database without -t searches
// the wrong namespace — point them at the lock that actually exists. The type
// found is read from the server's lock, so its control runes are removed.
func WriteLockExistsUnderOtherType(database, requestedType, foundType string) {
	foundType = ui.TerminalSafeLine(foundType)
	fmt.Printf("No lock found for %s (%s), but a %s lock exists for this database.\n", database, requestedType, foundType)
	fmt.Printf("Release it with: %s\n", cliname.Command("unlock", "-d", database, "-t", foundType))
}

// WriteLockTypeScanFailed writes the message when the check for locks under
// other database types could not run, so a "no lock found" answer is only
// authoritative for the requested type.
func WriteLockTypeScanFailed(err error) {
	fmt.Printf("Could not check for locks under other database types: %s\n", ui.TerminalSafeInline(err.Error()))
}

// WriteUnlockNotOwned writes the message when trying to unlock without ownership.
func WriteUnlockNotOwned(database, dbType, currentOwner string) {
	fmt.Println()
	fmt.Println(glyph.Attention + "  Cannot release lock")
	fmt.Println()
	fmt.Printf("  Database:      %s (%s)\n", database, dbType)
	fmt.Printf("  Current owner: %s\n", ui.TerminalSafeLine(currentOwner))
	fmt.Println()
	fmt.Println("You can only release locks that you own.")
	fmt.Println("Use --force to release a lock owned by someone else.")
	fmt.Println()
}

// formatLockTime formats a time for lock display.
func formatLockTime(t time.Time) string {
	if t.IsZero() {
		return "unknown"
	}
	return ui.FormatTimeAgo(t)
}
