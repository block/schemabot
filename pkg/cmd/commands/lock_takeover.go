package commands

import (
	"fmt"
	"os"

	"github.com/block/schemabot/pkg/cmd/client"
	"github.com/block/schemabot/pkg/cmd/internal/templates"
	"github.com/block/schemabot/pkg/ui"
)

// stdinIsTerminal reports whether a person can answer a prompt. It is a
// variable so tests can stand in for a terminal.
var stdinIsTerminal = func() bool {
	if !ui.IsTerminal(os.Stdin) {
		return false
	}
	// The null device is a character device too, and it is what cron, CI, and
	// a container started without a terminal give a command as its input.
	// Nobody answers from it.
	in, inErr := os.Stdin.Stat()
	null, nullErr := os.Stat(os.DevNull)
	return inErr != nil || nullErr != nil || !os.SameFile(in, null)
}

// heldByAnotherOwner reports whether a lock exists and its owner is not the
// given owner. A lock this command's owner already holds, and no lock at all,
// are not taken from anyone.
func heldByAnotherOwner(lock *client.LockInfo, owner string) bool {
	return lock != nil && lock.Owner != owner
}

// sameLockHeld reports whether two lookups found the same lock: the same
// owner, and the same lock row when the server reports one. A lock released and
// taken again between the lookups is a different lock.
func sameLockHeld(a, b *client.LockInfo) bool {
	return a != nil && b != nil && a.Owner == b.Owner && a.ID == b.ID
}

// lockHolderData is the lock record as the takeover warning shows it.
func lockHolderData(database, dbType string, lock *client.LockInfo) templates.LockConflictData {
	return templates.LockConflictData{
		Database:     database,
		DatabaseType: dbType,
		Owner:        lock.Owner,
		Repository:   lock.Repository,
		PullRequest:  lock.PullRequest,
		CreatedAt:    lock.CreatedAt,
	}
}

// errLockTakeoverNeedsApproval is the refusal for a forced command that would
// take a lock from another owner when nothing can ask the operator. The holder
// is whatever the caller that took the lock named itself, so its control runes
// are removed. notDone says what the command left undone.
func errLockTakeoverNeedsApproval(database, dbType string, lock *client.LockInfo, notDone, instead string) error {
	return fmt.Errorf("the lock on %s (%s) is held by %s and there is no terminal to ask you on, so %s; add -y to %s without asking",
		database, dbType, ui.TerminalSafeLine(lock.Owner), notDone, instead)
}
