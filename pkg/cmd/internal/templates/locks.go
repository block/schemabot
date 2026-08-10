package templates

import (
	"fmt"
	"time"

	"github.com/block/schemabot/pkg/cmd/cliname"
	"github.com/block/schemabot/pkg/ui"
)

// LockData contains data for rendering lock information.
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

// WriteLockForceReleased writes the force release message.
func WriteLockForceReleased(database, dbType, previousOwner string) {
	fmt.Printf("⚠️  Force released lock for %s (%s)\n", database, dbType)
	fmt.Printf("   Previous owner: %s\n", previousOwner)
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
	fmt.Println("❌ Apply Blocked: Database Locked")
	fmt.Println()

	// Show a table of lock info
	rows := []BoxRow{
		{"Database", fmt.Sprintf("%s (%s)", data.Database, data.DatabaseType)},
		{"Locked by", data.Owner},
		{"Since", formatLockTime(data.CreatedAt)},
	}
	if data.Repository != "" && data.PullRequest > 0 {
		rows = append(rows, BoxRow{"PR", fmt.Sprintf("%s#%d", data.Repository, data.PullRequest)})
	}
	WriteBox(rows, "", nil)
	fmt.Println()

	fmt.Println("Another schema change is in progress for this database.")
	fmt.Println()
	fmt.Println("Options:")
	fmt.Println("  • Wait for the current schema change to complete")
	fmt.Printf("  • Ask the lock owner to release: %s unlock\n", cliname.Name())
	fmt.Printf("  • Force unlock: %s unlock -d %s --force\n", cliname.Name(), data.Database)
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
		fmt.Printf("  %d. %s (%s)\n", i+1, lock.Database, lock.DatabaseType)
		fmt.Printf("     Owner: %s\n", lock.Owner)
		fmt.Printf("     Since: %s\n", formatLockTime(lock.CreatedAt))
		if lock.Repository != "" && lock.PullRequest > 0 {
			fmt.Printf("     PR:    %s#%d\n", lock.Repository, lock.PullRequest)
		}
		if !lock.UpdatedAt.IsZero() && lock.UpdatedAt.After(lock.CreatedAt) {
			fmt.Printf("     Last activity: %s\n", formatLockTime(lock.UpdatedAt))
		}
		fmt.Println()
	}

	fmt.Println("To release a lock:")
	fmt.Printf("  %s unlock -d <database> -t <type>\n", cliname.Name())
	fmt.Printf("  %s unlock -d <database> -t <type> --force  # override ownership\n", cliname.Name())
}

// WriteNoLockFound writes the message when a lock doesn't exist.
func WriteNoLockFound(database, dbType string) {
	fmt.Printf("No lock found for %s (%s)\n", database, dbType)
}

// WriteLockExistsUnderOtherType writes the message when the database is not
// locked under the requested type but does hold a lock under another database
// type. Lock lookups are keyed by (database, type) and the type flag defaults
// to mysql, so an operator targeting a vitess database without -t searches
// the wrong namespace — point them at the lock that actually exists.
func WriteLockExistsUnderOtherType(database, requestedType, foundType string) {
	fmt.Printf("No lock found for %s (%s), but a %s lock exists for this database.\n", database, requestedType, foundType)
	fmt.Printf("Release it with: %s unlock -d %s -t %s\n", cliname.Name(), database, foundType)
}

// WriteLockTypeScanFailed writes the message when the check for locks under
// other database types could not run, so a "no lock found" answer is only
// authoritative for the requested type.
func WriteLockTypeScanFailed(err error) {
	fmt.Printf("Could not check for locks under other database types: %v\n", err)
}

// WriteUnlockNotOwned writes the message when trying to unlock without ownership.
func WriteUnlockNotOwned(database, dbType, currentOwner string) {
	fmt.Println()
	fmt.Println("⚠️  Cannot release lock")
	fmt.Println()
	fmt.Printf("  Database:      %s (%s)\n", database, dbType)
	fmt.Printf("  Current owner: %s\n", currentOwner)
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
