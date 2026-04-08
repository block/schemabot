package templates

import (
	"fmt"
	"time"

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
	fmt.Println("  • Ask the lock owner to release: schemabot unlock")
	fmt.Printf("  • Force unlock: schemabot unlock -d %s --force\n", data.Database)
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
	fmt.Println("  schemabot unlock -d <database> -t <type>")
	fmt.Println("  schemabot unlock -d <database> -t <type> --force  # override ownership")
}

// WriteNoLockFound writes the message when a lock doesn't exist.
func WriteNoLockFound(database, dbType string) {
	fmt.Printf("No lock found for %s (%s)\n", database, dbType)
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
