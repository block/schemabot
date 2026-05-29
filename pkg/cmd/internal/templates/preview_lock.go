package templates

import (
	"fmt"
	"time"
)

func previewLockAcquiredOutput() {
	WriteLockAcquired(LockData{
		Database:     "testapp",
		DatabaseType: "mysql",
		Owner:        "cli:aparajon@macbook",
		CreatedAt:    previewTime,
	})
}

func previewLockConflictOutput() {
	WriteLockConflict(LockConflictData{
		Database:     "testapp",
		DatabaseType: "mysql",
		Owner:        "block/schemabot#123",
		Repository:   "block/schemabot",
		PullRequest:  123,
		CreatedAt:    previewTime.Add(-2 * time.Hour),
	})
}

func previewLockReleasedOutput() {
	WriteLockReleased("testapp", "mysql")
	fmt.Println()
	fmt.Println("--- Force release ---")
	fmt.Println()
	WriteLockForceReleased("testapp", "mysql", "block/schemabot#123")
}

func previewLocksListOutput() {
	locks := []LockData{
		{
			Database:     "testapp",
			DatabaseType: "mysql",
			Owner:        "cli:aparajon@macbook",
			CreatedAt:    previewTime.Add(-30 * time.Minute),
			UpdatedAt:    previewTime.Add(-5 * time.Minute),
		},
		{
			Database:     "payments",
			DatabaseType: "vitess",
			Owner:        "block/payments-api#456",
			Repository:   "block/payments-api",
			PullRequest:  456,
			CreatedAt:    previewTime.Add(-3 * time.Hour),
		},
	}
	WriteLocksList(locks)
	fmt.Println("--- No locks ---")
	fmt.Println()
	WriteLocksList(nil)
}

func previewLockConflictByCLIOutput() {
	WriteLockConflict(LockConflictData{
		Database:     "testapp",
		DatabaseType: "mysql",
		Owner:        "cli:deploy@prod-host.example.com",
		CreatedAt:    previewTime.Add(-45 * time.Minute),
	})
}

func previewNoLockFoundOutput() {
	WriteNoLockFound("testapp", "mysql")
}

func previewUnlockNotOwnedOutput() {
	WriteUnlockNotOwned("testapp", "mysql", "block/schemabot#123")
}
