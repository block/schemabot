package templates

import (
	"fmt"
	"time"
)

func previewLockAcquiredOutput() {
	WriteLockAcquired(LockData{
		Database:     "testapp",
		DatabaseType: "mysql",
		Owner:        "cli:jackjackbits@macbook",
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

func previewLockKeptOutput() {
	WriteLockKept("testapp", "mysql", "apply apply-a1b2c3d4 is stopped and can still be resumed")
}

func previewUnsubmittedLockNotReleasedOutput() {
	WriteUnsubmittedLockKept("testapp", "mysql", fmt.Errorf("API error (500): internal error"))
	fmt.Println()
	fmt.Println("--- Nothing submitted, release request got no answer ---")
	fmt.Println()
	WriteUnsubmittedLockReleaseUnknown("testapp", "mysql")
	fmt.Println()
	fmt.Println("--- Nothing submitted, lock already released ---")
	fmt.Println()
	WriteUnsubmittedLockAlreadyReleased("testapp", "mysql")
	fmt.Println()
	fmt.Println("--- Nothing submitted, lock held by another owner ---")
	fmt.Println()
	WriteUnsubmittedLockHeldByAnotherOwner("testapp", "mysql")
}

func previewAcquireUnknownOutput() {
	WriteAcquireUnknownLockTaken("testapp", "mysql")
	fmt.Println()
	fmt.Println("--- Lock is the one you already held ---")
	fmt.Println()
	WriteAcquireUnknownLockHeldBefore("testapp", "mysql")
	fmt.Println()
	fmt.Println("--- Lock held by another owner ---")
	fmt.Println()
	WriteAcquireUnknownLockHeldByAnotherOwner("testapp", "mysql", "block/schemabot#123")
	fmt.Println()
	fmt.Println("--- Lock not held ---")
	fmt.Println()
	WriteAcquireUnknownLockFree("testapp", "mysql")
	fmt.Println()
	fmt.Println("--- Lock could not be checked ---")
	fmt.Println()
	WriteAcquireUnknownLockUnchecked("testapp", "mysql")
}

func previewLockInUseOutput() {
	WriteLockInUse("testapp", "mysql", "apply-b2c3d4e5", "running", "staging")
}

func previewLockReplacedOutput() {
	WriteLockReplaced("testapp", "mysql")
	fmt.Println()
	fmt.Println("--- Apply settled, lock now held by another owner ---")
	fmt.Println()
	WriteLockHeldByAnotherOwner("testapp", "mysql")
}

func previewLockKeptWithoutIDOutput() {
	WriteLockKeptWithoutID("testapp", "mysql")
}

func previewLocksListOutput() {
	locks := []LockData{
		{
			Database:     "testapp",
			DatabaseType: "mysql",
			Owner:        "cli:jackjackbits@macbook",
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

func previewLockExistsUnderOtherTypeOutput() {
	WriteLockExistsUnderOtherType("testapp", "mysql", "vitess")
}

func previewUnlockNotOwnedOutput() {
	WriteUnlockNotOwned("testapp", "mysql", "block/schemabot#123")
}
