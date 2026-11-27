package apitypes

import (
	"fmt"
	"strconv"
	"time"
)

// Progress metadata keys describing a step that waits outside the database's
// lock queue for a conflicting session to end before it requests its lock:
// the waiting session's process ID, the table, how long the step has waited,
// and how long it waits at most, both in milliseconds. The keys are absent
// while no step waits.
const (
	LockHolderPIDMetadataKey    = "lock_holder_pid"
	LockHolderTableMetadataKey  = "lock_holder_table"
	LockHolderWaitedMetadataKey = "lock_holder_waited_ms"
	LockHolderLimitMetadataKey  = "lock_holder_limit_ms"
)

// LockHolderMetadataKeys lists every key a lock holder wait publishes, so an
// engine can clear them all once the wait ends.
var LockHolderMetadataKeys = []string{LockHolderPIDMetadataKey, LockHolderTableMetadataKey, LockHolderWaitedMetadataKey, LockHolderLimitMetadataKey}

// LockHolderWait is a step waiting for another session, which holds a lock
// that conflicts with the step's, to end. The step waits outside the lock
// queue, so the reads and writes queued behind it keep running. Table is
// empty when the engine does not name it. The zero value is no wait.
type LockHolderWait struct {
	PID    int64
	Table  string
	Waited time.Duration
	Limit  time.Duration
}

// Waiting reports whether a step is waiting on a lock holder.
func (w LockHolderWait) Waiting() bool { return w.PID > 0 }

// parseLockHolderWait decodes a lock holder wait from progress metadata. It
// returns the zero value with a nil error when no step waits, and an error when a
// published value is not a non-negative integer or the process ID is not
// positive.
func parseLockHolderWait(metadata map[string]string) (LockHolderWait, error) {
	rawPID, present := metadata[LockHolderPIDMetadataKey]
	if !present {
		return LockHolderWait{}, nil
	}
	pid, err := strconv.ParseInt(rawPID, 10, 64)
	if err != nil {
		return LockHolderWait{}, fmt.Errorf("progress metadata %s %q is not an integer: %w", LockHolderPIDMetadataKey, rawPID, err)
	}
	if pid <= 0 {
		return LockHolderWait{}, fmt.Errorf("progress metadata %s %d is not a process ID", LockHolderPIDMetadataKey, pid)
	}
	wait := LockHolderWait{PID: pid, Table: metadata[LockHolderTableMetadataKey]}
	for _, field := range []struct {
		key   string
		value *time.Duration
	}{
		{LockHolderWaitedMetadataKey, &wait.Waited},
		{LockHolderLimitMetadataKey, &wait.Limit},
	} {
		raw, present := metadata[field.key]
		if !present {
			continue
		}
		ms, err := strconv.ParseInt(raw, 10, 64)
		if err != nil {
			return LockHolderWait{}, fmt.Errorf("progress metadata %s %q is not an integer: %w", field.key, raw, err)
		}
		if ms < 0 {
			return LockHolderWait{}, fmt.Errorf("progress metadata %s %d is negative", field.key, ms)
		}
		*field.value = time.Duration(ms) * time.Millisecond
	}
	return wait, nil
}
