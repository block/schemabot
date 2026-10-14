package planetscale

import (
	"fmt"
	"maps"
	"time"

	"github.com/block/schemabot/pkg/engine"
)

// waitHeartbeatInterval paces the progress events a long engine wait emits. It
// sits well inside the window after which the driver treats a drive that has
// written nothing as wedged. It is a variable so tests can shorten it.
var waitHeartbeatInterval = time.Minute

// waitHeartbeat reports a wait that can outlast the driver's stall window:
// preparing a branch, PlanetScale computing a deploy request's diff, deploy
// validation, a schema snapshot that defers a VSchema write. Without it these
// waits are silent, and the driver cancels a healthy drive as wedged and
// resumes it, only to wait again. Each event reaches the apply's timeline, so
// an operator also sees what the apply is waiting on.
//
// The heartbeat beats from the wait's own poll loop rather than from a
// goroutine, so its events are never delivered concurrently with the drive's
// other events, and a wait that is truly blocked emits nothing and is still
// cancelled. Every wait that uses it is bounded on its own terms.
type waitHeartbeat struct {
	emit     func(engine.ApplyEvent)
	message  string
	metadata map[string]string
	start    time.Time
	last     time.Time
}

// newWaitHeartbeat starts timing a wait. A nil emit makes beat a no-op, for
// callers outside a drive.
func newWaitHeartbeat(emit func(engine.ApplyEvent), message string, metadata map[string]string) *waitHeartbeat {
	now := time.Now()
	return &waitHeartbeat{emit: emit, message: message, metadata: metadata, start: now, last: now}
}

// beat emits a progress event when waitHeartbeatInterval has passed since the
// wait began or since the previous event.
func (h *waitHeartbeat) beat() {
	if h.emit == nil {
		return
	}
	now := time.Now()
	if now.Sub(h.last) < waitHeartbeatInterval {
		return
	}
	h.last = now
	h.emit(engine.ApplyEvent{
		Message:  fmt.Sprintf("%s (%s elapsed)", h.message, now.Sub(h.start).Round(time.Second)),
		Metadata: maps.Clone(h.metadata),
	})
}
