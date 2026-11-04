package tern

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/block/schemabot/pkg/storage"
)

// pendingControlReadSweepAfter is how long a finished read of an apply's
// pending control requests is kept before it is swept. It is far longer than
// any drive's poll interval, so only the reads of applies no drive is polling
// any more are dropped.
const pendingControlReadSweepAfter = time.Minute

// SharedPendingControlReads lets the drives of one apply on this process share
// their reads of its pending control requests. Every drive of an apply checks
// them on every poll, so an apply with many operations in flight would
// otherwise send the same query once per drive per poll. A drive asks through
// its own drivePendingControlReads, which joins a read another drive started
// recently, or one still in flight, instead of making its own.
//
// An apply's drives can run on different clients, such as one per target or
// one per deployment, so the code that builds those clients passes them one
// SharedPendingControlReads through LocalConfig or Config. Every client given
// the same value must use the same storage, since reads are keyed by the
// apply's storage ID. A client given none shares reads only among its own
// drives.
//
// Sharing a read only changes which query answers the check, not what a
// drive does with the answer. The set gates which processors run, and each
// processor loads its own request again before acting on it (see
// pendingControlOperations), so a request recorded after the shared read is
// answered on the drive's next poll, exactly as one recorded after a read of
// its own would be.
//
// The zero value is ready to use.
type SharedPendingControlReads struct {
	// now reads the clock. Nil selects time.Now; tests set it.
	now func() time.Time

	mu        sync.Mutex
	reads     map[int64]*pendingControlRead
	lastSweep time.Time
}

// pendingControlReadsOrNew returns shared, or a new set when it is nil.
func pendingControlReadsOrNew(shared *SharedPendingControlReads) *SharedPendingControlReads {
	if shared != nil {
		return shared
	}
	return &SharedPendingControlReads{}
}

// pendingControlRead is one read of an apply's pending control requests. done
// closes when the read finishes, after pending and err are set.
type pendingControlRead struct {
	started time.Time
	done    chan struct{}
	pending pendingControlOperations
	err     error
}

// drivePendingControlReads is one drive's handle on the shared reads. It
// remembers the last read the drive used, so the drive never answers two
// checks with the same read: every check it makes sees a read that started
// after its previous one. A drive running alone therefore reads storage on
// every check, as it would without sharing.
//
// A nil handle reads storage directly on every check.
type drivePendingControlReads struct {
	shared *SharedPendingControlReads
	// maxAge is how long before a check a read may have started and still
	// answer it.
	maxAge time.Duration
	last   *pendingControlRead
}

// forDrive returns a handle for one drive whose checks may be answered by a
// read that started up to maxAge earlier. A nil set returns a nil handle,
// which reads storage directly on every check.
func (s *SharedPendingControlReads) forDrive(maxAge time.Duration) *drivePendingControlReads {
	if s == nil {
		return nil
	}
	return &drivePendingControlReads{shared: s, maxAge: maxAge}
}

// load answers a check at the start of a poll. It joins the apply's latest
// read when the drive has not used it yet and it is still in flight or
// started within maxAge, and otherwise starts a new read.
func (d *drivePendingControlReads) load(ctx context.Context, store storage.Storage, apply *storage.Apply, logger *slog.Logger) (pendingControlOperations, error) {
	if d == nil {
		return loadPendingControlOperations(ctx, store, apply)
	}
	return d.loadStartedAfter(ctx, store, apply, d.shared.clock().Add(-d.maxAge), false, logger)
}

// loadFresh answers a check that must see every request recorded before it,
// such as the one a poll makes after its progress sync to catch a stop before
// it starts a cutover. It always starts a new read, which other drives' checks
// may then join.
func (d *drivePendingControlReads) loadFresh(ctx context.Context, store storage.Storage, apply *storage.Apply, logger *slog.Logger) (pendingControlOperations, error) {
	if d == nil {
		return loadPendingControlOperations(ctx, store, apply)
	}
	return d.loadStartedAfter(ctx, store, apply, time.Time{}, true, logger)
}

func (d *drivePendingControlReads) loadStartedAfter(ctx context.Context, store storage.Storage, apply *storage.Apply, notBefore time.Time, fresh bool, logger *slog.Logger) (pendingControlOperations, error) {
	read, leader := d.shared.join(apply.ID, d.last, notBefore, fresh)
	d.last = read
	if leader {
		read.pending, read.err = loadPendingControlOperations(ctx, store, apply)
		close(read.done)
		return read.pending, read.err
	}
	select {
	case <-ctx.Done():
		return nil, fmt.Errorf("wait for the shared read of pending control requests for apply %s: %w", apply.ApplyIdentifier, ctx.Err())
	case <-read.done:
	}
	if read.err != nil {
		// The read belonged to another drive, and its failure may be that
		// drive's own cancellation. This drive reads for itself, so the error
		// it acts on, if any, is one it hit.
		logger.DebugContext(ctx, "shared read of pending control requests failed; reading them for this drive",
			append(apply.MutableLogAttrs(), "error", read.err)...)
		return loadPendingControlOperations(ctx, store, apply)
	}
	return read.pending, nil
}

// join returns the read that answers a check. Unless fresh is set, it is the
// apply's latest read when that read is not the one the drive used last and is
// still in flight or started at or after notBefore. Otherwise join records a new read and reports
// that the caller leads it, which means the caller must run it and close done.
func (s *SharedPendingControlReads) join(applyID int64, last *pendingControlRead, notBefore time.Time, fresh bool) (read *pendingControlRead, leader bool) {
	now := s.clock()
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.reads == nil {
		s.reads = make(map[int64]*pendingControlRead)
	}
	if latest := s.reads[applyID]; !fresh && latest.answers(last, notBefore) {
		return latest, false
	}
	s.sweep(now)
	read = &pendingControlRead{started: now, done: make(chan struct{})}
	s.reads[applyID] = read
	return read, true
}

// sweep drops the finished reads of applies no drive has checked in a while.
// The caller holds mu.
func (s *SharedPendingControlReads) sweep(now time.Time) {
	if now.Sub(s.lastSweep) < pendingControlReadSweepAfter {
		return
	}
	s.lastSweep = now
	for applyID, read := range s.reads {
		if now.Sub(read.started) < pendingControlReadSweepAfter || read.inFlight() {
			continue
		}
		delete(s.reads, applyID)
	}
}

// answers reports whether this read can answer a drive's check: the drive
// has not used it yet, and it is still in flight or started at or after
// notBefore. A read in flight answers however long ago it started, because a
// new query would wait on the same slow storage, and every drive that joins
// it instead of starting its own keeps a slow store from also being asked
// the same query over and over.
func (r *pendingControlRead) answers(last *pendingControlRead, notBefore time.Time) bool {
	if r == nil || r == last {
		return false
	}
	return r.inFlight() || !r.started.Before(notBefore)
}

// inFlight reports whether the read has not finished yet.
func (r *pendingControlRead) inFlight() bool {
	select {
	case <-r.done:
		return false
	default:
		return true
	}
}

func (s *SharedPendingControlReads) clock() time.Time {
	if s.now != nil {
		return s.now()
	}
	return time.Now()
}
