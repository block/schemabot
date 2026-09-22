package commands

import (
	"fmt"
	"io"
	"time"

	"github.com/block/schemabot/pkg/api"
	"github.com/block/schemabot/pkg/engine"
	"github.com/block/schemabot/pkg/ui"
)

// Between the plan and the answer, a convergence used to say nothing. On a
// storage database with any history that gap is the whole command: an index
// build over a long-lived applies table runs for as long as it runs, and an
// operator watching a blank terminal cannot tell a slow copy from a stuck one
// — which is exactly when somebody kills a run that was about to finish.
//
// So the direct path prints what the convergence reports, in the same log mode
// `schemabot apply --output log` prints: logfmt lines, immediately on a
// transition and on a heartbeat while something is still moving. An operator
// who has read one of these has read both, and the same log scrapers work on
// either.
//
// Only the direct path: a convergence reached over the API answers once, when
// it is done, and there is nothing to stream through a unary response.

// storageProgressPrinter turns a convergence's observations into log lines.
//
// It holds no lock. The convergence calls it from one goroutine, in order,
// which is the only caller the option promises.
type storageProgressPrinter struct {
	out      io.Writer
	colors   bool
	now      func() time.Time
	subjects map[string]*storageProgressSubjectLog
	// stopped is set once a write fails. There is nobody to report a terminal
	// that has gone away to, and nothing to gain from writing to it again for
	// the rest of a convergence that is still running either way.
	stopped bool
}

// storageProgressSubjectLog is the last thing said about one subject, which is
// what decides whether the next observation is worth a line.
type storageProgressSubjectLog struct {
	status     string
	startedAt  time.Time
	lastEmit   time.Time
	heartbeats int
}

// heartbeatInterval is short for the first heartbeat and then settles, so a
// convergence confirms early that it is moving without filling a scrollback
// for the rest of an hour-long copy. The intervals are log mode's own.
func (s *storageProgressSubjectLog) heartbeatInterval() time.Duration {
	if s.heartbeats == 0 {
		return logFirstHeartbeat
	}
	return logHeartbeatDefault
}

func newStorageProgressPrinter(out io.Writer, colors bool) *storageProgressPrinter {
	return &storageProgressPrinter{
		out:    out,
		colors: colors,
		// UTC, because log mode's other surface stamps in UTC and the line
		// carries no zone to tell them apart. A host outside UTC would
		// otherwise put two different clocks in one format.
		now:      func() time.Time { return time.Now().UTC() },
		subjects: map[string]*storageProgressSubjectLog{},
	}
}

func (p *storageProgressPrinter) observe(o api.StorageConvergenceProgress) {
	if p.stopped {
		return
	}
	for _, subject := range storageProgressSubjects(o) {
		p.observeSubject(subject)
	}
}

// observeSubject decides what one subject's observation is: its first sighting,
// a transition, a heartbeat, or nothing that has moved since the last line.
func (p *storageProgressPrinter) observeSubject(s storageProgressSubject) {
	now := p.now()

	last, seen := p.subjects[s.table]
	if !seen {
		last = &storageProgressSubjectLog{startedAt: now}
		p.subjects[s.table] = last
		if s.finished() {
			// The subject was already finished the first time a poll saw it:
			// an instant DDL, or a copy that fit between two polls. Announcing
			// it as started would say it began after it ended, and no
			// transition can correct that later, because a terminal state is
			// the last one there is. It gets the event it is actually in, and
			// no duration, since nothing ever observed it running.
			p.emit(now, s, s.transition())
		} else {
			p.emit(now, s, s.announcement())
		}
		last.status, last.lastEmit = s.status, now
		return
	}

	switch {
	case s.status != last.status:
		// The transition is what an operator is waiting for — the copy
		// finishing, the cutover starting — and there are a handful in a whole
		// run, so it prints whatever the heartbeat interval says.
		p.emit(now, s, append(s.transition(), "duration", ui.FormatHumanDuration(now.Sub(last.startedAt))))
		last.heartbeats = 0
	case s.finished():
		// Reported terminal once already. A convergence keeps polling until
		// every subject is done, so a table that finished early is still in
		// every observation until the last one — and heartbeating it would
		// spend the rest of the run insisting a finished table is still
		// copying rows. There is nothing left to confirm about a subject the
		// engine has let go of.
		return
	case now.Sub(last.lastEmit) < last.heartbeatInterval():
		// Ten polls a second are one thing happening, and the interval is what
		// collapses them into one line. It is deliberately the only thing that
		// does. Suppressing an observation for saying what the last one said
		// would silence the copy whose numbers have stopped moving — a table
		// in checksum, or one the throttler is holding — and that is the
		// moment the line confirming it is still alive is worth the most.
		// `apply --output log`, the surface this one matches, heartbeats on
		// elapsed time alone for the same reason.
		return
	default:
		p.emit(now, s, s.heartbeat())
		last.heartbeats++
	}
	last.status, last.lastEmit = s.status, now
}

func (p *storageProgressPrinter) emit(now time.Time, s storageProgressSubject, kvs []string) {
	kvs = append(kvs, s.kvs...)
	if _, err := fmt.Fprintln(p.out, logfmtLine(now, p.colors, kvs...)); err != nil {
		p.stopped = true
	}
}

// storageProgressSubject is one thing a line can be about: a table the
// convergence is working on, or — before any table progress exists — the
// convergence itself, under an empty name.
type storageProgressSubject struct {
	table  string
	status string
	// kvs carries the measurements this dialect could take, and only those. A
	// zero printed as progress=0% is a measurement nobody made, and on
	// PostgreSQL — which converges a table per transaction and has no partial
	// state — it would be on every line.
	kvs []string
}

// noun names what the line is about, so `table=` stays an identifier rather
// than carrying the subject too.
func (s storageProgressSubject) noun() string {
	if s.table != "" {
		return "Table"
	}
	return "Convergence"
}

// announcement is a subject's first line. The state goes in its own key rather
// than the message, because "started" is the event and the engine's state is a
// separate fact about it.
func (s storageProgressSubject) announcement() []string {
	kvs := []string{"msg", s.noun() + " started"}
	if s.table != "" {
		kvs = append(kvs, "table", s.table)
	}
	if s.status != "" {
		kvs = append(kvs, "status", s.status)
	}
	return kvs
}

// finished reports whether the engine has said this subject is done, in
// whichever way it finished. It is asked of the engine's own state rather than
// of a percentage, because a copy sits at 100% through its whole checksum and
// is not finished at all.
func (s storageProgressSubject) finished() bool {
	return engine.State(s.status).IsTerminal()
}

// transition puts the new state in the message, which is where log mode reads
// it from: a state the engine calls "complete" or "failed" is what decides the
// line's color, and a duplicate status key beside it says nothing new.
func (s storageProgressSubject) transition() []string {
	event := s.status
	if event == "" {
		// The engine stopped naming a state. That the subject moved is still
		// worth the line; there is just no word for where it moved to.
		event = "advanced"
	}
	kvs := []string{"msg", s.noun() + " " + event}
	if s.table != "" {
		kvs = append(kvs, "table", s.table)
	}
	return kvs
}

// heartbeat borrows log mode's wording for a row copy when rows are what moved,
// so the two surfaces read alike on the line an operator sees most.
func (s storageProgressSubject) heartbeat() []string {
	msg := s.noun() + " running"
	for i := 0; i+1 < len(s.kvs); i += 2 {
		if s.kvs[i] == "rows_copied" {
			msg = "Copying rows"
			break
		}
	}
	kvs := []string{"msg", msg}
	if s.table != "" {
		kvs = append(kvs, "table", s.table)
	}
	return kvs
}

// storageProgressSubjects splits an observation into the subjects worth a line.
// An observation that measured nothing at all yields none, and nothing is what
// gets printed.
func storageProgressSubjects(o api.StorageConvergenceProgress) []storageProgressSubject {
	if len(o.Tables) == 0 {
		if o.State == "" && o.Percent <= 0 {
			return nil
		}
		s := storageProgressSubject{status: o.State}
		if o.Percent > 0 {
			s.kvs = append(s.kvs, "progress", fmt.Sprintf("%d%%", o.Percent))
		}
		return []storageProgressSubject{s}
	}

	subjects := make([]storageProgressSubject, 0, len(o.Tables))
	for _, t := range o.Tables {
		if t.Table == "" {
			continue
		}
		s := storageProgressSubject{table: t.Table, status: t.State}
		switch {
		case t.Percent > 0:
			s.kvs = append(s.kvs, "progress", fmt.Sprintf("%d%%", t.Percent))
		case o.Percent > 0:
			// The dialect knows which table it is on but not how far into it,
			// so the only measurement it took is how much of the run is
			// behind it. That is worth printing — an operator waiting out a
			// long CREATE INDEX is asking exactly it — but not under
			// `progress`, which on the other dialect counts rows within this
			// one table. Two denominators under one key is how an alert
			// written against one of them silently matches the other.
			s.kvs = append(s.kvs, "converged", fmt.Sprintf("%d%%", o.Percent))
		}
		if t.RowsCopied > 0 {
			s.kvs = append(s.kvs, "rows_copied", ui.FormatNumber(t.RowsCopied))
		}
		subjects = append(subjects, s)
	}
	return subjects
}
