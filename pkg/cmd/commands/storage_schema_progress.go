package commands

import (
	"fmt"
	"io"
	"strings"
	"time"

	"github.com/block/schemabot/pkg/api"
	"github.com/block/schemabot/pkg/ui"
)

// Between the plan and the answer, a convergence used to say nothing. On a
// storage database with any history that gap is the whole command: an index
// build over a long-lived applies table runs for as long as it runs, and an
// operator watching a blank terminal cannot tell a slow copy from a stuck one
// — which is exactly when somebody kills a run that was about to finish.
//
// So the direct path prints what the convergence reports, raw. Only the direct
// path: a convergence reached over the API answers once, when it is done, and
// there is nothing to stream through a unary response.

// storageProgressMinInterval is the floor between two progress lines that say
// the same thing in different numbers. Rows copied moves on every poll, and a
// line per poll is a scrollback an operator cannot read; a line every couple
// of seconds is enough to see that it is moving.
const storageProgressMinInterval = 2 * time.Second

// storageProgressPrinter turns a convergence's observations into lines, on the
// two rules that keep them readable: never print the same line twice in a row,
// and do not print more often than storageProgressMinInterval — except for a
// change of state, which is the transition an operator is waiting for and is
// rare enough to always be worth a line.
//
// It holds no lock. The convergence calls it from one goroutine, in order,
// which is the only caller the option promises.
type storageProgressPrinter struct {
	out       io.Writer
	now       func() time.Time
	lastLine  string
	lastState string
	lastAt    time.Time
	// stopped is set once a write fails. There is nobody to report a terminal
	// that has gone away to, and nothing to gain from writing to it again for
	// the rest of a convergence that is still running either way.
	stopped bool
}

func newStorageProgressPrinter(out io.Writer) *storageProgressPrinter {
	return &storageProgressPrinter{out: out, now: time.Now}
}

func (p *storageProgressPrinter) observe(o api.StorageConvergenceProgress) {
	if p.stopped {
		return
	}
	line := storageProgressLine(o)
	if line == "" {
		return
	}
	stateChanged := o.State != p.lastState
	if !stateChanged {
		if line == p.lastLine {
			return
		}
		if p.now().Sub(p.lastAt) < storageProgressMinInterval {
			return
		}
	}
	p.lastState = o.State
	p.lastLine = line
	p.lastAt = p.now()
	if _, err := fmt.Fprintln(p.out, line); err != nil {
		p.stopped = true
	}
}

// storageProgressLine renders one observation, leaving out what the dialect
// could not say rather than printing a zero for it. An empty line means the
// observation carried nothing at all, and nothing is what gets printed.
func storageProgressLine(o api.StorageConvergenceProgress) string {
	var head string
	switch {
	case o.State != "" && o.Percent > 0:
		head = fmt.Sprintf("%s %d%%", o.State, o.Percent)
	case o.State != "":
		head = o.State
	case o.Percent > 0:
		head = fmt.Sprintf("%d%%", o.Percent)
	}

	tables := make([]string, 0, len(o.Tables))
	for _, t := range o.Tables {
		if table := storageProgressTable(t); table != "" {
			tables = append(tables, table)
		}
	}

	switch {
	case head == "" && len(tables) == 0:
		return ""
	case len(tables) == 0:
		return "  " + head
	case head == "":
		return "  " + strings.Join(tables, ", ")
	default:
		return "  " + head + " · " + strings.Join(tables, ", ")
	}
}

func storageProgressTable(t api.StorageConvergenceTableProgress) string {
	if t.Table == "" {
		return ""
	}
	line := t.Table
	if t.State != "" {
		line += " " + t.State
	}
	if t.Percent > 0 {
		line += fmt.Sprintf(" %d%%", t.Percent)
	}
	if t.RowsCopied > 0 {
		line += fmt.Sprintf(" (%s rows)", ui.FormatNumber(t.RowsCopied))
	}
	return line
}
