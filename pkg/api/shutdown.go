package api

import (
	"sync"
	"time"

	"github.com/block/schemabot/pkg/drain"
)

// monitorDrainTimeout bounds how long each Stop*Monitor waits for its loop to
// return once it has been cancelled. The loops are tickers that check their
// context between passes, so they normally return the instant they are asked
// to; the bound is for the pass that is blocked inside a storage query against
// a database that has stopped answering. That is the shutdown worth designing
// for, because it is also the database the rest of the close is about to wait
// on, and one unresponsive database should not compound into a process that
// never exits.
//
// A monitor abandoned here loses no work. Each one recomputes its whole view
// from storage on its next pass, so the pass it did not finish is simply the
// pass the next process runs.
const monitorDrainTimeout = 5 * time.Second

// SetShutdownBudget gives every stage of this service's shutdown one deadline
// to share, so the stages cost their maximum rather than their sum. Call it
// before the first stop call; a service that is never given one waits each
// stage's own bound in full.
func (s *Service) SetShutdownBudget(budget *drain.Budget) {
	s.shutdownBudget.Store(budget)
}

// shutdownWait waits for wg within own, or within what is left of the shutdown
// budget when that is less.
func (s *Service) shutdownWait(wg *sync.WaitGroup, own time.Duration) bool {
	return s.shutdownBudget.Load().Wait(wg, own)
}

// shutdownAllot returns how long a shutdown stage whose own bound is own may
// take, given what is left of the budget.
func (s *Service) shutdownAllot(own time.Duration) time.Duration {
	return s.shutdownBudget.Load().Allot(own)
}

// drainMonitor waits for a cancelled monitor loop to return, naming the monitor
// in the log if it does not, and reports whether it returned.
//
// Callers must not announce that the monitor stopped without checking: past the
// bound the loop is still running, and an operator reading that it stopped is
// reading the opposite of what is true at the one moment the distinction
// matters.
func (s *Service) drainMonitor(wg *sync.WaitGroup, monitor string) bool {
	if s.shutdownWait(wg, monitorDrainTimeout) {
		return true
	}
	s.logger.Warn("background monitor did not return within the shutdown drain; the close continues without it and its next pass runs in whichever process starts next",
		"monitor", monitor,
		"drain_timeout", monitorDrainTimeout,
		"budget_spent", s.shutdownBudget.Load().Spent())
	return false
}
