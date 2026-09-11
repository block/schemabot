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

// drainMonitor waits for a cancelled monitor loop to return, naming the monitor
// in the log if it does not.
func (s *Service) drainMonitor(wg *sync.WaitGroup, monitor string) {
	if drain.Wait(wg, monitorDrainTimeout) {
		return
	}
	s.logger.Warn("background monitor did not return within the shutdown drain; the close continues without it and its next pass runs in whichever process starts next",
		"monitor", monitor,
		"drain_timeout", monitorDrainTimeout)
}
