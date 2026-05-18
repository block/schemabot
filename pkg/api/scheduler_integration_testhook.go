//go:build integration

package api

import (
	"fmt"
	"time"
)

// SetSchedulerPollIntervalForTest shortens scheduler polling in integration
// tests. Call before StartScheduler so workers create their tickers with the
// intended interval.
func (s *Service) SetSchedulerPollIntervalForTest(interval time.Duration) error {
	if interval <= 0 {
		return fmt.Errorf("scheduler poll interval must be positive")
	}
	if s.stopRecovery != nil {
		return fmt.Errorf("scheduler already running")
	}
	s.schedulerPollInterval = interval
	return nil
}
