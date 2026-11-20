package api

import (
	"context"
	"slices"
	"sort"
	"time"

	"github.com/block/schemabot/pkg/metrics"
	"github.com/block/schemabot/pkg/storage"
)

const (
	// FreezeMetricsInterval is how often SchemaBot records which environments
	// are frozen. A planned freeze starts and ends by time rather than by an
	// API call, so only a periodic snapshot keeps the gauge current.
	FreezeMetricsInterval = 30 * time.Second

	// FreezeMetricsTimeout bounds a single snapshot so a slow database cannot
	// stall the monitor loop.
	FreezeMetricsTimeout = 5 * time.Second
)

// StartFreezeMonitor starts a background loop that records, per environment,
// whether a freeze is in effect. It is a no-op without freeze storage.
func (s *Service) StartFreezeMonitor(ctx context.Context) {
	if s.storage == nil {
		s.logger.Debug("freeze monitor not started because storage is unavailable")
		return
	}

	s.freezeMonitorMu.Lock()
	if s.freezeMonitorCancel != nil {
		s.freezeMonitorMu.Unlock()
		s.logger.Info("freeze monitor already running")
		return
	}
	monitorCtx, cancel := context.WithCancel(ctx)
	s.freezeMonitorCancel = cancel
	s.freezeMonitorMu.Unlock()

	// Export the failure counter at 0 before any pass runs, so the first
	// failure shows up as an increase rather than as a new series at 1.
	metrics.InitEnvironmentFreezeCollectionFailures(monitorCtx)

	s.freezeMonitorWg.Go(func() {
		ticker := time.NewTicker(FreezeMetricsInterval)
		defer ticker.Stop()
		s.CollectFreezeMetrics(monitorCtx)
		for {
			select {
			case <-monitorCtx.Done():
				s.logger.Debug("freeze monitor stopping", "error", monitorCtx.Err())
				return
			case <-ticker.C:
				s.CollectFreezeMetrics(monitorCtx)
			}
		}
	})
	s.logger.Info("freeze monitor started", "interval", FreezeMetricsInterval, "timeout", FreezeMetricsTimeout)
}

// StopFreezeMonitor stops the background freeze monitor. Safe to call
// multiple times.
func (s *Service) StopFreezeMonitor() {
	s.freezeMonitorMu.Lock()
	cancel := s.freezeMonitorCancel
	if cancel == nil {
		s.freezeMonitorMu.Unlock()
		return
	}
	s.freezeMonitorCancel = nil
	s.freezeMonitorMu.Unlock()

	cancel()
	s.drainMonitor(&s.freezeMonitorWg, "freeze_monitor")
}

// CollectFreezeMetrics records the freeze gauges once. For each environment
// this instance serves, schemabot.environment.frozen is 1 per scope with a
// freeze in effect (a whole-environment freeze, or one narrowed to some
// deployments or databases) and 0 otherwise, and
// schemabot.environment.freeze_open_seconds is the age of the oldest
// open-ended freeze in effect. A peer environment has its own store, so this
// instance cannot know whether it is frozen and reports nothing for it. Both
// "in effect" and the age use the database clock, as the change gate does. It
// is exported so tests can run one synchronous collection.
func (s *Service) CollectFreezeMetrics(ctx context.Context) {
	if err := ctx.Err(); err != nil {
		s.logger.Debug("freeze metrics collection skipped because context is done", "error", err)
		return
	}
	if s.storage == nil || s.storage.Freezes() == nil {
		s.logger.Debug("freeze metrics collection skipped because freeze storage is unavailable")
		return
	}

	collectCtx, cancel := context.WithTimeout(ctx, FreezeMetricsTimeout)
	defer cancel()

	now, inEffect, err := freezeSnapshot(collectCtx, s.storage.Freezes())
	if err != nil {
		// A stop that lands mid-query cancels the monitor context. That is an
		// orderly shutdown, not a stale gauge, so it must not count as a failure.
		if ctx.Err() != nil {
			s.logger.Debug("freeze metrics collection aborted because context is done", "error", err)
			return
		}
		// The gauges are last-value: leaving them untouched re-exports the last
		// snapshot. The failure counter tells operators they are stale.
		metrics.RecordEnvironmentFreezeCollectionFailure(ctx)
		s.logger.Warn("freeze metrics collection failed", "error", err)
		return
	}

	states := make(map[string]*environmentFreezeState)
	state := func(environment string) *environmentFreezeState {
		environment = storage.CanonicalKey(environment)
		if states[environment] == nil {
			states[environment] = &environmentFreezeState{}
		}
		return states[environment]
	}
	for _, environment := range s.config.KnownEnvironments() {
		if s.config.ServesEnvironment(environment) {
			state(environment)
		}
	}
	for _, freeze := range inEffect {
		// A row for an environment this instance does not serve is left over
		// from before the environment moved to a peer or out of config. The
		// peer's store decides whether it is frozen, so the row is not
		// reported; the seen environments below still return to 0.
		if !s.config.ServesEnvironment(freeze.Environment) {
			continue
		}
		state(freeze.Environment).add(freeze, now)
	}
	// Record every environment that was ever reported frozen too, so an
	// environment dropped from config after its freeze ends returns to 0
	// instead of re-exporting 1 forever.
	s.freezeMonitorMu.Lock()
	if s.freezeMonitorSeen == nil {
		s.freezeMonitorSeen = make(map[string]bool)
	}
	for environment := range s.freezeMonitorSeen {
		state(environment)
	}
	for environment, st := range states {
		if st.whole || st.partial {
			s.freezeMonitorSeen[environment] = true
		}
	}
	s.freezeMonitorMu.Unlock()

	environments := make([]string, 0, len(states))
	for environment := range states {
		environments = append(environments, environment)
	}
	sort.Strings(environments)
	for _, environment := range environments {
		st := states[environment]
		// Record the scope in effect before clearing the other, so a scrape
		// between the two never sees both at 0 while a freeze holds
		// throughout, such as a whole-environment freeze replaced by a
		// partial one.
		scopes := []struct {
			name   string
			frozen bool
		}{{metrics.FreezeScopeEnvironment, st.whole}, {metrics.FreezeScopePartial, st.partial}}
		if st.partial && !st.whole {
			slices.Reverse(scopes)
		}
		for _, scope := range scopes {
			metrics.RecordEnvironmentFrozen(ctx, environment, scope.name, scope.frozen)
		}
		metrics.RecordEnvironmentFreezeOpenSeconds(ctx, environment, st.openFor)
	}
}

// freezeSnapshot reads the database clock and the freezes in effect.
func freezeSnapshot(ctx context.Context, freezes storage.FreezeStore) (time.Time, []*storage.EnvironmentFreeze, error) {
	now, err := freezes.Now(ctx)
	if err != nil {
		return time.Time{}, nil, err
	}
	inEffect, err := freezes.ListInEffect(ctx, "")
	if err != nil {
		return time.Time{}, nil, err
	}
	return now, inEffect, nil
}

// environmentFreezeState is one environment's freeze gauges.
type environmentFreezeState struct {
	whole   bool
	partial bool
	openFor time.Duration
}

func (st *environmentFreezeState) add(freeze *storage.EnvironmentFreeze, now time.Time) {
	if len(freeze.Deployments) == 0 && len(freeze.Databases) == 0 {
		st.whole = true
	} else {
		st.partial = true
	}
	if freeze.EndsAt == nil {
		st.openFor = max(st.openFor, now.Sub(freeze.StartsAt))
	}
}
