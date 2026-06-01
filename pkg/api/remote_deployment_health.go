package api

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"time"

	"github.com/block/schemabot/pkg/metrics"
)

const (
	// RemoteDeploymentHealthCheckInterval is how often SchemaBot checks remote
	// deployment health for steady-state availability metrics.
	RemoteDeploymentHealthCheckInterval = 30 * time.Second

	// RemoteDeploymentHealthCheckTimeout bounds a single remote deployment health
	// check so an unhealthy deployment cannot stall other checks.
	RemoteDeploymentHealthCheckTimeout = 5 * time.Second
)

type remoteDeploymentHealthTarget struct {
	deployment  string
	environment string
}

// StartRemoteDeploymentHealthMonitor starts a background loop that emits
// steady-state health metrics for every configured remote deployment.
func (s *Service) StartRemoteDeploymentHealthMonitor(ctx context.Context) {
	if len(s.remoteDeploymentHealthTargets()) == 0 {
		s.logger.Debug("remote deployment health monitor not started because no remote deployments are configured")
		return
	}

	s.remoteHealthMu.Lock()
	if s.remoteHealthCancel != nil {
		s.remoteHealthMu.Unlock()
		s.logger.Info("remote deployment health monitor already running")
		return
	}
	monitorCtx, cancel := context.WithCancel(ctx)
	s.remoteHealthCancel = cancel
	interval := s.remoteHealthInterval
	s.remoteHealthMu.Unlock()

	s.remoteHealthWg.Go(func() {
		s.remoteDeploymentHealthMonitor(monitorCtx, interval)
	})
	s.logger.Info("remote deployment health monitor started", "interval", interval, "timeout", RemoteDeploymentHealthCheckTimeout)
}

// StopRemoteDeploymentHealthMonitor stops the background remote deployment
// health monitor. Safe to call multiple times.
func (s *Service) StopRemoteDeploymentHealthMonitor() {
	s.remoteHealthMu.Lock()
	cancel := s.remoteHealthCancel
	if cancel == nil {
		s.remoteHealthMu.Unlock()
		return
	}
	s.remoteHealthCancel = nil
	s.remoteHealthMu.Unlock()

	cancel()
	s.remoteHealthWg.Wait()
}

// SetRemoteDeploymentHealthCheckInterval sets the background health check
// interval. Call before StartRemoteDeploymentHealthMonitor.
func (s *Service) SetRemoteDeploymentHealthCheckInterval(interval time.Duration) error {
	if interval <= 0 {
		return fmt.Errorf("remote deployment health check interval must be positive")
	}
	s.remoteHealthMu.Lock()
	defer s.remoteHealthMu.Unlock()
	if s.remoteHealthCancel != nil {
		return fmt.Errorf("remote deployment health monitor already running")
	}
	s.remoteHealthInterval = interval
	return nil
}

func (s *Service) remoteDeploymentHealthMonitor(ctx context.Context, interval time.Duration) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	s.CheckRemoteDeploymentHealth(ctx)

	for {
		select {
		case <-ctx.Done():
			s.logger.Debug("remote deployment health monitor stopping", "error", ctx.Err())
			return
		case <-ticker.C:
			s.CheckRemoteDeploymentHealth(ctx)
		}
	}
}

// CheckRemoteDeploymentHealth checks every configured remote deployment once
// and records health metrics. It is exported so tests and diagnostics can run a
// single synchronous check without starting the background monitor.
func (s *Service) CheckRemoteDeploymentHealth(ctx context.Context) {
	if err := ctx.Err(); err != nil {
		s.logger.Debug("remote deployment health check skipped because context is done", "error", err)
		return
	}

	targets := s.remoteDeploymentHealthTargets()
	if len(targets) == 0 {
		s.logger.Debug("remote deployment health check skipped because no remote deployments are configured")
		return
	}

	for _, target := range targets {
		s.checkRemoteDeploymentHealth(ctx, target)
	}
}

func (s *Service) remoteDeploymentHealthTargets() []remoteDeploymentHealthTarget {
	targets := make([]remoteDeploymentHealthTarget, 0)
	for deployment, endpoints := range s.config.TernDeployments {
		for environment := range endpoints {
			targets = append(targets, remoteDeploymentHealthTarget{
				deployment:  deployment,
				environment: environment,
			})
		}
	}
	sort.Slice(targets, func(i, j int) bool {
		if targets[i].deployment == targets[j].deployment {
			return targets[i].environment < targets[j].environment
		}
		return targets[i].deployment < targets[j].deployment
	})
	return targets
}

func (s *Service) checkRemoteDeploymentHealth(ctx context.Context, target remoteDeploymentHealthTarget) {
	client, err := s.TernClient(target.deployment, target.environment)
	if err != nil {
		s.logger.Warn("remote deployment health check failed because client could not be resolved",
			"deployment", target.deployment,
			"environment", target.environment,
			"error", err)
		metrics.RecordRemoteDeploymentHealth(ctx, target.deployment, target.environment, false)
		metrics.RecordRemoteDeploymentHealthCheck(ctx, target.deployment, target.environment, "error", "client_config_error")
		return
	}

	checkCtx, cancel := context.WithTimeout(ctx, RemoteDeploymentHealthCheckTimeout)
	defer cancel()

	if err := client.Health(checkCtx); err != nil {
		if errors.Is(ctx.Err(), context.Canceled) || errors.Is(ctx.Err(), context.DeadlineExceeded) {
			s.logger.Debug("remote deployment health check stopped because context is done",
				"deployment", target.deployment,
				"environment", target.environment,
				"error", ctx.Err())
			return
		}

		reason := "unavailable"
		if errors.Is(checkCtx.Err(), context.DeadlineExceeded) {
			reason = "timeout"
		}
		s.logger.Warn("remote deployment health check failed",
			"deployment", target.deployment,
			"environment", target.environment,
			"reason", reason,
			"error", err)
		metrics.RecordRemoteDeploymentHealth(ctx, target.deployment, target.environment, false)
		metrics.RecordRemoteDeploymentHealthCheck(ctx, target.deployment, target.environment, "error", reason)
		return
	}

	s.logger.Debug("remote deployment health check succeeded",
		"deployment", target.deployment,
		"environment", target.environment)
	metrics.RecordRemoteDeploymentHealth(ctx, target.deployment, target.environment, true)
	metrics.RecordRemoteDeploymentHealthCheck(ctx, target.deployment, target.environment, "success", "healthy")
}
