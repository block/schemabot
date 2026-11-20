package metrics

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	"go.opentelemetry.io/otel/attribute"
	otelmetric "go.opentelemetry.io/otel/metric"
)

// GitHubOperationRateLimitSnapshot identifies installation-wide observations,
// without the operation and repository dimensions of diagnostic samples.
const GitHubOperationRateLimitSnapshot = "rate_limit_snapshot"

// GitHubRateLimitSnapshotMaxAge is how long an installation snapshot stays
// exported after GitHub reported it. Past this age the budget is unknown, so
// anything that keeps an idle installation's quota visible has to observe it
// again more often than this.
const GitHubRateLimitSnapshotMaxAge = 120 * time.Second

// Like the global OTel provider, the active recorder is installed once during
// telemetry setup. Each provider's callback owns its own state and lifetime.
var activeGitHubRateLimits atomic.Pointer[githubRateLimitRecorder]

type githubRateLimitIdentity struct {
	app          string
	installation int64
	resource     string
}

type githubRateLimitDiagnostic struct {
	sample GitHubRateLimitSample
	attrs  []attribute.KeyValue
}

// githubQuotaProbeTargets is the set of installations one App's quota probe is
// responsible for keeping observed: every installation that serves a
// configured repository, plus the count of repository owners whose
// installation could not be resolved on the probe's last pass. reportedAt is
// when the probe last reported them.
type githubQuotaProbeTargets struct {
	githubApp     string
	installations []int64
	unresolved    int
	reportedAt    time.Time
}

type githubRateLimitRecorder struct {
	mu          sync.Mutex
	diagnostics map[attribute.Distinct]githubRateLimitDiagnostic
	snapshots   map[githubRateLimitIdentity]GitHubRateLimitSample
	// probeTargets is keyed by App ID rather than by the App label, so a probe
	// whose label changes (the slug is learned after startup) replaces its
	// earlier entry instead of leaving it behind.
	probeTargets map[int64]githubQuotaProbeTargets
	now          func() time.Time
}

// RegisterGitHubRateLimitMetrics installs the process recorder and observable
// gauges on provider. Call once during global metrics-provider setup, before
// recording requests. Observable collection removes expired snapshots from
// every export path instead of repeatedly scraping a retained synchronous gauge.
func RegisterGitHubRateLimitMetrics(provider otelmetric.MeterProvider) error {
	recorder := &githubRateLimitRecorder{
		diagnostics:  make(map[attribute.Distinct]githubRateLimitDiagnostic),
		snapshots:    make(map[githubRateLimitIdentity]GitHubRateLimitSample),
		probeTargets: make(map[int64]githubQuotaProbeTargets),
		now:          time.Now,
	}
	if err := recorder.register(provider); err != nil {
		return err
	}
	activeGitHubRateLimits.Store(recorder)
	return nil
}

func (r *githubRateLimitRecorder) register(provider otelmetric.MeterProvider) error {
	meter := provider.Meter(meterName)
	descriptions := map[string]string{
		"limit":     "GitHub primary rate limit for the observed API resource",
		"remaining": "GitHub primary rate limit requests remaining for the observed API resource",
		"used":      "GitHub primary rate limit requests used for the observed API resource",
	}
	gauges := make(map[string]otelmetric.Int64ObservableGauge)
	for name, description := range descriptions {
		gauge, err := meter.Int64ObservableGauge("schemabot.github.rate_limit."+name,
			otelmetric.WithDescription(description), otelmetric.WithUnit("{request}"))
		if err != nil {
			return fmt.Errorf("register GitHub rate-limit %s: %w", name, err)
		}
		gauges[name] = gauge
	}
	age, err := meter.Float64ObservableGauge("schemabot.github.rate_limit.observation_age_seconds",
		otelmetric.WithDescription("Age of the last trustworthy GitHub installation quota observation; past the export age the budget is unknown"),
		otelmetric.WithUnit("s"))
	if err != nil {
		return fmt.Errorf("register GitHub quota observation age: %w", err)
	}
	unobserved, err := meter.Int64ObservableGauge("schemabot.github.rate_limit.unobserved_installations",
		otelmetric.WithDescription("Installations serving configured repositories whose GitHub core budget is unknown: no fresh quota snapshot, or the installation could not be resolved"),
		otelmetric.WithUnit("{installation}"))
	if err != nil {
		return fmt.Errorf("register GitHub unobserved installations: %w", err)
	}
	_, err = meter.RegisterCallback(func(_ context.Context, observer otelmetric.Observer) error {
		r.mu.Lock()
		defer r.mu.Unlock()
		now := r.now()
		// Reserve collection capacity for installation snapshots before the more
		// numerous diagnostic series can reach the exporter cardinality limit.
		for _, sample := range r.snapshots {
			elapsed := now.Sub(sample.ObservedAt)
			opts := otelmetric.WithAttributes(gitHubMetricAttributes(GitHubOperationRateLimitSnapshot,
				sample.Resource, "", sample.GitHubApp, sample.InstallationID)...)
			observer.ObserveFloat64(age, elapsed.Seconds(), opts)
			if snapshotExportable(sample, now) {
				observer.ObserveInt64(gauges["remaining"], sample.Remaining, opts)
			}
		}
		// Two Apps report under one label while neither slug is known yet, so
		// counts are summed per label: one observation per attribute set, and
		// neither App's unobserved installations can hide the other's.
		unobservedByApp := make(map[string]int64)
		for _, targets := range r.probeTargets {
			unobservedByApp[targets.githubApp] += r.unobservedInstallations(targets, now)
		}
		for githubApp, count := range unobservedByApp {
			opts := otelmetric.WithAttributes(gitHubMetricAttributes(GitHubOperationRateLimitSnapshot,
				GitHubRateLimitResourceCore, "", githubApp, 0)...)
			observer.ObserveInt64(unobserved, count, opts)
		}
		for _, diagnostic := range r.diagnostics {
			opts := otelmetric.WithAttributes(diagnostic.attrs...)
			observer.ObserveInt64(gauges["limit"], diagnostic.sample.Limit, opts)
			observer.ObserveInt64(gauges["remaining"], diagnostic.sample.Remaining, opts)
			observer.ObserveInt64(gauges["used"], diagnostic.sample.Used, opts)
		}

		return nil
	}, gauges["limit"], gauges["remaining"], gauges["used"], age, unobserved)
	if err != nil {
		return fmt.Errorf("register GitHub quota collection: %w", err)
	}
	return nil
}

func (r *githubRateLimitRecorder) record(sample GitHubRateLimitSample) {
	attrs := gitHubMetricAttributes(sample.Operation, sample.Resource, sample.Repository, sample.GitHubApp, sample.InstallationID)
	r.mu.Lock()
	defer r.mu.Unlock()
	attrSet := attribute.NewSet(attrs...)
	r.diagnostics[attrSet.Equivalent()] = githubRateLimitDiagnostic{sample: sample, attrs: attrs}
	if !validGitHubRateLimitSnapshot(sample) {
		slog.Debug("GitHub quota headers do not provide a trustworthy installation snapshot", "github_app", sample.GitHubApp, "installation_id", sample.InstallationID, "resource", sample.Resource)
		return
	}
	key := githubRateLimitIdentity{sample.GitHubApp, sample.InstallationID, sample.Resource}
	if previous, ok := r.snapshots[key]; ok {
		if sample.ResetAt.Before(previous.ResetAt) {
			slog.Debug("Ignoring older GitHub quota window", "github_app", sample.GitHubApp, "installation_id", sample.InstallationID)
			return
		}
		// Within one window, depletion takes precedence over receipt order.
		// Keep the lower observation's original timestamp so recording it late
		// cannot extend its export lifetime.
		if sample.ResetAt.Equal(previous.ResetAt) {
			if sample.Remaining > previous.Remaining {
				slog.Debug("Ignoring out-of-order GitHub quota allowance", "github_app", sample.GitHubApp, "installation_id", sample.InstallationID)
				return
			}
			if sample.Remaining == previous.Remaining && !sample.ObservedAt.After(previous.ObservedAt) {
				slog.Debug("Ignoring older identical GitHub quota observation", "github_app", sample.GitHubApp, "installation_id", sample.InstallationID)
				return
			}
		}
	}
	r.snapshots[key] = sample
}

// snapshotExportable reports whether a snapshot still stands for the
// installation's budget: GitHub reported it no longer than the export age ago
// and its window has not reset since.
func snapshotExportable(sample GitHubRateLimitSample, now time.Time) bool {
	elapsed := now.Sub(sample.ObservedAt)
	return elapsed >= 0 && elapsed <= GitHubRateLimitSnapshotMaxAge && now.Before(sample.ResetAt)
}

// unobservedInstallations counts the probe targets whose core budget is
// unknown: owners whose installation could not be resolved, installations
// with no exportable core snapshot, and one more when the targets themselves
// are stale. The caller holds r.mu.
func (r *githubRateLimitRecorder) unobservedInstallations(targets githubQuotaProbeTargets, now time.Time) int64 {
	unobserved := int64(targets.unresolved)
	if probeTargetsStale(targets, now) {
		unobserved++
	}
	for _, installation := range targets.installations {
		sample, ok := r.snapshots[githubRateLimitIdentity{targets.githubApp, installation, GitHubRateLimitResourceCore}]
		if !ok || !snapshotExportable(sample, now) {
			unobserved++
		}
	}
	return unobserved
}

// probeTargetsStale reports whether a probe's targets are older than a
// snapshot may be. Webhook traffic keeps refreshing the snapshots of
// installations the targets already name, so those snapshots staying fresh
// does not show the probe is still running: a stalled probe would leave an
// installation added since its last pass, and its last unresolved count,
// standing indefinitely. Stale targets therefore count one more unobserved
// installation until the probe reports again.
func probeTargetsStale(targets githubQuotaProbeTargets, now time.Time) bool {
	return now.Sub(targets.reportedAt) > GitHubRateLimitSnapshotMaxAge
}

// SetGitHubQuotaProbeTargets records the installations an App's quota probe
// keeps observed, replacing what the probe reported on its previous pass. The
// unobserved-installations gauge is computed from these targets at every
// collection, so an installation whose reads start failing shows as
// unobserved once its last snapshot ages out, and a probe that stops reporting
// shows one more once its targets age out, rather than keeping the count from
// the last pass.
func SetGitHubQuotaProbeTargets(appID int64, githubApp string, installations []int64, unresolved int) {
	recorder := activeGitHubRateLimits.Load()
	if recorder == nil {
		slog.Debug("GitHub rate-limit metrics are not registered; quota probe targets are not recorded", "github_app", githubApp)
		return
	}
	recorder.mu.Lock()
	defer recorder.mu.Unlock()
	recorder.probeTargets[appID] = githubQuotaProbeTargets{
		githubApp:     githubApp,
		installations: append([]int64(nil), installations...),
		unresolved:    unresolved,
		reportedAt:    recorder.now(),
	}
}

func validGitHubRateLimitSnapshot(sample GitHubRateLimitSample) bool {
	return sample.GitHubApp != "" && sample.InstallationID > 0 &&
		isKnownGitHubRateLimitResource(sample.Resource) &&
		sample.Limit > 0 && sample.Remaining >= 0 && sample.Remaining <= sample.Limit &&
		!sample.ObservedAt.IsZero() && sample.ResetAt.After(sample.ObservedAt)
}
