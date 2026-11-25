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

// maxGitHubQuotaWindowsPerIdentity bounds how many unexpired reset times the
// recorder keeps for one installation and resource. GitHub normally reports one
// or two; the bound keeps the per-response scan small, under the shared lock,
// if reset times ever jitter between responses.
const maxGitHubQuotaWindowsPerIdentity = 8

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
	// snapshots holds each identity's current window: the one GitHub reported
	// on the most recently received response. It is derived from windows.
	snapshots map[githubRateLimitIdentity]GitHubRateLimitSample
	// windows holds, per identity, the lowest remaining seen for every reset
	// time that has not yet passed, keyed by reset time in Unix nanoseconds.
	// Keeping each one separately means a window that becomes current again
	// still carries the depletion already observed in it, whatever order the
	// responses are recorded in.
	windows map[githubRateLimitIdentity]map[int64]GitHubRateLimitSample
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
		windows:      make(map[githubRateLimitIdentity]map[int64]GitHubRateLimitSample),
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
	windows := r.windows[key]
	if windows == nil {
		windows = make(map[int64]GitHubRateLimitSample)
		r.windows[key] = windows
	}
	reset := sample.ResetAt.UnixNano()
	previousWindow, seenWindow := windows[reset]
	if seenWindow {
		merged, changed := sameWindowGitHubRateLimitSnapshot(previousWindow, sample)
		if !changed {
			slog.Debug("Ignoring GitHub quota observation that is neither lower nor newer", "github_app", sample.GitHubApp, "installation_id", sample.InstallationID, "resource", sample.Resource)
			return
		}
		if merged.Remaining < sample.Remaining {
			slog.Debug("Keeping lower GitHub quota allowance from earlier in the window", "github_app", sample.GitHubApp, "installation_id", sample.InstallationID, "resource", sample.Resource, "remaining", merged.Remaining, "reported_remaining", sample.Remaining)
		}
		windows[reset] = merged
	} else {
		windows[reset] = sample
	}
	current := currentGitHubQuotaWindow(windows)
	pruneGitHubQuotaWindows(windows, current)
	previous, hadPrevious := r.snapshots[key]
	switch {
	case !current.ResetAt.Equal(sample.ResetAt):
		slog.Debug("Recorded GitHub quota window from an earlier response; a newer response's window stays current", "github_app", sample.GitHubApp, "installation_id", sample.InstallationID, "resource", sample.Resource, "reset_at", sample.ResetAt, "current_reset_at", current.ResetAt)
	case hadPrevious && current.ResetAt.Before(previous.ResetAt) && seenWindow:
		// GitHub can alternate between two reset times; the switch was logged
		// at Info the first time this reset time was seen.
		slog.Debug("GitHub quota window moved back to an earlier reset already seen; following the newer response", "github_app", sample.GitHubApp, "installation_id", sample.InstallationID, "resource", sample.Resource, "reset_at", current.ResetAt, "previous_reset_at", previous.ResetAt, "remaining", current.Remaining, "previous_remaining", previous.Remaining)
	case hadPrevious && current.ResetAt.Before(previous.ResetAt):
		slog.Info("GitHub reported an earlier quota reset than the last response; following the newer response", "github_app", sample.GitHubApp, "installation_id", sample.InstallationID, "resource", sample.Resource, "reset_at", current.ResetAt, "previous_reset_at", previous.ResetAt, "remaining", current.Remaining, "previous_remaining", previous.Remaining)
	}
	r.snapshots[key] = current
}

// currentGitHubQuotaWindow returns the window reported on the most recently
// received response, using supersedesGitHubQuotaWindow to order them. windows
// is never empty: record inserts the incoming sample before calling it.
func currentGitHubQuotaWindow(windows map[int64]GitHubRateLimitSample) GitHubRateLimitSample {
	var current GitHubRateLimitSample
	first := true
	for _, window := range windows {
		if first || supersedesGitHubQuotaWindow(current, window) {
			current = window
			first = false
		}
	}
	return current
}

// pruneGitHubQuotaWindows drops windows that had reset by the newest receipt,
// then, past maxGitHubQuotaWindowsPerIdentity, the windows least recently
// reported, which are the least likely to become current again. The current
// window is never dropped: a trustworthy snapshot always resets after it was
// received, and it is the most recently reported.
func pruneGitHubQuotaWindows(windows map[int64]GitHubRateLimitSample, current GitHubRateLimitSample) {
	for reset, window := range windows {
		if !window.ResetAt.After(current.ObservedAt) {
			delete(windows, reset)
		}
	}
	for len(windows) > maxGitHubQuotaWindowsPerIdentity {
		var oldestReset int64
		var oldest GitHubRateLimitSample
		first := true
		for reset, window := range windows {
			if first || supersedesGitHubQuotaWindow(window, oldest) {
				oldestReset, oldest = reset, window
				first = false
			}
		}
		slog.Debug("Dropping least recently reported GitHub quota window past the per-installation bound", "github_app", oldest.GitHubApp, "installation_id", oldest.InstallationID, "resource", oldest.Resource, "reset_at", oldest.ResetAt, "observed_at", oldest.ObservedAt)
		delete(windows, oldestReset)
	}
}

// snapshotExportable reports whether a snapshot still stands for the
// installation's budget: GitHub reported it no longer than the export age ago.
//
// A snapshot whose window has reset since stays exported until a newer reading
// replaces it or it ages out. Like any snapshot, it is a reading no older than
// the export age, not a bound on the budget now: requests made since, by this
// replica or any other consumer of the installation, are not in it. Dropping
// it at the reset instead would report a budget that was read moments ago as
// unknown until the next quota probe pass, which fails a quota gate at every
// routine reset. An installation that stops being
// read still turns unknown, because its snapshot ages out.
func snapshotExportable(sample GitHubRateLimitSample, now time.Time) bool {
	elapsed := now.Sub(sample.ObservedAt)
	return elapsed >= 0 && elapsed <= GitHubRateLimitSnapshotMaxAge
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

// supersedesGitHubQuotaWindow reports whether sample, which reports a different
// quota window than previous, replaces it. The response received most recently
// says which window is current, whichever way its reset time moved: GitHub can
// report one reset time and then an earlier one for the rest of the window, and
// holding on to the first would stop the installation's budget from being
// refreshed until it ages out. A window last reported on an earlier response
// is kept with its lowest remaining but is not current. Between responses
// received at the same instant, the later window wins.
func supersedesGitHubQuotaWindow(previous, sample GitHubRateLimitSample) bool {
	switch {
	case sample.ObservedAt.After(previous.ObservedAt):
		return true
	case sample.ObservedAt.Equal(previous.ObservedAt):
		return sample.ResetAt.After(previous.ResetAt)
	default:
		return false
	}
}

// sameWindowGitHubRateLimitSnapshot combines two observations of one quota
// window. Depletion takes precedence over receipt order, so the lower
// remaining is kept even when GitHub later reports more. Freshness follows the
// later receipt, since either response proves the installation was observed
// at its receipt time: an installation read every minute never ages out, and
// a response recorded late cannot extend export lifetime past the newest
// receipt. changed reports whether the result differs from previous.
func sameWindowGitHubRateLimitSnapshot(previous, sample GitHubRateLimitSample) (merged GitHubRateLimitSample, changed bool) {
	merged = previous
	if sample.Remaining < previous.Remaining {
		merged = sample
		changed = true
	}
	merged.ObservedAt = previous.ObservedAt
	if sample.ObservedAt.After(previous.ObservedAt) {
		merged.ObservedAt = sample.ObservedAt
		changed = true
	}
	return merged, changed
}

func validGitHubRateLimitSnapshot(sample GitHubRateLimitSample) bool {
	return sample.GitHubApp != "" && sample.InstallationID > 0 &&
		isKnownGitHubRateLimitResource(sample.Resource) &&
		sample.Limit > 0 && sample.Remaining >= 0 && sample.Remaining <= sample.Limit &&
		!sample.ObservedAt.IsZero() && sample.ResetAt.After(sample.ObservedAt)
}
