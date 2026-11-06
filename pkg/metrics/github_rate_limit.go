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

const githubRateLimitMaxAge = 120 * time.Second

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

type githubRateLimitRecorder struct {
	mu          sync.Mutex
	diagnostics map[attribute.Distinct]githubRateLimitDiagnostic
	snapshots   map[githubRateLimitIdentity]GitHubRateLimitSample
	now         func() time.Time
}

// RegisterGitHubRateLimitMetrics installs the process recorder and observable
// gauges on provider. Call once during global metrics-provider setup, before
// recording requests. Observable collection removes expired snapshots from
// every export path instead of repeatedly scraping a retained synchronous gauge.
func RegisterGitHubRateLimitMetrics(provider otelmetric.MeterProvider) error {
	recorder := &githubRateLimitRecorder{
		diagnostics: make(map[attribute.Distinct]githubRateLimitDiagnostic),
		snapshots:   make(map[githubRateLimitIdentity]GitHubRateLimitSample),
		now:         time.Now,
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
		otelmetric.WithDescription("Age of the last trustworthy GitHub installation quota observation; idle installations become unknown"),
		otelmetric.WithUnit("s"))
	if err != nil {
		return fmt.Errorf("register GitHub quota observation age: %w", err)
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
			if elapsed >= 0 && elapsed <= githubRateLimitMaxAge && now.Before(sample.ResetAt) {
				observer.ObserveInt64(gauges["remaining"], sample.Remaining, opts)
			}
		}
		for _, diagnostic := range r.diagnostics {
			opts := otelmetric.WithAttributes(diagnostic.attrs...)
			observer.ObserveInt64(gauges["limit"], diagnostic.sample.Limit, opts)
			observer.ObserveInt64(gauges["remaining"], diagnostic.sample.Remaining, opts)
			observer.ObserveInt64(gauges["used"], diagnostic.sample.Used, opts)
		}

		return nil
	}, gauges["limit"], gauges["remaining"], gauges["used"], age)
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

func validGitHubRateLimitSnapshot(sample GitHubRateLimitSample) bool {
	return sample.GitHubApp != "" && sample.InstallationID > 0 &&
		isKnownGitHubRateLimitResource(sample.Resource) &&
		sample.Limit > 0 && sample.Remaining >= 0 && sample.Remaining <= sample.Limit &&
		!sample.ObservedAt.IsZero() && sample.ResetAt.After(sample.ObservedAt)
}
