package metrics

import (
	"context"
	"log/slog"
	"strconv"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	otelmetric "go.opentelemetry.io/otel/metric"
)

const (
	GitHubRejectionOriginHTTP  = "http"
	GitHubRejectionOriginLocal = "local"

	GitHubWaitLayerReadRetry       = "read_retry"
	GitHubWaitReasonSecondaryLimit = "secondary_rate_limit"
	GitHubWaitReasonTransientError = "transient_error"
)

// GitHubClientMetricContext attributes quota rejections, cooldown reports and retry waits without
// repository or request identifiers. Unknown identities remain explicit so an
// unresolved App cannot disappear from an installation-scoped alert.
type GitHubClientMetricContext struct {
	Operation      string
	Resource       string
	GitHubApp      string
	InstallationID int64
}

func (s GitHubClientMetricContext) attributes() []attribute.KeyValue {
	if s.Operation == "" {
		s.Operation = GitHubOperationUnknown
	}
	if s.Resource == "" {
		s.Resource = gitHubMetricValueUnknown
	}
	app := s.GitHubApp
	if app == "" {
		app = gitHubMetricValueUnknown
	}
	installation := gitHubMetricValueUnknown
	if s.InstallationID > 0 {
		installation = strconv.FormatInt(s.InstallationID, 10)
	}
	return []attribute.KeyValue{
		EnvironmentAttribute(""),
		attribute.String("operation", normalizeGitHubOperation(s.Operation)),
		attribute.String("resource", normalizeGitHubRateLimitResource(s.Resource)),
		attribute.String("github_app", app),
		attribute.String("installation_id", installation),
	}
}

// RecordGitHubPrimaryRateLimitRejection counts at the producing layer only.
// HTTP rejections also appear in requests_total; local refusals do not consume
// GitHub quota. Alert on either origin without adding these to HTTP totals.
func RecordGitHubPrimaryRateLimitRejection(ctx context.Context, sample GitHubClientMetricContext, origin string) {
	origin = normalizeGitHubLabel("origin", origin, func(v string) bool {
		return v == GitHubRejectionOriginHTTP || v == GitHubRejectionOriginLocal
	})
	attrs := append(sample.attributes(), attribute.String("origin", origin))
	addCounter(ctx, "schemabot.github.primary_rate_limit_rejections_total",
		"GitHub primary quota rejections, including requests prevented locally", "{rejection}", attrs...)
}

// RecordGitHubWait records actual elapsed time for one positive sleep entered,
// including interrupted waits. Counts and sums aggregate across callers; the
// distribution is per wait, not per operation or outage.
func RecordGitHubWait(ctx context.Context, sample GitHubClientMetricContext, layer, reason string, elapsed time.Duration) {
	if elapsed <= 0 {
		return
	}
	layer = normalizeGitHubLabel("layer", layer, func(v string) bool {
		return v == GitHubWaitLayerReadRetry
	})
	reason = normalizeGitHubLabel("reason", reason, func(v string) bool {
		return v == GitHubWaitReasonSecondaryLimit || v == GitHubWaitReasonTransientError
	})
	attrs := append(sample.attributes(), attribute.String("layer", layer), attribute.String("reason", reason))
	hist, err := otel.Meter(meterName).Float64Histogram("schemabot.github.wait_duration_seconds",
		otelmetric.WithDescription("Actual elapsed seconds per GitHub outer read-retry wait"),
		otelmetric.WithUnit("s"),
		otelmetric.WithExplicitBucketBoundaries(0.01, 0.05, 0.1, 0.25, 0.5, 1, 2, 5, 10, 20, 30, 60),
	)
	if err != nil {
		slog.Warn("failed to create GitHub wait histogram", "error", err)
		return
	}
	hist.Record(ctx, elapsed.Seconds(), otelmetric.WithAttributes(attrs...))
}

const (
	GitHubCooldownAccepted                 = "accepted"
	GitHubCooldownSingleSleepLimitExceeded = "single_sleep_limit_exceeded"
	GitHubCooldownTotalSleepLimitExceeded  = "total_sleep_limit_exceeded"
)

// RecordGitHubSecondaryCooldownReport emits matching report counts and reported
// seconds. These are limiter decisions, not actual sleep or unique callers.
func RecordGitHubSecondaryCooldownReport(ctx context.Context, sample GitHubClientMetricContext, disposition string, reported time.Duration) {
	disposition = normalizeGitHubLabel("disposition", disposition, func(v string) bool {
		return v == GitHubCooldownAccepted || v == GitHubCooldownSingleSleepLimitExceeded || v == GitHubCooldownTotalSleepLimitExceeded
	})
	attrs := append(sample.attributes(), attribute.String("disposition", disposition))
	meter := otel.Meter(meterName)
	reports, err := meter.Int64Counter("schemabot.github.secondary_rate_limit_cooldown_reports_total",
		otelmetric.WithDescription("Secondary rate limit cooldown decisions; not caller waits or final failures"), otelmetric.WithUnit("{report}"))
	if err != nil {
		slog.Warn("failed to create GitHub cooldown report counter", "error", err)
		return
	}
	seconds, err := meter.Float64Counter("schemabot.github.secondary_rate_limit_cooldown_reported_seconds_total",
		otelmetric.WithDescription("Sum of reported secondary cooldown seconds, including declined cooldowns; not actual waiting"), otelmetric.WithUnit("s"))
	if err != nil {
		slog.Warn("failed to create GitHub cooldown seconds counter", "error", err)
		return
	}
	options := otelmetric.WithAttributes(attrs...)
	reports.Add(ctx, 1, options)
	seconds.Add(ctx, max(0, reported.Seconds()), options)
}
