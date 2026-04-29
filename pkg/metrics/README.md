# SchemaBot Metrics

SchemaBot exposes metrics via OpenTelemetry. All metrics are available at `GET /metrics` (Prometheus format) and optionally pushed via OTLP when `OTEL_EXPORTER_OTLP_ENDPOINT` is set.

## Custom Metrics

| Metric | Type | Attributes | Description |
|---|---|---|---|
| `schemabot.plans.total` | Counter | database, environment, status | Total plan operations |
| `schemabot.plan.duration_seconds` | Histogram | database, environment, status | Plan execution time |
| `schemabot.applies.total` | Counter | database, environment, status | Total apply operations |
| `schemabot.apply.duration_seconds` | Histogram | database, environment, status | Apply API call time |
| `schemabot.active_applies` | UpDownCounter | database, environment | In-progress applies |
| `schemabot.webhook.events_total` | Counter | event_type, action, repository, status | GitHub webhook events |

### Attribute Values

**status** (plans/applies): `success`, `error`, `rejected`

**event_type** (webhooks): `issue_comment`, `pull_request`, `check_run`, `ping`

**action** (webhooks): `created`, `opened`, `synchronize`, `reopened`, `closed`, `requested`, `completed` (omitted for events without actions like `ping`)

**status** (webhooks): `processed`, `invalid_signature`, `ignored`

## HTTP Server Metrics

The `otelhttp` middleware automatically produces standard HTTP metrics for every endpoint:

| Metric | Type | Description |
|---|---|---|
| `http.server.request.duration` | Histogram | Request latency by method and status code |
| `http.server.request.body.size` | Histogram | Request body sizes |
| `http.server.response.body.size` | Histogram | Response body sizes |

## Adding New Metrics

Define recording functions in `metrics.go` following the existing pattern:

```go
func RecordXxx(ctx context.Context, attrs ...string) {
    meter := otel.Meter(meterName)
    counter, err := meter.Int64Counter("schemabot.xxx.total",
        otelmetric.WithDescription("Description"),
        otelmetric.WithUnit("{unit}"),
    )
    if err != nil {
        slog.Warn("failed to create counter", "error", err)
        return
    }
    counter.Add(ctx, 1, otelmetric.WithAttributes(...))
}
```

The OTel SDK deduplicates instruments with the same name, so calling `Int64Counter` on every invocation is safe and cheap after the first registration.
