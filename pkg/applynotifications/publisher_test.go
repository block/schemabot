package applynotifications

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"strconv"
	"testing"
	"time"

	"github.com/block/schemabot/pkg/state"
	"github.com/block/schemabot/pkg/storage"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
)

type publisherFunc func(context.Context, Event) error

func (f publisherFunc) Publish(ctx context.Context, event Event) error { return f(ctx, event) }

func TestConfig(t *testing.T) {
	for _, tc := range []struct {
		name      string
		cfg       Config
		wantError string
	}{
		{name: "default off"},
		{name: "disabled ignores unused settings", cfg: Config{Type: "unused"}},
		{name: "enabled requires type", cfg: Config{Enabled: true}, wantError: "type must be sqs"},
		{name: "no kafka", cfg: Config{Enabled: true, Type: "kafka"}, wantError: "type must be sqs"},
		{name: "enabled requires queue", cfg: Config{Enabled: true, Type: "sqs"}, wantError: "queue_url is required"},
		{name: "sqs", cfg: Config{Enabled: true, Type: "sqs", QueueURL: "https://sqs.example/queue"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			err := tc.cfg.Validate()
			if tc.wantError != "" {
				require.ErrorContains(t, err, tc.wantError)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

func TestWrapPreservesSummaryAndFiltersStates(t *testing.T) {
	completedAt := time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)
	for _, applyState := range []string{state.Apply.Completed, state.Apply.Failed, state.Apply.Stopped, state.Apply.Cancelled, state.Apply.Reverted, state.Apply.Running} {
		for _, summaryFails := range []bool{false, true} {
			t.Run(applyState+"/summary_error="+strconv.FormatBool(summaryFails), func(t *testing.T) {
				apply := &storage.Apply{ApplyIdentifier: "apply-123", Database: "shop", DatabaseType: "mysql", Environment: "staging", Repository: "octo/schema", PullRequest: 42, State: applyState, CompletedAt: &completedAt}
				tasks := []*storage.Task{{ID: 1}, {ID: 2}}
				var order []string
				var summaryErr error
				if summaryFails {
					summaryErr = errors.New("GitHub unavailable")
				}
				next := func(_ context.Context, a *storage.Apply, ts []*storage.Task) error {
					order = append(order, "github")
					assert.Same(t, apply, a)
					assert.Equal(t, tasks, ts)
					return summaryErr
				}
				publisher := publisherFunc(func(ctx context.Context, event Event) error {
					order = append(order, "sqs")
					deadline, ok := ctx.Deadline()
					assert.True(t, ok)
					assert.WithinDuration(t, time.Now(), deadline, 3*time.Second)
					assert.Equal(t, Event{ApplyIdentifier: "apply-123", Database: "shop", DatabaseType: "mysql", Environment: "staging", Repository: "octo/schema", PullRequest: 42, CompletedAt: &completedAt}, event)
					return nil
				})
				err := Wrap(next, publisher, slog.Default())(t.Context(), apply, tasks)
				assert.Equal(t, summaryErr, err)
				wantOrder := []string{"github"}
				if applyState == state.Apply.Completed {
					wantOrder = append(wantOrder, "sqs")
				}
				assert.Equal(t, wantOrder, order)
			})
		}
	}
}

func TestWrapDisabled(t *testing.T) {
	var logs bytes.Buffer
	calls := 0
	callback := Wrap(func(context.Context, *storage.Apply, []*storage.Task) error { calls++; return nil }, nil, slog.New(slog.NewTextHandler(&logs, nil)))
	require.NoError(t, callback(t.Context(), &storage.Apply{State: state.Apply.Completed}, nil))
	assert.Equal(t, 1, calls)
	assert.Empty(t, logs.String())
	assert.Nil(t, Wrap(nil, nil, slog.Default()))
}

func TestWrapFailureIsOnlyLoggedAndCounted(t *testing.T) {
	for _, panics := range []bool{false, true} {
		t.Run(strconv.FormatBool(panics), func(t *testing.T) {
			reader := metric.NewManualReader()
			provider := metric.NewMeterProvider(metric.WithReader(reader))
			previous := otel.GetMeterProvider()
			otel.SetMeterProvider(provider)
			t.Cleanup(func() {
				otel.SetMeterProvider(previous)
				require.NoError(t, provider.Shutdown(context.WithoutCancel(t.Context())))
			})
			var logs bytes.Buffer
			apply := &storage.Apply{ApplyIdentifier: "apply-123", State: state.Apply.Completed}
			summaryErr := errors.New("GitHub failure")
			callback := Wrap(func(context.Context, *storage.Apply, []*storage.Task) error { return summaryErr }, publisherFunc(func(context.Context, Event) error {
				if panics {
					panic("queue panic")
				}
				return errors.New("queue unavailable")
			}), slog.New(slog.NewTextHandler(&logs, nil)))
			require.ErrorIs(t, callback(t.Context(), apply, nil), summaryErr)
			assert.Equal(t, state.Apply.Completed, apply.State)
			assert.Contains(t, logs.String(), "apply-123")
			assert.Contains(t, logs.String(), "notification dropped")
			var rm metricdata.ResourceMetrics
			require.NoError(t, reader.Collect(t.Context(), &rm))
			var failures int64
			for _, scope := range rm.ScopeMetrics {
				for _, m := range scope.Metrics {
					if m.Name == "schemabot.operator.terminal_summary_publish_failures_total" {
						for _, point := range m.Data.(metricdata.Sum[int64]).DataPoints {
							failures += point.Value
							value, ok := point.Attributes.Value("reason")
							assert.True(t, ok)
							assert.Equal(t, "notification_send_error", value.AsString())
						}
					}
				}
			}
			assert.Equal(t, int64(1), failures)
		})
	}
}
