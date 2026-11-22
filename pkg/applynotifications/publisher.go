// Package applynotifications sends best-effort successful apply notifications.
package applynotifications

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/block/schemabot/pkg/metrics"
	"github.com/block/schemabot/pkg/panicsafe"
	"github.com/block/schemabot/pkg/state"
	"github.com/block/schemabot/pkg/storage"
)

// Config selects an optional completion publisher. The zero value is disabled.
type Config struct {
	Enabled  bool   `yaml:"enabled"`
	Type     string `yaml:"type"`
	QueueURL string `yaml:"queue_url"`
}

// Validate checks enabled publisher settings without contacting the queue.
func (c Config) Validate() error {
	if !c.Enabled {
		return nil
	}
	if c.Type != "sqs" {
		return fmt.Errorf("apply_notifications.type must be sqs when enabled")
	}
	if c.QueueURL == "" {
		return fmt.Errorf("apply_notifications.queue_url is required when enabled")
	}
	return nil
}

// Event is a successful apply's stored completion. Receivers deduplicate on
// ApplyIdentifier; neither receipt nor eventual delivery is guaranteed.
type Event struct {
	ApplyIdentifier string     `json:"apply_identifier"`
	Database        string     `json:"database"`
	DatabaseType    string     `json:"database_type"`
	Environment     string     `json:"environment"`
	Repository      string     `json:"repository,omitempty"`
	PullRequest     int        `json:"pull_request,omitempty"`
	CompletedAt     *time.Time `json:"completed_at"`
}

// Publisher sends a completion to one transport.
type Publisher interface {
	Publish(context.Context, Event) error
}

// Wrap preserves the existing terminal-summary callback and its return value.
// A nil publisher leaves it unchanged. Only callers that already reach this
// callback can emit; this does not add hooks to direct or recovery completions.
func Wrap(next func(context.Context, *storage.Apply, []*storage.Task) error, publisher Publisher, logger *slog.Logger) func(context.Context, *storage.Apply, []*storage.Task) error {
	if publisher == nil {
		return next
	}
	return func(ctx context.Context, apply *storage.Apply, tasks []*storage.Task) error {
		var summaryErr error
		if next != nil {
			summaryErr = next(ctx, apply, tasks)
		}
		if !state.IsState(apply.State, state.Apply.Completed) {
			return summaryErr
		}
		// The state is already durable. Bound the inline side effect and contain
		// publisher panics so it cannot change the apply's outcome.
		sendCtx, cancel := context.WithTimeout(ctx, 3*time.Second)
		defer cancel()
		err := panicsafe.Call(func() error {
			return publisher.Publish(sendCtx, Event{
				ApplyIdentifier: apply.ApplyIdentifier,
				Database:        apply.Database,
				DatabaseType:    apply.DatabaseType,
				Environment:     apply.Environment,
				Repository:      apply.Repository,
				PullRequest:     apply.PullRequest,
				CompletedAt:     apply.CompletedAt,
			})
		})
		if err != nil {
			logger.Warn("completion notification dropped; apply remains completed", append(apply.LogAttrs(), "error", err)...)
			metrics.RecordOperatorTerminalSummaryFailure(ctx, "notification_send_error")
		}
		return summaryErr
	}
}
