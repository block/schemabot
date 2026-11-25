// Package changeevents delivers configurable, best-effort schema change events.
package changeevents

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

const (
	OperationStarted  = "operation.started"
	OperationFinished = "operation.finished"
	ApplyFinished     = "apply.finished"

	queueCapacity      = 128
	sendTimeout        = 3 * time.Second
	dropReportInterval = time.Minute
)

// Destination selects and formats events sent to one HTTP endpoint.
type Destination struct {
	URL          string   `yaml:"url"`
	Environments []string `yaml:"environments"`
	Events       []string `yaml:"events"`
	Body         string   `yaml:"body"`
}

// Event is a value snapshot offered after a lifecycle boundary.
type Event struct {
	Type            string
	App             string
	Environment     string
	OccurredAtNanos int64
	Description     string
	// Details holds structured lifecycle fields for publishers. HTTP templates
	// that encode the entire event retain their existing JSON representation.
	Details EventDetails `json:"-"`
}

// EventDetails identifies the apply, and the operation attempt when applicable.
// EventID is shared by all destinations that receive this event.
type EventDetails struct {
	EventID      string    `json:"event_id"`
	ApplyID      string    `json:"id"`
	Database     string    `json:"database"`
	DatabaseType string    `json:"database_type"`
	Repository   string    `json:"repository,omitempty"`
	PullRequest  int       `json:"pull_request,omitempty"`
	Outcome      string    `json:"outcome,omitempty"`
	CompletedAt  time.Time `json:"completed_at,omitzero"`
	OperationID  int64     `json:"operation_id,omitempty"`
	Deployment   string    `json:"deployment,omitempty"`
	Attempt      int       `json:"attempt,omitempty"`
}

// Publisher sends one encoded JSON payload. Publish respects ctx and returns
// nil once the transport accepts the payload.
type Publisher interface {
	Publish(context.Context, json.RawMessage) error
}

// PublisherDestination selects events for one publisher. Encode runs on the
// destination's worker and must return a complete JSON value.
type PublisherDestination struct {
	// ConfigPath identifies the entry in configuration validation errors.
	ConfigPath   string
	Environments []string
	Events       []string
	Encode       func(Event) (json.RawMessage, error)
	Publisher    Publisher
}

type delivery struct {
	destination PublisherDestination
	queue       chan queuedEvent
	drops       atomic.Uint64
}

type queuedEvent struct {
	event   Event
	resolve func() (Event, bool)
}

// Sender queues events for independent destinations. A nil Sender is disabled.
type Sender struct {
	deliveries []*delivery
	cancel     context.CancelFunc
	ctx        context.Context
}

// New starts delivery to the configured HTTP destinations.
func New(destinations []Destination, logger *slog.Logger) (*Sender, error) {
	return NewWithPublishers(destinations, nil, logger)
}

// NewWithPublishers starts one bounded worker per HTTP or injected destination.
// With no destinations it returns a disabled (nil) sender.
func NewWithPublishers(httpDestinations []Destination, publishers []PublisherDestination, logger *slog.Logger) (*Sender, error) {
	if len(httpDestinations) == 0 && len(publishers) == 0 {
		return nil, nil
	}
	destinations := make([]PublisherDestination, 0, len(httpDestinations)+len(publishers))
	for i, config := range httpDestinations {
		dest, err := newHTTPDestination(config)
		if err != nil {
			return nil, fmt.Errorf("change_events[%d]: %w", i, err)
		}
		dest.ConfigPath = fmt.Sprintf("change_events[%d]", i)
		destinations = append(destinations, dest)
	}
	destinations = append(destinations, publishers...)

	sender := &Sender{}
	for i, dest := range destinations {
		path := dest.ConfigPath
		if path == "" {
			path = fmt.Sprintf("publishers[%d]", i-len(httpDestinations))
		}
		if len(dest.Environments) == 0 || slices.ContainsFunc(dest.Environments, func(env string) bool { return strings.TrimSpace(env) == "" }) {
			return nil, fmt.Errorf("%s: environments must name at least one environment", path)
		}
		if len(dest.Events) == 0 {
			return nil, fmt.Errorf("%s: events must select at least one event", path)
		}
		for _, kind := range dest.Events {
			switch kind {
			case OperationStarted, OperationFinished, ApplyFinished:
			default:
				return nil, fmt.Errorf("%s: unknown event %q", path, kind)
			}
		}
		if dest.Encode == nil || dest.Publisher == nil {
			return nil, fmt.Errorf("%s: encoder and publisher are required", path)
		}
		dest.Environments = slices.Clone(dest.Environments)
		dest.Events = slices.Clone(dest.Events)
		sender.deliveries = append(sender.deliveries, &delivery{
			destination: dest, queue: make(chan queuedEvent, queueCapacity),
		})
	}
	sender.ctx, sender.cancel = context.WithCancel(context.Background())
	for i, dest := range sender.deliveries {
		go dest.run(sender.ctx, logger.With("change_event_destination", i))
	}
	return sender, nil
}

func (d *delivery) accepts(kind, env string) bool {
	return slices.Contains(d.destination.Events, kind) && slices.Contains(d.destination.Environments, env)
}

// Selects reports whether a destination selects this event in any environment.
func (s *Sender) Selects(kind string) bool {
	return s != nil && slices.ContainsFunc(s.deliveries, func(d *delivery) bool {
		return slices.Contains(d.destination.Events, kind)
	})
}

// Enabled reports whether a destination selects the event and environment.
func (s *Sender) Enabled(kind, env string) bool {
	return s != nil && slices.ContainsFunc(s.deliveries, func(d *delivery) bool {
		return d.accepts(kind, env)
	})
}

// Offer queues an event without waiting for a receiver or calling the logger.
func (s *Sender) Offer(event Event) {
	s.offer(queuedEvent{event: event})
}

// OfferDeferred queues preparation on the delivery workers. The workers
// share one resolution, which uses the sender's shutdown context. A false
// result discards the event.
func (s *Sender) OfferDeferred(kind, environment string, resolve func(context.Context) (Event, bool)) {
	if !s.Enabled(kind, environment) {
		return
	}
	s.offer(queuedEvent{
		event:   Event{Type: kind, Environment: environment},
		resolve: sync.OnceValues(func() (Event, bool) { return resolve(s.ctx) }),
	})
}

func (s *Sender) offer(work queuedEvent) {
	if s == nil || s.ctx.Err() != nil {
		return
	}
	for _, d := range s.deliveries {
		if !d.accepts(work.event.Type, work.event.Environment) {
			continue
		}
		select {
		case d.queue <- work:
		default:
			d.drops.Add(1)
		}
	}
}

// Close cancels outstanding delivery and abandons queued events.
func (s *Sender) Close() {
	if s != nil {
		s.cancel()
	}
}

func (d *delivery) run(ctx context.Context, logger *slog.Logger) {
	reportDrops := func() {
		if count := d.drops.Swap(0); count > 0 {
			logger.Warn("change event queue full", "dropped", count)
		}
	}
	ticker := time.NewTicker(dropReportInterval)
	defer ticker.Stop()
	defer reportDrops()
	for ctx.Err() == nil {
		select {
		case work := <-d.queue:
			event := work.event
			if work.resolve != nil {
				var ready bool
				event, ready = work.resolve()
				if !ready || !d.accepts(event.Type, event.Environment) {
					continue
				}
			}
			if err := d.send(ctx, event); err != nil && ctx.Err() == nil {
				logger.Warn("change event delivery failed", "event", event.Type, "environment", event.Environment, "error", err)
			}
		case <-ticker.C:
			reportDrops()
		case <-ctx.Done():
			return
		}
	}
}

func (d *delivery) send(ctx context.Context, event Event) error {
	payload, err := d.destination.Encode(event)
	if err != nil {
		return fmt.Errorf("render body: %w", err)
	}
	if !json.Valid(payload) {
		return fmt.Errorf("rendered body is invalid JSON")
	}
	sendCtx, cancel := context.WithTimeout(ctx, sendTimeout)
	defer cancel()
	return d.destination.Publisher.Publish(sendCtx, payload)
}
