package changeevents

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/url"
	"slices"
	"sync/atomic"
	"text/template"
	"time"
)

const (
	queueSize   = 128
	sendTimeout = 3 * time.Second
)

// Destination configures one HTTP endpoint and selects the events it receives.
type Destination struct {
	URL          string   `yaml:"url"`
	Environments []string `yaml:"environments"`
	Events       []Type   `yaml:"events"`
	Body         string   `yaml:"body"`
}

type destination struct {
	url          string
	environments []string
	events       []Type
	body         *template.Template
	queue        chan Event
	dropped      atomic.Uint64
}

// Sink accepts immutable events without waiting for delivery.
type Sink interface {
	Offer(Event)
}

// Sender delivers to independently bounded HTTP destinations.
type Sender struct {
	ctx          context.Context
	cancel       context.CancelFunc
	destinations []*destination
	client       *http.Client
	logger       *slog.Logger
}

func compile(configs []Destination) ([]*destination, error) {
	destinations := make([]*destination, 0, len(configs))
	for i, cfg := range configs {
		target, err := url.Parse(cfg.URL)
		if err != nil || target.Hostname() == "" || target.User != nil || target.Fragment != "" ||
			(target.Scheme != "http" && target.Scheme != "https") {
			return nil, fmt.Errorf("change_events[%d].url must be an HTTP or HTTPS URL without credentials or a fragment", i)
		}
		if len(cfg.Environments) == 0 || slices.Contains(cfg.Environments, "") {
			return nil, fmt.Errorf("change_events[%d].environments must list at least one nonempty environment", i)
		}
		if len(cfg.Events) == 0 {
			return nil, fmt.Errorf("change_events[%d].events must list at least one event type", i)
		}
		for _, event := range cfg.Events {
			switch event {
			case OperationStarted, OperationFinished, ApplyFinished:
			default:
				return nil, fmt.Errorf("change_events[%d].events has unknown event type %q", i, event)
			}
		}

		body, err := template.New("change event").Funcs(template.FuncMap{
			"json": func(value any) (string, error) {
				encoded, err := json.Marshal(value)
				return string(encoded), err
			},
		}).Parse(cfg.Body)
		if err != nil {
			return nil, fmt.Errorf("change_events[%d].body: %w", i, err)
		}
		dest := &destination{
			url: target.String(), environments: slices.Clone(cfg.Environments),
			events: slices.Clone(cfg.Events), body: body,
		}
		for _, event := range cfg.Events {
			sample := New(event, "example-app", cfg.Environments[0], "example description")
			if _, err := dest.render(sample); err != nil {
				return nil, fmt.Errorf("change_events[%d].body: %w", i, err)
			}
		}
		destinations = append(destinations, dest)
	}
	return destinations, nil
}

// Validate checks the destination configuration without starting a sender.
func Validate(configs []Destination) error {
	_, err := compile(configs)
	return err
}

// NewSender starts one delivery worker per destination. Close cancels those
// workers and discards queued work. Empty configuration returns a nil sender.
func NewSender(ctx context.Context, configs []Destination, logger *slog.Logger) (*Sender, error) {
	if len(configs) == 0 {
		return nil, nil
	}
	destinations, err := compile(configs)
	if err != nil {
		return nil, err
	}
	if logger == nil {
		logger = slog.Default()
	}
	ctx, cancel := context.WithCancel(context.WithoutCancel(ctx))
	s := &Sender{
		ctx: ctx, cancel: cancel, destinations: destinations, logger: logger,
		client: &http.Client{
			Timeout: sendTimeout,
			CheckRedirect: func(*http.Request, []*http.Request) error {
				return http.ErrUseLastResponse
			},
		},
	}
	for i, dest := range destinations {
		dest.queue = make(chan Event, queueSize)
		go s.deliver(i, dest)
	}
	return s, nil
}

// Accepts reports whether a destination selects this event type and environment.
func (s *Sender) Accepts(kind Type, environment string) bool {
	return slices.ContainsFunc(s.destinations, func(dest *destination) bool {
		return dest.accepts(kind, environment)
	})
}

func (d *destination) accepts(kind Type, environment string) bool {
	return slices.Contains(d.events, kind) && slices.Contains(d.environments, environment)
}

// Offer queues an event for any matching destinations. A full queue drops the
// event and records the drop for the delivery worker to report.
func (s *Sender) Offer(event Event) {
	if s == nil || s.ctx.Err() != nil {
		return
	}
	for _, dest := range s.destinations {
		if !dest.accepts(event.Type, event.Environment) {
			continue
		}
		select {
		case dest.queue <- event:
		default:
			dest.dropped.Add(1)
		}
	}
}

// Close stops delivery without waiting for a request or draining the queue.
func (s *Sender) Close() {
	if s != nil {
		s.cancel()
	}
}

func (s *Sender) deliver(index int, dest *destination) {
	for s.ctx.Err() == nil {
		select {
		case <-s.ctx.Done():
			return
		case event := <-dest.queue:
			if err := s.post(dest, event); err != nil && s.ctx.Err() == nil {
				s.logger.Warn("change event delivery failed", "destination", index, "event", event.Type, "error", err)
			}
			if dropped := dest.dropped.Swap(0); dropped > 0 {
				s.logger.Warn("change event queue full", "destination", index, "dropped", dropped)
			}
		}
	}
}

func (d *destination) render(event Event) ([]byte, error) {
	var body bytes.Buffer
	if err := d.body.Execute(&body, event); err != nil {
		return nil, fmt.Errorf("render change event: %w", err)
	}
	if !json.Valid(body.Bytes()) {
		return nil, fmt.Errorf("change event template rendered invalid JSON")
	}
	return body.Bytes(), nil
}

func (s *Sender) post(dest *destination, event Event) error {
	body, err := dest.render(event)
	if err != nil {
		return err
	}
	req, err := http.NewRequestWithContext(s.ctx, http.MethodPost, dest.url, bytes.NewReader(body))
	if err != nil {
		return fmt.Errorf("build change event request")
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := s.client.Do(req)
	if err != nil {
		// An HTTP client error can contain the configured URL, including query parameters.
		var requestErr *url.Error
		if errors.As(err, &requestErr) {
			err = requestErr.Err
		}
		return fmt.Errorf("POST change event: %w", err)
	}
	defer resp.Body.Close()
	_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, 4096))
	if resp.StatusCode < http.StatusOK || resp.StatusCode >= http.StatusMultipleChoices {
		return fmt.Errorf("change event endpoint returned HTTP %d", resp.StatusCode)
	}
	return nil
}
