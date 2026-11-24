package notifications

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"maps"
	"slices"
	"strings"

	"gopkg.in/yaml.v3"
)

// Destination selects a publisher and the events recorded for it. Empty filters
// match every supported event and environment. Settings belong to the factory;
// they are inline so an embedder need not add transport fields to core types.
type Destination struct {
	Type         string         `yaml:"type"`
	Events       []string       `yaml:"events"`
	Environments []string       `yaml:"environments"`
	Settings     map[string]any `yaml:",inline"`
}

// Validate checks the common envelope without rejecting embedder-owned types.
// Transport settings are validated by the registered factory during resolution.
func (d Destination) Validate() error {
	if strings.TrimSpace(d.Type) == "" {
		return fmt.Errorf("change_events destination type is required")
	}
	for _, event := range d.Events {
		switch event {
		case "apply.finished", "operation.finished":
		default:
			return fmt.Errorf("change_events destination %q has unsupported event %q", d.Type, event)
		}
	}
	return nil
}

// Matches selects new intent only. Delivery must not reapply current filters to
// an already recorded event, whose destination identity is durable.
func (d Destination) Matches(eventType, environment string) bool {
	return (len(d.Events) == 0 || slices.Contains(d.Events, eventType)) &&
		(len(d.Environments) == 0 || slices.Contains(d.Environments, environment))
}

// DecodeSettings decodes factory-owned fields and rejects unknown settings.
func (d Destination) DecodeSettings(target any) error {
	data, err := yaml.Marshal(d.Settings)
	if err != nil {
		return fmt.Errorf("encode settings for publisher %q: %w", d.Type, err)
	}
	decoder := yaml.NewDecoder(bytes.NewReader(data))
	decoder.KnownFields(true)
	if err := decoder.Decode(target); err != nil {
		return fmt.Errorf("decode settings for publisher %q: %w", d.Type, err)
	}
	return nil
}

// PublisherFactory resolves transport settings once at startup, outside storage
// transactions. Identity must name the effective destination, excluding filters
// and credentials. Changing its meaning would redirect previously queued intent.
type PublisherFactory func(context.Context, Destination) (ResolvedPublisher, error)

// ResolvedPublisher couples a client to the canonical identity of the settings
// it actually uses. Factories must return both a nonempty identity and a client.
type ResolvedPublisher struct {
	Identity  string
	Publisher Publisher
}

// Route is a resolved destination. Key is stable across configuration reordering
// and filter or credential edits, but changes when the transport identity changes.
type Route struct {
	Key         string
	Destination Destination
	Publisher   Publisher
}

// Registry owns publisher factories. SQS is the only built-in transport;
// embedder registrations override built-ins, as serve.WithEngine does.
type Registry struct {
	factories map[string]PublisherFactory
}

// NewRegistry validates registrations even when no destination uses them.
func NewRegistry(factories map[string]PublisherFactory) (*Registry, error) {
	registered := map[string]PublisherFactory{"sqs": newSQSDestination}
	for kind, factory := range factories {
		if strings.TrimSpace(kind) == "" {
			return nil, fmt.Errorf("register publisher: type is required")
		}
		if factory == nil {
			return nil, fmt.Errorf("register publisher %q: factory is required", kind)
		}
		registered[kind] = factory
	}
	return &Registry{factories: registered}, nil
}

// Resolve constructs each destination once and rejects ambiguous duplicate
// identities. An empty list loads no AWS configuration and builds no clients.
func (r *Registry) Resolve(ctx context.Context, destinations []Destination) ([]Route, error) {
	var routes []Route
	keys := make(map[string]bool, len(destinations))
	for i, destination := range destinations {
		if err := destination.Validate(); err != nil {
			return nil, fmt.Errorf("change_events entry %d: %w", i, err)
		}
		factory, ok := r.factories[destination.Type]
		if !ok {
			return nil, fmt.Errorf("change_events entry %d: publisher type %q is not registered", i, destination.Type)
		}
		resolved, err := factory(ctx, destination)
		if err != nil {
			return nil, fmt.Errorf("resolve change_events entry %d (%s): %w", i, destination.Type, err)
		}
		if resolved.Identity == "" {
			return nil, fmt.Errorf("resolve change_events entry %d: publisher identity is required", i)
		}
		if resolved.Publisher == nil {
			return nil, fmt.Errorf("resolve change_events entry %d: publisher is required", i)
		}
		identity, err := json.Marshal([]string{"change_events.v1", destination.Type, resolved.Identity})
		if err != nil {
			return nil, fmt.Errorf("encode destination identity for entry %d: %w", i, err)
		}
		key := fmt.Sprintf("%x", sha256.Sum256(identity))
		if keys[key] {
			return nil, fmt.Errorf("change_events entry %d duplicates destination %s", i, key)
		}
		keys[key] = true
		destination.Events = slices.Clone(destination.Events)
		destination.Environments = slices.Clone(destination.Environments)
		destination.Settings = maps.Clone(destination.Settings)
		routes = append(routes, Route{Key: key, Destination: destination, Publisher: resolved.Publisher})
	}
	return routes, nil
}

func newSQSDestination(ctx context.Context, destination Destination) (ResolvedPublisher, error) {
	var settings struct {
		QueueURL string `yaml:"queue_url"`
		Region   string `yaml:"region"`
	}
	if err := destination.DecodeSettings(&settings); err != nil {
		return ResolvedPublisher{}, err
	}
	if err := validateSQSQueue(settings.QueueURL); err != nil {
		return ResolvedPublisher{}, err
	}
	return resolveSQSPublisher(ctx, settings.QueueURL, settings.Region)
}
