package notifications

import (
	"context"
	"encoding/json"
	"errors"
	"testing"

	"github.com/aws/aws-sdk-go-v2/service/sqs"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gopkg.in/yaml.v3"
)

type destinationPublisher struct{}

func (destinationPublisher) Publish(context.Context, json.RawMessage) error { return nil }

func TestDestinationFilters(t *testing.T) {
	for _, tt := range []struct {
		name               string
		destination        Destination
		event, environment string
		matches            bool
	}{
		{"omitted", Destination{}, "apply.finished", "production", true},
		{"empty", Destination{Events: []string{}, Environments: []string{}}, "operation.finished", "staging", true},
		{"event", Destination{Events: []string{"operation.finished"}}, "operation.finished", "staging", true},
		{"other event", Destination{Events: []string{"apply.finished"}}, "operation.finished", "production", false},
		{"environment", Destination{Environments: []string{"production"}}, "apply.finished", "staging", false},
		{"both", Destination{Events: []string{"apply.finished"}, Environments: []string{"production"}}, "apply.finished", "production", true},
	} {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.matches, tt.destination.Matches(tt.event, tt.environment))
		})
	}
}

func TestDestinationFactorySettings(t *testing.T) {
	var destinations []Destination
	require.NoError(t, yaml.Unmarshal([]byte(`
- type: custom
  events: [operation.finished]
  environments: [production]
  endpoint: example
`), &destinations))
	var received Destination
	registry, err := NewRegistry(map[string]PublisherFactory{
		"custom": func(_ context.Context, destination Destination) (ResolvedPublisher, error) {
			received = destination
			var settings struct {
				Endpoint string `yaml:"endpoint"`
			}
			if err := destination.DecodeSettings(&settings); err != nil {
				return ResolvedPublisher{}, err
			}
			return ResolvedPublisher{Identity: settings.Endpoint, Publisher: destinationPublisher{}}, nil
		},
	})
	require.NoError(t, err)
	routes, err := registry.Resolve(t.Context(), destinations)
	require.NoError(t, err)
	require.Len(t, routes, 1)
	assert.Equal(t, []string{"operation.finished"}, received.Events)
	assert.Equal(t, []string{"production"}, received.Environments)
	assert.Equal(t, map[string]any{"endpoint": "example"}, received.Settings)
	assert.Equal(t, "example", routes[0].Destination.Settings["endpoint"])
	destinations[0].Events[0] = "apply.finished"
	assert.Equal(t, []string{"operation.finished"}, routes[0].Destination.Events)

	destination := Destination{Type: "custom", Settings: map[string]any{"typo": true}}
	var settings struct {
		Endpoint string `yaml:"endpoint"`
	}
	require.ErrorContains(t, destination.DecodeSettings(&settings), "field typo not found")
}

func TestDestinationIdentity(t *testing.T) {
	registry, err := NewRegistry(map[string]PublisherFactory{
		"custom": func(_ context.Context, destination Destination) (ResolvedPublisher, error) {
			return ResolvedPublisher{Identity: destination.Settings["endpoint"].(string), Publisher: destinationPublisher{}}, nil
		},
	})
	require.NoError(t, err)
	a := Destination{Type: "custom", Settings: map[string]any{"endpoint": "a", "credential": "first"}}
	b := Destination{Type: "custom", Settings: map[string]any{"endpoint": "b"}}
	original, err := registry.Resolve(t.Context(), []Destination{a, b})
	require.NoError(t, err)
	a.Events = []string{"operation.finished"}
	a.Environments = []string{"staging"}
	a.Settings["credential"] = "rotated"
	reordered, err := registry.Resolve(t.Context(), []Destination{b, a})
	require.NoError(t, err)
	assert.Equal(t, original[0].Key, reordered[1].Key)
	assert.Equal(t, original[1].Key, reordered[0].Key)
	assert.NotEqual(t, original[0].Key, original[1].Key)
	_, err = registry.Resolve(t.Context(), []Destination{a, a})
	require.ErrorContains(t, err, "duplicates destination")
}

func TestDestinationRegistryValidation(t *testing.T) {
	_, err := NewRegistry(map[string]PublisherFactory{"custom": nil})
	require.ErrorContains(t, err, "factory is required")
	_, err = NewRegistry(map[string]PublisherFactory{" ": newSQSDestination})
	require.ErrorContains(t, err, "type is required")
	registry, err := NewRegistry(map[string]PublisherFactory{
		"failure": func(context.Context, Destination) (ResolvedPublisher, error) {
			return ResolvedPublisher{}, errors.New("factory failed")
		},
		"empty": func(context.Context, Destination) (ResolvedPublisher, error) { return ResolvedPublisher{}, nil },
		"nil": func(context.Context, Destination) (ResolvedPublisher, error) {
			return ResolvedPublisher{Identity: "example"}, nil
		},
	})
	require.NoError(t, err)
	for _, tt := range []struct {
		destination Destination
		message     string
	}{
		{Destination{}, "type is required"},
		{Destination{Type: "unknown"}, "not registered"},
		{Destination{Type: "sqs", Events: []string{"operation.started"}}, "unsupported event"},
		{Destination{Type: "sqs"}, "queue_url must be"},
		{Destination{Type: "failure"}, "factory failed"},
		{Destination{Type: "empty"}, "identity is required"},
		{Destination{Type: "nil"}, "publisher is required"},
	} {
		_, err := registry.Resolve(t.Context(), []Destination{tt.destination})
		require.ErrorContains(t, err, tt.message)
	}
}

func TestSQSDestinationEffectiveRegion(t *testing.T) {
	t.Setenv("AWS_REGION", "us-west-2")
	t.Setenv("AWS_CONFIG_FILE", t.TempDir()+"/config")
	t.Setenv("AWS_SHARED_CREDENTIALS_FILE", t.TempDir()+"/credentials")
	t.Setenv("AWS_EC2_METADATA_DISABLED", "true")
	registry, err := NewRegistry(nil)
	require.NoError(t, err)
	destination := Destination{Type: "sqs", Settings: map[string]any{"queue_url": "https://sqs.us-west-2.amazonaws.com/123456789012/outcomes"}}
	ambient, err := registry.Resolve(t.Context(), []Destination{destination})
	require.NoError(t, err)
	destination.Settings["region"] = "us-west-2"
	explicit, err := registry.Resolve(t.Context(), []Destination{destination})
	require.NoError(t, err)
	assert.Equal(t, ambient[0].Key, explicit[0].Key)
	client := ambient[0].Publisher.(*sqsPublisher).client
	assert.Equal(t, "us-west-2", client.(*sqs.Client).Options().Region)
	destination.Settings["region"] = "us-east-1"
	changed, err := registry.Resolve(t.Context(), []Destination{destination})
	require.NoError(t, err)
	assert.NotEqual(t, ambient[0].Key, changed[0].Key)
	destination.Settings["queue_url"] = "https://sqs.us-west-2.amazonaws.com/123456789012/other"
	other, err := registry.Resolve(t.Context(), []Destination{destination})
	require.NoError(t, err)
	assert.NotEqual(t, changed[0].Key, other[0].Key)
}

func TestNoDestinationsBuildNoPublishers(t *testing.T) {
	calls := 0
	registry, err := NewRegistry(map[string]PublisherFactory{
		"sqs": func(context.Context, Destination) (ResolvedPublisher, error) {
			calls++
			return ResolvedPublisher{}, errors.New("must not initialize")
		},
	})
	require.NoError(t, err)
	routes, err := registry.Resolve(t.Context(), nil)
	require.NoError(t, err)
	assert.Empty(t, routes)
	assert.Zero(t, calls)
}
