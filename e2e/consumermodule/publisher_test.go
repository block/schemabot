package consumermodule

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/block/schemabot/pkg/api"
	"github.com/block/schemabot/pkg/notifications"
	"github.com/block/schemabot/pkg/serve"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type hostPublisher struct{}

func (hostPublisher) Publish(context.Context, json.RawMessage) error { return nil }

// A host can own transport settings and its publisher implementation without
// adding either to SchemaBot's core configuration or transport packages.
func TestHostPublisherExtension(t *testing.T) {
	factory := func(_ context.Context, destination notifications.Destination) (notifications.ResolvedPublisher, error) {
		var settings struct {
			Address string `yaml:"address"`
		}
		if err := destination.DecodeSettings(&settings); err != nil {
			return notifications.ResolvedPublisher{}, err
		}
		return notifications.ResolvedPublisher{Identity: settings.Address, Publisher: hostPublisher{}}, nil
	}
	registry, err := notifications.NewRegistry(map[string]notifications.PublisherFactory{"host": factory})
	require.NoError(t, err)
	routes, err := registry.Resolve(t.Context(), []notifications.Destination{{Type: "host", Settings: map[string]any{"address": "example"}}})
	require.NoError(t, err)
	require.Len(t, routes, 1)
	assert.Len(t, routes[0].Key, 64)
	require.NoError(t, routes[0].Publisher.Publish(t.Context(), json.RawMessage(`{"type":"apply.finished"}`)))

	server, err := serve.Build(t.Context(), &api.ServerConfig{}, serve.WithPublisher("host", nil))
	require.ErrorContains(t, err, "factory is required")
	assert.Nil(t, server)
}
