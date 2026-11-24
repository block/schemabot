package serve

import (
	"context"
	"errors"
	"testing"

	"github.com/block/schemabot/pkg/api"
	"github.com/block/schemabot/pkg/notifications"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestWithPublisherRecordsFactory(t *testing.T) {
	var o options
	first := func(context.Context, notifications.Destination) (notifications.ResolvedPublisher, error) {
		return notifications.ResolvedPublisher{}, errors.New("first")
	}
	second := func(context.Context, notifications.Destination) (notifications.ResolvedPublisher, error) {
		return notifications.ResolvedPublisher{}, errors.New("second")
	}
	WithPublisher("custom", first)(&o)
	WithPublisher("custom", second)(&o)
	registry, err := notifications.NewRegistry(o.publishers)
	require.NoError(t, err)
	_, err = registry.Resolve(t.Context(), []notifications.Destination{{Type: "custom"}})
	assert.ErrorContains(t, err, "second")
}

// Invalid publisher registrations fail before Build opens storage or starts any
// background work, including when no destination selects the invalid factory.
func TestBuildRejectsInvalidPublisher(t *testing.T) {
	for _, kind := range []string{"", "custom"} {
		t.Run(kind, func(t *testing.T) {
			server, err := Build(t.Context(), &api.ServerConfig{}, WithPublisher(kind, nil))
			require.ErrorContains(t, err, "configure publisher factories")
			assert.Nil(t, server)
		})
	}
}
