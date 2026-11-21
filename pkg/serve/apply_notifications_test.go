package serve

import (
	"bytes"
	"context"
	"log/slog"
	"testing"

	"github.com/block/schemabot/pkg/api"
	"github.com/block/schemabot/pkg/applynotifications"
	"github.com/block/schemabot/pkg/state"
	"github.com/block/schemabot/pkg/storage"
	"github.com/block/schemabot/pkg/storage/mysqlstore"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestConfigureApplyNotificationsDisabled(t *testing.T) {
	var logs bytes.Buffer
	logger := slog.New(slog.NewTextHandler(&logs, nil))
	cfg := &api.ServerConfig{}
	svc := api.New(mysqlstore.New(nil), cfg, nil, logger)
	summaryCalls := 0
	svc.OnApplyTerminalSummary = func(context.Context, *storage.Apply, []*storage.Task) error { summaryCalls++; return nil }
	// An unusable AWS region would fail a real send; disabled wiring never
	// constructs a publisher or touches credentials.
	t.Setenv("AWS_REGION", "")
	require.NoError(t, configureApplyNotifications(t.Context(), cfg, svc, logger))
	require.NoError(t, svc.OnApplyTerminalSummary(t.Context(), &storage.Apply{State: state.Apply.Completed}, nil))
	assert.Equal(t, 1, summaryCalls)
	assert.Empty(t, logs.String())
}

func TestConfigureApplyNotificationsWithoutWebhook(t *testing.T) {
	cfg := &api.ServerConfig{ApplyNotifications: applynotifications.Config{Enabled: true, Type: "sqs", QueueURL: "https://sqs.example/completions"}}
	logger := slog.New(slog.DiscardHandler)
	svc := api.New(mysqlstore.New(nil), cfg, nil, logger)
	require.NoError(t, configureApplyNotifications(t.Context(), cfg, svc, logger))
	assert.Nil(t, svc.OnApplyTerminalSummary, "without a webhook handler this option adds no completion hook")
}
