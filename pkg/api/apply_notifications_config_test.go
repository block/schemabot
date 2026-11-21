package api

import (
	"testing"

	"github.com/block/schemabot/pkg/applynotifications"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gopkg.in/yaml.v3"
)

func TestApplyNotificationsConfig(t *testing.T) {
	cfg := testServerConfig()
	require.NoError(t, cfg.Validate())
	assert.False(t, cfg.ApplyNotifications.Enabled)
	require.NoError(t, yaml.Unmarshal([]byte("apply_notifications:\n  enabled: true\n  type: sqs\n  queue_url: https://sqs.example/completions\n"), cfg))
	require.NoError(t, cfg.Validate())
	assert.Equal(t, applynotifications.Config{Enabled: true, Type: "sqs", QueueURL: "https://sqs.example/completions"}, cfg.ApplyNotifications)
	cfg.ApplyNotifications.QueueURL = ""
	require.ErrorContains(t, cfg.Validate(), "apply_notifications.queue_url")
}
