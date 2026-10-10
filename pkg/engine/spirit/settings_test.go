package spirit

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSettingsFromMetadata(t *testing.T) {
	tests := []struct {
		name     string
		metadata map[string]string
		want     Settings
		wantErr  string
	}{
		{
			name:     "nil metadata leaves all fields zero",
			metadata: nil,
			want:     Settings{},
		},
		{
			name: "unrelated keys are ignored",
			metadata: map[string]string{
				"pending_drops": "false",
				"organization":  "acme",
			},
			want: Settings{},
		},
		{
			name: "checkpoint max age parses",
			metadata: map[string]string{
				MetadataCheckpointMaxAge: "24h",
			},
			want: Settings{CheckpointMaxAge: 24 * time.Hour},
		},
		{
			name: "invalid checkpoint duration errors",
			metadata: map[string]string{
				MetadataCheckpointMaxAge: "3 days",
			},
			wantErr: MetadataCheckpointMaxAge,
		},
		{
			name: "non-positive checkpoint duration errors",
			metadata: map[string]string{
				MetadataCheckpointMaxAge: "0s",
			},
			wantErr: "must be positive",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := SettingsFromMetadata(tt.metadata)
			if tt.wantErr != "" {
				require.ErrorContains(t, err, tt.wantErr)
				return
			}
			require.NoError(t, err)
			assert.Equal(t, tt.want, got)
		})
	}
}

// A database whose metadata still sets a run setting this engine no longer
// exposes must not silently run with Spirit's default instead: the operator
// set that key to change how the run behaves, so the settings fail to build
// and the error names the key to delete.
func TestSettingsFromMetadataRejectsRemovedKeys(t *testing.T) {
	for _, key := range []string{
		"enable_experimental_autoscaling",
		"enable_experimental_lockless_checksum",
		"checksum_yield_timeout",
	} {
		_, err := SettingsFromMetadata(map[string]string{key: "false"})
		require.ErrorContains(t, err, "metadata key "+key+" is no longer supported")
	}
}

// TestNewResolvesSettings verifies that New resolves zero-value Settings
// fields to the fleet defaults and preserves explicit overrides, so every
// embedder that constructs the engine without configuration runs with the
// documented defaults.
func TestNewResolvesSettings(t *testing.T) {
	t.Run("defaults", func(t *testing.T) {
		eng := New(Config{})
		assert.Equal(t, DefaultCheckpointMaxAge, eng.checkpointMaxAge)
	})

	t.Run("overrides", func(t *testing.T) {
		eng := New(Config{Settings: Settings{CheckpointMaxAge: 24 * time.Hour}})
		assert.Equal(t, 24*time.Hour, eng.checkpointMaxAge)
	})
}
