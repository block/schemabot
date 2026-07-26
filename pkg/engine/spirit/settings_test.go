package spirit

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSettingsFromMetadata(t *testing.T) {
	boolPtr := func(v bool) *bool { return &v }

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
			name: "all overrides parse",
			metadata: map[string]string{
				MetadataEnableExperimentalAutoscaling: "false",
				MetadataEnableExperimentalGTID:        "false",
				MetadataCheckpointMaxAge:              "24h",
				MetadataChecksumYieldTimeout:          "6h",
			},
			want: Settings{
				EnableExperimentalAutoscaling: boolPtr(false),
				EnableExperimentalGTID:        boolPtr(false),
				CheckpointMaxAge:              24 * time.Hour,
				ChecksumYieldTimeout:          6 * time.Hour,
			},
		},
		{
			name: "autoscaling true is preserved as an explicit value",
			metadata: map[string]string{
				MetadataEnableExperimentalAutoscaling: "true",
			},
			want: Settings{EnableExperimentalAutoscaling: boolPtr(true)},
		},
		{
			name: "invalid gtid value errors",
			metadata: map[string]string{
				MetadataEnableExperimentalGTID: "yep",
			},
			wantErr: MetadataEnableExperimentalGTID,
		},
		{
			name: "invalid autoscaling value errors",
			metadata: map[string]string{
				MetadataEnableExperimentalAutoscaling: "yep",
			},
			wantErr: MetadataEnableExperimentalAutoscaling,
		},
		{
			name: "invalid checkpoint duration errors",
			metadata: map[string]string{
				MetadataCheckpointMaxAge: "3 days",
			},
			wantErr: MetadataCheckpointMaxAge,
		},
		{
			name: "non-positive checksum yield errors",
			metadata: map[string]string{
				MetadataChecksumYieldTimeout: "0s",
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

// TestNewResolvesSettings verifies that New resolves zero-value Settings
// fields to the fleet defaults and preserves explicit overrides, so every
// embedder that constructs the engine without configuration runs with the
// documented defaults.
func TestNewResolvesSettings(t *testing.T) {
	t.Run("defaults", func(t *testing.T) {
		eng := New(Config{})
		assert.Equal(t, DefaultCheckpointMaxAge, eng.checkpointMaxAge)
		assert.Equal(t, DefaultChecksumYieldTimeout, eng.checksumYieldTimeout)
		assert.True(t, eng.autoscaling)
		assert.True(t, eng.gtid, "GTID change source defaults to enabled")
	})

	t.Run("overrides", func(t *testing.T) {
		disabled := false
		eng := New(Config{Settings: Settings{
			EnableExperimentalAutoscaling: &disabled,
			EnableExperimentalGTID:        &disabled,
			CheckpointMaxAge:              24 * time.Hour,
			ChecksumYieldTimeout:          6 * time.Hour,
		}})
		assert.Equal(t, 24*time.Hour, eng.checkpointMaxAge)
		assert.Equal(t, 6*time.Hour, eng.checksumYieldTimeout)
		assert.False(t, eng.autoscaling)
		assert.False(t, eng.gtid)
	})
}
