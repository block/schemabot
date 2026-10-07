// settings.go defines the tunable Spirit run settings and their fleet
// defaults. The defaults are applied in New whenever a Settings field is left
// at its zero value, so every embedder — the SchemaBot server, custom engine
// factories, and external data planes that construct this engine directly —
// runs with the same defaults unless it explicitly overrides one.
package spirit

import (
	"fmt"
	"slices"
	"time"
)

// DefaultCheckpointMaxAge bounds how old a Spirit checkpoint may be and still
// be resumed. A copy that has been stalled longer than this restarts cleanly
// instead of replaying days of old binlogs, which on a busy target can be
// slower and riskier than starting over.
const DefaultCheckpointMaxAge = 3 * 24 * time.Hour

// Settings tunes the Spirit runs this engine starts. A zero value resolves to
// the corresponding default in New; fields are set only to deviate from the
// fleet defaults. Everything else about a run (thread autoscaling on Aurora,
// the lockless checksum) is Spirit's own default and not configurable here.
type Settings struct {
	// CheckpointMaxAge bounds how old a checkpoint may be and still be
	// resumed. Zero defaults to DefaultCheckpointMaxAge.
	CheckpointMaxAge time.Duration
}

// Engine metadata keys read by SettingsFromMetadata. These are the
// per-database override surface: the server config's spirit block is
// translated into these keys, and a database's own metadata entry wins over
// the server-level value.
const (
	MetadataCheckpointMaxAge = "checkpoint_max_age"
)

// removedSettingKeys are run settings this engine no longer exposes because
// Spirit now chooses them itself: thread autoscaling and the checksum
// algorithm.
var removedSettingKeys = []string{
	"enable_experimental_autoscaling",
	"enable_experimental_lockless_checksum",
	"checksum_yield_timeout",
}

// RemovedSettingKeys returns the run setting keys this engine no longer
// accepts. A config that still sets one is rejected rather than ignored: the
// operator set it to change how runs behave, and Spirit's default would
// silently replace that intent.
func RemovedSettingKeys() []string {
	return slices.Clone(removedSettingKeys)
}

// SettingsFromMetadata builds Settings from engine metadata key-value pairs.
// Absent keys leave the corresponding field at its zero value so New resolves
// the default; present keys must parse, because silently ignoring a
// misconfigured override would run the apply with settings the operator
// believes they changed. A removed setting key is an error for the same
// reason.
func SettingsFromMetadata(metadata map[string]string) (Settings, error) {
	for _, key := range removedSettingKeys {
		if _, ok := metadata[key]; ok {
			return Settings{}, fmt.Errorf("metadata key %s is no longer supported: Spirit now chooses this itself; remove the key from the database's metadata", key)
		}
	}
	var settings Settings
	var err error
	if settings.CheckpointMaxAge, err = parsePositiveDuration(metadata, MetadataCheckpointMaxAge); err != nil {
		return Settings{}, err
	}
	return settings, nil
}

// parsePositiveDuration parses an optional duration metadata value, requiring
// it to be positive when present. Absent keys return zero so the engine
// default applies.
func parsePositiveDuration(metadata map[string]string, key string) (time.Duration, error) {
	raw, ok := metadata[key]
	if !ok {
		return 0, nil
	}
	d, err := time.ParseDuration(raw)
	if err != nil {
		return 0, fmt.Errorf("parse %s %q: %w", key, raw, err)
	}
	if d <= 0 {
		return 0, fmt.Errorf("%s must be positive, got %q", key, raw)
	}
	return d, nil
}
