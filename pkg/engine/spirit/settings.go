// settings.go defines the tunable Spirit run settings and their fleet
// defaults. The defaults are applied in New whenever a Settings field is left
// at its zero value, so every embedder — the SchemaBot server, custom engine
// factories, and external data planes that construct this engine directly —
// runs with the same defaults unless it explicitly overrides one.
package spirit

import (
	"fmt"
	"strconv"
	"time"
)

// DefaultCheckpointMaxAge bounds how old a Spirit checkpoint may be and still
// be resumed. A copy that has been stalled longer than this restarts cleanly
// instead of replaying days of old binlogs, which on a busy target can be
// slower and riskier than starting over.
const DefaultCheckpointMaxAge = 3 * 24 * time.Hour

// DefaultChecksumYieldTimeout bounds how long each checksum read transaction
// may run before yielding. The checksum reads under a REPEATABLE READ
// snapshot; without a yield bound a long checksum pins InnoDB purge and
// history-list growth degrades the whole target instance.
const DefaultChecksumYieldTimeout = 12 * time.Hour

// Settings tunes the Spirit runs this engine starts. A zero value resolves to
// the corresponding default in New; fields are set only to deviate from the
// fleet defaults.
type Settings struct {
	// EnableExperimentalAutoscaling scales Spirit's write threads dynamically
	// from throttler feedback so apply throughput tracks the target instance's
	// capacity instead of a fixed constant. nil defaults to enabled; false is
	// the operator kill switch when autoscaling misbehaves on a target fleet.
	EnableExperimentalAutoscaling *bool

	// EnableExperimentalGTID selects Spirit's GTID-based change source, which
	// tracks replication position across binlog rotation and failover more
	// robustly than binlog file+position. nil defaults to enabled, which is
	// safe on any fleet: the engine probes each target and uses the GTID
	// source only where gtid_mode=ON and enforce_gtid_consistency=ON, so a
	// target without GTIDs always keeps the universally supported binlog
	// file+position source and picks up the GTID source automatically once
	// GTIDs are enabled on it. false is the operator kill switch that keeps
	// every target on binlog file+position.
	EnableExperimentalGTID *bool

	// CheckpointMaxAge bounds how old a checkpoint may be and still be
	// resumed. Zero defaults to DefaultCheckpointMaxAge.
	CheckpointMaxAge time.Duration

	// ChecksumYieldTimeout bounds each checksum read transaction before it
	// yields its snapshot. Zero defaults to DefaultChecksumYieldTimeout.
	ChecksumYieldTimeout time.Duration
}

// Engine metadata keys read by SettingsFromMetadata. These are the
// per-database override surface: the server config's spirit block is
// translated into these keys, and a database's own metadata entry wins over
// the server-level value.
const (
	MetadataEnableExperimentalAutoscaling = "enable_experimental_autoscaling"
	MetadataEnableExperimentalGTID        = "enable_experimental_gtid"
	MetadataCheckpointMaxAge              = "checkpoint_max_age"
	MetadataChecksumYieldTimeout          = "checksum_yield_timeout"
)

// SettingsFromMetadata builds Settings from engine metadata key-value pairs.
// Absent keys leave the corresponding field at its zero value so New resolves
// the default; present keys must parse, because silently ignoring a
// misconfigured override would run the apply with settings the operator
// believes they changed.
func SettingsFromMetadata(metadata map[string]string) (Settings, error) {
	var settings Settings
	if raw, ok := metadata[MetadataEnableExperimentalAutoscaling]; ok {
		enabled, err := strconv.ParseBool(raw)
		if err != nil {
			return Settings{}, fmt.Errorf("parse %s %q: %w", MetadataEnableExperimentalAutoscaling, raw, err)
		}
		settings.EnableExperimentalAutoscaling = &enabled
	}
	if raw, ok := metadata[MetadataEnableExperimentalGTID]; ok {
		enabled, err := strconv.ParseBool(raw)
		if err != nil {
			return Settings{}, fmt.Errorf("parse %s %q: %w", MetadataEnableExperimentalGTID, raw, err)
		}
		settings.EnableExperimentalGTID = &enabled
	}
	var err error
	if settings.CheckpointMaxAge, err = parsePositiveDuration(metadata, MetadataCheckpointMaxAge); err != nil {
		return Settings{}, err
	}
	if settings.ChecksumYieldTimeout, err = parsePositiveDuration(metadata, MetadataChecksumYieldTimeout); err != nil {
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
