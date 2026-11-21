// Package notifications publishes completed applies without coupling delivery
// to the apply write path or to GitHub.
package notifications

import (
	"fmt"
	"time"
)

const (
	// DefaultLookback provisionally follows summary reconciliation, not a
	// receiver delivery requirement. Older completions are never sent.
	DefaultLookback            = 24 * time.Hour
	DefaultObservationInterval = 5 * time.Minute
	DefaultInterval            = 30 * time.Second
	DefaultBatchSize           = 100
	ClaimTTL                   = time.Minute
	SendTimeout                = 15 * time.Second
	QueryTimeout               = 5 * time.Second
	ObservationPageSize        = 500
)

// Config opts a server into completion publication. Empty Type disables it.
type Config struct {
	ObservationInterval string `yaml:"observation_interval,omitempty"`
	Type                string `yaml:"type,omitempty"`
	QueueURL            string `yaml:"queue_url,omitempty"`
	Region              string `yaml:"region,omitempty"`
	Lookback            string `yaml:"lookback,omitempty"`
	Interval            string `yaml:"interval,omitempty"`
	BatchSize           int    `yaml:"batch_size,omitempty"`
}

func (c Config) settings() (time.Duration, time.Duration, int, error) {
	lookback, interval, batch := DefaultLookback, DefaultInterval, c.BatchSize
	if c.Lookback != "" {
		parsed, err := time.ParseDuration(c.Lookback)
		if err != nil || parsed <= 0 {
			return 0, 0, 0, fmt.Errorf("notifications.lookback must be a positive duration")
		}
		lookback = parsed
	}
	if c.Interval != "" {
		parsed, err := time.ParseDuration(c.Interval)
		if err != nil || parsed <= 0 {
			return 0, 0, 0, fmt.Errorf("notifications.interval must be a positive duration")
		}
		interval = parsed
	}
	if batch == 0 {
		batch = DefaultBatchSize
	}
	if batch < 1 || batch > 1000 {
		return 0, 0, 0, fmt.Errorf("notifications.batch_size must be between 1 and 1000")
	}
	return lookback, interval, batch, nil
}

// Validate rejects an unsupported publisher or incomplete opt-in at startup.
func (c Config) Validate() error {
	switch c.Type {
	case "":
		return nil
	case "sqs":
		if c.QueueURL == "" {
			return fmt.Errorf("notifications.queue_url is required for sqs")
		}
	default:
		return fmt.Errorf("unsupported notifications.type %q", c.Type)
	}
	_, _, _, err := c.settings()
	if err != nil {
		return err
	}
	_, err = c.observationInterval()
	return err
}

func (c Config) observationInterval() (time.Duration, error) {
	if c.ObservationInterval == "" {
		return DefaultObservationInterval, nil
	}
	interval, err := time.ParseDuration(c.ObservationInterval)
	if err != nil || interval <= 0 {
		return 0, fmt.Errorf("notifications.observation_interval must be a positive duration")
	}
	return interval, nil
}
