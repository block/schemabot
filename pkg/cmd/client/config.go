package client

import (
	"fmt"
	"os"
	"path/filepath"

	"gopkg.in/yaml.v3"
)

// Config represents the global SchemaBot CLI configuration.
type Config struct {
	DefaultProfile string             `yaml:"default_profile,omitempty"`
	Profiles       map[string]Profile `yaml:"profiles"`
}

// Profile represents a named configuration profile.
type Profile struct {
	Endpoint string `yaml:"endpoint"`
}

// ConfigPath returns the path to the config file (~/.schemabot/config.yaml).
func ConfigPath() (string, error) {
	home, err := os.UserHomeDir()
	if err != nil {
		return "", fmt.Errorf("get home directory: %w", err)
	}
	return filepath.Join(home, ".schemabot", "config.yaml"), nil
}

// LoadConfig loads the global configuration from ~/.schemabot/config.yaml.
// Returns an empty config if the file doesn't exist.
func LoadConfig() (*Config, error) {
	path, err := ConfigPath()
	if err != nil {
		return nil, err
	}

	data, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			return &Config{Profiles: make(map[string]Profile)}, nil
		}
		return nil, fmt.Errorf("read config: %w", err)
	}

	var cfg Config
	if err := yaml.Unmarshal(data, &cfg); err != nil {
		return nil, fmt.Errorf("parse config: %w", err)
	}

	if cfg.Profiles == nil {
		cfg.Profiles = make(map[string]Profile)
	}

	return &cfg, nil
}

// SaveConfig saves the configuration to ~/.schemabot/config.yaml.
func SaveConfig(cfg *Config) error {
	path, err := ConfigPath()
	if err != nil {
		return err
	}

	// Create directory if needed
	dir := filepath.Dir(path)
	if err := os.MkdirAll(dir, 0700); err != nil {
		return fmt.Errorf("create config directory: %w", err)
	}

	data, err := yaml.Marshal(cfg)
	if err != nil {
		return fmt.Errorf("marshal config: %w", err)
	}

	if err := os.WriteFile(path, data, 0600); err != nil {
		return fmt.Errorf("write config: %w", err)
	}

	return nil
}

// GetProfile returns the profile to use based on:
// 1. Explicit profile flag
// 2. SCHEMABOT_PROFILE environment variable
// 3. Default profile from config
func GetProfile(profileFlag string) (*Profile, error) {
	cfg, err := LoadConfig()
	if err != nil {
		return nil, err
	}

	// Determine which profile to use.
	// Track whether the user explicitly requested a profile (flag or env),
	// so we can error if it doesn't exist instead of silently falling back.
	profileName := profileFlag
	explicit := profileFlag != ""
	if profileName == "" {
		profileName = os.Getenv("SCHEMABOT_PROFILE")
		explicit = profileName != ""
	}
	if profileName == "" {
		profileName = cfg.DefaultProfile
	}
	if profileName == "" {
		profileName = "default"
	}

	profile, ok := cfg.Profiles[profileName]
	if !ok {
		if explicit {
			return nil, fmt.Errorf("unknown profile %q", profileName)
		}
		return &Profile{}, nil
	}

	return &profile, nil
}

// ResolveEndpointWithProfile returns the endpoint to use, checking in order:
// 1. Explicit --endpoint flag
// 2. SCHEMABOT_ENDPOINT environment variable
// 3. Profile endpoint (from --profile flag, SCHEMABOT_PROFILE env, or default profile)
// 4. schemabot.yaml endpoint (if provided)
func ResolveEndpointWithProfile(endpointFlag, profileFlag string, configEndpoint ...string) (string, error) {
	// 1. Explicit flag
	if endpointFlag != "" {
		return trimSlash(endpointFlag), nil
	}

	// 2. Environment variable
	if env := os.Getenv("SCHEMABOT_ENDPOINT"); env != "" {
		return trimSlash(env), nil
	}

	// 3. Profile
	profile, err := GetProfile(profileFlag)
	if err != nil {
		return "", err
	}
	if profile.Endpoint != "" {
		return trimSlash(profile.Endpoint), nil
	}

	// 4. Config file (schemabot.yaml)
	if len(configEndpoint) > 0 && configEndpoint[0] != "" {
		return trimSlash(configEndpoint[0]), nil
	}

	return "", nil
}

func trimSlash(s string) string {
	if len(s) > 0 && s[len(s)-1] == '/' {
		return s[:len(s)-1]
	}
	return s
}
