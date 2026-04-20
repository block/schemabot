package local

import (
	"fmt"
	"os"
	"path/filepath"

	"gopkg.in/yaml.v3"
)

// LocalDatabase holds config for a locally-managed database.
type LocalDatabase struct {
	Type         string                      `yaml:"type"`
	Environments map[string]LocalEnvironment `yaml:"environments"`
}

// LocalEnvironment holds connection details for one environment of a database.
type LocalEnvironment struct {
	DSN          string `yaml:"dsn,omitempty"`
	Organization string `yaml:"organization,omitempty"`
	Token        string `yaml:"token,omitempty"`
}

// CLIConfig is the structure of ~/.schemabot/config.yaml.
// It extends the existing profile-based config with a top-level local section.
type CLIConfig struct {
	DefaultProfile string                   `yaml:"default_profile,omitempty"`
	Profiles       map[string]CLIProfile    `yaml:"profiles,omitempty"`
	Local          map[string]LocalDatabase `yaml:"local,omitempty"`
}

// CLIProfile is a named endpoint configuration.
type CLIProfile struct {
	Endpoint string `yaml:"endpoint,omitempty"`
}

// LoadCLIConfig loads ~/.schemabot/config.yaml.
func LoadCLIConfig() (*CLIConfig, error) {
	path, err := configPath()
	if err != nil {
		return nil, err
	}

	data, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			return &CLIConfig{}, nil
		}
		return nil, fmt.Errorf("read config: %w", err)
	}

	var cfg CLIConfig
	if err := yaml.Unmarshal(data, &cfg); err != nil {
		return nil, fmt.Errorf("parse config: %w", err)
	}
	return &cfg, nil
}

// UpsertLocalEnvironment adds or updates an environment for a database
// in the local section and saves.
func UpsertLocalEnvironment(database, dbType, environment string, env LocalEnvironment) error {
	cfg, err := LoadCLIConfig()
	if err != nil {
		return err
	}

	if cfg.Local == nil {
		cfg.Local = make(map[string]LocalDatabase)
	}
	db, ok := cfg.Local[database]
	if !ok {
		db = LocalDatabase{Type: dbType}
	}
	db.Type = dbType
	if db.Environments == nil {
		db.Environments = make(map[string]LocalEnvironment)
	}
	db.Environments[environment] = env
	cfg.Local[database] = db

	return saveCLIConfig(cfg)
}

// saveCLIConfig writes the config back to ~/.schemabot/config.yaml.
func saveCLIConfig(cfg *CLIConfig) error {
	path, err := configPath()
	if err != nil {
		return err
	}

	if err := os.MkdirAll(filepath.Dir(path), 0700); err != nil {
		return fmt.Errorf("create config directory: %w", err)
	}

	data, err := yaml.Marshal(cfg)
	if err != nil {
		return fmt.Errorf("marshal config: %w", err)
	}

	return os.WriteFile(path, data, 0600)
}

func configPath() (string, error) {
	dir, err := schemabotDir()
	if err != nil {
		return "", err
	}
	return filepath.Join(dir, "config.yaml"), nil
}

func schemabotDir() (string, error) {
	home, err := os.UserHomeDir()
	if err != nil {
		return "", fmt.Errorf("get home directory: %w", err)
	}
	return filepath.Join(home, ".schemabot"), nil
}
