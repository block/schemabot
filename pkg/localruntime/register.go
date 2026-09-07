package localruntime

import (
	"fmt"
	"os"
	"path/filepath"
	"reflect"

	"github.com/block/spirit/pkg/utils"
	"gopkg.in/yaml.v3"

	"github.com/block/schemabot/pkg/api"
	"github.com/block/schemabot/pkg/serve"
)

// Registration enrolls one database/environment without replacing existing
// registrations or selecting a different authority for durable state.
// Connections retain their secret references; validation resolves them only
// to check the shared configuration and storage-isolation guards.
type Registration struct {
	Database    string
	Environment string
	Engine      string
	Connection  api.EnvironmentConfig
	Storage     api.StorageConfig
}

// Register atomically extends the runtime configuration while the runtime is
// stopped. An identical registration is safe to repeat, including while it is
// running. It does not connect to or create databases, start an executor, or
// apply changes to the target.
func (m Manager) Register(r Registration) (bool, error) {
	requested := api.ServerConfig{
		Storage: r.Storage,
		Databases: map[string]api.DatabaseConfig{r.Database: {
			Type: r.Engine, Environments: map[string]api.EnvironmentConfig{r.Environment: r.Connection},
		}},
	}
	if err := serve.ValidateLocalConfig(&requested); err != nil {
		return false, err
	}
	if err := privateDirectory(m.Dir); err != nil {
		return false, err
	}
	lease, available, err := lock(m.Dir)
	if err != nil {
		return false, err
	}
	if available {
		defer utils.CloseAndLog(lease)
	}
	path := filepath.Join(m.Dir, "runtime.yaml")
	data, err := ReadPrivate(path)
	if err != nil && !os.IsNotExist(err) {
		return false, fmt.Errorf("read runtime configuration: %w", err)
	}
	cfg := requested
	if err == nil {
		existing, err := api.ParseServerConfig(data)
		if err != nil {
			return false, fmt.Errorf("parse runtime configuration: %w", err)
		}
		if err := serve.ValidateLocalConfig(existing); err != nil {
			return false, err
		}
		if !reflect.DeepEqual(existing.Storage, requested.Storage) {
			return false, fmt.Errorf("runtime storage differs; registration cannot replace durable state storage")
		}
		cfg = *existing
		wanted := requested.Databases[r.Database]
		database, exists := cfg.Databases[r.Database]
		if exists {
			if database.Type != wanted.Type {
				return false, fmt.Errorf("database %s is already registered with a different engine", r.Database)
			}
			if environment, exists := database.Environments[r.Environment]; exists {
				if !reflect.DeepEqual(environment, wanted.Environments[r.Environment]) {
					return false, fmt.Errorf("database %s environment %s already has a different registration", r.Database, r.Environment)
				}
				return false, nil
			}
			database.Environments[r.Environment] = wanted.Environments[r.Environment]
			cfg.Databases[r.Database] = database
		} else {
			cfg.Databases[r.Database] = wanted
		}
	}
	if !available {
		return false, fmt.Errorf("runtime is active; finish active work and stop it before adding a registration")
	}
	if err := serve.ValidateLocalConfig(&cfg); err != nil {
		return false, err
	}
	data, err = yaml.Marshal(cfg)
	if err != nil {
		return false, fmt.Errorf("encode runtime configuration: %w", err)
	}
	// Read back through the host's strict parser before publishing the config.
	if _, err := api.ParseServerConfig(data); err != nil {
		return false, fmt.Errorf("validate generated runtime configuration: %w", err)
	}
	if err := writeAtomic(path, data); err != nil {
		return false, err
	}
	return true, nil
}
