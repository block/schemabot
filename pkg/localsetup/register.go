package localsetup

import (
	"fmt"
	"reflect"

	"gopkg.in/yaml.v3"

	"github.com/block/schemabot/pkg/api"
	"github.com/block/schemabot/pkg/localruntime"
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
func Register(m localruntime.Manager, r Registration) (bool, error) {
	requested := api.ServerConfig{
		Storage: r.Storage,
		Databases: map[string]api.DatabaseConfig{r.Database: {
			Type: r.Engine, Environments: map[string]api.EnvironmentConfig{r.Environment: r.Connection},
		}},
	}
	if err := serve.ValidateLocalConfig(&requested); err != nil {
		return false, err
	}
	return m.UpdateConfig(func(data []byte) ([]byte, error) {
		cfg := requested
		if data != nil {
			existing, err := api.ParseServerConfig(data)
			if err != nil {
				return nil, fmt.Errorf("parse runtime configuration: %w", err)
			}
			if err := serve.ValidateLocalConfig(existing); err != nil {
				return nil, err
			}
			// Pool tuning is not a change of durable state authority. Keep the
			// existing pool settings rather than asking setup to reproduce them.
			wantedStorage := requested.Storage
			wantedStorage.Pool = existing.Storage.Pool
			if !reflect.DeepEqual(existing.Storage, wantedStorage) {
				return nil, fmt.Errorf("runtime storage differs; registration cannot replace durable state storage")
			}
			cfg = *existing
			wanted := requested.Databases[r.Database]
			database, exists := cfg.Databases[r.Database]
			if exists {
				if database.Type != wanted.Type {
					return nil, fmt.Errorf("database %s is already registered with a different engine", r.Database)
				}
				if environment, exists := database.Environments[r.Environment]; exists {
					if !reflect.DeepEqual(environment, wanted.Environments[r.Environment]) {
						return nil, fmt.Errorf("database %s environment %s already has a different registration", r.Database, r.Environment)
					}
					return data, nil
				}
				database.Environments[r.Environment] = wanted.Environments[r.Environment]
				cfg.Databases[r.Database] = database
			} else {
				cfg.Databases[r.Database] = wanted
			}
		}
		if err := serve.ValidateLocalConfig(&cfg); err != nil {
			return nil, err
		}
		data, err := yaml.Marshal(cfg)
		if err != nil {
			return nil, fmt.Errorf("encode runtime configuration: %w", err)
		}
		// Read back through the host's strict parser before publishing the config.
		if _, err := api.ParseServerConfig(data); err != nil {
			return nil, fmt.Errorf("validate generated runtime configuration: %w", err)
		}
		return data, nil
	})
}
