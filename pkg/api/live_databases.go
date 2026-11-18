package api

import (
	"fmt"
	"maps"
	"reflect"
	"sync/atomic"
)

type databaseSnapshot struct {
	declared  map[string]DatabaseConfig
	effective map[string]DatabaseConfig
}
type liveDatabaseRegistry struct {
	current atomic.Pointer[databaseSnapshot]
}

// DatabaseConfigs returns a read-only snapshot. Published maps and their values
// must never be mutated. Existing engine clients keep their original settings.
func (c *ServerConfig) DatabaseConfigs() map[string]DatabaseConfig {
	if c.liveDatabases != nil {
		return c.liveDatabases.current.Load().effective
	}
	return c.Databases
}

// withDatabaseSnapshot pins database lookups for one operation, including helper calls.
func (c *ServerConfig) withDatabaseSnapshot() *ServerConfig {
	if c == nil || c.liveDatabases == nil {
		return c
	}
	snapshot := *c
	snapshot.Databases = c.DatabaseConfigs()
	snapshot.liveDatabases = nil
	return &snapshot
}

// EnableLocalRegistration is called before the local server starts any goroutines.
func (c *ServerConfig) EnableLocalRegistration() {
	c.liveDatabases = &liveDatabaseRegistry{}
	c.liveDatabases.current.Store(&databaseSnapshot{declared: c.Databases, effective: c.Databases})
}

// PrepareLocalRegistration accepts only additional database/environment entries.
// The host serializes updates and calls publish only after durable publication.
// Resolved DSNs stay in memory, allowing a new shell's env references to work
// without changing process-wide environment variables or persisting secrets.
func (c *ServerConfig) PrepareLocalRegistration(next *ServerConfig, resolved map[string]map[string]string) (*ServerConfig, func(), error) {
	if c.liveDatabases == nil {
		return nil, nil, fmt.Errorf("live registration is unavailable")
	}
	current := c.liveDatabases.current.Load()
	before, after := *c, *next
	before.Databases = nil
	after.Databases = nil
	before.liveDatabases = nil
	after.liveDatabases = nil
	if !reflect.DeepEqual(before, after) {
		return nil, nil, fmt.Errorf("registration cannot change server or storage settings")
	}
	effective := maps.Clone(current.effective)
	for name, old := range current.declared {
		incoming, ok := next.Databases[name]
		if !ok {
			return nil, nil, fmt.Errorf("registration cannot remove database %q", name)
		}
		oldEnvs, newEnvs := old.Environments, incoming.Environments
		old.Environments = nil
		incoming.Environments = nil
		if !reflect.DeepEqual(old, incoming) {
			return nil, nil, fmt.Errorf("registration cannot change database %q", name)
		}
		for env, connection := range oldEnvs {
			if !reflect.DeepEqual(connection, newEnvs[env]) {
				return nil, nil, fmt.Errorf("registration cannot change database %q environment %q", name, env)
			}
		}
	}
	for name, db := range next.Databases {
		previous := current.declared[name]
		active := current.effective[name]
		updated := db
		updated.Environments = maps.Clone(active.Environments)
		if updated.Environments == nil {
			updated.Environments = make(map[string]EnvironmentConfig)
		}
		for env, connection := range db.Environments {
			if _, exists := previous.Environments[env]; exists {
				continue
			}
			if !connection.HasLocalDSN() {
				return nil, nil, fmt.Errorf("live registration requires a direct connection")
			}
			dsn := resolved[name][env]
			if dsn == "" {
				return nil, nil, fmt.Errorf("new database connection is missing")
			}
			connection.resolvedLocalDSN = dsn
			updated.Environments[env] = connection
		}
		effective[name] = updated
	}
	validated := *next
	validated.Databases = effective
	publish := func() {
		c.liveDatabases.current.Store(&databaseSnapshot{declared: next.Databases, effective: effective})
	}
	return &validated, publish, nil
}
