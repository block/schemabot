package api

import (
	"maps"

	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestLocalRegistrationPublishesOnlyAdditions(t *testing.T) {
	original := &ServerConfig{Databases: map[string]DatabaseConfig{"app": {Type: "mysql", Environments: map[string]EnvironmentConfig{"dev": {DSN: "old"}}}}}
	original.EnableLocalRegistration()
	next := &ServerConfig{Databases: map[string]DatabaseConfig{
		"app":     {Type: "mysql", Environments: map[string]EnvironmentConfig{"dev": {DSN: "old"}, "test": {DSN: "env:NEW_CONNECTION"}}},
		"billing": {Type: "postgres", Environments: map[string]EnvironmentConfig{"dev": {DSN: "env:BILLING"}}},
	}}
	prospective, publish, err := original.PrepareLocalRegistration(next, map[string]map[string]string{"app": {"test": "new-private"}, "billing": {"dev": "billing-private"}})
	require.NoError(t, err)
	require.Nil(t, original.Database("billing"))
	require.Equal(t, "new-private", prospective.Databases["app"].Environments["test"].resolvedLocalDSN)
	// Readers remain on complete immutable snapshots while an addition is published.
	var wg sync.WaitGroup
	for range 4 {
		wg.Go(func() {
			for range 100 {
				db := original.Database("app")
				assert.Equal(t, "old", db.Environments["dev"].DSN)
			}
		})
	}
	publish()
	wg.Wait()
	require.Equal(t, "env:BILLING", original.Database("billing").Environments["dev"].DSN)
	dsn, err := original.Database("billing").Environments["dev"].ResolveDSN()
	require.NoError(t, err)
	require.Equal(t, "billing-private", dsn)
	for name, mutate := range map[string]func(*ServerConfig){
		"remove database": func(c *ServerConfig) { delete(c.Databases, "app") },
		"replace connection": func(c *ServerConfig) {
			db := c.Databases["app"]
			db.Environments["dev"] = EnvironmentConfig{DSN: "changed"}
			c.Databases["app"] = db
		},
		"replace storage": func(c *ServerConfig) { c.Storage.DSN = "different-state" },
	} {
		t.Run(name, func(t *testing.T) {
			clone := *next
			clone.Databases = make(map[string]DatabaseConfig)
			for name, db := range next.Databases {
				envs := make(map[string]EnvironmentConfig)
				maps.Copy(envs, db.Environments)
				db.Environments = envs
				clone.Databases[name] = db
			}
			mutate(&clone)
			_, _, err := original.PrepareLocalRegistration(&clone, nil)
			require.Error(t, err)
		})
	}
}
