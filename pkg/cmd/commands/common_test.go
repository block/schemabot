//go:build !integration && !e2e

package commands

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/block/schemabot/pkg/e2eutil"
)

func TestLoadCLIConfig_WithEnvironments(t *testing.T) {
	dir := e2eutil.WriteSchemaDir(t, "testapp", "mysql", map[string]string{
		"users.sql": "CREATE TABLE users (id BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY);",
	}, e2eutil.WithEnvironments(map[string]string{
		"staging":    "cash-aurora-staging-001",
		"production": "cash-aurora-production-001",
	}))

	cfg, err := LoadCLIConfig(dir)
	require.NoError(t, err)

	assert.Equal(t, "testapp", cfg.Database)
	assert.Equal(t, "mysql", cfg.Type)
	assert.Equal(t, "cash-aurora-staging-001", cfg.GetTarget("staging"))
	assert.Equal(t, "cash-aurora-production-001", cfg.GetTarget("production"))
	assert.Equal(t, "testapp", cfg.GetTarget("unknown"), "unknown env falls back to database name")
}

func TestLoadCLIConfig_WithoutEnvironments(t *testing.T) {
	dir := e2eutil.WriteSchemaDir(t, "testapp", "mysql", map[string]string{
		"users.sql": "CREATE TABLE users (id BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY);",
	})

	cfg, err := LoadCLIConfig(dir)
	require.NoError(t, err)

	assert.Equal(t, "testapp", cfg.Database)
	assert.Equal(t, "testapp", cfg.GetTarget("staging"), "no environments falls back to database name")
}
