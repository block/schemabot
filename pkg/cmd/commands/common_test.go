//go:build !integration && !e2e

package commands

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/block/schemabot/pkg/e2eutil"
	ghclient "github.com/block/schemabot/pkg/github"
)

func TestLoadCLIConfig_WithEnvironments(t *testing.T) {
	dir := e2eutil.WriteSchemaDir(t, "testapp", "mysql", map[string]string{
		"users.sql": "CREATE TABLE users (id BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY);",
	}, e2eutil.WithEnvironmentNames("staging", "production"))

	cfg, err := LoadCLIConfig(dir)
	require.NoError(t, err)

	assert.Equal(t, "testapp", cfg.Database)
	assert.Equal(t, "mysql", cfg.Type)
	assert.Equal(t, ghclient.EnvironmentList{{Name: "production"}, {Name: "staging"}}, cfg.Environments)
}

func TestLoadCLIConfig_WithoutEnvironments(t *testing.T) {
	dir := e2eutil.WriteSchemaDir(t, "testapp", "mysql", map[string]string{
		"users.sql": "CREATE TABLE users (id BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY);",
	})

	cfg, err := LoadCLIConfig(dir)
	require.NoError(t, err)

	assert.Equal(t, "testapp", cfg.Database)
}

func TestLoadCLIConfig_RejectsDeployment(t *testing.T) {
	dir := t.TempDir()
	content := "database: mydb\ntype: mysql\ndeployment: us-west\n"
	require.NoError(t, os.WriteFile(filepath.Join(dir, "schemabot.yaml"), []byte(content), 0644))
	require.NoError(t, os.WriteFile(filepath.Join(dir, "users.sql"), []byte("CREATE TABLE users (id BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY);"), 0644))

	cfg, err := LoadCLIConfig(dir)
	require.Error(t, err)
	assert.Nil(t, cfg)
	assert.Contains(t, err.Error(), "field deployment not found")
}
