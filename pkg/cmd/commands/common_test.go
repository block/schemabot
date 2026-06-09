//go:build !integration && !e2e

package commands

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/block/schemabot/pkg/e2eutil"
)

func TestLoadCLIConfig_RejectsEnvironments(t *testing.T) {
	dir := t.TempDir()
	content := "database: mydb\ntype: mysql\nenvironments:\n  - staging\n"
	require.NoError(t, os.WriteFile(filepath.Join(dir, "schemabot.yaml"), []byte(content), 0644))
	require.NoError(t, os.WriteFile(filepath.Join(dir, "users.sql"), []byte("CREATE TABLE users (id BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY);"), 0644))

	cfg, err := LoadCLIConfig(dir)
	require.Error(t, err)
	assert.Nil(t, cfg)
	assert.Contains(t, err.Error(), "field environments not found")
}

func TestLoadCLIConfig_WithoutEnvironments(t *testing.T) {
	dir := e2eutil.WriteSchemaDir(t, "testapp", "mysql", map[string]string{
		"users.sql": "CREATE TABLE users (id BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY);",
	})

	cfg, err := LoadCLIConfig(dir)
	require.NoError(t, err)

	assert.Equal(t, "testapp", cfg.Database)
	assert.Equal(t, "mysql", cfg.Type)
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

func TestApplyChangeCountsSummary(t *testing.T) {
	tables := []tableProgress{
		{ChangeType: "CREATE"},
		{ChangeType: "CHANGE_TYPE_CREATE"},
		{ChangeType: "ALTER"},
		{ChangeType: "DROP"},
		{ChangeType: "DROP"},
		{ChangeType: "VSCHEMA_UPDATE"},
		{ChangeType: "CHANGE_TYPE_VSCHEMA"},
	}

	assert.Equal(t, "Changes: 2 created, 1 altered, 2 dropped, 2 VSchema updates.", countTableProgressChanges(tables).summary())
}

func TestApplyChangeCountsSummaryVSchemaOnly(t *testing.T) {
	tables := []tableProgress{{ChangeType: "vschema_update"}}

	assert.Equal(t, "Changes: 1 VSchema update.", countTableProgressChanges(tables).summary())
}

func TestApplyChangeCountsSummaryEmpty(t *testing.T) {
	assert.Empty(t, countTableProgressChanges(nil).summary())
}
