//go:build !integration && !e2e

package commands

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/block/schemabot/pkg/cmd/internal/templates"
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

// TestLoadCLIConfig_CanonicalizesIdentityKeys covers a schemabot.yaml that
// spells the database and dialect in mixed case: the CLI must name the same
// canonical identity the server stores so plan, lock, and apply requests
// address the configured database rather than an unknown one.
func TestLoadCLIConfig_CanonicalizesIdentityKeys(t *testing.T) {
	dir := t.TempDir()
	content := "database: Payments\ntype: Postgres\n"
	require.NoError(t, os.WriteFile(filepath.Join(dir, "schemabot.yaml"), []byte(content), 0644))

	cfg, err := LoadCLIConfig(dir)
	require.NoError(t, err)

	assert.Equal(t, "payments", cfg.Database)
	assert.Equal(t, "postgres", cfg.Type)
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
	tables := []templates.TableProgress{
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
	tables := []templates.TableProgress{{ChangeType: "vschema_update"}}

	assert.Equal(t, "Changes: 1 VSchema update.", countTableProgressChanges(tables).summary())
}

func TestApplyChangeCountsSummaryEmpty(t *testing.T) {
	assert.Empty(t, countTableProgressChanges(nil).summary())
}

func TestLoadCLIConfig_ParsesIgnoreNamespaces(t *testing.T) {
	dir := t.TempDir()
	content := "database: mydb\ntype: vitess\nignore_namespaces:\n  - local_fixtures\n"
	require.NoError(t, os.WriteFile(filepath.Join(dir, "schemabot.yaml"), []byte(content), 0644))

	cfg, err := LoadCLIConfig(dir)
	require.NoError(t, err)
	assert.Equal(t, []string{"local_fixtures"}, cfg.IgnoreNamespaces)
}

func TestLoadCLIConfigParsesLegacyBaseline(t *testing.T) {
	dir := t.TempDir()
	content := `database: mydb
type: mysql
legacy_baseline:
  version: 1
  base_commit: 0123456789abcdef0123456789abcdef01234567
  legacy_paths:
    - service/db/changes
`
	require.NoError(t, os.WriteFile(filepath.Join(dir, "schemabot.yaml"), []byte(content), 0o644))

	cfg, err := LoadCLIConfig(dir)
	require.NoError(t, err)
	require.NotNil(t, cfg.LegacyBaseline)
	assert.Equal(t, 1, cfg.LegacyBaseline.Version)
	assert.Equal(t, []string{"service/db/changes"}, cfg.LegacyBaseline.LegacyPaths)
}

func TestLoadCLIConfig_RejectsIgnoreNamespacePaths(t *testing.T) {
	dir := t.TempDir()
	content := "database: mydb\ntype: vitess\nignore_namespaces:\n  - schema/local_fixtures\n"
	require.NoError(t, os.WriteFile(filepath.Join(dir, "schemabot.yaml"), []byte(content), 0644))

	cfg, err := LoadCLIConfig(dir)
	require.Error(t, err)
	assert.Nil(t, cfg)
	assert.Contains(t, err.Error(), "not a path")
}

func TestLoadCLIConfig_ParsesIgnoreTables(t *testing.T) {
	dir := t.TempDir()
	content := "database: mydb\ntype: mysql\nignore_tables:\n  - flyway_schema_history\n  - legacy_audit_log\n"
	require.NoError(t, os.WriteFile(filepath.Join(dir, "schemabot.yaml"), []byte(content), 0644))

	cfg, err := LoadCLIConfig(dir)
	require.NoError(t, err)
	assert.Equal(t, []string{"flyway_schema_history", "legacy_audit_log"}, cfg.IgnoreTables)
	assert.Equal(t, []string{"flyway_schema_history", "legacy_audit_log"}, cfg.PlanExclusions().Tables)
	assert.Nil(t, cfg.PlanExclusions().Namespaces)
}

func TestLoadCLIConfig_RejectsIgnoreTablePaths(t *testing.T) {
	dir := t.TempDir()
	content := "database: mydb\ntype: mysql\nignore_tables:\n  - app/flyway_schema_history\n"
	require.NoError(t, os.WriteFile(filepath.Join(dir, "schemabot.yaml"), []byte(content), 0644))

	cfg, err := LoadCLIConfig(dir)
	require.Error(t, err)
	assert.Nil(t, cfg)
	assert.Contains(t, err.Error(), "not a path")
}
