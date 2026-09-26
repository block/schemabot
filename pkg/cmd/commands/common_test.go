//go:build !integration && !e2e

package commands

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/block/schemabot/pkg/cmd/internal/templates"
	"github.com/block/schemabot/pkg/ddl"
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

// Index builds and drops are counted in their own clauses whether the change
// type arrives in the REST form or the proto form, so an apply that only ran
// index work does not complete with an empty summary and a table that also
// gained an index is not counted as altered twice.
func TestApplyChangeCountsSummaryNamesIndexWork(t *testing.T) {
	tables := []templates.TableProgress{
		{TableName: "orders", ChangeType: "alter"},
		{TableName: "orders", ChangeType: "create_index"},
		{TableName: "events", ChangeType: "CHANGE_TYPE_CREATE_INDEX"},
		{TableName: "orders", ChangeType: "CHANGE_TYPE_DROP_INDEX"},
	}

	assert.Equal(t, "Changes: 1 altered, 2 indexes created, 1 index dropped.", countTableProgressChanges(tables).summary())
	assert.Equal(t, "Changes: 1 index created.", countTableProgressChanges(tables[2:3]).summary())
}

// A change type outside the named buckets — a DDL kind the counter does not
// name, or an empty one the producer could not map — is reported as other DDL
// rather than dropped, and the clause sits between the typed counts and the
// VSchema clause so the typed counts never read as the whole apply.
func TestApplyChangeCountsSummaryNamesOtherDDL(t *testing.T) {
	tables := []templates.TableProgress{
		{TableName: "orders", ChangeType: "alter"},
		{TableName: "orders", ChangeType: "rename"},
		{TableName: "events", ChangeType: ""},
		{ChangeType: "vschema_update"},
	}

	assert.Equal(t, "Changes: 1 altered, 2 other DDL statements, 1 VSchema update.", countTableProgressChanges(tables).summary())
	assert.Equal(t, "Changes: 1 other DDL statement.", countTableProgressChanges(tables[1:2]).summary())
}

// TestApplyChangeCountsSummaryNamesEveryDDLKind walks the DDL statement
// vocabulary and pins that a task of every kind appears in the completion
// summary, so a kind without a named bucket is still reported instead of
// leaving an apply that ran it with an empty or short Changes clause.
func TestApplyChangeCountsSummaryNamesEveryDDLKind(t *testing.T) {
	for st := range ddl.StatementType(64) {
		if !st.IsDDL() {
			continue
		}
		op := ddl.StatementTypeToOp(st)
		t.Run(fmt.Sprintf("%d_%s", int(st), op), func(t *testing.T) {
			tables := []templates.TableProgress{{TableName: "orders", ChangeType: op}}
			assert.NotEmpty(t, countTableProgressChanges(tables).summary(), "a %q task is missing from the completion summary", op)
		})
	}
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
