package e2eutil

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestWriteSchemaDir_NoOptions(t *testing.T) {
	dir := WriteSchemaDir(t, "testdb", "mysql", map[string]string{
		"001.sql": "CREATE TABLE t1 (id INT);",
	})

	config, err := os.ReadFile(filepath.Join(dir, "schemabot.yaml"))
	require.NoError(t, err)
	assert.Equal(t, "database: testdb\ntype: mysql\n", string(config))

	sql, err := os.ReadFile(filepath.Join(dir, "001.sql"))
	require.NoError(t, err)
	assert.Equal(t, "CREATE TABLE t1 (id INT);", string(sql))
}

func TestWriteSchemaDir_WithEnvironments(t *testing.T) {
	dir := WriteSchemaDir(t, "testdb", "mysql", map[string]string{
		"001.sql": "CREATE TABLE t1 (id INT);",
	}, WithEnvironments(map[string]string{
		"staging":    "my-dsid-staging-001",
		"production": "my-dsid-production-001",
	}))

	config, err := os.ReadFile(filepath.Join(dir, "schemabot.yaml"))
	require.NoError(t, err)

	configStr := string(config)
	assert.Contains(t, configStr, "database: testdb")
	assert.Contains(t, configStr, "type: mysql")
	assert.Contains(t, configStr, "environments:")
	assert.Contains(t, configStr, "  staging:\n    target: my-dsid-staging-001")
	assert.Contains(t, configStr, "  production:\n    target: my-dsid-production-001")
}
