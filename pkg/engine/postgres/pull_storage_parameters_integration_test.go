//go:build integration

package postgres

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/block/schemabot/pkg/engine"
	ternv1 "github.com/block/schemabot/pkg/proto/ternv1"
)

// A table tuned with storage parameters, on the table and on its TOAST table,
// pulls with those parameters in its file. Planning the pulled files against
// the same database plans no changes, so the baseline an owner commits keeps
// the tuning instead of leaving the table out of the pull.
func TestEnginePullSchemaWritesStorageParameters(t *testing.T) {
	eng, credentials := pullForeignKeyTarget(t, "pull_storage_parameters_test", `
		CREATE SCHEMA app;
		CREATE TABLE app.events (id bigint PRIMARY KEY, payload text)
			WITH (fillfactor = 70, autovacuum_enabled = off, toast.autovacuum_enabled = false);
		CREATE TABLE app.users (id bigint PRIMARY KEY)`)

	response, err := eng.PullSchema(t.Context(), &ternv1.PullSchemaRequest{Namespace: "app"})
	require.NoError(t, err)
	tables := response.Namespaces["app"].GetTables()
	assert.Equal(t, []string{"events", "users"}, sortedKeys(tables))
	assert.Contains(t, tables["events"], "WITH (autovacuum_enabled='false', fillfactor='70', toast.autovacuum_enabled='false')")
	assert.NotContains(t, tables["users"], "WITH (", "a table without storage parameters declares none")

	result, err := New().Plan(t.Context(), &engine.PlanRequest{
		Database:    "pull_storage_parameters_test",
		SchemaFiles: pulledSchemaFiles(response),
		Credentials: credentials,
	})
	require.NoError(t, err)
	assert.Empty(t, result.Changes, "the pulled files must describe the live schema exactly")
}
