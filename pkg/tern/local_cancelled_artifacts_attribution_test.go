package tern

import (
	"log/slog"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/block/schemabot/pkg/state"
	"github.com/block/schemabot/pkg/storage"
)

// A cancel removes only a constraint a finished step of the apply added. An
// ADD that completed committed its constraint. An ADD that failed, for example
// because someone else added a constraint under the same name, added nothing,
// so the constraint on the table is not the apply's to remove. Neither is one
// a step that never ran would have added.
func TestCancelledArtifactTablesAttributesOnlyCompletedSteps(t *testing.T) {
	apply := &storage.Apply{ID: 7, ApplyIdentifier: "apply-7", Database: "shop"}
	add := func(table string) string {
		return "ALTER TABLE public." + table + " ADD CONSTRAINT " + table + "_name_not_null CHECK (name IS NOT NULL) NOT VALID"
	}
	tasks := []*storage.Task{
		{ApplyID: apply.ID, Namespace: "public", TableName: "users", DDL: add("users"), State: state.Task.Completed},
		{ApplyID: apply.ID, Namespace: "public", TableName: "teams", DDL: add("teams"), State: state.Task.Failed},
		{ApplyID: apply.ID, Namespace: "public", TableName: "orgs", DDL: add("orgs"), State: state.Task.FailedRetryable},
		{ApplyID: apply.ID, Namespace: "public", TableName: "roles", DDL: add("roles"), State: state.Task.Pending},
	}
	c := &LocalClient{config: LocalConfig{Database: "shop", Type: storage.DatabaseTypePostgres}, logger: slog.Default()}

	artifacts := c.cancelledArtifactTables(apply, tasks)["public"]
	require.NotNil(t, artifacts)
	assert.Equal(t, map[string][]string{"users": {"users_name_not_null"}}, artifacts.addedConstraints)
	assert.ElementsMatch(t, []string{"users", "teams", "orgs", "roles"}, artifacts.tables, "every table's other artifacts are still released")
}
