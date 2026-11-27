package tern

import (
	"log/slog"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/block/schemabot/pkg/state"
	"github.com/block/schemabot/pkg/storage"
)

// A release learns, per table, which constraints the apply's steps added and
// which its plan validates in a step of its own. A foreign key added NOT VALID
// by a step that ran is both, so a failed or cancelled validation lets the
// release drop it. A constraint a step drops and adds again under the same
// name replaces the old rule in place, so it is neither: the unvalidated new
// rule is the only one checking the table's writes. A step that never ran
// added nothing, though the plan still validates what it would have added.
func TestCancelledArtifactTablesAttributesUnvalidatedConstraints(t *testing.T) {
	apply := &storage.Apply{ID: 7, ApplyIdentifier: "apply-7", Database: "shop"}
	task := func(table, ddl, taskState string) *storage.Task {
		return &storage.Task{ApplyID: apply.ID, Namespace: "public", TableName: table, DDL: ddl, State: taskState}
	}
	tasks := []*storage.Task{
		task("orders", "ALTER TABLE public.orders ADD CONSTRAINT orders_user_id_fkey FOREIGN KEY (user_id) REFERENCES public.users (id) NOT VALID", state.Task.Completed),
		task("orders", "ALTER TABLE public.orders VALIDATE CONSTRAINT orders_user_id_fkey", state.Task.Failed),
		task("items", "ALTER TABLE public.items DROP CONSTRAINT items_qty_check, ADD CONSTRAINT items_qty_check CHECK (qty > 5) NOT VALID", state.Task.Completed),
		task("items", "ALTER TABLE public.items VALIDATE CONSTRAINT items_qty_check", state.Task.Failed),
		task("refunds", "ALTER TABLE public.refunds ADD CONSTRAINT refunds_order_id_fkey FOREIGN KEY (order_id) REFERENCES public.orders (id) NOT VALID", state.Task.Pending),
		task("refunds", "ALTER TABLE public.refunds VALIDATE CONSTRAINT refunds_order_id_fkey", state.Task.Pending),
	}
	c := &LocalClient{config: LocalConfig{Database: "shop", Type: storage.DatabaseTypePostgres}, logger: slog.Default()}

	byNamespace := c.cancelledArtifactTables(apply, tasks)
	require.Contains(t, byNamespace, "public")
	artifacts := byNamespace["public"]

	assert.Equal(t, []string{"orders_user_id_fkey"}, artifacts.addedConstraints["orders"])
	assert.Equal(t, []string{"orders_user_id_fkey"}, artifacts.validatedConstraints["orders"])

	assert.Empty(t, artifacts.addedConstraints["items"], "a replaced rule is the table's only one, so the release keeps it")
	assert.Empty(t, artifacts.validatedConstraints["items"], "a replaced rule is not reported as one the table had before")

	assert.Empty(t, artifacts.addedConstraints["refunds"], "a step that never ran added nothing")
	assert.Equal(t, []string{"refunds_order_id_fkey"}, artifacts.validatedConstraints["refunds"])
}
