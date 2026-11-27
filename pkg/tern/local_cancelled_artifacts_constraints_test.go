package tern

import (
	"log/slog"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/block/schemabot/pkg/engine"
	"github.com/block/schemabot/pkg/state"
	"github.com/block/schemabot/pkg/storage"
)

// A release learns, per table, which constraints the apply's steps added and
// which its plan validates in a step of its own. A foreign key added NOT VALID
// by a completed step is both, so a failed or cancelled validation lets the
// release drop it. On a table whose plan drops a rule the table had, the
// release drops no constraint: the unvalidated new rule may be the only one
// checking the table's writes, whether it keeps the old rule's name or takes
// a new one, and whether the drop and the add share a statement or run as
// steps of their own. A SET NOT NULL helper the plan adds and later drops is
// the plan's own, so its drop does not count, but on a table whose plan also
// drops a rule the table had, the helper is kept with the rest and recorded
// as held for that reason. A step that never ran added nothing, though the
// plan still validates what it would have added.
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
		task("accounts", "ALTER TABLE public.accounts DROP CONSTRAINT accounts_id_check", state.Task.Completed),
		task("accounts", "ALTER TABLE public.accounts ADD CONSTRAINT accounts_id_check CHECK (id > 50) NOT VALID", state.Task.Completed),
		task("accounts", "ALTER TABLE public.accounts VALIDATE CONSTRAINT accounts_id_check", state.Task.Failed),
		task("teams", "ALTER TABLE public.teams DROP CONSTRAINT teams_id_check", state.Task.Completed),
		task("teams", "ALTER TABLE public.teams ADD CONSTRAINT teams_id_check2 CHECK (id > 50) NOT VALID", state.Task.Completed),
		task("teams", "ALTER TABLE public.teams VALIDATE CONSTRAINT teams_id_check2", state.Task.Failed),
		task("users", "ALTER TABLE public.users ADD CONSTRAINT users_name_not_null_check CHECK (name IS NOT NULL) NOT VALID", state.Task.Completed),
		task("users", "ALTER TABLE public.users VALIDATE CONSTRAINT users_name_not_null_check", state.Task.Failed),
		task("users", "ALTER TABLE public.users ALTER COLUMN name SET NOT NULL", state.Task.Pending),
		task("users", "ALTER TABLE public.users DROP CONSTRAINT users_name_not_null_check", state.Task.Pending),
		task("members", "ALTER TABLE public.members DROP CONSTRAINT members_age_check", state.Task.Completed),
		task("members", "ALTER TABLE public.members ADD CONSTRAINT members_name_not_null_check CHECK (name IS NOT NULL) NOT VALID", state.Task.Completed),
		task("members", "ALTER TABLE public.members VALIDATE CONSTRAINT members_name_not_null_check", state.Task.Failed),
		task("members", "ALTER TABLE public.members ALTER COLUMN name SET NOT NULL", state.Task.Pending),
		task("members", "ALTER TABLE public.members DROP CONSTRAINT members_name_not_null_check", state.Task.Pending),
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

	assert.Empty(t, artifacts.addedConstraints["accounts"], "a rule replaced in steps of its own is the table's only one once the old rule is dropped")
	assert.Empty(t, artifacts.validatedConstraints["accounts"])

	assert.Empty(t, artifacts.addedConstraints["teams"], "a rule replaced under a new name is the table's only one once the old rule is dropped")
	assert.Empty(t, artifacts.validatedConstraints["teams"])

	assert.Equal(t, []string{"users_name_not_null_check"}, artifacts.addedConstraints["users"], "the plan drops only the helper it added, so the release may drop it")
	assert.Equal(t, []string{"users_name_not_null_check"}, artifacts.validatedConstraints["users"])

	assert.Empty(t, artifacts.addedConstraints["members"], "a helper on a table whose plan drops a rule it had is kept with the rest")
	assert.Equal(t, map[string]string{"members_name_not_null_check": "its plan also drops a rule the table had"}, artifacts.heldConstraints["members"])
	assert.Equal(t, map[string]string{"items_qty_check": "its plan also drops a rule the table had"}, artifacts.heldConstraints["items"])
	assert.NotContains(t, artifacts.heldConstraints, "users", "a release that may drop the helper holds nothing")
	assert.NotContains(t, artifacts.heldConstraints, "refunds", "a constraint no finished step added is not held")

	assert.Empty(t, artifacts.addedConstraints["refunds"], "a step that never ran added nothing")
	assert.Equal(t, []string{"refunds_order_id_fkey"}, artifacts.validatedConstraints["refunds"])
}

// A statement the release cannot read might have dropped the rule an
// unvalidated constraint replaces, so the release removes no constraint on
// that table.
func TestCancelledArtifactTablesRemovesNoConstraintOnAnUnreadTable(t *testing.T) {
	apply := &storage.Apply{ID: 8, ApplyIdentifier: "apply-8", Database: "shop"}
	tasks := []*storage.Task{
		{ApplyID: apply.ID, Namespace: "public", TableName: "orders", State: state.Task.Completed, DDL: "ALTER TABLE public.orders DROP CONSTRAINT"},
		{ApplyID: apply.ID, Namespace: "public", TableName: "orders", State: state.Task.Completed, DDL: "ALTER TABLE public.orders ADD CONSTRAINT orders_id_check CHECK (id > 50) NOT VALID"},
		{ApplyID: apply.ID, Namespace: "public", TableName: "orders", State: state.Task.Failed, DDL: "ALTER TABLE public.orders VALIDATE CONSTRAINT orders_id_check"},
	}
	c := &LocalClient{config: LocalConfig{Database: "shop", Type: storage.DatabaseTypePostgres}, logger: slog.Default()}

	artifacts := c.cancelledArtifactTables(apply, tasks)["public"]
	require.NotNil(t, artifacts)
	assert.Equal(t, []string{"orders"}, artifacts.tables, "the table's other artifacts are still released")
	assert.Empty(t, artifacts.addedConstraints["orders"])
	assert.Empty(t, artifacts.validatedConstraints["orders"])
	assert.Equal(t, map[string]string{"orders_id_check": "SchemaBot could not read every statement of its plan"}, artifacts.heldConstraints["orders"])
}

// A constraint the release kept on purpose reads, in the apply log, as one
// the schema change added, with the reason it is still there and the
// statement that drops it.
func TestHeldConstraintMessage(t *testing.T) {
	msg := heldConstraintMessage(engine.EngineConstraint{
		Table:   "members",
		Name:    "members_name_not_null_check",
		Effect:  "rejects writes that leave name NULL",
		Removal: `ALTER TABLE "public"."members" DROP CONSTRAINT "members_name_not_null_check"`,
	}, "failed", "its plan also drops a rule the table had")
	assert.Equal(t, `Constraint members_name_not_null_check is still on members. The failed schema change added it, but its plan also drops a rule the table had, so SchemaBot kept every constraint on the table rather than risk leaving it with no rule. It rejects writes that leave name NULL until it is dropped. Drop it with: ALTER TABLE "public"."members" DROP CONSTRAINT "members_name_not_null_check"`, msg)
}
