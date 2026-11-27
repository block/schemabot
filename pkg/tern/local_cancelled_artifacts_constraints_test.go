package tern

import (
	"context"
	"errors"
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
	assert.Equal(t, []string{"items_qty_check"}, artifacts.validatedConstraints["items"], "the kept rule is still found, so the release reports it")

	assert.Empty(t, artifacts.addedConstraints["accounts"], "a rule replaced in steps of its own is the table's only one once the old rule is dropped")
	assert.Equal(t, []string{"accounts_id_check"}, artifacts.validatedConstraints["accounts"])
	assert.Equal(t, map[string]string{"accounts_id_check": "its plan also drops a rule the table had"}, artifacts.heldConstraints["accounts"])

	assert.Empty(t, artifacts.addedConstraints["teams"], "a rule replaced under a new name is the table's only one once the old rule is dropped")
	assert.Equal(t, []string{"teams_id_check2"}, artifacts.validatedConstraints["teams"])
	assert.Equal(t, map[string]string{"teams_id_check2": "its plan also drops a rule the table had"}, artifacts.heldConstraints["teams"])

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
	assert.Equal(t, []string{"orders_id_check"}, artifacts.validatedConstraints["orders"], "the kept constraint is still found, so the release reports it")
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

// The engine reports each constraint under its schema-qualified table, in the
// target's physical schema. A constraint the release kept on purpose is
// matched there, so the apply log says why it was kept rather than that no
// finished step added it.
func TestRecordReleaseResultExplainsAHeldConstraint(t *testing.T) {
	apply := &storage.Apply{ID: 9, ApplyIdentifier: "apply-9", Database: "shop"}
	c, logs := newFailureLogTestClient(apply, nil)
	held := map[string]map[string]string{"members": {"members_name_not_null_check": "its plan also drops a rule the table had"}}
	c.recordReleaseResult(t.Context(), apply, "public", "failed", &engine.ReleaseArtifactsResult{
		PreexistingConstraints: []engine.EngineConstraint{{
			Table:   "public.members",
			Name:    "members_name_not_null_check",
			Effect:  "rejects writes that break CHECK ((name IS NOT NULL))",
			Removal: `ALTER TABLE "public"."members" DROP CONSTRAINT "members_name_not_null_check"`,
		}},
	}, heldByQualifiedTable("public", held))
	require.Len(t, logs.entries, 1)
	assert.Contains(t, logs.entries[0].Message, "The failed schema change added it, but its plan also drops a rule the table had")
	assert.NotContains(t, logs.entries[0].Message, "No finished step")
}

// A resumed drive carries only the tasks it still has to weigh. The release
// reads the rest from storage, so the completed ADD that proves the apply
// added its constraint is still there, and the drive's own copy of a task
// stands in for the stored one it updates.
func TestTaskHistoryAddsTheStoredStepsAResumedDriveLeftOut(t *testing.T) {
	apply := &storage.Apply{ID: 10, ApplyIdentifier: "apply-10", Database: "shop"}
	add := &storage.Task{ApplyID: apply.ID, TaskIdentifier: "add", Namespace: "public", TableName: "users", State: state.Task.Completed,
		DDL: "ALTER TABLE public.users ADD CONSTRAINT users_name_not_null_check CHECK (name IS NOT NULL) NOT VALID"}
	storedValidate := &storage.Task{ApplyID: apply.ID, TaskIdentifier: "validate", Namespace: "public", TableName: "users", State: state.Task.Running,
		DDL: "ALTER TABLE public.users VALIDATE CONSTRAINT users_name_not_null_check"}
	failedValidate := *storedValidate
	failedValidate.State = state.Task.Failed
	c, _ := newFailureLogTestClient(apply, []*storage.Task{add, storedValidate})

	history, err := c.taskHistory(t.Context(), apply, []*storage.Task{&failedValidate})
	require.NoError(t, err)
	require.Len(t, history, 2)
	assert.Same(t, add, history[0])
	assert.Same(t, &failedValidate, history[1])

	c.config.Type = storage.DatabaseTypePostgres
	artifacts := c.cancelledArtifactTables(apply, history)["public"]
	require.NotNil(t, artifacts)
	assert.Equal(t, []string{"users_name_not_null_check"}, artifacts.addedConstraints["users"])
}

// A resumed drive fails the validation of a constraint an earlier drive
// added, and the release cannot read the apply's stored tasks. The completed
// ADD that proves the apply added the constraint is among them, so the
// release drops nothing: the constraint stays on the table, and the apply log
// says the cleanup was skipped and why.
func TestReleaseFailedApplyArtifactsKeepsConstraintsWhenTheTaskHistoryIsUnread(t *testing.T) {
	apply := &storage.Apply{ID: 11, ApplyIdentifier: "apply-11", Database: "shop"}
	failedValidate := &storage.Task{ApplyID: apply.ID, TaskIdentifier: "validate", Namespace: "public", TableName: "users", State: state.Task.Failed,
		DDL: "ALTER TABLE public.users VALIDATE CONSTRAINT users_name_not_null_check"}
	c, logs := newFailureLogTestClient(apply, nil)
	c.config.Type = storage.DatabaseTypePostgres
	c.storage.(*controlTestStorage).tasks = &unreadableTaskStore{err: errors.New("connection refused")}
	releaser := &recordingFailedReleaser{}
	c.postgresEngine = releaser

	c.releaseFailedApplyArtifacts(t.Context(), apply, []*storage.Task{failedValidate}, failedValidate)

	assert.Zero(t, releaser.calls, "a release that cannot prove what the apply added drops nothing")
	require.Len(t, logs.entries, 1)
	assert.Equal(t, storage.LogLevelWarn, logs.entries[0].Level)
	assert.Equal(t, "What the failed schema change left on the target was not reclaimed because SchemaBot could not read the apply's task history, so it could not prove which constraints the apply added",
		logs.entries[0].Message)
}

type unreadableTaskStore struct {
	storage.TaskStore
	err error
}

func (s *unreadableTaskStore) GetByApplyID(context.Context, int64) ([]*storage.Task, error) {
	return nil, s.err
}

type recordingFailedReleaser struct {
	engine.Engine
	calls int
}

func (r *recordingFailedReleaser) ReleaseFailedArtifacts(context.Context, *engine.ReleaseArtifactsRequest) (*engine.ReleaseArtifactsResult, error) {
	r.calls++
	return &engine.ReleaseArtifactsResult{}, nil
}
