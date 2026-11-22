//go:build integration

package postgres

import (
	"database/sql"
	"fmt"
	"net/url"
	"testing"
	"time"

	"github.com/block/spirit/pkg/utils"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/block/schemabot/pkg/ddl"
	"github.com/block/schemabot/pkg/engine"
	"github.com/block/schemabot/pkg/schema"
	"github.com/block/schemabot/pkg/testutil"
)

// ordersTableFile declares a table whose status column uses the enum type,
// matching the live table each test creates, so the table plans clean and
// only the types file can produce a change.
const ordersTableFile = `CREATE TABLE orders (
    id bigint PRIMARY KEY,
    status order_status NOT NULL
);`

// planEnumFiles plans the given schema files for namespace public against a
// fresh database seeded with the live enum type and a table that uses it.
func planEnumFiles(t *testing.T, database, liveLabels string, files map[string]string) (*engine.PlanResult, error) {
	t.Helper()
	return planEnumFilesAgainst(t, database, `
		CREATE TYPE public.order_status AS ENUM `+liveLabels+`;
		CREATE TABLE public.orders (id bigint PRIMARY KEY, status public.order_status NOT NULL)`, files)
}

// planEnumFilesAgainst plans the given schema files for namespace public
// against a fresh database seeded by setup, which may be empty.
func planEnumFilesAgainst(t *testing.T, database, setup string, files map[string]string) (*engine.PlanResult, error) {
	t.Helper()
	dsn, db := testutil.StartPostgres(t, database)
	if setup != "" {
		_, err := db.ExecContext(t.Context(), setup)
		require.NoError(t, err)
	}
	return New().Plan(t.Context(), &engine.PlanRequest{
		Database:    database,
		SchemaFiles: schema.SchemaFiles{"public": {Files: files}},
		Credentials: &engine.Credentials{DSN: dsn},
	})
}

// A schema directory keeps its enum type in a file next to the table that
// uses it. When the live type already carries every label the file lists,
// the plan is empty: the types file is reviewed like any other schema file,
// not refused.
func TestEnginePlanConvergedEnumTypeFile(t *testing.T) {
	result, err := planEnumFiles(t, "plan_enum_converged_test", `('new', 'shipped')`, map[string]string{
		"order_status.sql": `CREATE TYPE order_status AS ENUM ('new', 'shipped');`,
		"orders.sql":       ordersTableFile,
	})
	require.NoError(t, err)
	assert.True(t, result.NoChanges, "a converged types file plans no change, got %+v", result.Changes)
}

// A types file that lists labels the live type lacks plans one ALTER TYPE
// ... ADD VALUE per label, in declared order and placed before the next live
// label, named by the type. Each step is executable: the engine role owns
// the type, so nothing blocks it. The table that uses the type is
// untouched.
func TestEnginePlanEnumTypeFileAddsLabels(t *testing.T) {
	result, err := planEnumFiles(t, "plan_enum_add_test", `('new', 'shipped')`, map[string]string{
		"order_status.sql": `CREATE TYPE order_status AS ENUM ('new', 'paid', 'shipped', 'delivered');`,
		"orders.sql":       ordersTableFile,
	})
	require.NoError(t, err)
	require.False(t, result.NoChanges)
	require.Len(t, result.Changes, 1)
	assert.Equal(t, "public", result.Changes[0].Namespace)

	changes := result.Changes[0].TableChanges
	require.Len(t, changes, 2, "one change per added label and nothing for the table")
	wantDDL := []string{
		"ALTER TYPE public.order_status ADD VALUE 'paid' BEFORE 'shipped'",
		"ALTER TYPE public.order_status ADD VALUE 'delivered'",
	}
	for i, change := range changes {
		assert.Equal(t, "order_status", change.Table)
		assert.Equal(t, ddl.StatementAlterType, change.Operation)
		assert.Equal(t, wantDDL[i], change.DDL)
		assert.False(t, change.IsUnsafe, "adding a label destroys nothing")
		assert.Empty(t, change.ExecutionMode, "a label addition runs on the engine's default path")
		assert.Empty(t, change.ModeReason)
	}
}

// An engine role that is not a member of the type's owner cannot add a
// label, so the plan blocks every label addition and names the grant that
// fixes it, rather than offer steps the apply would refuse.
func TestEnginePlanEnumTypeFileBlockedWithoutOwnership(t *testing.T) {
	const database = "plan_enum_owner_test"
	dsn, db := testutil.StartPostgres(t, database)
	_, err := db.ExecContext(t.Context(), `
		CREATE TYPE public.order_status AS ENUM ('new', 'shipped');
		CREATE ROLE planner LOGIN PASSWORD 'planner';
		GRANT CONNECT ON DATABASE `+database+` TO planner;
		GRANT USAGE ON SCHEMA public TO planner`)
	require.NoError(t, err)

	result, err := New().Plan(t.Context(), &engine.PlanRequest{
		Database: database,
		SchemaFiles: schema.SchemaFiles{"public": {Files: map[string]string{
			"order_status.sql": `CREATE TYPE order_status AS ENUM ('new', 'paid', 'shipped', 'delivered');`,
		}}},
		Credentials: &engine.Credentials{DSN: dsnAs(t, dsn, "planner", "planner")},
	})
	require.NoError(t, err)
	require.Len(t, result.Changes, 1)
	changes := result.Changes[0].TableChanges
	require.Len(t, changes, 2)
	for _, change := range changes {
		assert.Equal(t, engine.ExecutionModeBlocked, change.ExecutionMode)
		assert.Contains(t, change.ModeReason, `the engine role lacks access to alter enum type "order_status"`)
		assert.Contains(t, change.ModeReason, `provision with: GRANT "schemabot" TO "planner"`)
	}
}

// dsnAs returns dsn with its credentials replaced by user and password.
func dsnAs(t *testing.T, dsn, user, password string) string {
	t.Helper()
	parsed, err := url.Parse(dsn)
	require.NoError(t, err)
	parsed.User = url.UserPassword(user, password)
	return parsed.String()
}

// liveLabels returns the labels of public.order_status in sort order.
func liveLabels(t *testing.T, db *sql.DB) []string {
	t.Helper()
	rows, err := db.QueryContext(t.Context(), `
		SELECT e.enumlabel FROM pg_enum e
		WHERE e.enumtypid = 'public.order_status'::regtype
		ORDER BY e.enumsortorder`)
	require.NoError(t, err)
	defer utils.CloseAndLog(rows)
	var labels []string
	for rows.Next() {
		var label string
		require.NoError(t, rows.Scan(&label))
		labels = append(labels, label)
	}
	require.NoError(t, rows.Err())
	return labels
}

// applyPlannedSteps applies a plan's changes one at a time through the
// engine, in the order the plan lists them, as the drive does, and returns
// each step's terminal progress. It stops at the first step that does not
// complete.
func applyPlannedSteps(t *testing.T, eng *Engine, dsn string, result *engine.PlanResult) []*engine.ProgressResult {
	t.Helper()
	var outcomes []*engine.ProgressResult
	for _, change := range result.Changes {
		for i, tc := range change.TableChanges {
			key := fmt.Sprintf("%s-%s-%d", change.Namespace, tc.Table, i)
			accepted, err := eng.Apply(t.Context(), &engine.ApplyRequest{
				Database:    "app",
				Changes:     []engine.SchemaChange{{Namespace: change.Namespace, TableChanges: []engine.TableChange{tc}}},
				Credentials: &engine.Credentials{DSN: dsn},
				ResumeState: &engine.ResumeState{MigrationContext: key},
			})
			require.NoError(t, err)
			require.True(t, accepted.Accepted, accepted.Message)
			progress := awaitPostgresProgressFor(t, eng, key)
			outcomes = append(outcomes, progress)
			if progress.State != engine.StateCompleted {
				return outcomes
			}
		}
	}
	return outcomes
}

// awaitPostgresProgressFor waits for the apply keyed by key to finish.
func awaitPostgresProgressFor(t *testing.T, eng *Engine, key string) *engine.ProgressResult {
	t.Helper()
	var result *engine.ProgressResult
	finished := assert.Eventually(t, func() bool {
		var err error
		result, err = eng.Progress(t.Context(), &engine.ProgressRequest{ResumeState: &engine.ResumeState{MigrationContext: key}})
		require.NoError(t, err)
		return result.State.IsTerminal()
	}, postgresApplyDeadline, 10*time.Millisecond)
	require.True(t, finished, "PostgreSQL apply %s did not finish; last progress: %+v", key, result)
	return result
}

// The apply adds every label the plan lists, one committed step at a time
// and in planned order, so a label placed BEFORE another lands in its
// declared position and an appended label lands last. Afterward the live
// type matches the file and a fresh plan is empty.
func TestEngineApplyEnumTypeFileAddsLabelsInOrder(t *testing.T) {
	const database = "apply_enum_order_test"
	dsn, db := testutil.StartPostgres(t, database)
	_, err := db.ExecContext(t.Context(), `
		CREATE TYPE public.order_status AS ENUM ('new', 'shipped');
		CREATE TABLE public.orders (id bigint PRIMARY KEY, status public.order_status NOT NULL)`)
	require.NoError(t, err)
	files := schema.SchemaFiles{"public": {Files: map[string]string{
		"order_status.sql": `CREATE TYPE order_status AS ENUM ('new', 'packed', 'paid', 'shipped', 'delivered');`,
		"orders.sql":       ordersTableFile,
	}}}
	eng := New()
	plan, err := eng.Plan(t.Context(), &engine.PlanRequest{Database: database, SchemaFiles: files, Credentials: &engine.Credentials{DSN: dsn}})
	require.NoError(t, err)
	require.Len(t, plan.Changes, 1)
	var planned []string
	for _, change := range plan.Changes[0].TableChanges {
		planned = append(planned, change.DDL)
	}
	require.Equal(t, []string{
		"ALTER TYPE public.order_status ADD VALUE 'packed' BEFORE 'shipped'",
		"ALTER TYPE public.order_status ADD VALUE 'paid' BEFORE 'shipped'",
		"ALTER TYPE public.order_status ADD VALUE 'delivered'",
	}, planned)

	outcomes := applyPlannedSteps(t, eng, dsn, plan)
	require.Len(t, outcomes, 3)
	for _, progress := range outcomes {
		assert.Equal(t, engine.StateCompleted, progress.State, progress.ErrorMessage)
	}
	assert.Equal(t, []string{"new", "packed", "paid", "shipped", "delivered"}, liveLabels(t, db))

	replan, err := eng.Plan(t.Context(), &engine.PlanRequest{Database: database, SchemaFiles: files, Credentials: &engine.Credentials{DSN: dsn}})
	require.NoError(t, err)
	assert.True(t, replan.NoChanges, "the applied type matches the file, got %+v", replan.Changes)
}

// Label additions lead their namespace's changes, even when a table's file
// sorts ahead of the types file, and the drive runs the changes in that
// order, each committed before the next. A mixed plan therefore adds the
// label first and then changes the table, and both converge.
func TestEngineApplyEnumLabelsBeforeTableChanges(t *testing.T) {
	const database = "apply_enum_mixed_test"
	dsn, db := testutil.StartPostgres(t, database)
	_, err := db.ExecContext(t.Context(), `
		CREATE TYPE public.order_status AS ENUM ('new', 'shipped');
		CREATE TABLE public.orders (id bigint PRIMARY KEY, status public.order_status NOT NULL)`)
	require.NoError(t, err)
	files := schema.SchemaFiles{"public": {Files: map[string]string{
		"orders.sql": `CREATE TABLE orders (
    id bigint PRIMARY KEY,
    status order_status NOT NULL,
    note text
);`,
		"status_types.sql": `CREATE TYPE order_status AS ENUM ('new', 'paid', 'shipped');`,
	}}}
	eng := New()
	plan, err := eng.Plan(t.Context(), &engine.PlanRequest{Database: database, SchemaFiles: files, Credentials: &engine.Credentials{DSN: dsn}})
	require.NoError(t, err)
	require.Len(t, plan.Changes, 1)
	changes := plan.Changes[0].TableChanges
	require.Len(t, changes, 2, "one label addition and one table change")
	assert.Equal(t, "order_status", changes[0].Table, "the label addition leads the namespace")
	assert.Equal(t, "ALTER TYPE public.order_status ADD VALUE 'paid' BEFORE 'shipped'", changes[0].DDL)
	assert.Equal(t, "orders", changes[1].Table)
	assert.Equal(t, "ALTER TABLE public.orders ADD COLUMN note text", changes[1].DDL)
	assert.Empty(t, changes[1].ExecutionMode, changes[1].ModeReason)

	outcomes := applyPlannedSteps(t, eng, dsn, plan)
	require.Len(t, outcomes, 2)
	for _, progress := range outcomes {
		assert.Equal(t, engine.StateCompleted, progress.State, progress.ErrorMessage)
	}
	assert.Equal(t, []string{"new", "paid", "shipped"}, liveLabels(t, db))
	_, err = db.ExecContext(t.Context(), `INSERT INTO public.orders (id, status, note) VALUES (1, 'paid', 'first')`)
	require.NoError(t, err, "the new label and the new column are both usable")

	replan, err := eng.Plan(t.Context(), &engine.PlanRequest{Database: database, SchemaFiles: files, Credentials: &engine.Credentials{DSN: dsn}})
	require.NoError(t, err)
	assert.True(t, replan.NoChanges, "the applied schema matches the files, got %+v", replan.Changes)
}

// A table is planned against the live type, so a table change that names a
// label the type does not carry yet fails the plan, naming the table's
// file, even when the same plan adds the label. The label lands in one
// apply and the table change that uses it in the next.
func TestEnginePlanTableChangeNamingANewLabelFails(t *testing.T) {
	_, err := planEnumFiles(t, "plan_enum_uses_new_label_test", `('new', 'shipped')`, map[string]string{
		"order_status.sql": `CREATE TYPE order_status AS ENUM ('new', 'paid', 'shipped');`,
		"orders.sql": `CREATE TABLE orders (
    id bigint PRIMARY KEY,
    status order_status NOT NULL DEFAULT 'paid'
);`,
	})
	require.Error(t, err)
	assert.ErrorContains(t, err, `plan PostgreSQL schema in "public"/"orders.sql"`)
	assert.ErrorContains(t, err, `invalid input value for enum order_status: "paid"`)
}

// Another session adds a planned label after the plan was made. The step
// that would add it again is refused, permanently, with a detail that names
// the label and says to re-plan; nothing is changed. A fresh plan then
// shows only what is still missing, and applying it converges the type.
func TestEngineApplyEnumLabelAddedAfterPlanning(t *testing.T) {
	const database = "apply_enum_stale_test"
	dsn, db := testutil.StartPostgres(t, database)
	_, err := db.ExecContext(t.Context(), `CREATE TYPE public.order_status AS ENUM ('new', 'shipped')`)
	require.NoError(t, err)
	files := schema.SchemaFiles{"public": {Files: map[string]string{
		"order_status.sql": `CREATE TYPE order_status AS ENUM ('new', 'paid', 'shipped', 'delivered');`,
	}}}
	eng := New()
	plan, err := eng.Plan(t.Context(), &engine.PlanRequest{Database: database, SchemaFiles: files, Credentials: &engine.Credentials{DSN: dsn}})
	require.NoError(t, err)
	require.Len(t, plan.Changes, 1)
	require.Len(t, plan.Changes[0].TableChanges, 2)

	_, err = db.ExecContext(t.Context(), `ALTER TYPE public.order_status ADD VALUE 'paid' BEFORE 'shipped'`)
	require.NoError(t, err)

	outcomes := applyPlannedSteps(t, eng, dsn, plan)
	require.Len(t, outcomes, 1, "the apply stops at the refused step")
	refused := outcomes[0]
	assert.Equal(t, engine.StateFailed, refused.State)
	assert.Equal(t, "refused", refused.Metadata["phase"])
	assert.False(t, refused.Retryable, "running the same statement again meets the same label")
	assert.Equal(t, `enum type "order_status" already has label "paid", which another change added after planning; re-plan against the current schema`, refused.ErrorMessage)
	assert.Equal(t, []string{"new", "paid", "shipped"}, liveLabels(t, db), "the refused step changed nothing")

	fresh, err := eng.Plan(t.Context(), &engine.PlanRequest{Database: database, SchemaFiles: files, Credentials: &engine.Credentials{DSN: dsn}})
	require.NoError(t, err)
	require.Len(t, fresh.Changes, 1)
	require.Len(t, fresh.Changes[0].TableChanges, 1)
	assert.Equal(t, "ALTER TYPE public.order_status ADD VALUE 'delivered'", fresh.Changes[0].TableChanges[0].DDL)
	outcomes = applyPlannedSteps(t, eng, dsn, fresh)
	require.Len(t, outcomes, 1)
	assert.Equal(t, engine.StateCompleted, outcomes[0].State, outcomes[0].ErrorMessage)
	assert.Equal(t, []string{"new", "paid", "shipped", "delivered"}, liveLabels(t, db))
}

// Another session renames the label a planned step places its new label
// BEFORE, after the plan was made. Running the same statement again can
// never succeed, so the step is refused, permanently, with a detail that
// names the label it adds and says to re-plan; nothing is changed.
func TestEngineApplyEnumPlacementLabelRenamedAfterPlanning(t *testing.T) {
	const database = "apply_enum_neighbor_test"
	dsn, db := testutil.StartPostgres(t, database)
	_, err := db.ExecContext(t.Context(), `CREATE TYPE public.order_status AS ENUM ('new', 'shipped')`)
	require.NoError(t, err)
	files := schema.SchemaFiles{"public": {Files: map[string]string{
		"order_status.sql": `CREATE TYPE order_status AS ENUM ('new', 'packed', 'shipped');`,
	}}}
	eng := New()
	plan, err := eng.Plan(t.Context(), &engine.PlanRequest{Database: database, SchemaFiles: files, Credentials: &engine.Credentials{DSN: dsn}})
	require.NoError(t, err)
	require.Len(t, plan.Changes, 1)
	require.Len(t, plan.Changes[0].TableChanges, 1)
	require.Equal(t, "ALTER TYPE public.order_status ADD VALUE 'packed' BEFORE 'shipped'", plan.Changes[0].TableChanges[0].DDL)

	_, err = db.ExecContext(t.Context(), `ALTER TYPE public.order_status RENAME VALUE 'shipped' TO 'sent'`)
	require.NoError(t, err)

	outcomes := applyPlannedSteps(t, eng, dsn, plan)
	require.Len(t, outcomes, 1)
	refused := outcomes[0]
	assert.Equal(t, engine.StateFailed, refused.State)
	assert.Equal(t, "refused", refused.Metadata["phase"])
	assert.False(t, refused.Retryable, "running the same statement again meets the same missing label")
	assert.Equal(t, `enum type "order_status" no longer has the label that "packed" is placed next to, which another change renamed or removed after planning; re-plan against the current schema`, refused.ErrorMessage)
	assert.Equal(t, []string{"new", "sent"}, liveLabels(t, db), "the refused step changed nothing")
}

// The apply proves access again in the session that executes, because a
// grant held at plan time can be revoked before the apply. A role that no
// longer belongs to the type's owner is refused before anything runs, with
// the grant that fixes it.
func TestEngineApplyEnumLabelRefusedWithoutOwnership(t *testing.T) {
	const database = "apply_enum_owner_test"
	dsn, db := testutil.StartPostgres(t, database)
	_, err := db.ExecContext(t.Context(), `
		CREATE TYPE public.order_status AS ENUM ('new', 'shipped');
		CREATE ROLE applier LOGIN PASSWORD 'applier';
		GRANT CONNECT ON DATABASE `+database+` TO applier;
		GRANT USAGE ON SCHEMA public TO applier`)
	require.NoError(t, err)

	eng := New()
	key := "public-order_status-0"
	_, err = eng.Apply(t.Context(), &engine.ApplyRequest{
		Database: "app",
		Changes: []engine.SchemaChange{{Namespace: "public", TableChanges: []engine.TableChange{{
			Table: "order_status", Operation: ddl.StatementAlterType, DDL: "ALTER TYPE public.order_status ADD VALUE 'paid'",
		}}}},
		Credentials: &engine.Credentials{DSN: dsnAs(t, dsn, "applier", "applier")},
		ResumeState: &engine.ResumeState{MigrationContext: key},
	})
	require.NoError(t, err)
	progress := awaitPostgresProgressFor(t, eng, key)
	assert.Equal(t, engine.StateFailed, progress.State)
	assert.Equal(t, "refused", progress.Metadata["phase"])
	assert.False(t, progress.Retryable)
	assert.Contains(t, progress.ErrorMessage, `the engine role lacks access to alter enum type "order_status"`)
	assert.Contains(t, progress.ErrorMessage, `GRANT "schemabot" TO "applier"`)
	assert.Equal(t, []string{"new", "shipped"}, liveLabels(t, db))
}

// A types file the live type cannot converge onto has no plan that reaches
// it: PostgreSQL cannot drop or move an enum label, and a plan never creates
// or replaces a type. Each case fails the plan with an error naming the
// schema file, the type, and what blocks it, and returns no partial plan,
// not even the labels it could add.
func TestEnginePlanEnumTypeFileRefusals(t *testing.T) {
	ordersTable := func(labels string) string {
		return `CREATE TYPE public.order_status AS ENUM ` + labels + `;
			CREATE TABLE public.orders (id bigint PRIMARY KEY, status public.order_status NOT NULL)`
	}
	tests := []struct {
		name     string
		database string
		setup    string
		files    map[string]string
		reason   string
	}{
		{
			name:     "dropped label",
			database: "plan_enum_drop_test",
			setup:    ordersTable(`('new', 'paid', 'shipped')`),
			files: map[string]string{
				"order_status.sql": `CREATE TYPE order_status AS ENUM ('new', 'shipped', 'delivered');`,
				"orders.sql":       ordersTableFile,
			},
			reason: "enum type public.order_status carries ('paid'), which the file no longer lists",
		},
		{
			name:     "reordered labels",
			database: "plan_enum_reorder_test",
			setup:    ordersTable(`('new', 'shipped')`),
			files: map[string]string{
				"order_status.sql": `CREATE TYPE order_status AS ENUM ('shipped', 'new', 'delivered');`,
				"orders.sql":       ordersTableFile,
			},
			reason: "enum type public.order_status sorts its labels as ('new', 'shipped'), and the file lists them as ('shipped', 'new')",
		},
		{
			name:     "missing type",
			database: "plan_enum_missing_test",
			files: map[string]string{
				"order_status.sql": `CREATE TYPE order_status AS ENUM ('new', 'shipped');`,
			},
			reason: "enum type public.order_status does not exist, and a plan never creates a type",
		},
		{
			name:     "name held by a composite type",
			database: "plan_enum_kind_test",
			setup:    `CREATE TYPE public.order_status AS (code text)`,
			files: map[string]string{
				"order_status.sql": `CREATE TYPE order_status AS ENUM ('new', 'shipped');`,
			},
			reason: "public.order_status is a type that is not an enum, and a plan never replaces a type",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := planEnumFilesAgainst(t, tt.database, tt.setup, tt.files)
			require.Error(t, err)
			assert.Nil(t, result, "a refused types file returns no partial plan")
			assert.ErrorContains(t, err, `plan PostgreSQL schema in "public"/"order_status.sql"`)
			assert.ErrorContains(t, err, `plan PostgreSQL enum type "order_status"`)
			assert.ErrorContains(t, err, tt.reason)
		})
	}
}

// Several types files in one namespace each declare their own type, and none
// declares a table, so they never read as one table declared twice. Two
// files that declare the same type fail the plan, naming both files.
func TestEnginePlanEnumTypeFileDeclarations(t *testing.T) {
	t.Run("distinct types plan side by side", func(t *testing.T) {
		dsn, db := testutil.StartPostgres(t, "plan_enum_distinct_test")
		_, err := db.ExecContext(t.Context(), `
			CREATE TYPE public.order_status AS ENUM ('new', 'shipped');
			CREATE TYPE public.payment_state AS ENUM ('pending', 'settled')`)
		require.NoError(t, err)
		result, err := New().Plan(t.Context(), &engine.PlanRequest{
			Database: "plan_enum_distinct_test",
			SchemaFiles: schema.SchemaFiles{"public": {Files: map[string]string{
				"order_status.sql":  `CREATE TYPE order_status AS ENUM ('new', 'shipped');`,
				"payment_state.sql": `CREATE TYPE payment_state AS ENUM ('pending', 'settled', 'refunded');`,
			}}},
			Credentials: &engine.Credentials{DSN: dsn},
		})
		require.NoError(t, err)
		require.Len(t, result.Changes, 1)
		changes := result.Changes[0].TableChanges
		require.Len(t, changes, 1, "only the type that lacks a label changes")
		assert.Equal(t, "payment_state", changes[0].Table)
		assert.Equal(t, "ALTER TYPE public.payment_state ADD VALUE 'refunded'", changes[0].DDL)
	})

	t.Run("one type declared twice fails the plan", func(t *testing.T) {
		_, err := planEnumFiles(t, "plan_enum_twice_test", `('new', 'shipped')`, map[string]string{
			"order_status.sql":    `CREATE TYPE order_status AS ENUM ('new', 'shipped');`,
			"order_status_v2.sql": `CREATE TYPE order_status AS ENUM ('new', 'paid', 'shipped');`,
			"orders.sql":          ordersTableFile,
		})
		require.EqualError(t, err, `plan PostgreSQL namespace "public": enum type "order_status" is declared by both schema files "order_status.sql" and "order_status_v2.sql". Declare each type in exactly one schema file`)
	})
}
