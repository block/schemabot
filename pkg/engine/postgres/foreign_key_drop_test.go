package postgres

import (
	"testing"

	pgplan "github.com/block/pg-sprite/pkg/plan"
	"github.com/block/pg-sprite/pkg/planner"
	"github.com/block/pg-sprite/pkg/router"
	"github.com/block/pg-sprite/pkg/schemadiff"
	"github.com/block/pg-sprite/pkg/verdict"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/block/schemabot/pkg/ddl"
	"github.com/block/schemabot/pkg/engine"
	"github.com/block/schemabot/pkg/schema"
)

const dropOrdersUserFK = "ALTER TABLE public.orders DROP CONSTRAINT orders_user_id_fkey"

func ordersUserFK() *schemadiff.DroppedObject {
	return &schemadiff.DroppedObject{
		Kind:       schemadiff.DroppedConstraint,
		Name:       "orders_user_id_fkey",
		Definition: "FOREIGN KEY (user_id) REFERENCES users(id)",
	}
}

// executedStatement is a planned statement the planner runs natively as
// written.
func executedStatement(kind schemadiff.ChangeKind, sql string) pgplan.Statement {
	return pgplan.Statement{
		SQL:         sql,
		Kind:        kind,
		Route:       planner.RouteNative,
		Backend:     router.BackendNative,
		Disposition: router.DispositionExecute,
		ExecSQL:     []string{sql},
	}
}

// refusedForeignKeyDropStatement is the planner's refused drop of the orders
// table's user foreign key.
func refusedForeignKeyDropStatement() pgplan.Statement {
	return pgplan.Statement{
		SQL:         dropOrdersUserFK,
		Kind:        schemadiff.ChangeDropForeignKey,
		Disposition: router.DispositionRefuse,
		Reason:      verdict.ReasonDeclarativeForeignKey,
		Destructive: true,
		Drops:       ordersUserFK(),
	}
}

func ordersReport(statements ...pgplan.Statement) pgplan.Report {
	exists := true
	report := pgplan.NewReport(pgplan.SourceDiff)
	report.Schema = "public"
	report.Table = "orders"
	report.TableExists = &exists
	report.Statements = statements
	return report
}

// A schema file leaves out the orders table's live foreign key to users. The
// planner runs that drop, so SchemaBot plans it as an executable step that
// needs --allow-unsafe, says which rule stops being checked, and carries the
// planner's record of the key, which the apply admits and checks under the
// table's lock before it drops anything.
func TestTableChangesRunsAForeignKeyDropAsAReviewedUnsafeStep(t *testing.T) {
	parser, err := ddl.ParserForDialect(schema.DialectPostgres)
	require.NoError(t, err)
	drop := executedStatement(schemadiff.ChangeDropForeignKey, dropOrdersUserFK)
	drop.Destructive = true
	drop.Drops = ordersUserFK()

	changes, _, unrecognized, err := tableChanges(ordersReport(drop), nil, parser)
	require.NoError(t, err)
	assert.Empty(t, unrecognized)
	require.Len(t, changes, 1)
	change := changes[0]
	assert.Equal(t, dropOrdersUserFK, change.DDL)
	assert.Empty(t, change.ExecutionMode, "the drop runs")
	assert.Empty(t, change.ModeReason)
	assert.True(t, change.IsUnsafe)
	assert.Equal(t, `statement drops foreign key "orders_user_id_fkey" from table "orders": rows written after the drop are not checked against FOREIGN KEY (user_id) REFERENCES users(id), so they can point at rows that do not exist`, change.UnsafeReason)

	reviewed, err := reviewedDropForApply("app", change, change.DDL)
	require.NoError(t, err)
	require.NotNil(t, reviewed, "the apply checks the drop against the plan's record of the key")
	assert.Equal(t, *ordersUserFK(), *reviewed)
}

// A foreign-key drop the planner still refuses is shown for review and
// blocked, and the reason names the one cause that applies to this plan and
// the change that gets past it.
func TestTableChangesNamesWhyAForeignKeyDropIsRefused(t *testing.T) {
	parser, err := ddl.ParserForDialect(schema.DialectPostgres)
	require.NoError(t, err)
	const removeOutside = "the plan stays blocked until the key is removed outside SchemaBot through a separately reviewed process"
	for _, tt := range []struct {
		name           string
		report         pgplan.Report
		crossNamespace []string
		want           string
	}{
		{
			name:           "key into another namespace",
			report:         ordersReport(refusedForeignKeyDropStatement()),
			crossNamespace: []string{"orders_region_id_fkey", "orders_user_id_fkey"},
			want:           `statement for table "orders" drops foreign key "orders_user_id_fkey", which references another namespace, so a schema file cannot declare it and SchemaBot does not drop it; ` + removeOutside,
		},
		{
			// A partitioned table cannot replace a changed key in one
			// statement, so the planner drops the key and adds it again.
			name: "key changed under the same name on a partitioned table",
			report: ordersReport(
				refusedForeignKeyDropStatement(),
				executedStatement(schemadiff.ChangeAddForeignKey, "ALTER TABLE public.orders ADD CONSTRAINT orders_user_id_fkey FOREIGN KEY (user_id) REFERENCES public.users (id) ON DELETE CASCADE"),
			),
			want: `statement for table "orders" drops foreign key "orders_user_id_fkey" before the plan adds its changed definition under the same name, so writes in between would be checked against no foreign key; SchemaBot does not apply it, so declare the changed key under a new name and re-plan, and the new key is added and validated before the old one is dropped`,
		},
		{
			name: "plan that also drops a column",
			report: ordersReport(
				executedStatement(schemadiff.ChangeDropColumn, "ALTER TABLE public.orders DROP COLUMN legacy"),
				refusedForeignKeyDropStatement(),
				executedStatement(schemadiff.ChangeAddForeignKey, "ALTER TABLE public.orders ADD CONSTRAINT orders_customer_id_fkey FOREIGN KEY (user_id) REFERENCES public.users (id) NOT VALID"),
			),
			want: `statement for table "orders" drops foreign key "orders_user_id_fkey" in a plan that also drops or changes the type of a column on the table, and such a plan runs its drops first, so writes would be checked against no foreign key until the plan's new one is in place; SchemaBot does not apply it, so make the column change and the foreign key change in separate pull requests`,
		},
		{
			// The table's other key into another namespace is not this one,
			// so the reason does not say this key references one.
			name:           "key a schema file cannot declare",
			report:         ordersReport(refusedForeignKeyDropStatement()),
			crossNamespace: []string{"orders_region_id_fkey"},
			want:           `statement for table "orders" drops foreign key "orders_user_id_fkey", which a schema file cannot declare, so SchemaBot does not drop it; ` + removeOutside,
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			changes, _, unrecognized, err := tableChanges(tt.report, tt.crossNamespace, parser)
			require.NoError(t, err)
			assert.Empty(t, unrecognized)
			var drop *engine.TableChange
			for i := range changes {
				if changes[i].DDL == dropOrdersUserFK {
					drop = &changes[i]
				}
			}
			require.NotNil(t, drop, "the refused drop is shown for review")
			assert.Equal(t, engine.ExecutionModeBlocked, drop.ExecutionMode)
			assert.Equal(t, tt.want, drop.ModeReason)
		})
	}
}
