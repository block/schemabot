package postgres

import (
	"testing"

	pgplan "github.com/block/pg-sprite/pkg/plan"
	"github.com/block/pg-sprite/pkg/schemadiff"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/block/schemabot/pkg/ddl"
	"github.com/block/schemabot/pkg/schema"
)

// An enum type's plan renders only label additions to that type in its own
// schema. A statement of any other kind, or one that targets a different
// type or the same type name in another schema, fails the plan instead of
// reaching review under a name SchemaBot never checked.
func TestEnumTypeChangesFailsClosedOnUnexpectedStatements(t *testing.T) {
	parser, err := ddl.ParserForDialect(schema.DialectPostgres)
	require.NoError(t, err)

	t.Run("a statement that is not a label addition", func(t *testing.T) {
		report := pgplan.Report{Type: "order_status", Statements: []pgplan.Statement{
			{Kind: schemadiff.ChangeAddColumn, SQL: "ALTER TABLE public.orders ADD COLUMN note text"},
		}}
		_, err := enumTypeChanges("public", report, parser)
		require.ErrorContains(t, err, `plan for enum type "order_status" holds a statement of kind add-column`)
	})

	t.Run("a label addition to another type", func(t *testing.T) {
		report := pgplan.Report{Type: "order_status", Statements: []pgplan.Statement{
			{Kind: schemadiff.ChangeAddEnumValue, SQL: "ALTER TYPE public.payment_state ADD VALUE 'refunded'"},
		}}
		_, err := enumTypeChanges("public", report, parser)
		require.EqualError(t, err, `planned statement for enum type "order_status" classified as ALTER TYPE on "payment_state"`)
	})

	t.Run("a label addition to the same type name in another schema", func(t *testing.T) {
		report := pgplan.Report{Type: "order_status", Statements: []pgplan.Statement{
			{Kind: schemadiff.ChangeAddEnumValue, SQL: "ALTER TYPE billing.order_status ADD VALUE 'refunded'"},
		}}
		_, err := enumTypeChanges("public", report, parser)
		require.EqualError(t, err, `planned statement for enum type "order_status" in schema "public" adds a label to "order_status" in schema "billing"`)
	})

	t.Run("a converged type renders nothing", func(t *testing.T) {
		changes, err := enumTypeChanges("public", pgplan.Report{Type: "order_status"}, parser)
		require.NoError(t, err)
		assert.Empty(t, changes)
	})
}
