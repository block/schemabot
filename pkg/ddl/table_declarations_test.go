package ddl

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// A diff set records each desired table once. Distinct tables are accepted,
// a second file declaring an already declared table is refused with both file
// names, a file declaring the same table twice is refused naming that file,
// and table names compare exactly, the same way the differ keys them.
func TestTableDeclarations_Declare(t *testing.T) {
	t.Run("distinct tables are accepted", func(t *testing.T) {
		var declared TableDeclarations
		require.NoError(t, declared.Declare("app/orders.sql", "orders"))
		require.NoError(t, declared.Declare("app/users.sql", "users"))
		require.NoError(t, declared.Declare("app/users.sql", "user_profiles"))
	})

	t.Run("a table declared by a second file is refused", func(t *testing.T) {
		var declared TableDeclarations
		require.NoError(t, declared.Declare("app/orders.sql", "orders"))
		err := declared.Declare("app/orders_extras.sql", "orders")
		require.EqualError(t, err, `table "orders" is declared by both schema files "app/orders.sql" and "app/orders_extras.sql". Declare each table in exactly one schema file`)
	})

	t.Run("a table declared twice in one file is refused", func(t *testing.T) {
		var declared TableDeclarations
		require.NoError(t, declared.Declare("app/orders.sql", "orders"))
		err := declared.Declare("app/orders.sql", "orders")
		require.EqualError(t, err, `table "orders" is declared more than once in schema file "app/orders.sql". Declare each table exactly once`)
	})

	t.Run("names differing only in case are distinct tables", func(t *testing.T) {
		var declared TableDeclarations
		require.NoError(t, declared.Declare("app/orders.sql", "orders"))
		assert.NoError(t, declared.Declare("app/Orders.sql", "Orders"))
	})
}
