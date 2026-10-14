package ddl

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// A diff set records each desired table once. Distinct tables are accepted,
// a second file declaring an already declared table is refused with both file
// names, a file declaring the same table twice is refused naming that file,
// and table names compare the way the target and the differ compare them: a
// pair differing only in case is two tables when lower_case_table_names is 0
// and one table, refused with both spellings and files named, when it is 1 or
// 2.
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

	t.Run("names differing only in case are distinct tables on a case-sensitive target", func(t *testing.T) {
		var declared TableDeclarations
		require.NoError(t, declared.Declare("app/orders.sql", "orders"))
		assert.NoError(t, declared.Declare("app/Orders.sql", "Orders"))
	})

	for _, lctn := range []int{1, 2} {
		t.Run(fmt.Sprintf("names differing only in case are one table when lower_case_table_names=%d", lctn), func(t *testing.T) {
			declared := TableDeclarations{LowerCaseTableNames: lctn}
			require.NoError(t, declared.Declare("app/orders.sql", "orders"))
			err := declared.Declare("app/Orders.sql", "Orders")
			require.EqualError(t, err, fmt.Sprintf(`tables "orders" (schema file "app/orders.sql") and "Orders" (schema file "app/Orders.sql") are the same table on this target, which compares table names case-insensitively (lower_case_table_names=%d). Declare each table exactly once`, lctn))
			require.NoError(t, declared.Declare("app/users.sql", "users"), "other tables are unaffected")
		})
	}
}
