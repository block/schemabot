package api

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

// A storage table is reported only when it is present in the target and no
// identity column was found on it; absent tables are never reported, and
// the order follows the embedded table list rather than discovery order.
func TestPresentTablesWithoutIdentity(t *testing.T) {
	t.Parallel()

	tables := []string{"applies", "plans", "settings", "tasks"}
	missing := []string{"tasks"}
	columns := []postgresIdentityColumn{
		{schema: "public", table: "settings", column: "id"},
	}

	assert.Equal(t, []string{"applies", "plans"}, presentTablesWithoutIdentity(tables, missing, columns))
	assert.Empty(t, presentTablesWithoutIdentity(tables, []string{"applies", "plans", "tasks"}, columns),
		"a target whose only present table carries an identity column passes")
	assert.Empty(t, presentTablesWithoutIdentity(tables, tables, nil),
		"nothing present, nothing to hold to the premise")
}
