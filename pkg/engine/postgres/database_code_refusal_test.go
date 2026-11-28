package postgres

import (
	"errors"
	"fmt"
	"testing"

	"github.com/jackc/pgx/v5/pgconn"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/block/schemabot/pkg/engine"
)

// Code on the target that raises during planning is refused in SchemaBot's
// words, even when a planner already declined the statement with text that
// quotes the raised message. Any other failure passes through unchanged.
func TestDatabaseCodeRefusal(t *testing.T) {
	raised := &pgconn.PgError{Code: "P0001", Message: "CREATE INDEX must use CONCURRENTLY on db-7.internal"}
	const want = `code on database "app" raised SQLSTATE P0001 while SchemaBot planned. ` +
		`Planning builds the declared tables in a schema named pgsprite_scratch_<suffix>, in a transaction it always rolls back, ` +
		`so an event trigger on DDL fires there too, on its CREATE SCHEMA and on the DDL inside it. ` +
		`Have the trigger allow both for a schema whose name starts with pgsprite_scratch_ ` +
		`(at ddl_command_start, check current_schema(); at ddl_command_end, check each command's schema_name, ` +
		`and the object_identity of a CREATE SCHEMA), then plan again`

	for name, err := range map[string]error{
		"raised":                    fmt.Errorf("execute desired statement on scratch schema: %w", raised),
		"already declined, quoting": &engine.PlanRefusedError{Err: fmt.Errorf("the server rejected a statement: %w", raised)},
	} {
		got := databaseCodeRefusal("app", err)
		refusal, ok := errors.AsType[*engine.PlanRefusedError](got)
		require.True(t, ok, name)
		text, _ := refusal.AuthoredText()
		assert.Equal(t, want, text, name)
		assert.ErrorIs(t, got, raised, "%s: the cause stays for the server log", name)
	}

	_, ok := errors.AsType[*engine.PlanRefusedError](databaseCodeRefusal("app", &pgconn.PgError{Code: "P0004"}))
	assert.True(t, ok, "a failed ASSERT is database code too")

	privilege := fmt.Errorf("create scratch schema: %w", &pgconn.PgError{Code: "42501"})
	assert.Same(t, privilege, databaseCodeRefusal("app", privilege), "a missing privilege is not database code")
	other := errors.New("connection reset")
	assert.Same(t, other, databaseCodeRefusal("app", other))
	assert.NoError(t, databaseCodeRefusal("app", nil))
}
