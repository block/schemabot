package postgres

import (
	"github.com/block/schemabot/pkg/engine"
)

// scratchSchemaPrefix starts the name of every schema pg-sprite builds the
// declared tables in while it plans. pg-sprite appends a random suffix. It
// copies pg-sprite's literal; schemadiff.ScratchSchemaPrefix retires the copy
// once the pinned pg-sprite release exports it.
const scratchSchemaPrefix = "pgsprite_scratch_"

// databaseCodeRefusal reports a plan that code installed on the target, such
// as an event trigger or a function it calls, refused by raising an error.
// Planning runs DDL only in a scratch schema inside a transaction it always
// rolls back, so a DDL policy enforced by an event trigger fires there too
// and fails every plan, whatever the schema files say.
//
// Planning again gets the same answer until the trigger stops firing on the
// scratch schema, so it is a refusal rather than a server fault, and its text
// names that fix. The text is SchemaBot's own: the raised message is written
// by the target's owners and can name anything, so it stays in the cause,
// which only the server log records.
//
// It takes precedence over a refusal already in the chain, such as a planner
// decline that quotes the server's message, because that message would carry
// the raised text onto the pull request. Any other error is returned as it
// came.
func databaseCodeRefusal(database string, err error) error {
	if err == nil {
		return err
	}
	code, ok := raisedByDatabaseCode(err)
	if !ok {
		return err
	}
	return &engine.PlanRefusedError{Err: engine.OperatorErrorf(err,
		"code on database %q raised SQLSTATE %s while SchemaBot planned. Planning builds the declared tables in a schema named %s<suffix>, in a transaction it always rolls back, so an event trigger on DDL fires there too, on its CREATE SCHEMA and on the DDL inside it. Have the trigger allow both for a schema whose name starts with %s (at ddl_command_start, check current_schema(), and let CREATE SCHEMA through, since its new name is not visible until ddl_command_end; at ddl_command_end, check each command's schema_name, and the object_identity of a CREATE SCHEMA), then plan again",
		database, code, scratchSchemaPrefix, scratchSchemaPrefix)}
}
