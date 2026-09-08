package spirit

import (
	"context"
	"errors"
	"fmt"
	"net"
	"testing"

	"github.com/block/mysql"
	"github.com/stretchr/testify/assert"

	"github.com/block/schemabot/pkg/engine"
	"github.com/block/schemabot/pkg/mysqlerr"
)

// What a schema change reports on a pull request when it fails is assembled
// from SchemaBot's own words. The errors reaching the failure path carry text
// from the target and its driver — the row that could not be copied, the host
// that would not answer — and none of it may be published, so an error only
// speaks for itself when SchemaBot marked it as its own.
func TestFailureReason(t *testing.T) {
	t.Run("a message SchemaBot wrote is shown as written", func(t *testing.T) {
		err := engine.OperatorErrorf(errors.New("Error 1205: Lock wait timeout exceeded"),
			"Table %q is busy: the change could not acquire the metadata lock within %ds.", "orders", 30)

		got := failureReason(err)
		assert.Equal(t, `Table "orders" is busy: the change could not acquire the metadata lock within 30s.`, got)
		assert.NotContains(t, got, "Lock wait timeout", "the cause is for the log, not the pull request")
	})

	// The busiest failure: the DDL is legal and the data already in the table
	// cannot satisfy it. The driver says so by quoting the offending row.
	t.Run("a driver error reports its code, not its words", func(t *testing.T) {
		err := fmt.Errorf("schema change failed: %w", &mysql.MySQLError{
			Number:  1062,
			Message: "Duplicate entry 'cst_xyz789' for key 'unq_token'",
		})

		got := failureReason(err)
		assert.Equal(t, mysqlerr.Reason(err), got)
		assert.Contains(t, got, "(error 1062)")
		assert.NotContains(t, got, "cst_xyz789")
	})

	// A dial failure names the target host, which is this deployment's own
	// topology and not something a pull request may show.
	t.Run("a target that was never reached does not name itself", func(t *testing.T) {
		err := fmt.Errorf("connect for direct execution: %w", &net.OpError{
			Op: "dial", Net: "tcp",
			Addr: &net.TCPAddr{IP: net.ParseIP("10.4.2.19"), Port: 3306},
			Err:  errors.New("connect: connection refused"),
		})

		got := failureReason(err)
		assert.Equal(t, mysqlerr.Unreachable, got)
		assert.NotContains(t, got, "10.4.2.19")
	})

	// The engine bounds its own steps — a chunk, a checksum, a cutover unlock —
	// on contexts it derives itself, so one of those budgets running out reaches
	// here while the change's own context is still live. That is a change that
	// ran, so it must not be reported as a target that could not be reached.
	t.Run("an engine deadline is not reported as a connection problem", func(t *testing.T) {
		err := fmt.Errorf("run Spirit: %w",
			fmt.Errorf("fix chunk: %w", context.DeadlineExceeded))

		got := failureReason(err)
		assert.Equal(t, mysqlerr.Timeout, got)
		assert.NotEqual(t, mysqlerr.Unreachable, got)
	})

	// The gate is closed by default: a failure path added later renders as the
	// generic reason until someone decides its message is safe to publish.
	t.Run("an unmarked error falls back to the generic reason", func(t *testing.T) {
		err := fmt.Errorf("failed to create Spirit runner: %w",
			errors.New("unsafe warning: Field 'name' doesn't have a default value"))

		got := failureReason(err)
		assert.Equal(t, mysqlerr.Generic, got)
		assert.NotContains(t, got, "'name'")
	})

	t.Run("a marked message wins over a code beneath it", func(t *testing.T) {
		err := engine.OperatorErrorf(&mysql.MySQLError{Number: 1062, Message: "Duplicate entry 'x'"},
			"Statement on table %q cannot run directly: dropping a primary key is not supported.", "orders")

		got := failureReason(err)
		assert.Contains(t, got, "dropping a primary key is not supported")
		assert.NotContains(t, got, "1062")
	})
}

// The state a progress poll reports has to carry the rendered reason, not the
// error the engine caught.
func TestSetSchemaChangeFailedRendersTheReason(t *testing.T) {
	e := engineWithTrackedChange(engine.StateRunning, "")

	e.setSchemaChangeFailed(fmt.Errorf("run ALTER on table %q: %w", "orders",
		&mysql.MySQLError{Number: 1048, Message: "Column 'lending_product' cannot be null"}))

	assert.Equal(t, engine.StateFailed, e.runningSchemaChange.state)
	assert.Contains(t, e.runningSchemaChange.errorMessage, "(error 1048)")
	assert.NotContains(t, e.runningSchemaChange.errorMessage, "lending_product")
}
