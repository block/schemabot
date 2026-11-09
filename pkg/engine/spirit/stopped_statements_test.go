package spirit

import (
	"io"
	"log/slog"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/block/schemabot/pkg/engine"
)

// When the engine cannot reach the target to check whether a stopped run's
// statement is still running, the schema change does not start. The failure is
// not reported as a held target, and the statement stays recorded, so a later
// start checks again.
func TestStartFailsWhenAStoppedRunsStatementCannotBeChecked(t *testing.T) {
	eng := New(Config{Logger: slog.New(slog.NewTextHandler(io.Discard, nil))})
	// Nothing listens on port 1, so the check cannot connect.
	recorded := unconfirmedStatement{host: "127.0.0.1:1", username: "schemabot", database: "orders_db", connID: 4242}
	eng.unconfirmedStatements = []unconfirmedStatement{recorded}

	result, err := eng.Apply(t.Context(), &engine.ApplyRequest{
		Database: "orders_db",
		Changes: []engine.SchemaChange{{
			Namespace:    "orders_db",
			TableChanges: []engine.TableChange{{Table: "orders", DDL: "CREATE TABLE orders (id BIGINT UNSIGNED NOT NULL PRIMARY KEY)"}},
		}},
		Credentials: &engine.Credentials{DSN: "schemabot@tcp(127.0.0.1:1)/orders_db"},
	})
	require.NoError(t, err, "Apply()")
	require.True(t, result.Accepted)

	var progress *engine.ProgressResult
	require.Eventually(t, func() bool {
		progress = pollProgress(t, eng)
		return progress.State.IsTerminal()
	}, 10*time.Second, 10*time.Millisecond, "the start did not settle")
	assert.Equal(t, engine.StateFailed, progress.State, "the schema change does not start")
	assert.False(t, progress.TargetHeld, "a failed check is not a held target")
	assert.Equal(t, []unconfirmedStatement{recorded}, eng.unconfirmedStatements, "the statement stays recorded for the next start")
}
