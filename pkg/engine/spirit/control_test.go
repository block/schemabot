package spirit

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/block/schemabot/pkg/engine"
)

func TestCancelMarksRunningSchemaChangeCancelled(t *testing.T) {
	eng := New(Config{})
	cancelCalled := false
	eng.runningSchemaChange = &runningSchemaChange{
		database: "testdb",
		tables:   []string{"users"},
		state:    engine.StateRunning,
		cancelFunc: func() {
			cancelCalled = true
		},
	}

	_, err := eng.Cancel(t.Context(), &engine.ControlRequest{})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "cleanup missing connection details")
	assert.True(t, cancelCalled)
	assert.Equal(t, engine.StateCancelled, eng.runningSchemaChange.state)
}

// registerRunningSchemaChange installs a simulated running schema change on the
// engine the same way Apply initializes one. The caller drives the change's
// lifecycle through rm.wg.
func registerRunningSchemaChange(eng *Engine) *runningSchemaChange {
	rm := &runningSchemaChange{
		database:       "testdb",
		tableNamespace: map[string]string{},
		state:          engine.StateRunning,
		host:           "127.0.0.1:1",
		username:       "root",
	}
	eng.installRunningSchemaChange(rm)
	return rm
}

// Stateless control operations (cutover, deferred cutover sentinel lookup)
// must address the schema the DSN connects to: under per-deployment schema
// overrides the DSN carries the physical schema name while the request carries
// the logical (canonical) database name. The request database is only a
// fallback for DSNs without a schema.
func TestStatelessControlDatabase(t *testing.T) {
	t.Run("DSN database wins over request database", func(t *testing.T) {
		got, err := statelessControlDatabase("root@tcp(localhost:3306)/bikeshare_eu_qa", "bikeshare")
		require.NoError(t, err)
		assert.Equal(t, "bikeshare_eu_qa", got)
	})

	t.Run("request database is the fallback for a namespace-free DSN", func(t *testing.T) {
		got, err := statelessControlDatabase("root@tcp(localhost:3306)/", "bikeshare")
		require.NoError(t, err)
		assert.Equal(t, "bikeshare", got)
	})

	t.Run("empty when neither names a schema", func(t *testing.T) {
		got, err := statelessControlDatabase("root@tcp(localhost:3306)/", "")
		require.NoError(t, err)
		assert.Empty(t, got)
	})

	t.Run("invalid DSN is an error", func(t *testing.T) {
		_, err := statelessControlDatabase("not a dsn", "bikeshare")
		require.Error(t, err)
	})
}
