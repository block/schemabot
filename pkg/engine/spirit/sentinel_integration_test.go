//go:build integration

package spirit

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/block/schemabot/pkg/engine"
)

// A cutover sentinel table can already be on the target, left by an operator
// or by an earlier deferred apply. Only a deferred apply waits on it: a normal
// apply cuts over and completes without touching it, so a stray sentinel never
// holds an apply nobody asked to defer. A deferred apply on the same target
// waits for cutover until the sentinel is dropped, then completes.
func TestEngine_PreexistingSentinelHoldsOnlyDeferredCutover(t *testing.T) {
	tests := []struct {
		name         string
		deferCutover bool
	}{
		{name: "a normal apply completes and leaves the sentinel", deferCutover: false},
		{name: "a deferred apply waits on the sentinel until it is dropped", deferCutover: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			dsn, db := setupTestMySQL(t)
			cleanupTables(t, db)

			_, err := db.ExecContext(t.Context(), "CREATE TABLE `sentinel_target` ("+
				"id INT PRIMARY KEY AUTO_INCREMENT, name VARCHAR(50) NOT NULL)")
			require.NoError(t, err, "create table")
			_, err = db.ExecContext(t.Context(), "CREATE TABLE `_spirit_sentinel` (id int NOT NULL PRIMARY KEY)")
			require.NoError(t, err, "create sentinel")
			// t.Context() is canceled by the time cleanup runs.
			t.Cleanup(func() {
				_, err := db.ExecContext(context.WithoutCancel(t.Context()), "DROP TABLE IF EXISTS `_spirit_sentinel`")
				assert.NoError(t, err, "drop sentinel")
			})

			options := map[string]string{}
			if tt.deferCutover {
				options["defer_cutover"] = "true"
			}
			eng := newDrainOutcomeTestEngine()
			result, err := eng.Apply(t.Context(), &engine.ApplyRequest{
				Database: testDatabase,
				Changes: []engine.SchemaChange{{
					Namespace: testDatabase,
					TableChanges: []engine.TableChange{{
						Table: "sentinel_target",
						// The index makes this a copy, which is the path that reaches
						// cutover; an instant ADD COLUMN alone never consults the sentinel.
						DDL: "ALTER TABLE `sentinel_target` ADD COLUMN `email` varchar(255) NULL, ADD INDEX `idx_email` (`email`)",
					}},
				}},
				Credentials: &engine.Credentials{DSN: dsn},
				Options:     options,
			})
			require.NoError(t, err, "Apply()")
			require.True(t, result.Accepted, "apply not accepted: %s", result.Message)

			if tt.deferCutover {
				waitForTerminalOutcome(t, eng, engine.StateWaitingForCutover)
				_, err = db.ExecContext(t.Context(), "DROP TABLE `_spirit_sentinel`")
				require.NoError(t, err, "drop sentinel to release the cutover")
			}
			waitForTerminalOutcome(t, eng, engine.StateCompleted)

			var columns int
			require.NoError(t, db.QueryRowContext(t.Context(),
				"SELECT COUNT(*) FROM information_schema.COLUMNS WHERE TABLE_SCHEMA = ? AND TABLE_NAME = 'sentinel_target' AND COLUMN_NAME = 'email'",
				testDatabase).Scan(&columns), "count columns")
			assert.Equal(t, 1, columns, "the apply cut over and added the column")

			if !tt.deferCutover {
				var sentinels int
				require.NoError(t, db.QueryRowContext(t.Context(),
					"SELECT COUNT(*) FROM information_schema.TABLES WHERE TABLE_SCHEMA = ? AND TABLE_NAME = '_spirit_sentinel'",
					testDatabase).Scan(&sentinels), "count sentinels")
				assert.Equal(t, 1, sentinels, "a normal apply leaves the sentinel in place")
			}
		})
	}
}
