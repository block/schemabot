//go:build integration

package spirit

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/block/schemabot/pkg/engine"
	"github.com/block/schemabot/pkg/mysqlerr"
)

// A change that adds NOT NULL to a column holding NULLs is legal DDL, so it
// fails during the row copy rather than at planning time. The copy runs as
// INSERT IGNORE, which turns the violation into a warning the copier treats as
// fatal — a path that reports its failure separately from a statement error.
//
// The operator has to be told which failure this was. Reporting the reason
// depends on the error code surviving the copier, so this drives a real 1048
// through the engine and asserts the code selected its sentence: a generic
// reason here means the code stopped arriving and the reason went with it.
func TestEngine_Progress_CopyFailureReportsTheErrorCode(t *testing.T) {
	dsn, db := setupTestMySQL(t)
	cleanupTables(t, db)

	_, err := db.ExecContext(t.Context(), "CREATE TABLE `copy_not_null` ("+
		"id INT PRIMARY KEY AUTO_INCREMENT, lending_product VARCHAR(50) NULL)")
	require.NoError(t, err, "create table")

	_, err = db.ExecContext(t.Context(),
		"INSERT INTO `copy_not_null` (`lending_product`) VALUES (NULL)")
	require.NoError(t, err, "seed the row the change cannot carry over")

	eng := newDrainOutcomeTestEngine()
	applyTableChange(t, eng, dsn, "copy_not_null",
		"ALTER TABLE `copy_not_null` MODIFY COLUMN `lending_product` varchar(50) NOT NULL")
	defer eng.Drain()

	result := waitForTerminalOutcome(t, eng, engine.StateFailed)

	assert.Equal(t, mysqlerr.ReasonFromText("(errno 1048)"), result.ErrorMessage)
	assert.NotEqual(t, mysqlerr.Generic, result.ErrorMessage,
		"the copy failure did not carry its error code")
	assert.NotContains(t, result.ErrorMessage, "lending_product")
}
