package spirit

import (
	"errors"
	"fmt"
	"testing"

	"github.com/block/spirit/pkg/checksum"
	"github.com/block/spirit/pkg/statement"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/block/schemabot/pkg/engine"
	"github.com/block/schemabot/pkg/mysqlerr"
)

// checksumKeptFindingDifferences is the error Spirit's runner returns when
// every checksum attempt found rows that differ between a table and its copy,
// worded the way the runner words it for a batch that adds a UNIQUE key.
func checksumKeptFindingDifferences() error {
	return fmt.Errorf("checksum failed after several attempts. This is likely related to your statement adding a UNIQUE index on non-unique data: %w",
		fmt.Errorf("%w (3 consecutive of 3 attempts)", checksum.ErrDifferencesExhausted))
}

func parseStatements(t *testing.T, sql string) []*statement.AbstractStatement {
	t.Helper()
	stmts, err := statement.New(sql)
	require.NoError(t, err)
	return stmts
}

// A schema change that adds a UNIQUE key over rows that share a value fails
// its checksum: the copy drops the duplicates, and every repair drops them
// again. The pull request names that cause in SchemaBot's own words, and the
// failure stays permanent, since applying the same change to the same rows
// fails the same way.
func TestUniqueKeyOverDuplicatesNamesTheCause(t *testing.T) {
	stmts := parseStatements(t, "ALTER TABLE `orders` ADD UNIQUE KEY `uk_token` (`token`)")

	err := explainRunnerError(checksumKeptFindingDifferences(), stmts)

	assert.Equal(t, UniqueKeyDuplicatesReason, failureReason(err))
	assert.ErrorIs(t, err, checksum.ErrDifferencesExhausted)
	assert.False(t, engine.IsRetryable(classifyRunnerError(err)), "the same rows fail the same way on a retry")
}

// The UNIQUE key is found in any statement of the batch, since Spirit runs a
// batch's statements together and the checksum error does not name its table.
func TestUniqueKeyInAnyStatementOfTheBatchNamesTheCause(t *testing.T) {
	stmts := parseStatements(t, "ALTER TABLE `customers` ADD COLUMN `nickname` varchar(64); ALTER TABLE `orders` ADD UNIQUE KEY `uk_token` (`token`)")

	err := explainRunnerError(checksumKeptFindingDifferences(), stmts)

	assert.Equal(t, UniqueKeyDuplicatesReason, failureReason(err))
}

// Only the pairing is named. A checksum that kept finding differences on a
// batch with no new UNIQUE key has some other cause, and a checksum whose
// attempts errored proved nothing about the rows, so both keep the generic
// reason rather than a confident wrong one.
func TestOtherChecksumFailuresKeepTheGenericReason(t *testing.T) {
	t.Run("differences without a new UNIQUE key", func(t *testing.T) {
		stmts := parseStatements(t, "ALTER TABLE `orders` MODIFY `created_at` datetime(0)")

		err := explainRunnerError(fmt.Errorf("checksum failed: %w", checksum.ErrDifferencesExhausted), stmts)

		assert.Equal(t, mysqlerr.Generic, failureReason(err))
	})

	t.Run("attempts that errored under a new UNIQUE key", func(t *testing.T) {
		stmts := parseStatements(t, "ALTER TABLE `orders` ADD UNIQUE KEY `uk_token` (`token`)")
		cause := fmt.Errorf("%w: %w", checksum.ErrAttemptsExhausted, errors.New("connection killed"))

		err := explainRunnerError(fmt.Errorf("checksum failed: %w", cause), stmts)

		assert.Equal(t, mysqlerr.Generic, failureReason(err))
	})
}
