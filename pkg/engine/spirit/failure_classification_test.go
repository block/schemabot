package spirit

import (
	"errors"
	"fmt"
	"testing"

	"github.com/go-sql-driver/mysql"
	"github.com/stretchr/testify/assert"

	"github.com/block/schemabot/pkg/engine"
)

// The rejections an apply cannot retry its way out of are reported as permanent
// failures, and everything else keeps the automatic retries. The transient cases
// are the ones the retry budget exists for: they are the boundary the set must
// not cross.
func TestExecutionFailureClassification(t *testing.T) {
	tests := []struct {
		name      string
		err       error
		retryable bool
	}{
		{
			name:      "duplicate value for a new unique index",
			err:       &mysql.MySQLError{Number: errDuplicateEntry, Message: "Duplicate entry 'a@example.com' for key 'idx_email'"},
			retryable: false,
		},
		{
			name:      "existing rows violate a new NOT NULL column",
			err:       &mysql.MySQLError{Number: errNullNotAllowed, Message: "Column 'name' cannot be null"},
			retryable: false,
		},
		{
			name:      "existing data does not fit the narrowed column",
			err:       &mysql.MySQLError{Number: errDataTooLong, Message: "Data too long for column 'name'"},
			retryable: false,
		},
		{
			name:      "dropping a column the table does not have",
			err:       &mysql.MySQLError{Number: errCantDropFieldOrKey, Message: "Can't DROP 'missing'"},
			retryable: false,
		},
		{
			name:      "the target refuses the operation outright",
			err:       &mysql.MySQLError{Number: errAlterOperationNotSupportedReason, Message: "ALGORITHM=INPLACE is not supported"},
			retryable: false,
		},
		{
			name:      "wrapped rejections are classified through the chain",
			err:       fmt.Errorf("schema change failed: %w", &mysql.MySQLError{Number: errDuplicateEntry}),
			retryable: false,
		},
		{
			name:      "lock wait timeout clears once the holder commits",
			err:       &mysql.MySQLError{Number: 1205, Message: "Lock wait timeout exceeded"},
			retryable: true,
		},
		{
			name:      "deadlock victims succeed on a later attempt",
			err:       &mysql.MySQLError{Number: 1213, Message: "Deadlock found when trying to get lock"},
			retryable: true,
		},
		{
			name:      "a read-only target accepts writes again after failover",
			err:       &mysql.MySQLError{Number: 1290, Message: "The MySQL server is running with the --read-only option"},
			retryable: true,
		},
		{
			name:      "a lost connection is reconnected on the next attempt",
			err:       &mysql.MySQLError{Number: 2013, Message: "Lost connection to MySQL server during query"},
			retryable: true,
		},
		{
			name:      "failures that never reached the target stay retryable",
			err:       errors.New("dial tcp: connect: connection refused"),
			retryable: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			classified := classifyExecutionFailure(tt.err)
			assert.Equal(t, tt.retryable, engine.IsRetryable(classified))
			assert.ErrorIs(t, classified, tt.err, "classification must preserve the original error for the caller to report")
		})
	}
}

// A classified failure keeps the target's own message, which is the only place
// an operator reads what the target objected to.
func TestPermanentClassificationPreservesTheTargetMessage(t *testing.T) {
	rejection := &mysql.MySQLError{Number: errDuplicateEntry, Message: "Duplicate entry 'a@example.com' for key 'idx_email'"}

	classified := classifyExecutionFailure(fmt.Errorf("schema change failed: %w", rejection))

	assert.False(t, engine.IsRetryable(classified))
	assert.Contains(t, classified.Error(), "Duplicate entry 'a@example.com' for key 'idx_email'")
}

func TestClassifyExecutionFailureIgnoresNil(t *testing.T) {
	assert.NoError(t, classifyExecutionFailure(nil))
}
