package spirit

import (
	"database/sql/driver"
	"errors"
	"fmt"
	"testing"

	"github.com/block/mysql"
	"github.com/block/spirit/pkg/checksum"
	"github.com/block/spirit/pkg/dbconn"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/block/schemabot/pkg/engine"
)

func TestClassifyRunnerError(t *testing.T) {
	t.Run("reproducible checksum differences are permanent", func(t *testing.T) {
		runnerErr := fmt.Errorf("runner stopped: checksum failed after several attempts: %w", checksum.ErrDifferencesExhausted)
		wrappedErr := fmt.Errorf("execute runner: %w", runnerErr)

		classifiedErr := classifyRunnerError(wrappedErr)

		assert.False(t, engine.IsRetryable(classifiedErr))
		assert.Equal(t, wrappedErr.Error(), classifiedErr.Error())
		assert.ErrorIs(t, classifiedErr, checksum.ErrDifferencesExhausted)
		var permanentErr *engine.PermanentError
		assert.ErrorAs(t, classifiedErr, &permanentErr)
	})

	// The lockless checksum reports a proven divergence with its own sentinel,
	// and a lossy ALTER under it fails with that verdict on every attempt, so
	// it is permanent exactly as the snapshot checksum's verdict is.
	t.Run("lockless permanent divergence is permanent", func(t *testing.T) {
		runnerErr := fmt.Errorf("checksum failed: %w",
			fmt.Errorf("%w: chunk `id` >= 1 AND `id` < 1001", checksum.ErrPermanentDivergence))

		classifiedErr := classifyRunnerError(runnerErr)

		assert.False(t, engine.IsRetryable(classifiedErr))
		assert.ErrorIs(t, classifiedErr, checksum.ErrPermanentDivergence)
	})

	// Running out of lockless passes proves nothing about the data, only that
	// ranges kept changing under the checksum, so a later attempt may verify
	// them and the failure stays retryable.
	t.Run("lockless unresolved verification remains retryable", func(t *testing.T) {
		runnerErr := fmt.Errorf("checksum failed: %w",
			fmt.Errorf("%w after 10 passes", checksum.ErrVerificationUnresolved))

		classifiedErr := classifyRunnerError(runnerErr)

		require.Same(t, runnerErr, classifiedErr)
		assert.True(t, engine.IsRetryable(classifiedErr))
	})

	t.Run("checksum attempt errors remain retryable", func(t *testing.T) {
		runnerErr := fmt.Errorf("checksum failed after several attempts: %w", checksum.ErrAttemptsExhausted)

		classifiedErr := classifyRunnerError(runnerErr)

		require.Same(t, runnerErr, classifiedErr)
		assert.True(t, engine.IsRetryable(classifiedErr))
	})

	t.Run("other runner errors remain retryable", func(t *testing.T) {
		runnerErr := errors.New("connection reset")

		classifiedErr := classifyRunnerError(runnerErr)

		require.Same(t, runnerErr, classifiedErr)
		assert.True(t, engine.IsRetryable(classifiedErr))
	})
}

// copyWarningError builds the error a Spirit copy fails with when the target
// raises a warning on a chunk it inserted, wrapped the way the runner returns
// it.
func copyWarningError(code uint16, message string) error {
	return fmt.Errorf("failed to execute chunklet insert: %w",
		&dbconn.UnsafeWarningError{Warning: &mysql.MySQLError{Number: code, Message: message}})
}

// A row the copy cannot write into the new table definition fails every
// attempt the same way, because every attempt copies the same rows into the
// same definition. Those failures are permanent; what a retry can clear — a
// lock that was held, a connection that dropped — stays retryable.
func TestClassifyRunnerErrorRowData(t *testing.T) {
	permanent := []struct {
		name string
		err  error
	}{
		{"NULL into a NOT NULL column", copyWarningError(1048, "Column 'c' cannot be null")},
		{"value truncated by the new type", copyWarningError(1265, "Data truncated for column 'status' at row 1")},
		{"value the new type cannot represent", copyWarningError(1366, "Incorrect integer value: 'abc' for column 'n' at row 1")},
		{"value longer than the new column", copyWarningError(1406, "Data too long for column 'name' at row 1")},
		{"row data error raised as an error rather than a warning", fmt.Errorf("failed to execute upsert: %w",
			&mysql.MySQLError{Number: 1048, Message: "Column 'c' cannot be null"})},
	}
	for _, tc := range permanent {
		t.Run(tc.name+" is permanent", func(t *testing.T) {
			classifiedErr := classifyRunnerError(tc.err)

			assert.False(t, engine.IsRetryable(classifiedErr))
			var permanentErr *engine.PermanentError
			assert.ErrorAs(t, classifiedErr, &permanentErr)
			assert.Equal(t, tc.err.Error(), classifiedErr.Error())
		})
	}

	retryable := []struct {
		name string
		err  error
	}{
		{"lock wait timeout", fmt.Errorf("failed to execute chunklet insert: %w",
			&mysql.MySQLError{Number: 1205, Message: "Lock wait timeout exceeded; try restarting transaction"})},
		{"deadlock", fmt.Errorf("failed to execute upsert: %w",
			&mysql.MySQLError{Number: 1213, Message: "Deadlock found when trying to get lock; try restarting transaction"})},
		{"lost connection", fmt.Errorf("failed to execute chunklet insert: %w", mysql.ErrInvalidConn)},
		{"bad connection", fmt.Errorf("failed to execute chunklet insert: %w", driver.ErrBadConn)},
		{"server gone away", fmt.Errorf("failed to execute chunklet insert: %w",
			&mysql.MySQLError{Number: 2013, Message: "Lost connection to MySQL server during query"})},
	}
	for _, tc := range retryable {
		t.Run(tc.name+" stays retryable", func(t *testing.T) {
			classifiedErr := classifyRunnerError(tc.err)

			require.Same(t, tc.err, classifiedErr)
			assert.True(t, engine.IsRetryable(classifiedErr))
		})
	}
}

// A runner failure's retry classification reaches the drive through progress.
// A copy whose checksum keeps finding rows it would lose fails the same way on
// every attempt, so progress reports it failed and not retryable, and the drive
// settles it instead of spending its recovery attempts re-copying the table. A
// failure that could succeed on a later attempt stays retryable. The same holds
// for a row the copy cannot store under the new definition. Both answers
// hold before and after the engine drains the finished change.
func TestFailedProgressCarriesRunnerRetryClassification(t *testing.T) {
	cases := []struct {
		name      string
		err       error
		retryable bool
		reason    string // the operator-facing reason, when the case pins one
	}{
		{
			name: "ALTER whose checksum keeps finding differences is not retryable",
			err: fmt.Errorf("schema change failed: %w", classifyRunnerError(
				fmt.Errorf("checksum failed after several attempts: %w", checksum.ErrDifferencesExhausted))),
			retryable: false,
		},
		{
			name: "CREATE TABLE whose checksum keeps finding differences is not retryable",
			err: fmt.Errorf("CREATE TABLE failed: %w", fmt.Errorf("run Spirit: %w", classifyRunnerError(
				fmt.Errorf("checksum failed after several attempts: %w", checksum.ErrDifferencesExhausted)))),
			retryable: false,
		},
		{
			name: "ALTER whose checksum attempts errored stays retryable",
			err: fmt.Errorf("schema change failed: %w", classifyRunnerError(
				fmt.Errorf("checksum failed after several attempts: %w", checksum.ErrAttemptsExhausted))),
			retryable: true,
		},
		{
			name: "ALTER that cannot store a NULL row under NOT NULL is not retryable",
			err: fmt.Errorf("schema change failed: %w", classifyRunnerError(
				copyWarningError(1048, "Column 'c' cannot be null"))),
			retryable: false,
			reason:    "A row held NULL in a column that cannot be null",
		},
		{
			name: "ALTER that would truncate a stored value is not retryable",
			err: fmt.Errorf("schema change failed: %w", classifyRunnerError(
				copyWarningError(1265, "Data truncated for column 'nickname' at row 1"))),
			retryable: false,
			reason:    "An existing value would be truncated by the column's target type or length",
		},
		{
			name: "ALTER that timed out on a lock wait stays retryable",
			err: fmt.Errorf("schema change failed: %w", classifyRunnerError(fmt.Errorf("failed to execute chunklet insert: %w",
				&mysql.MySQLError{Number: 1205, Message: "Lock wait timeout exceeded; try restarting transaction"}))),
			retryable: true,
		},
		{
			name:      "ALTER that lost its connection stays retryable",
			err:       fmt.Errorf("schema change failed: %w", classifyRunnerError(errors.New("connection reset"))),
			retryable: true,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			eng := New(Config{})
			registerRunningSchemaChange(eng)

			eng.setSchemaChangeFailed(tc.err)

			live := pollProgress(t, eng)
			assert.Equal(t, engine.StateFailed, live.State)
			assert.NotEmpty(t, live.ErrorMessage)
			assert.Equal(t, tc.retryable, live.Retryable, "live progress")
			if tc.reason != "" {
				assert.Contains(t, live.ErrorMessage, tc.reason)
			}

			eng.Drain()

			drained := pollProgress(t, eng)
			assert.Equal(t, engine.StateFailed, drained.State)
			assert.Equal(t, live.ErrorMessage, drained.ErrorMessage)
			assert.Equal(t, tc.retryable, drained.Retryable, "drained progress")
		})
	}
}
