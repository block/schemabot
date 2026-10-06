package spirit

import (
	"errors"
	"fmt"
	"testing"

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

	// The continuous checksum during the deferred cutover wait reports a
	// divergence without repairing it, since a cutover may be imminent. The
	// resumed run's initial checksum repairs the range, so the failure stays
	// retryable.
	t.Run("continuous checksum divergence remains retryable", func(t *testing.T) {
		runnerErr := fmt.Errorf("continuous checksum: %w",
			fmt.Errorf("%w: chunk `id` >= 1 AND `id` < 1001", checksum.ErrPermanentDivergence))

		classifiedErr := classifyRunnerError(runnerErr)

		assert.True(t, engine.IsRetryable(classifiedErr))
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

	// Another run of a schema change holding the table's advisory lock is not
	// this run failing: the refusal is classified so the drive waits for the
	// holder instead of recording a failed attempt.
	t.Run("lock held by another run is a held target", func(t *testing.T) {
		runnerErr := fmt.Errorf("could not acquire advisory lock for orders.line_items: %w", dbconn.ErrLockHeld)

		classifiedErr := classifyRunnerError(runnerErr)

		assert.ErrorIs(t, classifiedErr, engine.ErrTargetHeld)
		assert.ErrorIs(t, classifiedErr, dbconn.ErrLockHeld)
		assert.True(t, engine.IsRetryable(classifiedErr))
	})

	t.Run("other runner errors remain retryable", func(t *testing.T) {
		runnerErr := errors.New("connection reset")

		classifiedErr := classifyRunnerError(runnerErr)

		require.Same(t, runnerErr, classifiedErr)
		assert.True(t, engine.IsRetryable(classifiedErr))
	})
}

// A runner failure's retry classification reaches the drive through progress.
// A copy whose checksum keeps finding rows it would lose fails the same way on
// every attempt, so progress reports it failed and not retryable, and the drive
// settles it instead of spending its recovery attempts re-copying the table. A
// failure that could succeed on a later attempt stays retryable. Both answers
// hold before and after the engine drains the finished change.
func TestFailedProgressCarriesRunnerRetryClassification(t *testing.T) {
	cases := []struct {
		name      string
		err       error
		retryable bool
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

			eng.Drain()

			drained := pollProgress(t, eng)
			assert.Equal(t, engine.StateFailed, drained.State)
			assert.Equal(t, live.ErrorMessage, drained.ErrorMessage)
			assert.Equal(t, tc.retryable, drained.Retryable, "drained progress")
		})
	}
}

// A run refused the table's lock reaches the drive through progress as a held
// target, before and after the engine drains it, so the drive waits for the
// holder rather than recording the refusal as a failed attempt. Any other
// failure is not a held target.
func TestFailedProgressReportsAHeldTarget(t *testing.T) {
	cases := []struct {
		name string
		err  error
		held bool
	}{
		{
			name: "lock held by another run",
			err: fmt.Errorf("schema change failed: %w", classifyRunnerError(
				fmt.Errorf("could not acquire advisory lock for testdb.line_items: %w", dbconn.ErrLockHeld))),
			held: true,
		},
		{
			name: "lost connection",
			err:  fmt.Errorf("schema change failed: %w", classifyRunnerError(errors.New("connection reset"))),
			held: false,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			eng := New(Config{})
			registerRunningSchemaChange(eng)

			eng.setSchemaChangeFailed(tc.err)

			live := pollProgress(t, eng)
			assert.Equal(t, engine.StateFailed, live.State)
			assert.Equal(t, tc.held, live.TargetHeld, "live progress")

			eng.Drain()

			drained := pollProgress(t, eng)
			assert.Equal(t, engine.StateFailed, drained.State)
			assert.Equal(t, tc.held, drained.TargetHeld, "drained progress")
		})
	}
}
