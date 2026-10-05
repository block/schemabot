package spirit

import (
	"errors"
	"fmt"
	"testing"

	"github.com/block/spirit/pkg/checksum"
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
