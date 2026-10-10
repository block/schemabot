package mysqlerr

import (
	"errors"
	"fmt"
	"testing"

	"github.com/block/mysql"
	"github.com/stretchr/testify/require"
)

func TestNumberReadsDriverErrors(t *testing.T) {
	const deadlock = 1213

	t.Run("driver error", func(t *testing.T) {
		number, ok := Number(&mysql.MySQLError{Number: deadlock, Message: "Deadlock found"})
		require.True(t, ok)
		require.Equal(t, uint16(deadlock), number)
	})

	t.Run("wrapped", func(t *testing.T) {
		// database/sql and every layer above it wrap, so unwrapping is not
		// optional — this is the reason call sites must not type-assert.
		number, ok := Number(fmt.Errorf("exec: %w", &mysql.MySQLError{Number: deadlock}))
		require.True(t, ok, "wrapped error was not unwrapped")
		require.Equal(t, uint16(deadlock), number)
	})

	t.Run("not a MySQL error", func(t *testing.T) {
		_, ok := Number(errors.New("connection refused"))
		require.False(t, ok)
		_, ok = Number(nil)
		require.False(t, ok)
	})
}

func TestIsMatchesAnyCode(t *testing.T) {
	err := error(&mysql.MySQLError{Number: 1205})
	require.True(t, Is(err, 1213, 1205), "lock-wait timeout not matched among several codes")
	require.False(t, Is(err, 1213, 1062), "matched a code the error does not carry")
	require.False(t, Is(errors.New("nope"), 1205))
	require.False(t, Is(err), "no codes must never match")
}
