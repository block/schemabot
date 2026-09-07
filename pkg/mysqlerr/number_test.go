package mysqlerr

import (
	"errors"
	"fmt"
	"testing"

	blockmysql "github.com/block/mysql"
	upstreammysql "github.com/go-sql-driver/mysql"
	"github.com/stretchr/testify/require"
)

// TestNumberReadsBothDrivers is the reason this helper exists. Both MySQL
// drivers are linked (see number.go), and the whole hazard is that neither
// driver's error type matches the other under errors.As — so a classifier
// written against one type silently stops recognizing errors from the other.
func TestNumberReadsBothDrivers(t *testing.T) {
	const deadlock = 1213

	t.Run("block/mysql", func(t *testing.T) {
		number, ok := Number(&blockmysql.MySQLError{Number: deadlock, Message: "Deadlock found"})
		require.True(t, ok, "an error from the fork was not recognized")
		require.Equal(t, uint16(deadlock), number)
	})

	t.Run("upstream go-sql-driver", func(t *testing.T) {
		number, ok := Number(&upstreammysql.MySQLError{Number: deadlock, Message: "Deadlock found"})
		require.True(t, ok, "an error from the hot-swap driver's upstream was not recognized")
		require.Equal(t, uint16(deadlock), number)
	})

	t.Run("wrapped", func(t *testing.T) {
		// database/sql and every layer above it wrap, so unwrapping is not
		// optional for either type.
		for name, err := range map[string]error{
			"block":    fmt.Errorf("exec: %w", &blockmysql.MySQLError{Number: deadlock}),
			"upstream": fmt.Errorf("exec: %w", &upstreammysql.MySQLError{Number: deadlock}),
		} {
			number, ok := Number(err)
			require.True(t, ok, "%s: wrapped error was not unwrapped", name)
			require.Equal(t, uint16(deadlock), number, name)
		}
	})

	t.Run("not a MySQL error", func(t *testing.T) {
		_, ok := Number(errors.New("connection refused"))
		require.False(t, ok)
		_, ok = Number(nil)
		require.False(t, ok)
	})
}

// TestDriverErrorTypesAreNotInterchangeable pins the premise. If a future
// dependency change ever made these the same type, Number's second branch
// would be dead code and this test says so out loud rather than leaving the
// helper looking like superstition.
func TestDriverErrorTypesAreNotInterchangeable(t *testing.T) {
	upstreamErr := error(&upstreammysql.MySQLError{Number: 1213})
	blockErr := error(&blockmysql.MySQLError{Number: 1213})

	var asBlock *blockmysql.MySQLError
	require.False(t, errors.As(upstreamErr, &asBlock),
		"upstream error matched the fork's type; Number's second branch would be unnecessary")

	var asUpstream *upstreammysql.MySQLError
	require.False(t, errors.As(blockErr, &asUpstream),
		"fork error matched upstream's type; Number's second branch would be unnecessary")
}

func TestIsMatchesAnyCode(t *testing.T) {
	err := error(&upstreammysql.MySQLError{Number: 1205})
	require.True(t, Is(err, 1213, 1205), "lock-wait timeout not matched among several codes")
	require.False(t, Is(err, 1213, 1062), "matched a code the error does not carry")
	require.False(t, Is(errors.New("nope"), 1205))
	require.False(t, Is(err), "no codes must never match")
}
