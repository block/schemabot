package commands

import (
	"testing"

	"github.com/alecthomas/kong"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Both forms an operator reaches for, the database as an argument and with -d,
// parse to the same database and environment.
func TestHistoryCmdAcceptsTheDatabaseAsArgumentOrFlag(t *testing.T) {
	for _, args := range [][]string{
		{"history", "orders", "-e", "staging"},
		{"history", "-d", "orders", "-e", "staging"},
		{"history", "orders", "-d", "orders", "-e", "staging"},
	} {
		var cli struct {
			History HistoryCmd `cmd:""`
		}
		parser, err := kong.New(&cli)
		require.NoError(t, err)
		_, err = parser.Parse(args)
		require.NoError(t, err, "parse %v", args)

		database, err := cli.History.database()
		require.NoError(t, err, "resolve the database of %v", args)
		assert.Equal(t, "orders", database, "database of %v", args)
		assert.Equal(t, "staging", cli.History.Environment, "environment of %v", args)
	}
}

// A call that names no database, or two different ones, is refused with the
// form to use.
func TestHistoryCmdRefusesAMissingOrConflictingDatabase(t *testing.T) {
	_, err := (&HistoryCmd{Environment: "staging"}).database()
	require.EqualError(t, err, "name the database: history <database> -e <environment>")

	_, err = (&HistoryCmd{DatabaseArg: "orders", Database: "payments"}).database()
	require.EqualError(t, err, `the argument names database "orders" and -d names "payments"; give one`)
}
