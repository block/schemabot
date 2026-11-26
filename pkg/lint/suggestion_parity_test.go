package lint

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// A lint finding carries two separate things: a message saying what is wrong,
// and the linter's advice on what to change. An operator reading a finding
// SchemaBot will not fix for them only has something to act on when the advice
// is there, so a rule that offers it on one engine and withholds it on another
// gives the same schema defect a different UX depending on the database behind
// it. These cases run the same defect through both dialects and hold the two
// to the same answer.
func TestSuggestionParityAcrossEngines(t *testing.T) {
	cases := []struct {
		linter string
		mysql  string
		pg     string
	}{
		{
			linter: postgresLinterPrimaryKey,
			mysql:  "CREATE TABLE orders (id INT AUTO_INCREMENT PRIMARY KEY) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;",
			pg:     "CREATE TABLE orders (id integer PRIMARY KEY);",
		},
		{
			linter: postgresLinterHasFloat,
			mysql:  "CREATE TABLE orders (id BIGINT AUTO_INCREMENT PRIMARY KEY, amount FLOAT) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;",
			pg:     "CREATE TABLE orders (id bigint PRIMARY KEY, amount real);",
		},
		{
			linter: postgresLinterNameCase,
			mysql:  "CREATE TABLE Orders (id BIGINT AUTO_INCREMENT PRIMARY KEY) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;",
			pg:     `CREATE TABLE "Orders" (id bigint PRIMARY KEY);`,
		},
		{
			linter: postgresLinterRedundantIndexes,
			mysql: "CREATE TABLE orders (id BIGINT AUTO_INCREMENT PRIMARY KEY, a INT, b INT, " +
				"KEY k_a (a), KEY k_ab (a, b)) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;",
			pg: "CREATE TABLE orders (id bigint PRIMARY KEY, a integer, b integer);\n" +
				"CREATE INDEX k_a ON orders (a);\nCREATE INDEX k_ab ON orders (a, b);",
		},
	}

	linter := New()

	for _, tc := range cases {
		t.Run(tc.linter, func(t *testing.T) {
			mysqlResults, err := linter.LintSchema(map[string]string{"orders.sql": tc.mysql})
			require.NoError(t, err)
			pgResults, err := linter.LintPostgresSchema(map[string]string{"orders": tc.pg})
			require.NoError(t, err)

			mysqlFinding, ok := findByLinter(mysqlResults, tc.linter)
			require.True(t, ok, "MySQL did not report %s; the case no longer exercises the rule", tc.linter)
			pgFinding, ok := findByLinter(pgResults, tc.linter)
			require.True(t, ok, "PostgreSQL did not report %s; the case no longer exercises the rule", tc.linter)

			assert.Equal(t, mysqlFinding.Suggestion != "", pgFinding.Suggestion != "",
				"%s offers advice on one engine and not the other: MySQL %q, PostgreSQL %q",
				tc.linter, mysqlFinding.Suggestion, pgFinding.Suggestion)
		})
	}

	// A rule the PostgreSQL audit reports with no case above is a rule whose
	// advice nothing holds to its MySQL counterpart, so the registry is what
	// this test is checked against rather than the cases it happens to carry.
	covered := make(map[string]bool, len(cases))
	for _, tc := range cases {
		covered[tc.linter] = true
	}
	for _, name := range postgresLinterNames {
		assert.True(t, covered[name], "rule %q has no cross-engine suggestion case", name)
	}
}

func findByLinter(results []Result, linter string) (Result, bool) {
	for _, r := range results {
		if r.Linter == linter {
			return r, true
		}
	}
	return Result{}, false
}
