package ddl

import (
	"fmt"

	spiritlint "github.com/block/spirit/pkg/lint"
	"github.com/block/spirit/pkg/statement"
)

// UnsafeStatement reports whether a DDL statement is unsafe in Spirit's
// vocabulary: an operation that destroys existing data, such as DROP TABLE
// or an ALTER TABLE clause like DROP COLUMN or DROP PARTITION. Spirit's
// UnsafeLinter is the single authority on the unsafe set, so every surface
// that gates on "unsafe" agrees with the --allow-unsafe plan gate. The
// reason is the linter's violation message, suitable for operator-facing
// logs. Statement types Spirit does not run (for example TRUNCATE TABLE)
// fail the parse and surface as errors — callers treat an error as unsafe,
// which is the fail-closed direction.
//
// The verdict is unconditional: unlike plan-time linting, it cannot be
// relaxed by lint configuration. Callers that gate safety behavior on it
// (the storage-schema bootstrap, the instant DDL decision) must stay gated
// even for changes an operator explicitly allowed, because allowing an
// unsafe change to run is not the same as allowing it to run without its
// safety net.
//
// The input must be parseable by the MySQL-family parser; multi-statement
// content must be split with SplitStatements first.
func UnsafeStatement(stmt string) (bool, string, error) {
	parsed, err := statement.New(stmt)
	if err != nil {
		return false, "", fmt.Errorf("parse statement: %w", err)
	}
	violations := (&spiritlint.UnsafeLinter{}).Lint(nil, parsed)
	if len(violations) == 0 {
		return false, "", nil
	}
	return true, violations[0].Message, nil
}
