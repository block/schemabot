package schema

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestValidateIgnoreTables(t *testing.T) {
	assert.NoError(t, ValidateIgnoreTables(nil))
	assert.NoError(t, ValidateIgnoreTables([]string{"flyway_schema_history", "legacy_audit_log"}))

	// $ENV is not substituted in table entries, so an entry carrying it is a
	// literal name like any other rather than a rejected one.
	assert.NoError(t, ValidateIgnoreTables([]string{"events_$ENV"}))

	err := ValidateIgnoreTables([]string{""})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "must not be blank")

	err = ValidateIgnoreTables([]string{"  "})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "must not be blank")

	err = ValidateIgnoreTables([]string{"flyway_schema_history "})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "whitespace")

	err = ValidateIgnoreTables([]string{" flyway_schema_history"})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "whitespace")

	err = ValidateIgnoreTables([]string{"app/flyway_schema_history"})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "not a path")

	err = ValidateIgnoreTables([]string{`app\flyway_schema_history`})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "not a path")
}

func TestUnmatchedIgnoreTables(t *testing.T) {
	assert.Nil(t, UnmatchedIgnoreTables(nil, nil))
	assert.Nil(t, UnmatchedIgnoreTables([]string{"flyway_schema_history"}, []string{"flyway_schema_history"}))

	// A configured entry the plan never withheld is reported; matching is
	// exact and case-sensitive, so a case mismatch withholds nothing.
	assert.Equal(t, []string{"typo"}, UnmatchedIgnoreTables(
		[]string{"flyway_schema_history", "typo"}, []string{"flyway_schema_history"}))
	assert.Equal(t, []string{"Flyway_Schema_History"}, UnmatchedIgnoreTables(
		[]string{"Flyway_Schema_History"}, []string{"flyway_schema_history"}))

	// withheld is the union across namespaces, so an entry that matched in one
	// namespace and not another still withheld a table and is not reported.
	assert.Nil(t, UnmatchedIgnoreTables(
		[]string{"audit_log"}, []string{"audit_log", "flyway_schema_history"}))
}
