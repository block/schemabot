package engine

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
)

// A message SchemaBot composed gives up its own words and not its cause, and
// they are not flagged as quoting the repository.
func TestOperatorMessageAuthoredTextOmitsTheCause(t *testing.T) {
	cause := errors.New("dial tcp 10.0.0.5:3306: connect: connection refused")

	text, quotesRepository := (&OperatorMessage{Message: `the copy of "orders" has not started`, Err: cause}).AuthoredText()

	assert.Equal(t, `the copy of "orders" has not started`, text)
	assert.NotContains(t, text, "10.0.0.5")
	assert.False(t, quotesRepository)
}

// An engine's refusal of the schema files is composed from the statements in
// them, so it is flagged as quoting the repository.
func TestPlanRefusedErrorAuthoredTextQuotesTheRepository(t *testing.T) {
	text, quotesRepository := (&PlanRefusedError{Err: errors.New(`the schema files drop table "orders_seq"`)}).AuthoredText()

	assert.Equal(t, `the schema files drop table "orders_seq"`, text)
	assert.True(t, quotesRepository)
}
