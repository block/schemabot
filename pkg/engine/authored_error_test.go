package engine

import (
	"errors"
	"fmt"
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

// A refusal built around SchemaBot's own message, such as the drop_tables and
// ignore_tables overlap, gives up that message without the engine's wrap or
// the message's cause.
func TestPlanRefusedErrorAuthoredTextPrefersTheOperatorMessage(t *testing.T) {
	cause := errors.New("dial tcp 10.0.0.5:3306: connect: connection refused")
	refusal := &PlanRefusedError{Err: fmt.Errorf("plan database orders_prod: %w",
		&OperatorMessage{Message: "drop_tables entry audit is also withheld by ignore_tables", Err: cause})}

	text, quotesRepository := refusal.AuthoredText()

	assert.Equal(t, "drop_tables entry audit is also withheld by ignore_tables", text)
	assert.NotContains(t, text, "orders_prod")
	assert.NotContains(t, text, "10.0.0.5")
	assert.False(t, quotesRepository)
}
