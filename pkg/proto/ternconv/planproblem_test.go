package ternconv

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/block/schemabot/pkg/engine"
)

// A refusal's problems survive the status detail unchanged, and a line the
// wire cannot carry is sent as unknown rather than wrapped around.
func TestPlanRefusalStatusRoundTripsProblems(t *testing.T) {
	problems := []engine.PlanProblem{
		{File: "app/orders.sql", Line: 12, Message: "Functions are not managed.", Statement: "CREATE FUNCTION f() RETURNS trigger AS $$ $$ LANGUAGE plpgsql"},
		{File: "app/events.sql", Line: 1 << 40, Message: "Partitions are not planned.", Statement: "CREATE TABLE events_2026 PARTITION OF events"},
	}
	st, err := PlanRefusalStatus("2 statements cannot be planned", problems)
	require.NoError(t, err)
	assert.Equal(t, codes.FailedPrecondition, st.Code())
	assert.Equal(t, "2 statements cannot be planned", st.Message())

	got := PlanProblemsFromStatus(st)
	require.Len(t, got, 2)
	assert.Equal(t, problems[0], got[0])
	assert.Equal(t, 0, got[1].Line, "a line past int32 is sent as unknown")
	assert.Equal(t, "app/events.sql", got[1].File)
}

// A refusal with no problems, and any status that is not a refusal, carries
// none back.
func TestPlanProblemsFromStatusWithoutDetail(t *testing.T) {
	st, err := PlanRefusalStatus("wait for drop_tables to go quiet", nil)
	require.NoError(t, err)
	assert.Empty(t, st.Details())
	assert.Nil(t, PlanProblemsFromStatus(st))

	assert.Nil(t, PlanProblemsFromStatus(status.New(codes.Internal, "boom")))
	assert.Nil(t, PlanProblemsFromStatus(nil))
}
