package ternconv

import (
	"strings"
	"testing"
	"unicode/utf8"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"

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

	got, omitted := PlanProblemsFromStatus(st)
	require.Len(t, got, 2)
	assert.Zero(t, omitted)
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
	for name, st := range map[string]*status.Status{"refusal without problems": st, "failure": status.New(codes.Internal, "boom"), "no status": nil} {
		problems, omitted := PlanProblemsFromStatus(st)
		assert.Nil(t, problems, name)
		assert.Zero(t, omitted, name)
	}
}

// A refusal naming many large statements still answers with a small detail:
// it lists as many problems as a PR comment renders, cuts each field to what
// a caller shows on a rune boundary, and counts the rest, so a proxy's header
// limit cannot turn the refusal into a transport error.
func TestPlanRefusalStatusBoundsItsDetail(t *testing.T) {
	var problems []engine.PlanProblem
	for i := range 200 {
		problems = append(problems, engine.PlanProblem{
			File:      "app/" + strings.Repeat("é", 400) + ".sql",
			Line:      i + 1,
			Message:   strings.Repeat("Functions are not managed. ", 200),
			Statement: "CREATE FUNCTION f() RETURNS trigger AS $$\n" + strings.Repeat("PERFORM 1;\n", 5000) + "$$ LANGUAGE plpgsql",
		})
	}
	st, err := PlanRefusalStatus("200 statements cannot be planned", problems)
	require.NoError(t, err)

	encoded, err := proto.Marshal(st.Proto())
	require.NoError(t, err)
	assert.Less(t, len(encoded), 40*1024, "the detail stays far below a proxy's header limit")

	got, omitted := PlanProblemsFromStatus(st)
	require.Len(t, got, maxPlanRefusalProblems)
	assert.Equal(t, 200-maxPlanRefusalProblems, omitted)
	for _, p := range got {
		for _, field := range []string{p.File, p.Message, p.Statement} {
			assert.True(t, utf8.ValidString(field), "a field is cut on a rune boundary")
			assert.True(t, strings.HasSuffix(field, " …"), "a cut field marks that more followed")
		}
		assert.LessOrEqual(t, len(p.File), maxPlanProblemFileBytes)
		assert.LessOrEqual(t, len(p.Message), maxPlanProblemMessageBytes)
		assert.LessOrEqual(t, len(p.Statement), maxPlanProblemStatementBytes)
	}
	assert.Equal(t, 1, got[0].Line)
	assert.True(t, strings.HasPrefix(got[0].Statement, "CREATE FUNCTION f() RETURNS trigger AS $$"))
}
