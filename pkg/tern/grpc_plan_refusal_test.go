package tern

import (
	"errors"
	"fmt"
	"log/slog"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/block/schemabot/pkg/engine"
	"github.com/block/schemabot/pkg/proto/ternconv"
	ternv1 "github.com/block/schemabot/pkg/proto/ternv1"
)

// A plan the engine refuses over statements in the schema files reaches a
// remote caller with each statement still located in its file, through Plan
// and through the PlanDiff a rollout member is planned with, so the plan
// comment can point the files' author at every one instead of quoting text.
func TestRemotePlanRefusalCarriesEveryProblem(t *testing.T) {
	problems := []engine.PlanProblem{
		{File: "app/accounts.sql", Line: 7, Message: "Comments are not managed.", Statement: "COMMENT ON TABLE accounts IS 'People'"},
		{File: "app/orders.sql", Line: 12, Message: "Functions are not managed.", Statement: "CREATE FUNCTION set_updated_at() RETURNS trigger AS $$\nBEGIN\nEND;\n$$ LANGUAGE plpgsql"},
	}
	const refusal = "2 statements in the schema files cannot be planned"
	server := NewServer(planErrorClient{err: fmt.Errorf("plan via 10.0.0.5:5432: %w", &engine.PlanRefusedError{
		Err:      errors.New(refusal),
		Problems: problems,
	})}, slog.Default())
	client := newSizeLimitedGRPCClient(t, server, nil, nil)

	_, err := client.Plan(t.Context(), &ternv1.PlanRequest{Database: "app"})
	require.Error(t, err)
	st := status.Convert(err)
	assert.Equal(t, codes.FailedPrecondition, st.Code())
	assert.Equal(t, refusal, st.Message(), "the message stays the refusal's own text")
	assert.Equal(t, problems, ternconv.PlanProblemsFromStatus(st))

	_, err = client.PlanDiff(t.Context(), &ternv1.PlanRequest{Database: "app"})
	require.Error(t, err)
	assert.Equal(t, problems, ternconv.PlanProblemsFromStatus(status.Convert(err)), "a rollout member is planned with PlanDiff")
}

// A refusal about the target rather than the files names no statements, so
// its status carries no detail and its message is the whole answer.
func TestRemotePlanRefusalWithoutProblemsCarriesNoDetail(t *testing.T) {
	server := NewServer(planErrorClient{err: &engine.PlanRefusedError{
		Err: errors.New("drop_tables entries were written in the last 24h"),
	}}, slog.Default())
	client := newSizeLimitedGRPCClient(t, server, nil, nil)

	_, err := client.Plan(t.Context(), &ternv1.PlanRequest{Database: "app"})
	require.Error(t, err)
	st := status.Convert(err)
	assert.Equal(t, codes.FailedPrecondition, st.Code())
	assert.Empty(t, st.Details())
	assert.Nil(t, ternconv.PlanProblemsFromStatus(st))
}
