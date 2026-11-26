package api

import (
	"errors"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	grpccodes "google.golang.org/grpc/codes"
	grpcstatus "google.golang.org/grpc/status"

	"github.com/block/schemabot/pkg/engine"
	"github.com/block/schemabot/pkg/proto/ternconv"
)

// A refused plan's statements are read back the same way whether a local
// engine refused it or a remote data plane did, under whatever wraps the
// caller added, so the plan comment lists them on every deployment.
func TestPlanRefusalProblemsReadsLocalAndRemoteRefusals(t *testing.T) {
	problems := []engine.PlanProblem{{File: "app/orders.sql", Line: 12, Message: "Functions are not managed.", Statement: "CREATE FUNCTION f()"}}
	remote, err := ternconv.PlanRefusalStatus("1 statement cannot be planned", problems)
	require.NoError(t, err)

	testCases := []struct {
		name string
		err  error
		want []engine.PlanProblem
	}{
		{"local refusal", fmt.Errorf("plan staging: %w", &engine.PlanRefusedError{Err: errors.New("refused"), Problems: problems}), problems},
		{"remote refusal", fmt.Errorf("plan staging: %w", remote.Err()), problems},
		{"refusal about the target", &engine.PlanRefusedError{Err: errors.New("wait for drop_tables")}, nil},
		{"remote refusal from a data plane without the detail", grpcstatus.Error(grpccodes.FailedPrecondition, "refused"), nil},
		{"remote failure", grpcstatus.Error(grpccodes.Internal, "boom"), nil},
		{"local failure", errors.New("connection refused"), nil},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.want, PlanRefusalProblems(tc.err))
		})
	}
}
