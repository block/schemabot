package tern

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/require"

	ternv1 "github.com/block/schemabot/pkg/proto/ternv1"
)

func TestGuardedPlanRechecksBeforeTransportRetry(t *testing.T) {
	server := &flakyTernServer{failPlans: 1}
	client := newRetryTestClient(t, server)
	revoked := errors.New("planning exception revoked")
	ctx := WithPlanAdmission(t.Context(), func(context.Context) error {
		count, _ := server.calls()
		if count == 1 {
			return revoked
		}
		return nil
	})

	_, err := client.Plan(ctx, &ternv1.PlanRequest{Database: "testdb"})
	require.ErrorIs(t, err, revoked)
	plans, _ := server.calls()
	require.Equal(t, 1, plans)
}
