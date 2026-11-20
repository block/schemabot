package tern

import (
	"context"
	"math/rand/v2"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	ternv1 "github.com/block/schemabot/pkg/proto/ternv1"
)

type planAdmissionKey struct{}

// WithPlanAdmission requires a fresh admission check before each remote Plan
// or PlanDiff attempt. Unrestricted readers retain the ordinary gRPC retries.
func WithPlanAdmission(ctx context.Context, admit func(context.Context) error) context.Context {
	return context.WithValue(ctx, planAdmissionKey{}, admit)
}

func guardedPlanRPC[T any](ctx context.Context, admit func(context.Context) error, rpc func(context.Context, *ternv1.PlanRequest, ...grpc.CallOption) (*T, error), req *ternv1.PlanRequest) (*T, error) {
	if _, hasDeadline := ctx.Deadline(); !hasDeadline {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, grpcHeavyRPCDeadline)
		defer cancel()
	}
	backoff := 500 * time.Millisecond
	for attempt := 1; ; attempt++ {
		if err := admit(ctx); err != nil {
			return nil, err
		}
		result, err := rpc(ctx, req)
		if status.Code(err) != codes.Unavailable || attempt == 5 {
			return result, err
		}

		// Match the retry budget used for unrestricted Plan and PlanDiff calls.
		delay := time.Duration(float64(backoff) * (0.8 + 0.4*rand.Float64()))
		timer := time.NewTimer(delay)
		select {
		case <-ctx.Done():
			timer.Stop()
			return nil, status.FromContextError(ctx.Err()).Err()
		case <-timer.C:
		}
		backoff = min(8*time.Second, 3*backoff)
	}
}
