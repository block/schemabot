package tern

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/block/schemabot/pkg/engine"
	ternv1 "github.com/block/schemabot/pkg/proto/ternv1"
	"github.com/block/schemabot/pkg/storage"
)

type progressErrorClient struct {
	Client
	err error
}

func (c progressErrorClient) Progress(context.Context, *ternv1.ProgressRequest) (*ternv1.ProgressResponse, error) {
	return nil, c.err
}

type logsErrorClient struct {
	Client
	err error
}

func (c logsErrorClient) Logs(context.Context, *ternv1.LogsRequest) (*ternv1.LogsResponse, error) {
	return nil, c.err
}

type applyErrorClient struct {
	Client
	err error
}

func (c applyErrorClient) Apply(context.Context, *ternv1.ApplyRequest) (*ternv1.ApplyResponse, error) {
	return nil, c.err
}

type pullSchemaErrorClient struct {
	Client
	err error
}

func (c pullSchemaErrorClient) PullSchema(context.Context, *ternv1.PullSchemaRequest) (*ternv1.PullSchemaResponse, error) {
	return nil, c.err
}

type noopControlClient struct {
	Client
}

func TestServerProgressMapsMissingApplyDataToNotFound(t *testing.T) {
	testCases := []struct {
		name string
		err  error
	}{
		{
			name: "missing apply",
			err:  fmt.Errorf("get apply missing: %w", storage.ErrApplyNotFound),
		},
		{
			name: "missing tasks",
			err:  fmt.Errorf("get tasks for apply missing: %w", storage.ErrTaskNotFound),
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			server := NewServer(progressErrorClient{err: tc.err})

			_, err := server.Progress(t.Context(), &ternv1.ProgressRequest{
				ApplyId:     "missing",
				Environment: "staging",
			})
			require.Error(t, err)
			assert.Equal(t, codes.NotFound, status.Code(err))
		})
	}
}

func TestServerLogsMapsErrorsToStatusCode(t *testing.T) {
	testCases := []struct {
		name string
		err  error
		want codes.Code
	}{
		{name: "missing apply", err: fmt.Errorf("get apply missing: %w", storage.ErrApplyNotFound), want: codes.NotFound},
		{name: "storage failure", err: errors.New("storage unavailable"), want: codes.Internal},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			server := NewServer(logsErrorClient{err: tc.err})
			_, err := server.Logs(t.Context(), &ternv1.LogsRequest{ApplyId: "apply-123"})
			require.Error(t, err)
			assert.Equal(t, tc.want, status.Code(err))
		})
	}
}

func TestServerApplyMapsEngineRetryabilityToStatusCode(t *testing.T) {
	// The data plane must preserve engine retryability across gRPC so the
	// control-plane operator can pause retryable errors without retrying
	// known-permanent engine failures.
	testCases := []struct {
		name string
		err  error
		want codes.Code
	}{
		{
			name: "retryable engine error",
			err:  errors.New("engine temporarily unavailable"),
			want: codes.Internal,
		},
		{
			name: "permanent engine error",
			err:  engine.NewPermanentError("schema change cannot be applied"),
			want: codes.FailedPrecondition,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			server := NewServer(applyErrorClient{err: tc.err})

			_, err := server.Apply(t.Context(), &ternv1.ApplyRequest{PlanId: "plan-123"})
			require.Error(t, err)
			assert.Equal(t, tc.want, status.Code(err))
		})
	}
}

func TestServerPullSchemaMapsKnownErrorsToStatusCode(t *testing.T) {
	testCases := []struct {
		name string
		err  error
		want codes.Code
	}{
		{
			name: "unsupported type",
			err:  fmt.Errorf("vitess is not supported yet: %w", ErrPullSchemaUnsupportedType),
			want: codes.Unimplemented,
		},
		{
			name: "invalid request",
			err:  fmt.Errorf("request type mismatch: %w", ErrPullSchemaInvalidRequest),
			want: codes.InvalidArgument,
		},
		{
			name: "unexpected error",
			err:  errors.New("database unavailable"),
			want: codes.Internal,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			server := NewServer(pullSchemaErrorClient{err: tc.err})

			_, err := server.PullSchema(t.Context(), &ternv1.PullSchemaRequest{Database: "orders"})
			require.Error(t, err)
			assert.Equal(t, tc.want, status.Code(err))
		})
	}
}

func TestServerApplyScopedRPCsRequireApplyID(t *testing.T) {
	server := NewServer(noopControlClient{})

	testCases := []struct {
		name string
		call func(context.Context) error
	}{
		{
			name: "progress",
			call: func(ctx context.Context) error {
				_, err := server.Progress(ctx, &ternv1.ProgressRequest{})
				return err
			},
		},
		{
			name: "cutover",
			call: func(ctx context.Context) error {
				_, err := server.Cutover(ctx, &ternv1.CutoverRequest{})
				return err
			},
		},
		{
			name: "stop",
			call: func(ctx context.Context) error {
				_, err := server.Stop(ctx, &ternv1.StopRequest{})
				return err
			},
		},
		{
			name: "start",
			call: func(ctx context.Context) error {
				_, err := server.Start(ctx, &ternv1.StartRequest{})
				return err
			},
		},
		{
			name: "volume",
			call: func(ctx context.Context) error {
				_, err := server.Volume(ctx, &ternv1.VolumeRequest{Volume: 5})
				return err
			},
		},
		{
			name: "revert",
			call: func(ctx context.Context) error {
				_, err := server.Revert(ctx, &ternv1.RevertRequest{})
				return err
			},
		},
		{
			name: "skip revert",
			call: func(ctx context.Context) error {
				_, err := server.SkipRevert(ctx, &ternv1.SkipRevertRequest{})
				return err
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			err := tc.call(t.Context())
			require.Error(t, err)
			assert.Equal(t, codes.InvalidArgument, status.Code(err))
			assert.Contains(t, err.Error(), "apply_id is required")
		})
	}
}
