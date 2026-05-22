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

type applyErrorClient struct {
	Client
	err error
}

func (c applyErrorClient) Apply(context.Context, *ternv1.ApplyRequest) (*ternv1.ApplyResponse, error) {
	return nil, c.err
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

			_, err := server.Progress(t.Context(), &ternv1.ProgressRequest{ApplyId: "missing"})
			require.Error(t, err)
			assert.Equal(t, codes.NotFound, status.Code(err))
		})
	}
}

func TestServerApplyMapsEngineRetryabilityToStatusCode(t *testing.T) {
	// The data plane must preserve engine retryability across gRPC so the
	// control-plane scheduler can pause retryable errors without retrying
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
