package tern

import (
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

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
