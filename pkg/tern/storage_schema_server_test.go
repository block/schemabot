package tern

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	ternv1 "github.com/block/schemabot/pkg/proto/ternv1"
)

type storageSchemaErrorService struct {
	err error
}

func (s storageSchemaErrorService) StorageSchemaPlan(context.Context, *ternv1.StorageSchemaPlanRequest) (*ternv1.StorageSchemaPlanResponse, error) {
	return nil, s.err
}

func (s storageSchemaErrorService) StorageSchemaApply(context.Context, *ternv1.StorageSchemaApplyRequest) (*ternv1.StorageSchemaApplyResponse, error) {
	return nil, s.err
}

// A storage schema failure is answered with the code that says whose problem it
// is. A caller who sent an unreadable schema file gets InvalidArgument and the
// reason, because they wrote the input and can correct it; anything else is the
// data plane's own and gets Internal with a pointer to its logs, because a DSN
// or a host has no business on a wire that reaches a workstation.
func TestServerStorageSchemaMapsCallerErrorsToInvalidArgument(t *testing.T) {
	testCases := []struct {
		name        string
		err         error
		want        codes.Code
		wantMessage string
	}{
		{
			name:        "unreadable schema the caller sent",
			err:         fmt.Errorf("%w: read the supplied storage schema: applies.sql is not DDL", ErrInvalidStorageSchemaRequest),
			want:        codes.InvalidArgument,
			wantMessage: "applies.sql is not DDL",
		},
		{
			name:        "the data plane's own storage is unreachable",
			err:         errors.New("dial tcp 10.0.0.1:5432: connection refused"),
			want:        codes.Internal,
			wantMessage: "see data plane logs",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			server := NewServer(nil, slog.New(slog.DiscardHandler),
				WithStorageSchemaService(storageSchemaErrorService{err: tc.err}))

			_, planErr := server.StorageSchemaPlan(t.Context(), &ternv1.StorageSchemaPlanRequest{})
			require.Error(t, planErr)
			assert.Equal(t, tc.want, status.Code(planErr))
			assert.Contains(t, status.Convert(planErr).Message(), tc.wantMessage)

			_, applyErr := server.StorageSchemaApply(t.Context(), &ternv1.StorageSchemaApplyRequest{})
			require.Error(t, applyErr)
			assert.Equal(t, tc.want, status.Code(applyErr))
			assert.Contains(t, status.Convert(applyErr).Message(), tc.wantMessage)
		})
	}
}

// An Internal answer carries no infrastructure detail, whatever the cause said.
// The cause is in the data plane's logs, where an operator with access to that
// deployment reads it.
func TestServerStorageSchemaInternalAnswersNameNoInfrastructure(t *testing.T) {
	server := NewServer(nil, slog.New(slog.DiscardHandler),
		WithStorageSchemaService(storageSchemaErrorService{
			err: errors.New("dial postgres://schemabot:hunter2@db.example:5432/schemabot: connection refused"),
		}))

	_, err := server.StorageSchemaPlan(t.Context(), &ternv1.StorageSchemaPlanRequest{})
	require.Error(t, err)
	message := status.Convert(err).Message()
	assert.NotContains(t, message, "db.example")
	assert.NotContains(t, message, "hunter2")
}
