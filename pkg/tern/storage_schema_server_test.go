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

// An endpoint whose embedder registered no storage-schema service refuses both
// RPCs rather than answering them against whatever storage happens to be at
// hand, and leaves a record naming which RPC was refused. The refusal is the
// only thing that distinguishes this deployment from one running a release
// that predates the RPCs, and the two have different remedies.
func TestServerRefusesStorageSchemaWithoutAdapter(t *testing.T) {
	var records []capturedLog
	server := NewServer(nil, slog.New(captureHandler{records: &records}))

	_, planErr := server.StorageSchemaPlan(t.Context(), &ternv1.StorageSchemaPlanRequest{})
	require.Error(t, planErr)
	assert.Equal(t, codes.Unimplemented, status.Code(planErr))
	assert.Contains(t, status.Convert(planErr).Message(), "does not serve storage schema requests")

	_, applyErr := server.StorageSchemaApply(t.Context(), &ternv1.StorageSchemaApplyRequest{Caller: "operator@example.com"})
	require.Error(t, applyErr)
	assert.Equal(t, codes.Unimplemented, status.Code(applyErr))

	require.Len(t, records, 2, "each refusal is recorded once")
	assert.Equal(t, slog.LevelWarn, records[0].level)
	assert.Equal(t, "StorageSchemaPlan", records[0].attrs["rpc"])
	assert.Equal(t, "StorageSchemaApply", records[1].attrs["rpc"])
	assert.Equal(t, "operator@example.com", records[1].attrs["caller"],
		"a refused convergence still says who asked for it")
}

// A data plane that answers Unimplemented — one registering no storage-schema
// service, or one running a release from before the RPCs existed — reaches the
// operator as an instruction to upgrade that data plane. There is no second way
// to read that deployment's storage from here, so the failure has to name the
// remedy rather than surfacing as a bare gRPC code.
func TestGRPCClientStorageSchemaNamesUpgradeOnUnimplemented(t *testing.T) {
	client := newRetryTestClient(t, NewServer(nil, slog.New(slog.DiscardHandler)))

	_, planErr := client.StorageSchemaPlan(t.Context(), &ternv1.StorageSchemaPlanRequest{})
	require.Error(t, planErr)
	assert.Equal(t, codes.Unimplemented, status.Code(planErr))
	assert.ErrorContains(t, planErr, "does not support storage schema reads")
	assert.ErrorContains(t, planErr, "upgrade that data plane")

	_, applyErr := client.StorageSchemaApply(t.Context(), &ternv1.StorageSchemaApplyRequest{})
	require.Error(t, applyErr)
	assert.Equal(t, codes.Unimplemented, status.Code(applyErr))
	assert.ErrorContains(t, applyErr, "does not support storage schema convergence")
	assert.ErrorContains(t, applyErr, "upgrade that data plane")
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
