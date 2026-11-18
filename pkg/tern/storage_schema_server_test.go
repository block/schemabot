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

// storageSchemaStaticService answers both RPCs with a fixed report, standing in
// for a data plane whose adapter read its own storage.
type storageSchemaStaticService struct {
	plan  *ternv1.StorageSchemaPlanResponse
	apply *ternv1.StorageSchemaApplyResponse
}

func (s storageSchemaStaticService) StorageSchemaPlan(context.Context, *ternv1.StorageSchemaPlanRequest) (*ternv1.StorageSchemaPlanResponse, error) {
	return s.plan, nil
}

func (s storageSchemaStaticService) StorageSchemaApply(context.Context, *ternv1.StorageSchemaApplyRequest) (*ternv1.StorageSchemaApplyResponse, error) {
	return s.apply, nil
}

// What the adapter answered is what the caller reads, over a real connection.
// Every statement a control plane renders — including the one the data plane
// refused as destructive — crosses the wire here, so a report that arrived
// short a list, or with one list's statements under another's name, would tell
// an operator that a DROP runs automatically or that nothing is outstanding at
// all.
func TestServerStorageSchemaAnswersWhatTheAdapterReported(t *testing.T) {
	outstanding := &ternv1.StorageSchemaStatement{
		Table:     "apply_operations",
		Operation: "add_column",
		Ddl:       `ALTER TABLE "apply_operations" ADD COLUMN "operation_kind" varchar(32) NOT NULL DEFAULT 'work'`,
	}
	destructive := &ternv1.StorageSchemaStatement{
		Table:     "vitess_tasks",
		Operation: "drop_table",
		Ddl:       `DROP TABLE "vitess_tasks"`,
		Reason:    "DROP TABLE destroys data",
	}
	report := &ternv1.StorageSchemaReport{
		Dialect:      "postgres",
		Database:     "schemabot",
		Host:         "storage.db.example:5432",
		SchemaSource: "the schema embedded in v0.1.68",
		Version:      "v0.1.68",
		Outstanding:  []*ternv1.StorageSchemaStatement{outstanding},
		Destructive:  []*ternv1.StorageSchemaStatement{destructive},
	}
	client := newRetryTestClient(t, NewServer(nil, slog.New(slog.DiscardHandler),
		WithStorageSchemaService(storageSchemaStaticService{
			plan: &ternv1.StorageSchemaPlanResponse{Report: report},
			apply: &ternv1.StorageSchemaApplyResponse{
				Planned:   report,
				Remaining: &ternv1.StorageSchemaReport{Dialect: "postgres", Database: "schemabot", Destructive: []*ternv1.StorageSchemaStatement{destructive}},
			},
		})))

	plan, err := client.StorageSchemaPlan(t.Context(), &ternv1.StorageSchemaPlanRequest{})
	require.NoError(t, err)
	require.NotNil(t, plan.GetReport())
	assert.Equal(t, "schemabot", plan.GetReport().GetDatabase())
	assert.Equal(t, "storage.db.example:5432", plan.GetReport().GetHost())
	assert.Equal(t, "the schema embedded in v0.1.68", plan.GetReport().GetSchemaSource())
	require.Len(t, plan.GetReport().GetOutstanding(), 1)
	assert.Equal(t, "apply_operations", plan.GetReport().GetOutstanding()[0].GetTable())
	assert.Contains(t, plan.GetReport().GetOutstanding()[0].GetDdl(), "operation_kind")
	require.Len(t, plan.GetReport().GetDestructive(), 1)
	assert.Equal(t, "DROP TABLE destroys data", plan.GetReport().GetDestructive()[0].GetReason(),
		"a refused statement arrives with why it was refused, not as one that runs")

	// Both halves of a convergence arrive: what ran and what is still
	// outstanding are different answers, and an operator deciding whether the
	// storage is ready reads the difference between them.
	apply, err := client.StorageSchemaApply(t.Context(), &ternv1.StorageSchemaApplyRequest{Caller: "operator@example.com"})
	require.NoError(t, err)
	require.NotNil(t, apply.GetPlanned())
	require.NotNil(t, apply.GetRemaining())
	require.Len(t, apply.GetPlanned().GetOutstanding(), 1)
	assert.Equal(t, "apply_operations", apply.GetPlanned().GetOutstanding()[0].GetTable())
	assert.Empty(t, apply.GetRemaining().GetOutstanding(), "the column converged")
	require.Len(t, apply.GetRemaining().GetDestructive(), 1)
	assert.Equal(t, "vitess_tasks", apply.GetRemaining().GetDestructive()[0].GetTable(),
		"the refused statement is still outstanding")
}
