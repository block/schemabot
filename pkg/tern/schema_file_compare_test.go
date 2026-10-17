package tern

import (
	"context"
	"errors"
	"log/slog"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/block/schemabot/pkg/engine"
	"github.com/block/schemabot/pkg/engine/postgres"
	ternv1 "github.com/block/schemabot/pkg/proto/ternv1"
	"github.com/block/schemabot/pkg/storage"
)

// The PostgreSQL engine is the one that compares schema files on its target.
var _ SchemaFileChangeComparer = (*postgres.Engine)(nil)

// comparingEngine is a PostgreSQL engine that can compare schema files,
// answering with resp or err and recording the request.
type comparingEngine struct {
	engine.Engine
	resp *ternv1.CompareSchemaFileChangeResponse
	err  error
	req  *ternv1.CompareSchemaFileChangeRequest
}

func (e *comparingEngine) CompareSchemaFileChange(_ context.Context, req *ternv1.CompareSchemaFileChangeRequest) (*ternv1.CompareSchemaFileChangeResponse, error) {
	e.req = req
	return e.resp, e.err
}

// stubEngine is an engine with no schema file comparison.
type stubEngine struct{ engine.Engine }

func newCompareTestClient(dbType string, eng engine.Engine) *LocalClient {
	c := &LocalClient{config: LocalConfig{Database: "orders", Type: dbType}, logger: slog.New(slog.DiscardHandler)}
	switch dbType {
	case storage.DatabaseTypePostgres:
		c.postgresEngine = eng
	case storage.DatabaseTypeMySQL:
		c.spiritEngine = eng
	}
	return c
}

// A data plane hands a schema file comparison to an engine that can run it
// and returns its verdict. An engine that cannot compare files, a request for
// another database type, or a comparison that fails is an error, which the
// review gate treats as a changed file.
func TestLocalClientCompareSchemaFileChange(t *testing.T) {
	req := &ternv1.CompareSchemaFileChangeRequest{
		Database: "orders", Type: storage.DatabaseTypePostgres,
		ApprovedBase: "approved base", Approved: "approved", HeadBase: "head base", Head: "head",
	}

	t.Run("an engine that compares files returns its verdict", func(t *testing.T) {
		eng := &comparingEngine{resp: &ternv1.CompareSchemaFileChangeResponse{Reason: "the head declares column note differently from the approved commit"}}
		resp, err := newCompareTestClient(storage.DatabaseTypePostgres, eng).CompareSchemaFileChange(t.Context(), req)
		require.NoError(t, err)
		assert.False(t, resp.GetSame())
		assert.Equal(t, "the head declares column note differently from the approved commit", resp.GetReason())
		assert.Same(t, req, eng.req)
	})
	t.Run("an engine that cannot compare files is unsupported", func(t *testing.T) {
		_, err := newCompareTestClient(storage.DatabaseTypeMySQL, &stubEngine{}).CompareSchemaFileChange(t.Context(),
			&ternv1.CompareSchemaFileChangeRequest{Database: "orders", Type: storage.DatabaseTypeMySQL})
		require.ErrorIs(t, err, ErrCompareSchemaFileChangeUnsupported)
		assert.ErrorContains(t, err, "orders")
	})
	t.Run("a request for another database type is rejected", func(t *testing.T) {
		eng := &comparingEngine{resp: &ternv1.CompareSchemaFileChangeResponse{Same: true}}
		_, err := newCompareTestClient(storage.DatabaseTypeMySQL, eng).CompareSchemaFileChange(t.Context(), req)
		require.ErrorContains(t, err, `request type "postgres" does not match client type "mysql"`)
		assert.Nil(t, eng.req, "the engine is never asked")
	})
	t.Run("a comparison the engine cannot run is an error", func(t *testing.T) {
		eng := &comparingEngine{err: errors.New("run table orders at the head: type no_such_type does not exist")}
		_, err := newCompareTestClient(storage.DatabaseTypePostgres, eng).CompareSchemaFileChange(t.Context(), req)
		require.ErrorContains(t, err, "no_such_type")
		assert.NotErrorIs(t, err, ErrCompareSchemaFileChangeUnsupported)
	})
}

// compareAnsweringClient answers a schema file comparison with resp or err,
// standing in for the data plane's client behind a tern Server.
type compareAnsweringClient struct {
	Client
	resp *ternv1.CompareSchemaFileChangeResponse
	err  error
}

func (c *compareAnsweringClient) CompareSchemaFileChange(context.Context, *ternv1.CompareSchemaFileChangeRequest) (*ternv1.CompareSchemaFileChangeResponse, error) {
	return c.resp, c.err
}

// Over gRPC, a data plane whose engine cannot compare files, and a data plane
// that predates the RPC, both reach the control plane as
// ErrCompareSchemaFileChangeUnsupported. Any other failure stays an ordinary
// error, and a verdict passes through unchanged.
func TestGRPCClientCompareSchemaFileChange(t *testing.T) {
	req := &ternv1.CompareSchemaFileChangeRequest{Database: "orders", Type: storage.DatabaseTypePostgres}
	logger := slog.New(slog.DiscardHandler)

	t.Run("a verdict passes through", func(t *testing.T) {
		client := newRetryTestClient(t, NewServer(&compareAnsweringClient{resp: &ternv1.CompareSchemaFileChangeResponse{Same: true}}, logger))
		resp, err := client.CompareSchemaFileChange(t.Context(), req)
		require.NoError(t, err)
		assert.True(t, resp.GetSame())
	})
	t.Run("an engine that cannot compare files", func(t *testing.T) {
		client := newRetryTestClient(t, NewServer(&compareAnsweringClient{err: ErrCompareSchemaFileChangeUnsupported}, logger))
		_, err := client.CompareSchemaFileChange(t.Context(), req)
		require.ErrorIs(t, err, ErrCompareSchemaFileChangeUnsupported)
		assert.ErrorContains(t, err, "orders")
	})
	t.Run("a data plane that predates the RPC", func(t *testing.T) {
		client := newRetryTestClient(t, &ternv1.UnimplementedTernServer{})
		_, err := client.CompareSchemaFileChange(t.Context(), req)
		require.ErrorIs(t, err, ErrCompareSchemaFileChangeUnsupported)
	})
	t.Run("a comparison that fails", func(t *testing.T) {
		client := newRetryTestClient(t, NewServer(&compareAnsweringClient{err: errors.New("connection refused")}, logger))
		_, err := client.CompareSchemaFileChange(t.Context(), req)
		require.ErrorContains(t, err, "connection refused")
		assert.NotErrorIs(t, err, ErrCompareSchemaFileChangeUnsupported)
	})
}
