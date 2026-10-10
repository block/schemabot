package tern

import (
	"context"
	"fmt"
	"net"
	"strings"
	"testing"

	"github.com/block/spirit/pkg/utils"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"

	ternv1 "github.com/block/schemabot/pkg/proto/ternv1"
)

// grpcDefaultMaxRecvMsgBytes is gRPC's own default response limit. The fixture
// below is sized against it so the tests state which limit they are crossing.
const grpcDefaultMaxRecvMsgBytes = 4 << 20 // 4 MiB

// bigPullTernServer answers PullSchema with a namespace of tableCount tables,
// each carrying ddlBytes of CREATE TABLE text. It stands in for a deployment
// whose schema is large enough that the response, not the request, is what
// bounds the call.
type bigPullTernServer struct {
	ternv1.UnimplementedTernServer
	tableCount int
	ddlBytes   int
}

func (s *bigPullTernServer) PullSchema(context.Context, *ternv1.PullSchemaRequest) (*ternv1.PullSchemaResponse, error) {
	ddl := strings.Repeat("x", s.ddlBytes)
	tables := make(map[string]string, s.tableCount)
	for i := range s.tableCount {
		tables[fmt.Sprintf("table_%05d", i)] = ddl
	}
	return &ternv1.PullSchemaResponse{
		Database:    "wide",
		Environment: "production",
		Namespaces:  map[string]*ternv1.PulledNamespace{"wide": {Tables: tables}},
		TableCount:  int32(s.tableCount),
	}, nil
}

// newRecvLimitTestClient starts an in-process Tern gRPC server and connects a
// client through the production constructor, so the dial options under test are
// the ones a deployment gets.
func newRecvLimitTestClient(t *testing.T, server ternv1.TernServer, config Config) *GRPCClient {
	t.Helper()

	lis, err := (&net.ListenConfig{}).Listen(t.Context(), "tcp", "localhost:0")
	require.NoError(t, err, "failed to listen")

	grpcServer := grpc.NewServer()
	ternv1.RegisterTernServer(grpcServer, server)
	go func() {
		_ = grpcServer.Serve(lis)
	}()

	config.Address = lis.Addr().String()
	client, err := NewGRPCClient(config)
	require.NoError(t, err, "failed to create client")

	t.Cleanup(func() {
		utils.CloseAndLog(client)
		grpcServer.Stop()
	})
	return client
}

// A database whose schema serializes to more than gRPC's own default response
// limit is still readable: the pull carries the CREATE TABLE text of every
// table, so the response grows with the schema, and a deployment must not
// become unreadable on table count alone.
func TestGRPCClientPullSchemaAcceptsResponseOverGRPCDefault(t *testing.T) {
	// 2000 tables of 4 KiB lands around 8 MiB — twice gRPC's default, and the
	// size a few thousand real tables produce.
	server := &bigPullTernServer{tableCount: 2000, ddlBytes: 4096}
	client := newRecvLimitTestClient(t, server, Config{})

	resp, err := client.PullSchema(t.Context(), &ternv1.PullSchemaRequest{
		Database:    "wide",
		Environment: "production",
	})
	require.NoError(t, err, "pull of a schema larger than gRPC's default limit must succeed")
	require.EqualValues(t, 2000, resp.GetTableCount())
	require.Len(t, resp.GetNamespaces()["wide"].GetTables(), 2000)

	// Pin the fixture to the limit it exists to cross: if the response ever
	// shrinks below gRPC's default, this test would pass without exercising
	// anything.
	require.Greater(t, proto.Size(resp), grpcDefaultMaxRecvMsgBytes,
		"fixture must serialize above gRPC's default limit to prove the raised ceiling is in effect")
}

// The default ceiling is a real bound, not just a raised one: a client that
// configures nothing still refuses a response above it rather than buffering
// whatever a deployment sends. The fixture is sized from the constant, so the
// bound stays pinned if the default moves.
func TestGRPCClientPullSchemaRefusesResponseOverDefaultCeiling(t *testing.T) {
	const ddlBytes = 4096
	server := &bigPullTernServer{
		tableCount: DefaultMaxRecvMsgBytes/ddlBytes + 64,
		ddlBytes:   ddlBytes,
	}

	// Measure the fixture through the same builder the server answers with, so
	// the test cannot silently stop exceeding the ceiling it is checking.
	probe, err := server.PullSchema(t.Context(), &ternv1.PullSchemaRequest{})
	require.NoError(t, err)
	require.Greater(t, proto.Size(probe), DefaultMaxRecvMsgBytes,
		"fixture must serialize above the default ceiling for this test to bound anything")

	client := newRecvLimitTestClient(t, server, Config{})

	_, err = client.PullSchema(t.Context(), &ternv1.PullSchemaRequest{
		Database:    "wide",
		Environment: "production",
	})
	require.Error(t, err, "a response above the default ceiling must be refused")
	require.Equal(t, codes.ResourceExhausted, status.Code(err))
}

// The ceiling stays a ceiling: a configured limit is honored, so an oversized
// response is refused rather than buffered without bound.
func TestGRPCClientPullSchemaHonorsConfiguredRecvLimit(t *testing.T) {
	server := &bigPullTernServer{tableCount: 2000, ddlBytes: 4096}
	client := newRecvLimitTestClient(t, server, Config{MaxRecvMsgBytes: 1 << 20})

	_, err := client.PullSchema(t.Context(), &ternv1.PullSchemaRequest{
		Database:    "wide",
		Environment: "production",
	})
	require.Error(t, err, "a response above the configured limit must be refused")
	require.Equal(t, codes.ResourceExhausted, status.Code(err))
}
