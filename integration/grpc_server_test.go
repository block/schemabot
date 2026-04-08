//go:build integration

package integration

import (
	"github.com/block/schemabot/pkg/tern"
	"google.golang.org/grpc"
)

// newGRPCServer creates a new gRPC server wrapping a Client.
func newGRPCServer(client tern.Client) *tern.Server {
	return tern.NewServer(client)
}

// registerGRPCServer registers the tern server on the given grpc.Server.
func registerGRPCServer(srv *tern.Server, grpcSrv *grpc.Server) {
	srv.Register(grpcSrv)
}
