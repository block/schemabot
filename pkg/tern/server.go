package tern

import (
	"context"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	ternv1 "github.com/block/schemabot/pkg/proto/ternv1"
)

// Server wraps a Client as a gRPC TernServer.
// Each RPC delegates to the underlying Client implementation.
type Server struct {
	client Client
}

var _ ternv1.TernServer = (*Server)(nil)

// NewServer creates a gRPC server wrapping a Client.
func NewServer(client Client) *Server {
	return &Server{client: client}
}

// Register registers the server on the given grpc.Server.
func (s *Server) Register(srv *grpc.Server) {
	ternv1.RegisterTernServer(srv, s)
}

func (s *Server) Health(ctx context.Context, req *ternv1.HealthRequest) (*ternv1.HealthResponse, error) {
	if err := s.client.Health(ctx); err != nil {
		return nil, status.Error(codes.Unavailable, "service unavailable")
	}
	return &ternv1.HealthResponse{Status: "ok"}, nil
}

func (s *Server) Plan(ctx context.Context, req *ternv1.PlanRequest) (*ternv1.PlanResponse, error) {
	resp, err := s.client.Plan(ctx, req)
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	return resp, nil
}

func (s *Server) Apply(ctx context.Context, req *ternv1.ApplyRequest) (*ternv1.ApplyResponse, error) {
	resp, err := s.client.Apply(ctx, req)
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	return resp, nil
}

func (s *Server) Progress(ctx context.Context, req *ternv1.ProgressRequest) (*ternv1.ProgressResponse, error) {
	resp, err := s.client.Progress(ctx, req)
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	return resp, nil
}

func (s *Server) Cutover(ctx context.Context, req *ternv1.CutoverRequest) (*ternv1.CutoverResponse, error) {
	resp, err := s.client.Cutover(ctx, req)
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	return resp, nil
}

func (s *Server) Stop(ctx context.Context, req *ternv1.StopRequest) (*ternv1.StopResponse, error) {
	resp, err := s.client.Stop(ctx, req)
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	return resp, nil
}

func (s *Server) Start(ctx context.Context, req *ternv1.StartRequest) (*ternv1.StartResponse, error) {
	resp, err := s.client.Start(ctx, req)
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	return resp, nil
}

func (s *Server) Volume(ctx context.Context, req *ternv1.VolumeRequest) (*ternv1.VolumeResponse, error) {
	resp, err := s.client.Volume(ctx, req)
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	return resp, nil
}

func (s *Server) Revert(ctx context.Context, req *ternv1.RevertRequest) (*ternv1.RevertResponse, error) {
	resp, err := s.client.Revert(ctx, req)
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	return resp, nil
}

func (s *Server) SkipRevert(ctx context.Context, req *ternv1.SkipRevertRequest) (*ternv1.SkipRevertResponse, error) {
	resp, err := s.client.SkipRevert(ctx, req)
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	return resp, nil
}
