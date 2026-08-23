package tern

import (
	"context"
	"errors"
	"log/slog"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/block/schemabot/pkg/engine"
	ternv1 "github.com/block/schemabot/pkg/proto/ternv1"
	"github.com/block/schemabot/pkg/storage"
)

// Server wraps a Client as a gRPC TernServer.
// Each RPC delegates to the underlying Client implementation.
type Server struct {
	client Client
	logger *slog.Logger
}

var _ ternv1.TernServer = (*Server)(nil)

// NewServer creates a gRPC server wrapping a Client. The logger carries the
// health-check causes the RPC sanitizes out of its response, so an embedder
// that wires its own logger sees them with the rest of its structured output
// and can attach its deployment identifiers to them. A nil logger falls back
// to the default rather than dropping the only record of the cause.
func NewServer(client Client, logger *slog.Logger) *Server {
	if logger == nil {
		logger = slog.Default()
	}
	return &Server{client: client, logger: logger}
}

// Register registers the server on the given grpc.Server.
func (s *Server) Register(srv *grpc.Server) {
	ternv1.RegisterTernServer(srv, s)
}

func (s *Server) Health(ctx context.Context, req *ternv1.HealthRequest) (*ternv1.HealthResponse, error) {
	if err := s.client.Health(ctx); err != nil {
		// The caller only ever sees a sanitized Unavailable, so this log is the
		// only record of why a deployment stopped reporting healthy — without it
		// the control plane marks the deployment down with no attributable cause.
		if callerAbandonedHealthCheck(ctx) {
			s.logger.DebugContext(ctx, "tern health check abandoned by its caller", "error", err)
		} else {
			s.logger.WarnContext(ctx, "tern health check failed; reporting deployment unavailable to caller", "error", err)
		}
		return nil, status.Error(codes.Unavailable, "service unavailable")
	}
	return &ternv1.HealthResponse{Status: "ok"}, nil
}

// callerAbandonedHealthCheck reports whether a failed health check ended because
// the caller hung up rather than because the deployment is unhealthy. A control
// plane shutting down or dropping the connection teaches this side nothing, and
// the caller already records it.
//
// The request context is the signal, not the returned error: gRPC cancels the
// handler context when the client disconnects, so a real hangup always shows up
// here, while an error merely wrapping context.Canceled can come from a
// sub-context the check cancelled itself — a genuine failure that must stay at
// warn.
//
// An expired deadline is deliberately excluded: it means the health check itself
// outran the caller's budget, which is a genuine unhealthy signal, and this side
// is the only one that knows what the check was waiting on.
//
// A hangup landing after the check already failed for a real cause files that
// cause at debug. The signal is delayed rather than lost: the deployment is
// still unhealthy on the caller's next poll, which warns then.
func callerAbandonedHealthCheck(ctx context.Context) bool {
	return errors.Is(ctx.Err(), context.Canceled)
}

func (s *Server) PullSchema(ctx context.Context, req *ternv1.PullSchemaRequest) (*ternv1.PullSchemaResponse, error) {
	resp, err := s.client.PullSchema(ctx, req)
	if err != nil {
		return nil, status.Error(pullSchemaErrorCode(err), err.Error())
	}
	return resp, nil
}

func pullSchemaErrorCode(err error) codes.Code {
	if err == nil {
		return codes.OK
	}
	if errors.Is(err, ErrPullSchemaUnsupportedType) {
		return codes.Unimplemented
	}
	if errors.Is(err, ErrPullSchemaInvalidRequest) {
		return codes.InvalidArgument
	}
	return codes.Internal
}

func (s *Server) Plan(ctx context.Context, req *ternv1.PlanRequest) (*ternv1.PlanResponse, error) {
	resp, err := s.client.Plan(ctx, req)
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	return resp, nil
}

func (s *Server) PlanDiff(ctx context.Context, req *ternv1.PlanRequest) (*ternv1.PlanDiffResponse, error) {
	resp, err := s.client.PlanDiff(ctx, req)
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	return resp, nil
}

func (s *Server) Apply(ctx context.Context, req *ternv1.ApplyRequest) (*ternv1.ApplyResponse, error) {
	resp, err := s.client.Apply(ctx, req)
	if err != nil {
		return nil, status.Error(applyErrorCode(err), err.Error())
	}
	return resp, nil
}

// applyErrorCode preserves engine retryability across the gRPC boundary for
// operator-driven dispatch.
func applyErrorCode(err error) codes.Code {
	if err == nil {
		return codes.OK
	}
	if !engine.IsRetryable(err) {
		return codes.FailedPrecondition
	}
	return codes.Internal
}

func (s *Server) Progress(ctx context.Context, req *ternv1.ProgressRequest) (*ternv1.ProgressResponse, error) {
	if err := requireApplyID(req.ApplyId); err != nil {
		return nil, err
	}
	resp, err := s.client.Progress(ctx, req)
	if err != nil {
		if errors.Is(err, storage.ErrApplyNotFound) || errors.Is(err, storage.ErrTaskNotFound) {
			return nil, status.Error(codes.NotFound, err.Error())
		}
		return nil, status.Error(codes.Internal, err.Error())
	}
	return resp, nil
}

func (s *Server) Logs(ctx context.Context, req *ternv1.LogsRequest) (*ternv1.LogsResponse, error) {
	if err := requireApplyID(req.GetApplyId()); err != nil {
		return nil, err
	}
	resp, err := s.client.Logs(ctx, req)
	if err != nil {
		if errors.Is(err, storage.ErrApplyNotFound) {
			return nil, status.Error(codes.NotFound, err.Error())
		}
		return nil, status.Error(codes.Internal, err.Error())
	}
	return resp, nil
}

func (s *Server) Cutover(ctx context.Context, req *ternv1.CutoverRequest) (*ternv1.CutoverResponse, error) {
	if err := requireApplyID(req.ApplyId); err != nil {
		return nil, err
	}
	resp, err := s.client.Cutover(ctx, req)
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	return resp, nil
}

func (s *Server) Stop(ctx context.Context, req *ternv1.StopRequest) (*ternv1.StopResponse, error) {
	if err := requireApplyID(req.ApplyId); err != nil {
		return nil, err
	}
	resp, err := s.client.Stop(ctx, req)
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	return resp, nil
}

func (s *Server) Cancel(ctx context.Context, req *ternv1.CancelRequest) (*ternv1.CancelResponse, error) {
	if err := requireApplyID(req.ApplyId); err != nil {
		return nil, err
	}
	resp, err := s.client.Cancel(ctx, req)
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	return resp, nil
}

func (s *Server) Start(ctx context.Context, req *ternv1.StartRequest) (*ternv1.StartResponse, error) {
	if err := requireApplyID(req.ApplyId); err != nil {
		return nil, err
	}
	resp, err := s.client.Start(ctx, req)
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	return resp, nil
}

func (s *Server) Volume(ctx context.Context, req *ternv1.VolumeRequest) (*ternv1.VolumeResponse, error) {
	if err := requireApplyID(req.ApplyId); err != nil {
		return nil, err
	}
	resp, err := s.client.Volume(ctx, req)
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	return resp, nil
}

func (s *Server) Revert(ctx context.Context, req *ternv1.RevertRequest) (*ternv1.RevertResponse, error) {
	if err := requireApplyID(req.ApplyId); err != nil {
		return nil, err
	}
	resp, err := s.client.Revert(ctx, req)
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	return resp, nil
}

func (s *Server) SkipRevert(ctx context.Context, req *ternv1.SkipRevertRequest) (*ternv1.SkipRevertResponse, error) {
	if err := requireApplyID(req.ApplyId); err != nil {
		return nil, err
	}
	resp, err := s.client.SkipRevert(ctx, req)
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	return resp, nil
}

func requireApplyID(applyID string) error {
	if applyID == "" {
		return status.Error(codes.InvalidArgument, "apply_id is required")
	}
	return nil
}
