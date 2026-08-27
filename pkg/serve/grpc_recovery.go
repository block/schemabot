package serve

import (
	"context"
	"errors"
	"fmt"
	"log/slog"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/block/schemabot/pkg/metrics"
	"github.com/block/schemabot/pkg/panicsafe"
)

// RecoveryUnaryInterceptor contains handler panics so a fault in one RPC fails
// that call instead of killing the process that drives applies. The panic
// value and stack are logged server-side and the client receives a fixed
// Internal error; panic text never crosses the wire because it can carry
// internal detail (DSN fragments, hostnames, driver internals). Embedders that
// register the Tern service on their own server should install this
// interceptor for the same containment.
func RecoveryUnaryInterceptor(logger *slog.Logger) grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (resp any, err error) {
		err = panicsafe.Call(func() error {
			var handlerErr error
			resp, handlerErr = handler(ctx, req)
			return handlerErr
		})
		if contained := containedPanicError(ctx, logger, info.FullMethod, err); contained != nil {
			return nil, contained
		}
		return resp, err
	}
}

// RecoveryStreamInterceptor is the streaming counterpart of
// RecoveryUnaryInterceptor. The Tern service is unary-only today; this exists
// so a future streaming method is contained by construction rather than
// remembering to add it then.
func RecoveryStreamInterceptor(logger *slog.Logger) grpc.StreamServerInterceptor {
	return func(srv any, ss grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
		err := panicsafe.Call(func() error {
			return handler(srv, ss)
		})
		if contained := containedPanicError(ss.Context(), logger, info.FullMethod, err); contained != nil {
			return contained
		}
		return err
	}
}

// containedPanicError returns the sanitized status error for a contained
// handler panic, logging the panic value and stack with the RPC method so the
// fault is triageable from logs alone. A nil return means err was not a
// contained panic and must be propagated as-is.
func containedPanicError(ctx context.Context, logger *slog.Logger, fullMethod string, err error) error {
	var handlerPanic *panicsafe.Error
	if !errors.As(err, &handlerPanic) {
		return nil
	}
	logger.Error("gRPC handler panicked; the RPC failed with Internal and the server kept serving",
		"method", fullMethod,
		"panic", fmt.Sprint(handlerPanic.Value),
		"stack", string(handlerPanic.Stack))
	metrics.RecordRecoveredPanic(ctx, "grpc_handler")
	return status.Error(codes.Internal, "internal error; see server logs")
}
