package serve

import (
	"context"
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
// interceptor for the same containment. Install it first in
// grpc.ChainUnaryInterceptor: the first chained interceptor is the outermost,
// so a panic in any interceptor ahead of this one escapes containment and
// kills the process. Do not combine it with the non-chained
// grpc.UnaryInterceptor option — grpc-go runs that interceptor before the
// entire chain, ahead of recovery. A nil logger falls back to slog.Default()
// so the containment path can never itself panic.
func RecoveryUnaryInterceptor(logger *slog.Logger) grpc.UnaryServerInterceptor {
	if logger == nil {
		logger = slog.Default()
	}
	return func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (resp any, err error) {
		handlerPanic, handlerErr := panicsafe.Catch(func() error {
			var innerErr error
			resp, innerErr = handler(ctx, req)
			return innerErr
		})
		if handlerPanic != nil {
			return nil, containedPanicError(ctx, logger, info.FullMethod, handlerPanic)
		}
		return resp, handlerErr
	}
}

// RecoveryStreamInterceptor is the streaming counterpart of
// RecoveryUnaryInterceptor. The Tern service is unary-only today; this exists
// so a future streaming method is contained by construction rather than
// remembering to add it then. The same installation rules apply: first in
// grpc.ChainStreamInterceptor, never combined with the non-chained
// grpc.StreamInterceptor option. A nil logger falls back to slog.Default().
func RecoveryStreamInterceptor(logger *slog.Logger) grpc.StreamServerInterceptor {
	if logger == nil {
		logger = slog.Default()
	}
	return func(srv any, ss grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
		handlerPanic, handlerErr := panicsafe.Catch(func() error {
			return handler(srv, ss)
		})
		if handlerPanic != nil {
			return containedPanicError(ss.Context(), logger, info.FullMethod, handlerPanic)
		}
		return handlerErr
	}
}

// newTernGRPCServer constructs the gRPC server Run serves the Tern service
// on, with the recovery interceptors installed as the outermost link of each
// chain so a handler panic fails only that RPC instead of killing the process
// that drives applies.
func newTernGRPCServer(logger *slog.Logger) *grpc.Server {
	return grpc.NewServer(
		grpc.ChainUnaryInterceptor(RecoveryUnaryInterceptor(logger)),
		grpc.ChainStreamInterceptor(RecoveryStreamInterceptor(logger)),
	)
}

// containedPanicError returns the sanitized status error for a contained
// handler panic, logging the panic value and stack with the RPC method so the
// fault is triageable from logs alone. Provenance is structural: the caller
// passes the *panicsafe.Error its own Catch call produced, so a handler error
// that merely wraps a *panicsafe.Error is never mistaken for a recovered panic
// and propagates untouched with its cause chain and status code intact.
func containedPanicError(ctx context.Context, logger *slog.Logger, fullMethod string, handlerPanic *panicsafe.Error) error {
	logger.Error("gRPC handler panicked; the RPC failed with Internal and the server kept serving",
		"method", fullMethod,
		"panic", fmt.Sprint(handlerPanic.Value),
		"stack", string(handlerPanic.Stack))
	metrics.RecordRecoveredPanic(ctx, "grpc_handler")
	return status.Error(codes.Internal, "internal error; see server logs")
}
