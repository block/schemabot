// capabilities.go is how a control plane learns, before it sends a request,
// which request features the data plane on the other end can read.
//
// A proto3 field the receiver does not know is dropped without a trace, so a
// data plane older than a field reads a request that states it as one that
// states something else. For most fields that is harmless. For a direct
// execution size bound it is not: a byte-bound policy reaching a data plane
// that only knows the row bound arrives as an enabled policy with no bound,
// which the engine refuses, and every plan to that data plane fails with an
// error naming a row bound nobody configured. The data plane therefore
// advertises the features it reads on its Health response, and the control
// plane refuses, with an error that names the actual cause, a request the data
// plane would misread.
package tern

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"slices"
	"strings"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	ternv1 "github.com/block/schemabot/pkg/proto/ternv1"
	"github.com/block/schemabot/pkg/storage"
)

// CapabilityDirectExecutionMaxTableBytes is advertised by a data plane that
// reads DirectExecutionPolicy.max_table_bytes and treats a zero
// max_table_rows as "no row bound" rather than as an unusable bound.
const CapabilityDirectExecutionMaxTableBytes = "direct_execution.max_table_bytes"

// serverCapabilities lists the request features this binary's data plane
// reads, in the order the Health response reports them.
func serverCapabilities() []string {
	return []string{CapabilityDirectExecutionMaxTableBytes}
}

// policyStatesByteBound reports whether a stated policy depends on the data
// plane reading max_table_bytes. A disabled policy grants nothing whichever
// bound it carries, so a data plane that drops the byte bound from it still
// reads it correctly. A negative byte bound counts: it is a stored bound that
// could not be read, and the data plane must see it to refuse it.
func policyStatesByteBound(policy *ternv1.DirectExecutionPolicy) bool {
	return policy.GetEnabled() && policy.GetMaxTableBytes() != 0
}

// dataPlaneReadsByteBound reports whether a Health response advertises the
// byte bound. A data plane that predates capability reporting advertises
// nothing, so absence is "unsupported" and the feature is never assumed.
func dataPlaneReadsByteBound(health *ternv1.HealthResponse) bool {
	return slices.Contains(health.GetCapabilities(), CapabilityDirectExecutionMaxTableBytes)
}

// UnsupportedDirectExecutionError refuses a request whose direct execution
// policy states a bound the target data plane does not advertise. A release
// that reads the bound but predates capability reporting is refused too: the
// control plane cannot tell it from one that would misread the policy, and
// the remedy, an upgrade, is the same. Its message is
// built only from the control plane's own deployment name, the config keys,
// and a sanitized version string, so it is safe to render on a pull request.
type UnsupportedDirectExecutionError struct {
	// Deployment is the control plane's name for the data plane. Empty when
	// the client was built without one.
	Deployment string
	// Version is the data plane's self-reported SchemaBot version, sanitized.
	// Empty when the data plane does not report one, which every data plane
	// older than capability reporting does.
	Version string
}

func (e *UnsupportedDirectExecutionError) Error() string {
	dataPlane := "the data plane"
	if e.Deployment != "" {
		dataPlane = fmt.Sprintf("the data plane for deployment %q", e.Deployment)
	}
	release := "a SchemaBot release that predates capability reporting"
	if e.Version != "" {
		release = "SchemaBot " + e.Version
	}
	return fmt.Sprintf("direct_execution max_table_bytes is configured, but %s (%s) does not advertise support for it, "+
		"and a data plane that cannot read it refuses every plan as having no size bound. "+
		"Upgrade the data plane before using max_table_bytes, or bound the policy with max_table_rows until then",
		dataPlane, release)
}

// maxRenderedVersionLength clamps a data plane's self-reported version, which
// travels into a PR-facing error.
const maxRenderedVersionLength = 64

// renderableVersion returns a data plane's self-reported version when it is
// a plain version string, and empty otherwise. The value comes from across
// the wire and ends up in markdown, so anything beyond the characters a
// version is spelled with is dropped rather than escaped.
func renderableVersion(version string) string {
	if version == "" || len(version) > maxRenderedVersionLength {
		return ""
	}
	for _, r := range version {
		if !isVersionRune(r) {
			return ""
		}
	}
	return version
}

func isVersionRune(r rune) bool {
	return (r >= 'a' && r <= 'z') || (r >= 'A' && r <= 'Z') || (r >= '0' && r <= '9') || strings.ContainsRune(".-+_", r)
}

// refuseUnreadableDirectExecution returns nil when the data plane can read the
// stated policy, and an error when it cannot or when its capabilities could
// not be read. Only a policy that depends on a feature costs a Health call;
// every other request is sent without one.
//
// A failure to read the capabilities is not a verdict: the error wraps the
// Health call's own, so its gRPC status still says whether the data plane was
// unreachable. It is never read as "supported", because the request would then
// reach a data plane that may misread it.
func (c *GRPCClient) refuseUnreadableDirectExecution(ctx context.Context, logger *slog.Logger, policy *ternv1.DirectExecutionPolicy) error {
	if !policyStatesByteBound(policy) {
		return nil
	}
	health, err := c.client.Health(ctx, &ternv1.HealthRequest{})
	if status.Code(err) == codes.Unimplemented {
		// An endpoint that does not serve Health cannot advertise anything,
		// which is the same answer as an empty capability list.
		health, err = &ternv1.HealthResponse{}, nil
	}
	if err != nil {
		logger.WarnContext(ctx, "could not read data plane capabilities; refusing to send a direct execution policy with max_table_bytes it may not read",
			"deployment", c.deployment, "endpoint", c.address, "error", err)
		return fmt.Errorf("read capabilities of the data plane for deployment %q before sending direct_execution max_table_bytes: %w", c.deployment, err)
	}
	if dataPlaneReadsByteBound(health) {
		return nil
	}
	refusal := &UnsupportedDirectExecutionError{Deployment: c.deployment, Version: renderableVersion(health.GetVersion())}
	logger.WarnContext(ctx, "refusing request: the data plane does not advertise direct execution max_table_bytes support, so it would read the policy as unbounded and refuse it",
		"deployment", c.deployment,
		"endpoint", c.address,
		"data_plane_version", health.GetVersion(),
		"data_plane_capabilities", health.GetCapabilities(),
		"max_table_bytes", policy.GetMaxTableBytes())
	return refusal
}

// refuseUnreadableDispatchPolicy gates a drive's dispatch of an admitted
// apply on the data plane reading the policy the apply was admitted under.
// The plan was refused already if its data plane could not read the policy,
// so this fires only when the data plane changed between plan and dispatch,
// such as a rollback of its release.
//
// A refusal fails the apply, because the data plane would refuse the policy
// anyway, only under a misleading error. A failure to read the capabilities
// leaves the apply as it is: nothing was sent, so the next claim dispatches
// again.
func (c *GRPCClient) refuseUnreadableDispatchPolicy(ctx context.Context, apply *storage.Apply, scope applyTaskScope, tasks []*storage.Task, policy *ternv1.DirectExecutionPolicy) error {
	logger := c.applyLogger(apply).With(apply.MutableLogAttrs()...)
	if scope.operation != nil {
		logger = logger.With("apply_operation_id", scope.operation.ID, "operation_deployment", scope.operation.Deployment)
	}
	err := c.refuseUnreadableDirectExecution(ctx, logger, policy)
	if err == nil {
		return nil
	}
	var refusal *UnsupportedDirectExecutionError
	if !errors.As(err, &refusal) {
		return fmt.Errorf("dispatch gRPC apply %s: %w", apply.ApplyIdentifier, err)
	}
	if markErr := c.markRemoteApplyFailed(ctx, apply, tasks, refusal.Error(), false, scope); markErr != nil {
		return fmt.Errorf("mark gRPC apply %s failed after its data plane could not read its direct execution policy: %w", apply.ApplyIdentifier, markErr)
	}
	return fmt.Errorf("dispatch gRPC apply %s: %w", apply.ApplyIdentifier, refusal)
}

// policyGrantsWithoutKnownBound reports whether a stated policy is enabled
// but carries none of the size bounds this binary reads. Configuration never
// produces one, because a bound is required, so on the wire it most likely
// means a newer control plane stated a bound in a field this data plane does
// not know, and the field was dropped on the way in.
func policyGrantsWithoutKnownBound(policy *ternv1.DirectExecutionPolicy) bool {
	return policy.GetEnabled() && policy.GetMaxTableRows() == 0 && policy.GetMaxTableBytes() == 0
}

// refuseUnboundedStatedPolicy refuses a request whose stated policy grants
// direct execution with no bound this data plane reads. The engine would
// refuse it too, but under an error about the bound keys that does not say
// where the policy came from; this one names the likely version skew.
func (s *Server) refuseUnboundedStatedPolicy(ctx context.Context, rpc string, policy *ternv1.DirectExecutionPolicy, database, environment string) error {
	if !policyGrantsWithoutKnownBound(policy) {
		return nil
	}
	s.logger.WarnContext(ctx, "refusing request: caller stated an enabled direct execution policy with no size bound this data plane reads",
		"rpc", rpc, "database", database, "environment", environment, "data_plane_version", s.version)
	return status.Error(codes.InvalidArgument,
		"the caller's direct_execution policy is enabled but carries no size bound this data plane reads (max_table_rows or max_table_bytes); "+
			"this likely means a newer control plane sent a bound this data plane does not support. "+
			"Upgrade the data plane, or configure the policy with a bound it supports")
}
