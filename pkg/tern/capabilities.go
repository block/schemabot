// capabilities.go keeps a caller from sending a request the server on the
// other end would misread.
//
// A proto3 field the receiver does not know is dropped without a trace. For
// most fields that is harmless, but for some the rest of the request then says
// something the caller never meant: a byte-bound direct execution policy
// reaching a data plane that only knows the row bound arrives as an enabled
// policy with no bound, and the data plane refuses every plan under an error
// about a row bound nobody configured.
//
// Such a field is declared in tern.proto with the requires_remote_capability
// option. A server advertises every capability its compiled schema declares
// on its Health response, and the caller's client interceptor reads that list
// before sending any request that sets an annotated field, refusing the
// request itself when a capability is missing. Neither side keeps a list: the
// annotation is the whole registration.
//
// The read and the request are separate calls, so a request routed to an
// older replica mid-rollout still reaches it. What keeps that safe is that an
// annotated field is one whose misread still fails closed on the older
// server; the gate turns the common case into a refusal that names the
// upgrade.
package tern

import (
	"context"
	"fmt"
	"log/slog"
	"maps"
	"slices"
	"strings"
	"sync"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/encoding/protowire"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"

	ternv1 "github.com/block/schemabot/pkg/proto/ternv1"
)

// fieldCapability is the capability a field is annotated with, or empty.
func fieldCapability(fd protoreflect.FieldDescriptor) string {
	name, _ := proto.GetExtension(fd.Options(), ternv1.E_RequiresRemoteCapability).(string)
	return name
}

// serverCapabilities is every capability this binary's tern schema declares,
// sorted. A binary that compiled an annotated field reads it, because the
// annotation lands with the code that does.
var serverCapabilities = sync.OnceValue(func() []string {
	declared := map[string]bool{}
	var collect func(protoreflect.MessageDescriptors)
	collect = func(messages protoreflect.MessageDescriptors) {
		for i := range messages.Len() {
			md := messages.Get(i)
			fields := md.Fields()
			for j := range fields.Len() {
				if name := fieldCapability(fields.Get(j)); name != "" {
					declared[name] = true
				}
			}
			collect(md.Messages())
		}
	}
	collect(ternv1.File_tern_proto.Messages())
	return slices.Sorted(maps.Keys(declared))
})

// forEachSetMessage calls visit on m and on every message nested in a field m
// sets, at any depth. path names the field that holds each message.
func forEachSetMessage(m protoreflect.Message, path string, visit func(path string, m protoreflect.Message)) {
	visit(path, m)
	m.Range(func(fd protoreflect.FieldDescriptor, v protoreflect.Value) bool {
		child := string(fd.Name())
		if path != "" {
			child = path + "." + child
		}
		switch {
		case fd.IsMap():
			if fd.MapValue().Message() == nil {
				return true
			}
			v.Map().Range(func(_ protoreflect.MapKey, entry protoreflect.Value) bool {
				forEachSetMessage(entry.Message(), child, visit)
				return true
			})
		case fd.IsList():
			if fd.Message() == nil {
				return true
			}
			list := v.List()
			for i := range list.Len() {
				forEachSetMessage(list.Get(i).Message(), child, visit)
			}
		case fd.Message() != nil:
			forEachSetMessage(v.Message(), child, visit)
		}
		return true
	})
}

// requiredCapabilities returns, sorted, the capability of every annotated
// field req sets. Range visits only populated fields, so a field left at its
// zero value requires nothing.
func requiredCapabilities(req proto.Message) []string {
	required := map[string]bool{}
	forEachSetMessage(req.ProtoReflect(), "", func(_ string, m protoreflect.Message) {
		m.Range(func(fd protoreflect.FieldDescriptor, _ protoreflect.Value) bool {
			if name := fieldCapability(fd); name != "" {
				required[name] = true
			}
			return true
		})
	})
	return slices.Sorted(maps.Keys(required))
}

// missingCapabilities returns the required capabilities a server does not
// advertise. A server that predates capability reporting advertises nothing,
// so everything is missing there and nothing is ever assumed.
func missingCapabilities(required, advertised []string) []string {
	var missing []string
	for _, name := range required {
		if !slices.Contains(advertised, name) {
			missing = append(missing, name)
		}
	}
	return missing
}

// MissingCapabilityError refuses a request that sets a field the target data
// plane does not advertise reading. A release that reads the field but
// predates capability reporting is refused too: the caller cannot tell it from
// one that would misread the request, and the remedy, an upgrade, is the same.
// The message is built only from the caller's own deployment name, the
// capability names in tern.proto, and a sanitized version string, so it is
// safe to render on a pull request.
type MissingCapabilityError struct {
	// Deployment is the caller's name for the data plane. Empty when the
	// client was built without one.
	Deployment string
	// Version is the data plane's self-reported SchemaBot version, sanitized.
	// Empty when the data plane reports none, as every one that predates
	// capability reporting does.
	Version string
	// Missing are the capabilities the request depends on that the data plane
	// does not advertise.
	Missing []string
}

func (e *MissingCapabilityError) Error() string {
	dataPlane := "the data plane"
	if e.Deployment != "" {
		dataPlane = fmt.Sprintf("the data plane for deployment %q", e.Deployment)
	}
	release := "a SchemaBot release that predates capability reporting"
	if e.Version != "" {
		release = "SchemaBot " + e.Version
	}
	missing := strings.Join(e.Missing, ", ")
	return fmt.Sprintf("%s (%s) does not advertise %s, which this request depends on, so the request was not sent: "+
		"a data plane without it would misread the request. Upgrade the data plane before using %s",
		dataPlane, release, missing, missing)
}

// GRPCStatus classifies the refusal as FailedPrecondition. It is definite: the
// request was not sent, and resending it fails the same way until the data
// plane is upgraded, so an apply refused at dispatch fails rather than
// returning to the claimable pool.
func (e *MissingCapabilityError) GRPCStatus() *status.Status {
	return status.New(codes.FailedPrecondition, e.Error())
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

// requestScope is what the routed tern requests expose for triage.
type requestScope interface {
	GetDatabase() string
	GetEnvironment() string
}

// requestScopeAttrs returns the database and environment a request names,
// when it names them.
func requestScopeAttrs(req any) []any {
	scope, ok := req.(requestScope)
	if !ok {
		return nil
	}
	return []any{"database", scope.GetDatabase(), "environment", scope.GetEnvironment()}
}

// capabilityGateInterceptor refuses, before sending, a request that sets an
// annotated field the data plane does not advertise reading. It sits on the
// client rather than at each call site so a field annotated later is gated on
// every RPC that can carry it, with nothing to wire.
//
// Only a request that sets an annotated field costs a Health call. A failure to
// read the capabilities is not a verdict: the request is not sent, and the
// error wraps the Health call's own so its gRPC status still says whether the
// data plane was unreachable.
func capabilityGateInterceptor(deployment, address string, logger *slog.Logger) grpc.UnaryClientInterceptor {
	return func(ctx context.Context, method string, req, reply any, cc *grpc.ClientConn, invoker grpc.UnaryInvoker, opts ...grpc.CallOption) error {
		msg, ok := req.(proto.Message)
		if !ok {
			return invoker(ctx, method, req, reply, cc, opts...)
		}
		required := requiredCapabilities(msg)
		if len(required) == 0 {
			return invoker(ctx, method, req, reply, cc, opts...)
		}
		attrs := append([]any{"rpc", method, "deployment", deployment, "endpoint", address, "required_capabilities", required},
			requestScopeAttrs(req)...)

		health, err := readCapabilities(ctx, cc)
		if err != nil {
			logger.WarnContext(ctx, "could not read data plane capabilities; the request will not be sent", append(attrs, "error", err)...)
			return fmt.Errorf("read capabilities of the data plane for deployment %q before sending a request that depends on %s: %w",
				deployment, strings.Join(required, ", "), err)
		}
		missing := missingCapabilities(required, health.GetCapabilities())
		if len(missing) == 0 {
			return invoker(ctx, method, req, reply, cc, opts...)
		}
		logger.WarnContext(ctx, "refusing request: the data plane does not advertise a capability the request depends on, so it would misread the request",
			append(attrs,
				"missing_capabilities", missing,
				"data_plane_version", health.GetVersion(),
				"data_plane_capabilities", health.GetCapabilities())...)
		return &MissingCapabilityError{Deployment: deployment, Version: renderableVersion(health.GetVersion()), Missing: missing}
	}
}

// readCapabilities asks the data plane what it advertises. An endpoint that
// does not serve Health cannot advertise anything, which is the same answer as
// an empty list.
func readCapabilities(ctx context.Context, cc *grpc.ClientConn) (*ternv1.HealthResponse, error) {
	ctx, cancel := context.WithTimeout(ctx, grpcControlRPCDeadline)
	defer cancel()
	health := &ternv1.HealthResponse{}
	err := cc.Invoke(ctx, ternv1.Tern_Health_FullMethodName, &ternv1.HealthRequest{}, health)
	if status.Code(err) == codes.Unimplemented {
		return &ternv1.HealthResponse{}, nil
	}
	if err != nil {
		return nil, err
	}
	return health, nil
}

// unknownFieldPaths names, sorted, the fields req set that this binary's
// schema does not know, such as "direct_execution#5". They survive decoding as
// unknown fields, which is the one trace a newer caller leaves.
func unknownFieldPaths(req proto.Message) []string {
	unknown := map[string]bool{}
	forEachSetMessage(req.ProtoReflect(), "", func(path string, m protoreflect.Message) {
		raw := m.GetUnknown()
		for len(raw) > 0 {
			number, _, n := protowire.ConsumeField(raw)
			if n < 0 {
				return
			}
			raw = raw[n:]
			unknown[fmt.Sprintf("%s#%d", path, number)] = true
		}
	})
	return slices.Sorted(maps.Keys(unknown))
}

// withUnknownFieldNote returns the message a server answers a failed request
// with. When the request set fields this server does not know, the caller runs
// a newer release, and a failure about what those fields carry would otherwise
// name only the fields this server did read; the note says so and names the
// remedy.
func (s *Server) withUnknownFieldNote(ctx context.Context, rpc string, req proto.Message, message string) string {
	unknown := unknownFieldPaths(req)
	if len(unknown) == 0 {
		return message
	}
	s.logger.WarnContext(ctx, "request failed and set fields this server does not know; the caller runs a newer release",
		append([]any{"rpc", rpc, "unknown_fields", unknown, "server_version", s.version, "error", message}, requestScopeAttrs(req)...)...)
	return fmt.Sprintf("%s (the request also set fields this data plane does not know: %s, so it came from a newer caller; "+
		"if the failure concerns what those fields carry, upgrade this data plane)", message, strings.Join(unknown, ", "))
}
