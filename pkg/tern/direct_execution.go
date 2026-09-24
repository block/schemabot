// direct_execution.go carries a caller's direct execution policy across the
// gRPC hop. A control plane resolves the policy from its own configuration and
// states it on the request; the server that runs the statement judges the
// statement under the stated policy instead of re-deriving one from config it
// does not have, because a target it reached through its target resolver
// carries no registration to state a policy in.
package tern

import (
	"maps"

	"github.com/block/schemabot/pkg/engine"
	ternv1 "github.com/block/schemabot/pkg/proto/ternv1"
	"github.com/block/schemabot/pkg/storage"
)

// DirectExecutionPolicyProto renders a resolved policy for the wire. Nil in,
// nil out: a caller that states no policy leaves the executing server's own
// configuration in force.
func DirectExecutionPolicyProto(policy *storage.DirectExecutionPolicy) *ternv1.DirectExecutionPolicy {
	if policy == nil {
		return nil
	}
	return &ternv1.DirectExecutionPolicy{
		Enabled:                       policy.Enabled,
		MaxTableRows:                  policy.MaxTableRows,
		LockAcquisitionTimeoutSeconds: policy.LockAcquisitionTimeoutSeconds,
	}
}

// DirectExecutionPolicyFromProto reads a stated policy off a request into the
// form an apply records durably.
func DirectExecutionPolicyFromProto(policy *ternv1.DirectExecutionPolicy) *storage.DirectExecutionPolicy {
	if policy == nil {
		return nil
	}
	return &storage.DirectExecutionPolicy{
		Enabled:                       policy.GetEnabled(),
		MaxTableRows:                  policy.GetMaxTableRows(),
		LockAcquisitionTimeoutSeconds: policy.GetLockAcquisitionTimeoutSeconds(),
	}
}

// credentialsWithStatedDirectExecution returns the credentials the engine
// should judge this request under: the caller's policy when it stated one, and
// the credentials unchanged when it did not.
func credentialsWithStatedDirectExecution(creds *engine.Credentials, policy *ternv1.DirectExecutionPolicy) *engine.Credentials {
	if policy == nil {
		return creds
	}
	return credentialsWithDirectExecution(creds, DirectExecutionPolicyFromProto(policy).EngineMetadata())
}

// applyRequestWithStatedDirectExecution returns the request routed under the
// policy its apply was admitted with. The policy travels on the apply's
// durable options rather than on the client, because the drive that reaches
// this point can be a later one, on another pod, after a restart: re-deriving
// a policy here would let a configuration change between dispatch and drive
// decide how a statement already admitted to the queue runs.
func applyRequestWithStatedDirectExecution(req *engine.ApplyRequest) *engine.ApplyRequest {
	policy := storage.ApplyOptionsFromMap(req.Options).DirectExecution
	if policy == nil {
		return req
	}
	// The request is copied rather than written through: its credentials are
	// the client's, shared by every request this target serves.
	stated := *req
	stated.Credentials = credentialsWithDirectExecution(req.Credentials, policy.EngineMetadata())
	return &stated
}

// credentialsWithDirectExecution returns a copy of creds carrying policy as
// its direct execution policy.
//
// The replacement is whole rather than key by key. A surface that states a
// policy states all of it, so an enabled flag can never pair with a row bound
// that came from somewhere else — and the bound is the only thing standing
// between a refused statement and an unbounded write outage. An empty policy
// therefore clears the keys rather than leaving the previous ones in place.
func credentialsWithDirectExecution(creds *engine.Credentials, policy map[string]string) *engine.Credentials {
	stated := engine.Credentials{}
	if creds != nil {
		stated = *creds
	}
	metadata := maps.Clone(stated.Metadata)
	if metadata == nil {
		metadata = map[string]string{}
	}
	for _, key := range engine.DirectExecutionKeys() {
		delete(metadata, key)
	}
	maps.Copy(metadata, policy)
	stated.Metadata = metadata
	return &stated
}
