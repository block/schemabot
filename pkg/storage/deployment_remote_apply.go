package storage

import (
	"fmt"

	"github.com/block/schemabot/pkg/routing"
)

// RemoteApplyID returns the remote data-plane apply identifier recorded on
// this operation row, or "" when none has been recorded yet. external_id is
// the canonical column; engine_resume_context is the legacy carrier remote
// drives persisted the id into before the external-id columns existed, so
// readers fall back to it. Meaningful only for operations dispatched to a
// remote (gRPC) data plane — on locally driven operations
// engine_resume_context holds engine-owned resume state, not an apply id, so
// callers must gate on the dispatch shape before treating the result as a
// remote apply id.
func (op *ApplyOperation) RemoteApplyID() string {
	if op == nil {
		return ""
	}
	if op.ExternalID != "" {
		return op.ExternalID
	}
	return op.EngineResumeContext
}

// MemberRemoteApplyID resolves the single remote data-plane apply id shared by
// the operations of one rollout member of an apply: member's deployment, and
// its target when that deployment addresses more than one. A data-plane apply
// drives exactly one target, so every operation dispatched for a member
// attaches into that member's one apply and records the same remote apply id.
// It returns "" when no operation of the member has recorded one yet, and an
// error when the member's operations disagree — two remote apply ids for one
// member means the planes have diverged (an in-flight apply spanning a
// dispatch-key rollout, or a data plane that lost its keyed apply) and callers
// must fail closed rather than pick one. Operations of other members are
// ignored: sibling deployments, and sibling targets of one deployment,
// legitimately carry their own distinct remote apply ids.
func MemberRemoteApplyID(ops []*ApplyOperation, member *ApplyOperation) (string, error) {
	if member == nil {
		return "", fmt.Errorf("resolve member remote apply id: no member operation")
	}
	return deploymentSharedID(RolloutMemberOperations(ops, member), member.Deployment, (*ApplyOperation).RemoteApplyID)
}

// RolloutMemberOperations returns the operations of ops that belong to the same
// rollout member as member. A deployment that addresses one target is one
// member, so every operation of the deployment is returned, whatever target
// each row names. A deployment that addresses several targets is one member per
// target, and only the operations naming member's target are returned. The
// multi-target test is the one the planner qualifies operation keys with, so
// the two can never disagree about which operations make up a member.
func RolloutMemberOperations(ops []*ApplyOperation, member *ApplyOperation) []*ApplyOperation {
	if member == nil {
		return nil
	}
	multiTarget := DeploymentAddressesSeveralTargets(ops, member.Deployment)
	matched := make([]*ApplyOperation, 0, len(ops))
	for _, op := range ops {
		if op == nil || op.Deployment != member.Deployment {
			continue
		}
		if multiTarget && op.Target != member.Target {
			continue
		}
		matched = append(matched, op)
	}
	return matched
}

// DeploymentAddressesSeveralTargets reports whether the operations of one
// deployment name more than one distinct target, which is the point at which
// the deployment stops identifying one rollout member (see
// routing.MultiTargetDeployments).
func DeploymentAddressesSeveralTargets(ops []*ApplyOperation, deployment string) bool {
	targets := make([]routing.ExecutionTarget, 0, len(ops))
	for _, op := range ops {
		if op == nil {
			continue
		}
		targets = append(targets, routing.ExecutionTarget{Deployment: op.Deployment, Target: op.Target})
	}
	return routing.MultiTargetDeployments(targets)[deployment]
}

// DeploymentExternalID resolves the deployment's shared data-plane apply id
// from the external_id column alone, without the legacy engine resume context
// fallback. Read paths that span both local and remote drives must use this
// variant: on locally driven operations engine_resume_context holds
// engine-owned resume state, which must never surface as an apply id.
// Locally driven operations never record an external_id, so this returns ""
// for them; the divergence error semantics match MemberRemoteApplyID.
func DeploymentExternalID(ops []*ApplyOperation, deployment string) (string, error) {
	return deploymentSharedID(ops, deployment, func(op *ApplyOperation) string { return op.ExternalID })
}

// deploymentSharedID resolves the single id shared by one deployment's
// operations, reading each row's candidate id with idOf. It returns "" when
// no operation of the deployment has recorded an id yet, and an error when
// the deployment's operations disagree. Operations of other deployments are
// ignored.
func deploymentSharedID(ops []*ApplyOperation, deployment string, idOf func(*ApplyOperation) string) (string, error) {
	shared := ""
	for _, op := range ops {
		if op == nil || op.Deployment != deployment {
			continue
		}
		id := idOf(op)
		if id == "" {
			continue
		}
		if shared == "" {
			shared = id
			continue
		}
		if id != shared {
			// Pin the offending row by apply_operation id: the operation key is
			// the readable handle but is legitimately empty on a legacy
			// single-operation row — the exact shape a dispatch-key rollout
			// leaves disagreeing.
			return "", fmt.Errorf("deployment %q operations record more than one data-plane apply id (%q on apply_operation %d (operation key %q) disagrees with %q)", deployment, id, op.ID, op.OperationKey, shared)
		}
	}
	return shared, nil
}
