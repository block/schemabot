package storage

import "fmt"

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

// DeploymentRemoteApplyID resolves the single remote data-plane apply id
// shared by one deployment's operations of an apply. A remote deployment has
// exactly one data-plane apply: every operation dispatched for it attaches
// into that apply, so every operation row of the deployment records the same
// remote apply id. It returns "" when no operation of the deployment has
// recorded one yet, and an error when the deployment's operations disagree —
// two remote apply ids for one deployment means the planes have diverged
// (an in-flight apply spanning a dispatch-key rollout, or a data plane that
// lost its keyed apply) and callers must fail closed rather than pick one.
// Operations of other deployments are ignored: sibling deployments of the
// same apply legitimately carry their own distinct remote apply ids.
func DeploymentRemoteApplyID(ops []*ApplyOperation, deployment string) (string, error) {
	return deploymentSharedID(ops, deployment, (*ApplyOperation).RemoteApplyID)
}

// DeploymentExternalID resolves the deployment's shared data-plane apply id
// from the external_id column alone, without the legacy engine resume context
// fallback. Read paths that span both local and remote drives must use this
// variant: on locally driven operations engine_resume_context holds
// engine-owned resume state, which must never surface as an apply id.
// Locally driven operations never record an external_id, so this returns ""
// for them; the divergence error semantics match DeploymentRemoteApplyID.
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
