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
	shared := ""
	for _, op := range ops {
		if op == nil || op.Deployment != deployment {
			continue
		}
		id := op.RemoteApplyID()
		if id == "" {
			continue
		}
		if shared == "" {
			shared = id
			continue
		}
		if id != shared {
			return "", fmt.Errorf("deployment %q operations record more than one remote apply id (%q on apply_operation %d disagrees with %q)", deployment, id, op.ID, shared)
		}
	}
	return shared, nil
}
