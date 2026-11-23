package tern

import (
	"maps"

	ternv1 "github.com/block/schemabot/pkg/proto/ternv1"
	"github.com/block/schemabot/pkg/storage"
)

func (c *GRPCClient) changeEventDispatch(apply *storage.Apply, scope applyTaskScope) *ternv1.ChangeEventDispatch {
	if scope.operation == nil {
		return nil
	}
	dispatch := &ternv1.ChangeEventDispatch{
		ApplyId: apply.ApplyIdentifier, App: apply.Database, Deployment: scope.operation.Deployment,
		OperationIds: scope.deploymentOperationIDs, PullRequestUrl: apply.ChangeEventPullRequestURL(),
	}
	if c.changeEventApp != nil {
		if app := c.changeEventApp(apply.Database); app != "" {
			dispatch.App = app
		}
	}
	if !scope.usesOperationRemoteResume() {
		dispatch.AttemptOffset = int32(scope.operation.Attempt)
	}
	return dispatch
}

func storedChangeEventDispatch(dispatch *ternv1.ChangeEventDispatch) *storage.ChangeEventDispatch {
	if dispatch.GetApplyId() == "" || dispatch.GetDeployment() == "" || len(dispatch.GetOperationIds()) == 0 {
		return nil
	}
	return &storage.ChangeEventDispatch{
		ApplyID: dispatch.GetApplyId(), App: dispatch.GetApp(), Deployment: dispatch.GetDeployment(),
		OperationIDs: maps.Clone(dispatch.GetOperationIds()), PullRequestURL: dispatch.GetPullRequestUrl(),
		AttemptOffset: int(dispatch.GetAttemptOffset()),
	}
}
