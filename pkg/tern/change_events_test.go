package tern

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	ternv1 "github.com/block/schemabot/pkg/proto/ternv1"
	"github.com/block/schemabot/pkg/storage"
)

func TestDispatchCarriesCorrelationForOperationsThatAttachLater(t *testing.T) {
	client := &GRPCClient{changeEventApp: func(string) string { return "storefront" }}
	apply := &storage.Apply{ApplyIdentifier: "apply-control", Database: "orders", Repository: "octocat/example", PullRequest: 5}
	scope := applyTaskScope{
		operation:              &storage.ApplyOperation{Deployment: "region-a", OperationKey: "orders"},
		multiOperation:         true,
		deploymentOperationIDs: map[string]int64{"orders": 101, "customers": 102},
	}
	wire, err := proto.Marshal(&ternv1.ApplyRequest{ChangeEventDispatch: client.changeEventDispatch(apply, scope)})
	require.NoError(t, err)
	var remote ternv1.ApplyRequest
	require.NoError(t, proto.Unmarshal(wire, &remote))
	stored := &storage.Apply{Options: storage.MarshalApplyOptions(storage.ApplyOptions{
		ChangeEventDispatch: storedChangeEventDispatch(remote.GetChangeEventDispatch()),
	})}
	options := stored.GetOptions()
	require.NotNil(t, options.ChangeEventDispatch)
	assert.Equal(t, "apply-control", options.ChangeEventDispatch.ApplyID)
	assert.Equal(t, "storefront", options.ChangeEventDispatch.App)
	assert.Equal(t, "region-a", options.ChangeEventDispatch.Deployment)
	assert.EqualValues(t, 102, options.ChangeEventDispatch.OperationIDs["customers"])
	assert.Equal(t, "https://github.com/octocat/example/pull/5", options.ChangeEventDispatch.PullRequestURL)
	assert.NotContains(t, options.Map(), "change_event_dispatch", "metadata is excluded from engine options")
}
