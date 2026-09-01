//go:build integration

package tern

import (
	"testing"

	"github.com/block/spirit/pkg/utils"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	ternv1 "github.com/block/schemabot/pkg/proto/ternv1"
	"github.com/block/schemabot/pkg/storage"
)

// requireControlRequestRequester asserts who a durable control request names.
func requireControlRequestRequester(t *testing.T, stor storage.Storage, applyID int64, operation storage.ControlOperation, want string) {
	t.Helper()
	req, err := stor.ControlRequests().GetByOperation(t.Context(), applyID, operation)
	require.NoError(t, err)
	require.NotNil(t, req, "the %s control request must exist", operation)
	assert.Equal(t, want, req.RequestedBy, "the %s control request must name %s", operation, want)
}

// The plane that records a durable control request is not always the one the
// operator talked to, so the operator's identity only reaches the row if the
// control RPC carries it. The row is what names the requester in the apply log
// and in the PR notice for a command that was accepted and never applied.
func TestLocalClient_ControlRequestsRecordTheOperatorWhoIssuedThem(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	_, dsn := setupMySQLContainer(t)
	setupStorageSchema(t, dsn)
	cleanupTasks(t, dsn)
	cleanupTestTables(t, dsn)

	ctx := t.Context()
	stor := createStorage(t, dsn)
	defer utils.CloseAndLog(stor)
	client := newControlRequestClient(t, dsn, stor)
	apply := dispatchQueuedApplyWithOptions(t, stor, client, nil)

	cutover, err := client.Cutover(ctx, &ternv1.CutoverRequest{
		ApplyId:     apply.ApplyIdentifier,
		Environment: apply.Environment,
		Caller:      "octocat",
	})
	require.NoError(t, err)
	require.True(t, cutover.Accepted, "the cutover request must be queued: %s", cutover.ErrorMessage)
	requireControlRequestRequester(t, stor, apply.ID, storage.ControlOperationCutover, "octocat")
}

// A command with no operator identity on it reached this plane from somewhere
// other than an operator — an internal resume, or a plane that predates the
// caller field. The row names the path it arrived on rather than inventing a
// person, and that name is the one the mirror will not let displace a known
// operator.
func TestLocalClient_ControlRequestWithoutACallerNamesTheForwardingPath(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	_, dsn := setupMySQLContainer(t)
	setupStorageSchema(t, dsn)
	cleanupTasks(t, dsn)
	cleanupTestTables(t, dsn)

	ctx := t.Context()
	stor := createStorage(t, dsn)
	defer utils.CloseAndLog(stor)
	client := newControlRequestClient(t, dsn, stor)
	apply := dispatchQueuedApplyWithOptions(t, stor, client, nil)

	resp, err := client.Cutover(ctx, &ternv1.CutoverRequest{
		ApplyId:     apply.ApplyIdentifier,
		Environment: apply.Environment,
	})
	require.NoError(t, err)
	require.True(t, resp.Accepted, "the cutover request must be queued: %s", resp.ErrorMessage)
	requireControlRequestRequester(t, stor, apply.ID, storage.ControlOperationCutover, storage.ForwardingControlRequestCaller)
}
