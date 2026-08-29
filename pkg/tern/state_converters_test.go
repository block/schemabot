package tern

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/block/schemabot/pkg/apitypes"
	"github.com/block/schemabot/pkg/ddl"
	"github.com/block/schemabot/pkg/engine"
	ternv1 "github.com/block/schemabot/pkg/proto/ternv1"
	"github.com/block/schemabot/pkg/state"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Every DDL operation with a dedicated proto change type survives the
// storage-op → proto → storage-op round trip exactly, so a remote deployment
// rebuilding a plan from a dispatch stores the same operation the planner
// classified. Index operations must keep their own change types: collapsing
// them into table create/drop would make a DROP INDEX materialize as a table
// drop and trip the fail-closed unsafe opt-in gate.
func TestChangeTypeProtoRoundTrip(t *testing.T) {
	for _, op := range []string{
		ddl.StatementTypeToOp(ddl.StatementCreateTable),
		ddl.StatementTypeToOp(ddl.StatementAlterTable),
		ddl.StatementTypeToOp(ddl.StatementDropTable),
		ddl.StatementTypeToOp(ddl.StatementCreateIndex),
		ddl.StatementTypeToOp(ddl.StatementDropIndex),
		ddl.StatementTypeToOp(ddl.StatementRenameTable),
		ddl.StatementTypeToOp(ddl.StatementTruncateTable),
		ddl.StatementTypeToOp(ddl.StatementCreateView),
		"vschema_update",
	} {
		assert.Equal(t, op, protoChangeTypeToDDLAction(ddlActionToProtoChangeType(op)),
			"round-trip failed for op %q", op)
	}
	assert.Equal(t, ternv1.ChangeType_CHANGE_TYPE_OTHER, ddlActionToProtoChangeType("unknown"))
}

// The reverting state round-trips across the engine, task, and proto boundaries
// so a revert in progress is reported end-to-end as "reverting" rather than
// collapsing to a generic running apply.
func TestRevertingStateConversions(t *testing.T) {
	assert.Equal(t, state.Task.Reverting, engineStateToStorage(engine.StateReverting))
	assert.Equal(t, ternv1.State_STATE_REVERTING, storageStateToProto(state.Task.Reverting))
	assert.Equal(t, ternv1.State_STATE_REVERTING, storageStateToProto(state.Apply.Reverting))
	assert.Equal(t, state.Apply.Reverting, ProtoStateToStorage(ternv1.State_STATE_REVERTING))
	assert.False(t, isTerminalProtoState(ternv1.State_STATE_REVERTING), "reverting is in-flight, not terminal")
}

// A retryable pause round-trips across the wire as its own state so the
// polling plane can tell "parked for the serving plane's own retry" apart from
// a settled failure without inspecting per-table statuses.
func TestFailedRetryableStateConversions(t *testing.T) {
	assert.Equal(t, ternv1.State_STATE_FAILED_RETRYABLE, storageStateToProto(state.Task.FailedRetryable))
	assert.Equal(t, ternv1.State_STATE_FAILED_RETRYABLE, storageStateToProto(state.Apply.FailedRetryable))
	assert.Equal(t, state.Apply.FailedRetryable, ProtoStateToStorage(ternv1.State_STATE_FAILED_RETRYABLE))
	assert.False(t, isTerminalProtoState(ternv1.State_STATE_FAILED_RETRYABLE), "a retryable pause is in-flight, not terminal")
	assert.True(t, isTerminalProtoState(ternv1.State_STATE_FAILED), "a settled failure stays terminal")
}

// The post-copy phases round-trip across the task and proto boundaries so a
// drain or verify in progress is reported end-to-end as its phase rather than
// collapsing to pending on the wire.
func TestPostCopyPhaseStateConversions(t *testing.T) {
	cases := []struct {
		task  string
		apply string
		proto ternv1.State
	}{
		{state.Task.CatchingUp, state.Apply.CatchingUp, ternv1.State_STATE_CATCHING_UP},
		{state.Task.Checksumming, state.Apply.Checksumming, ternv1.State_STATE_CHECKSUMMING},
		{state.Task.PostChecksum, state.Apply.PostChecksum, ternv1.State_STATE_POST_CHECKSUM},
	}
	for _, tc := range cases {
		assert.Equal(t, tc.proto, storageStateToProto(tc.task))
		assert.Equal(t, tc.apply, ProtoStateToStorage(tc.proto))
		assert.False(t, isTerminalProtoState(tc.proto), "%s is in-flight, not terminal", tc.task)
	}
}

// A null namespace value in the proto map (e.g. JSON `{"default": null}`)
// converts to an empty namespace rather than dereferencing a nil pointer.
func TestProtoToSchemaFiles_NilNamespaceValue(t *testing.T) {
	result := protoToSchemaFiles(map[string]*ternv1.SchemaFiles{
		"default": nil,
		"payments": {Files: map[string]string{
			"users.sql": "CREATE TABLE users (id bigint primary key)",
		}},
	})

	require.Contains(t, result, "default")
	require.NotNil(t, result["default"])
	assert.Empty(t, result["default"].Files)

	require.Contains(t, result, "payments")
	require.NotNil(t, result["payments"])
	assert.Equal(t, map[string]string{
		"users.sql": "CREATE TABLE users (id bigint primary key)",
	}, result["payments"].Files)
}

func TestPSDisplayMetadata(t *testing.T) {
	// A populated resume-state blob projects every display field the renderer
	// surfaces for a PlanetScale apply.
	m, err := PSDisplayMetadata(`{"branch_name":"schemabot-db-1","deploy_request_url":"https://app/deploy/9","is_instant":true,"deferred_deploy":true,"vschema_status":"applying","vschema_diffs":[{"namespace":"commerce","diff":"+ \"x\": {}"}]}`)
	require.NoError(t, err)
	require.NotNil(t, m)
	assert.Equal(t, "schemabot-db-1", m["branch_name"])
	assert.Equal(t, "https://app/deploy/9", m["deploy_request_url"])
	assert.Equal(t, "true", m["is_instant"])
	assert.Equal(t, "true", m["deferred_deploy"])

	// Per-keyspace VSchema is projected as JSON under a shared key; decode it
	// back to assert each keyspace carries the deploy-level status and its diff.
	changes, err := apitypes.ParseVSchemaChanges(m)
	require.NoError(t, err)
	require.Len(t, changes, 1)
	assert.Equal(t, "commerce", changes[0].Namespace)
	assert.Equal(t, "applying", changes[0].Status)
	assert.Equal(t, `+ "x": {}`, changes[0].Diff)

	// An empty blob yields no fields and no error.
	m, err = PSDisplayMetadata("")
	require.NoError(t, err)
	assert.Nil(t, m)

	// A blob with no display fields set yields a nil map, never an empty alloc.
	m, err = PSDisplayMetadata(`{"deploy_request_id":42}`)
	require.NoError(t, err)
	assert.Nil(t, m)

	// Malformed JSON surfaces an error rather than silently dropping fields.
	_, err = PSDisplayMetadata(`{not json`)
	require.Error(t, err)

	// A persisted revert_expires_at is surfaced for the revert-window countdown.
	m, err = PSDisplayMetadata(`{"branch_name":"b","revert_expires_at":"2026-06-29T18:30:00Z"}`)
	require.NoError(t, err)
	require.NotNil(t, m)
	assert.Equal(t, "2026-06-29T18:30:00Z", m["revert_expires_at"])
}

// setRevertExpiresAtMetadata must set revert_expires_at without dropping engine
// fields the storage struct does not model — it merges at the JSON-object level
// rather than re-encoding the typed struct, so an arbitrary unmodeled key
// survives.
func TestSetRevertExpiresAtMetadata(t *testing.T) {
	expires := time.Date(2026, 6, 29, 18, 30, 0, 0, time.UTC)
	in := `{"branch_name":"b","unmodeled_engine_field":{"commerce":{"started_at":"2026-06-29T18:00:00Z"}}}`

	out, err := setRevertExpiresAtMetadata(in, expires)
	require.NoError(t, err)

	var obj map[string]json.RawMessage
	require.NoError(t, json.Unmarshal([]byte(out), &obj))
	assert.JSONEq(t, `"2026-06-29T18:30:00Z"`, string(obj["revert_expires_at"]))
	assert.Contains(t, obj, "unmodeled_engine_field", "unmodeled engine field must survive the rewrite")
	assert.JSONEq(t, `"b"`, string(obj["branch_name"]))

	// Surfaced back through the display projection.
	m, err := PSDisplayMetadata(out)
	require.NoError(t, err)
	assert.Equal(t, "2026-06-29T18:30:00Z", m["revert_expires_at"])

	// Empty input starts a fresh object rather than erroring.
	out, err = setRevertExpiresAtMetadata("", expires)
	require.NoError(t, err)
	assert.Contains(t, out, "revert_expires_at")
}

// The display map a data-plane progress poll returns round-trips through
// PSDisplayMetadataStorageBlob back into the same display fields when read
// via PSDisplayMetadata — the path the control plane uses to mirror a remote
// apply's deploy-request URL and VSchema status onto its operation so the PR
// comment can render them.
func TestPSDisplayMetadataStorageBlobRoundTrip(t *testing.T) {
	encodedVSchema, err := apitypes.EncodeVSchemaChanges([]apitypes.VSchemaChange{
		{Namespace: "commerce_sharded", Status: "applied", Diff: `+ "xxhash": {}`},
	})
	require.NoError(t, err)
	display := map[string]string{
		"branch_name":                      "schemabot-db-7",
		"deploy_request_url":               "https://app.planetscale.com/org/db/deploy-requests/106",
		"is_instant":                       "true",
		"revert_expires_at":                "2026-06-29T18:30:00Z",
		apitypes.VSchemaChangesMetadataKey: encodedVSchema,
	}

	blob, err := PSDisplayMetadataStorageBlob(display)
	require.NoError(t, err)
	require.NotEmpty(t, blob)

	got, err := PSDisplayMetadata(blob)
	require.NoError(t, err)
	assert.Equal(t, "https://app.planetscale.com/org/db/deploy-requests/106", got["deploy_request_url"])
	assert.Equal(t, "schemabot-db-7", got["branch_name"])
	assert.Equal(t, "true", got["is_instant"])
	assert.Equal(t, "2026-06-29T18:30:00Z", got["revert_expires_at"])

	changes, err := apitypes.ParseVSchemaChanges(got)
	require.NoError(t, err)
	require.Len(t, changes, 1)
	assert.Equal(t, "commerce_sharded", changes[0].Namespace)
	assert.Equal(t, "applied", changes[0].Status)
}

// The revert-window deadline is a display field in its own right: the
// "nothing worth storing" guard counts revert_expires_at like the other
// display fields, so a map carrying only the deadline still yields a blob
// rather than being discarded as empty. (Live progress responses carry the
// deploy-request URL alongside the deadline; this pins the guard itself.)
func TestPSDisplayMetadataStorageBlobRevertExpiresOnly(t *testing.T) {
	blob, err := PSDisplayMetadataStorageBlob(map[string]string{
		"revert_expires_at": "2026-06-29T18:30:00Z",
	})
	require.NoError(t, err)
	require.NotEmpty(t, blob)

	got, err := PSDisplayMetadata(blob)
	require.NoError(t, err)
	assert.Equal(t, "2026-06-29T18:30:00Z", got["revert_expires_at"])
}

// A malformed revert-window deadline fails the whole conversion rather than
// storing a corrupt or partial blob: the mirror logs the error and persists
// nothing from that poll, leaving any previously stored blob as-is. A value
// that no writer emits is a loud version-skew tripwire, not display state to
// degrade around.
func TestPSDisplayMetadataStorageBlobBadRevertExpires(t *testing.T) {
	_, err := PSDisplayMetadataStorageBlob(map[string]string{
		"deploy_request_url": "https://app.planetscale.com/org/db/deploy-requests/106",
		"revert_expires_at":  "not-a-timestamp",
	})
	require.ErrorContains(t, err, "revert_expires_at")
}

// A display map with nothing worth storing yields an empty blob, so the caller
// leaves the operation's metadata untouched rather than clobbering it with "{}".
func TestPSDisplayMetadataStorageBlobEmpty(t *testing.T) {
	blob, err := PSDisplayMetadataStorageBlob(nil)
	require.NoError(t, err)
	assert.Empty(t, blob)

	blob, err = PSDisplayMetadataStorageBlob(map[string]string{"volume": "2"})
	require.NoError(t, err)
	assert.Empty(t, blob)
}
