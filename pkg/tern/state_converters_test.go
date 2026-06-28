package tern

import (
	"testing"

	"github.com/block/schemabot/pkg/apitypes"
	ternv1 "github.com/block/schemabot/pkg/proto/ternv1"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

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

	changes, err := apitypes.ParseVSchemaChanges(got)
	require.NoError(t, err)
	require.Len(t, changes, 1)
	assert.Equal(t, "commerce_sharded", changes[0].Namespace)
	assert.Equal(t, "applied", changes[0].Status)
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
