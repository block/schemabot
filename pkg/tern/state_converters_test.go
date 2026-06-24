package tern

import (
	"testing"

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
	m, err := PSDisplayMetadata(`{"branch_name":"schemabot-db-1","deploy_request_url":"https://app/deploy/9","is_instant":true,"deferred_deploy":true,"vschema_status":"applying"}`)
	require.NoError(t, err)
	require.NotNil(t, m)
	assert.Equal(t, "schemabot-db-1", m["branch_name"])
	assert.Equal(t, "https://app/deploy/9", m["deploy_request_url"])
	assert.Equal(t, "true", m["is_instant"])
	assert.Equal(t, "true", m["deferred_deploy"])
	assert.Equal(t, "applying", m["vschema_status"])

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
