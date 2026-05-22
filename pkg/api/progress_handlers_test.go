package api

import (
	"testing"

	"github.com/stretchr/testify/assert"

	ternv1 "github.com/block/schemabot/pkg/proto/ternv1"
)

func TestChangeTypeToStringMapsVSchema(t *testing.T) {
	assert.Equal(t, "vschema_update", changeTypeToString(ternv1.ChangeType_CHANGE_TYPE_VSCHEMA))
}
