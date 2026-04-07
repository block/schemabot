package api

import (
	"testing"

	ternv1 "github.com/block/schemabot/pkg/proto/ternv1"
	"github.com/stretchr/testify/assert"
)

func TestChangeTypeRoundTrip(t *testing.T) {
	// Proto → storage → proto should round-trip correctly
	for _, ct := range []ternv1.ChangeType{
		ternv1.ChangeType_CHANGE_TYPE_CREATE,
		ternv1.ChangeType_CHANGE_TYPE_ALTER,
		ternv1.ChangeType_CHANGE_TYPE_DROP,
		ternv1.ChangeType_CHANGE_TYPE_OTHER,
	} {
		op := protoChangeTypeToOperation(ct)
		result := changeTypeToProto(op)
		assert.Equal(t, ct, result, "round-trip failed for %v (op=%q)", ct, op)
	}
}

func TestChangeTypeToProto_CaseInsensitive(t *testing.T) {
	assert.Equal(t, ternv1.ChangeType_CHANGE_TYPE_ALTER, changeTypeToProto("alter"))
	assert.Equal(t, ternv1.ChangeType_CHANGE_TYPE_ALTER, changeTypeToProto("ALTER"))
	assert.Equal(t, ternv1.ChangeType_CHANGE_TYPE_CREATE, changeTypeToProto("Create"))
	assert.Equal(t, ternv1.ChangeType_CHANGE_TYPE_OTHER, changeTypeToProto("unknown"))
}
