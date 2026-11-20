package webhook

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/block/schemabot/pkg/webhook/action"
)

// `freeze approve` takes exactly one override request ID, no environment, and
// reads as a sentence when it is not addressed to SchemaBot as a command.
func TestParseCommandFreezeApprove(t *testing.T) {
	p := NewCommandParser()
	tests := []struct {
		name       string
		body       string
		wantFound  bool
		wantID     string
		wantTenant string
		wantMention,
		wantProse bool
	}{
		{name: "approve", body: "schemabot freeze approve ovr-0123456789abcdef", wantFound: true, wantID: "ovr-0123456789abcdef", wantMention: true},
		{name: "capitals", body: "SchemaBot Freeze APPROVE ovr-0123456789abcdef", wantFound: true, wantID: "ovr-0123456789abcdef", wantMention: true},
		{name: "tenant", body: "schemabot freeze approve ovr-0123456789abcdef -t acme", wantFound: true, wantID: "ovr-0123456789abcdef", wantTenant: "acme", wantMention: true},
		{name: "no id", body: "schemabot freeze approve", wantMention: true},
		{name: "bare command word", body: "schemabot freeze", wantMention: true},
		{name: "placeholder is not an id", body: "schemabot freeze approve <ovr-id>", wantMention: true},
		{name: "two ids", body: "schemabot freeze approve ovr-1 ovr-2 please", wantProse: true},
		{name: "environment flag", body: "schemabot freeze approve ovr-0123456789abcdef -e staging", wantMention: true},
		{name: "other flag", body: "schemabot freeze approve ovr-0123456789abcdef --allow-unsafe", wantFound: true, wantID: "ovr-0123456789abcdef", wantMention: true},
		{name: "another subcommand is a sentence", body: "schemabot freeze is on until noon", wantProse: true},
		{name: "punctuated command word", body: "schemabot freeze. approve ovr-1", wantMention: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := p.ParseCommand(tt.body)
			assert.Equal(t, tt.wantMention, got.IsMention, "IsMention")
			assert.Equal(t, tt.wantProse, got.ProseMention, "ProseMention")
			assert.Equal(t, tt.wantFound, got.Found, "Found")
			assert.Equal(t, tt.wantID, got.OverrideID, "OverrideID")
			assert.Equal(t, tt.wantTenant, got.Tenant, "Tenant")
			if tt.wantFound {
				assert.Equal(t, action.Freeze, got.Action)
				assert.False(t, got.MissingEnv, "the request carries its environment")
			}
		})
	}
}

func TestCommandSpecs_FreezeApprove(t *testing.T) {
	spec := specByName[action.Freeze]
	assert.Equal(t, "approve", spec.Subcommand)
	assert.True(t, spec.HasOverrideID)
	assert.False(t, spec.RequiresEnv)
	assert.False(t, spec.SupportsOverride, "approving takes no override reason")
	assert.True(t, actionFansOutUnscoped(action.Freeze), "the request is in one deployment's storage, so the others skip it")
}
