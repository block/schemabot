package storage

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

const (
	operatorApplyID = "apply-0123456789abcdef"
	remoteApplyID   = "apply-fedcba9876543210"
	remoteOpApplyID = "apply-aaaabbbbccccdddd"
)

// A rejection authored by a remote data plane names the identifier that data
// plane knows the schema change by. An operator reading the reply — in a PR
// comment, the CLI, or a stored control request — has no way to resolve that
// identifier, so every remote identifier is translated to the one they asked
// about — including a claimed operation's, which is the only remote identifier a
// multi-operation apply has.
func TestApplyOperatorFacingMessage(t *testing.T) {
	tests := []struct {
		name              string
		apply             *Apply
		message           string
		additionalRemotes []string
		want              string
	}{
		{
			name:    "rewrites the remote identifier the data plane named",
			apply:   &Apply{ApplyIdentifier: operatorApplyID, ExternalID: remoteApplyID},
			message: "Schema change " + remoteApplyID + " is completed; volume can only be adjusted while it is running",
			want:    "Schema change " + operatorApplyID + " is completed; volume can only be adjusted while it is running",
		},
		{
			name:    "rewrites every occurrence in one message",
			apply:   &Apply{ApplyIdentifier: operatorApplyID, ExternalID: remoteApplyID},
			message: remoteApplyID + " cannot be reverted; " + remoteApplyID + " already finalized",
			want:    operatorApplyID + " cannot be reverted; " + operatorApplyID + " already finalized",
		},
		{
			name:              "rewrites a claimed operation's remote identifier when the parent carries none",
			apply:             &Apply{ApplyIdentifier: operatorApplyID},
			message:           "revert was not accepted: " + remoteOpApplyID + " is not in its revert window",
			additionalRemotes: []string{remoteOpApplyID},
			want:              "revert was not accepted: " + operatorApplyID + " is not in its revert window",
		},
		{
			name:              "rewrites the apply and operation identifiers together",
			apply:             &Apply{ApplyIdentifier: operatorApplyID, ExternalID: remoteApplyID},
			message:           remoteApplyID + " / " + remoteOpApplyID + " were both rejected",
			additionalRemotes: []string{remoteOpApplyID},
			want:              operatorApplyID + " / " + operatorApplyID + " were both rejected",
		},
		{
			name:    "leaves a local apply's message untouched",
			apply:   &Apply{ApplyIdentifier: operatorApplyID},
			message: "Schema change " + operatorApplyID + " is completed",
			want:    "Schema change " + operatorApplyID + " is completed",
		},
		{
			name:    "returns an empty message unchanged",
			apply:   &Apply{ApplyIdentifier: operatorApplyID, ExternalID: remoteApplyID},
			message: "",
			want:    "",
		},
		{
			name:    "returns the message unchanged when the apply has no identifier",
			apply:   &Apply{ExternalID: remoteApplyID},
			message: remoteApplyID + " was rejected",
			want:    remoteApplyID + " was rejected",
		},
		{
			name:    "returns the message unchanged for a nil apply",
			apply:   nil,
			message: remoteApplyID + " was rejected",
			want:    remoteApplyID + " was rejected",
		},
		{
			name:              "ignores empty remote identifiers",
			apply:             &Apply{ApplyIdentifier: operatorApplyID},
			message:           "cutover request was not applied because apply is completed",
			additionalRemotes: []string{"", ""},
			want:              "cutover request was not applied because apply is completed",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := tt.apply.OperatorFacingMessage(tt.message, tt.additionalRemotes...)
			assert.Equal(t, tt.want, got)
		})
	}
}
