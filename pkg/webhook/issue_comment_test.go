package webhook

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

// The support-channel footer is offered on comments that report a problem the
// operator may need help with. The apply-blocked refusal (unsafe changes
// without --allow-unsafe) qualifies, in both singular and plural forms. The
// plan comment's advisory unsafe-change summary does not: it is informational,
// appears on routine plans, and would otherwise put the footer on nearly every
// plan for schemas that trip a common lint.
func TestShouldShowSupportChannel_UnsafeChanges(t *testing.T) {
	tests := []struct {
		name string
		body string
		want bool
	}{
		{
			name: "plan advisory summary plural",
			body: "## MySQL Schema Change Plan\n\n⚠️ **Issues**: **3** unsafe changes detected\n- `orders`: DROP INDEX\n",
			want: false,
		},
		{
			name: "plan advisory summary singular",
			body: "## MySQL Schema Change Plan\n\n⚠️ **Issues**: **1** unsafe change detected\n- `orders`: DROP INDEX\n",
			want: false,
		},
		{
			name: "apply-blocked header plural",
			body: "## MySQL Schema Change Plan\n\n**⛔ 3 Unsafe Changes Detected:**\n- `orders`: DROP INDEX\n",
			want: true,
		},
		{
			name: "apply-blocked header singular",
			body: "## MySQL Schema Change Plan\n\n**⛔ 1 Unsafe Change Detected:**\n- `orders`: DROP INDEX\n",
			want: true,
		},
		{
			name: "clean plan with no issues",
			body: "## MySQL Schema Change Plan\n\n📋 **Plan**: **2** tables to create\n",
			want: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, shouldShowSupportChannel(tt.body))
		})
	}
}
