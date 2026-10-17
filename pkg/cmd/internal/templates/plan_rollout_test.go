package templates

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/block/schemabot/pkg/apitypes"
	"github.com/block/schemabot/pkg/presentation"
)

// Past the inline limit, a refused rollout counts the narrowed applies it
// leaves out, singular for one and plural for several.
func TestWriteRolloutApplyRefused_CountsRerunsPastTheInlineLimit(t *testing.T) {
	for _, tc := range []struct {
		members int
		want    string
	}{
		{members: memberNamesInlineLimit + 1, want: "  and the same for 1 more target\n"},
		{members: memberNamesInlineLimit + 2, want: "  and the same for 2 more targets\n"},
	} {
		var refused []*apitypes.PlanMemberRefusalResponse
		var reruns []string
		for i := range tc.members {
			name := fmt.Sprintf("payments-%03d", i+1)
			refused = append(refused, &apitypes.PlanMemberRefusalResponse{Member: name, Target: name, Reason: apitypes.PlanMemberNeedsTarget, Detail: "planned against its own schema"})
			reruns = append(reruns, "apply -e production --target "+name)
		}
		out := captureStdout(t, func() { WriteRolloutApplyRefused(presentation.TargetNoun, refused, reruns) })
		assert.Contains(t, out, tc.want, out)
	}
}
