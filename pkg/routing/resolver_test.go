package routing

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestMultiTargetDeployments(t *testing.T) {
	tests := []struct {
		name    string
		members []ExecutionTarget
		want    map[string]bool
	}{
		{
			name: "a deployment addressing two targets is multi-target",
			members: []ExecutionTarget{
				{Deployment: "eu", Target: "testapp-001"},
				{Deployment: "eu", Target: "testapp-002"},
			},
			want: map[string]bool{"eu": true},
		},
		{
			// A keyed or sharded apply puts several members on one target. The
			// target is the same for all of them, so naming it would add a
			// component that still does not tell them apart.
			name: "repeated members on one target are not multi-target",
			members: []ExecutionTarget{
				{Deployment: "eu", Target: "testapp"},
				{Deployment: "eu", Target: "testapp"},
			},
			want: map[string]bool{},
		},
		{
			// Mirrored deployments each address one target, so no deployment of
			// the set has to name it.
			name: "one target per deployment is not multi-target",
			members: []ExecutionTarget{
				{Deployment: "eu", Target: "testapp"},
				{Deployment: "us", Target: "testapp"},
			},
			want: map[string]bool{},
		},
		{
			name: "only the deployment with several targets is reported",
			members: []ExecutionTarget{
				{Deployment: "eu", Target: "testapp-001"},
				{Deployment: "eu", Target: "testapp-002"},
				{Deployment: "us", Target: "testapp"},
			},
			want: map[string]bool{"eu": true},
		},
		{
			// A target the resolver left blank names nothing, so it cannot make
			// a deployment's members distinguishable by target.
			name: "blank targets do not count toward the set",
			members: []ExecutionTarget{
				{Deployment: "eu", Target: "testapp"},
				{Deployment: "eu", Target: ""},
			},
			want: map[string]bool{},
		},
		{
			name:    "no members",
			members: nil,
			want:    map[string]bool{},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, MultiTargetDeployments(tt.members))
		})
	}
}
