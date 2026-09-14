package routing

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestDisplayNames(t *testing.T) {
	tests := []struct {
		name    string
		members []ExecutionTarget
		want    []string
	}{
		{
			name: "one deployment per target is named by the deployment",
			members: []ExecutionTarget{
				{Deployment: "us-east", Target: "orders-us"},
				{Deployment: "eu-west", Target: "orders-eu"},
			},
			want: []string{"us-east", "eu-west"},
		},
		{
			name: "a deployment addressing several targets names every one of its members in full",
			members: []ExecutionTarget{
				{Deployment: "primary", Target: "testapp-001"},
				{Deployment: "primary", Target: "testapp-002"},
				{Deployment: "eu-west", Target: "orders-eu"},
			},
			want: []string{"primary/testapp-001", "primary/testapp-002", "eu-west"},
		},
		{
			// A keyed or sharded apply runs several operations of one deployment
			// against the same target. The target half would not tell them apart,
			// so it is not added.
			name: "several members on one target stay named by the deployment",
			members: []ExecutionTarget{
				{Deployment: "us-east", Target: "orders-us"},
				{Deployment: "us-east", Target: "orders-us"},
			},
			want: []string{"us-east", "us-east"},
		},
		{
			name: "members that have not recorded a target are named by the deployment",
			members: []ExecutionTarget{
				{Deployment: "us-east"},
				{Deployment: "eu-west"},
			},
			want: []string{"us-east", "eu-west"},
		},
		{
			// A member whose own target is still unrecorded is named in full when
			// its deployment addresses several: the deployment name is ambiguous
			// for it too, even though its own half is empty.
			name: "an unrecorded target under a multi-target deployment is still named in full",
			members: []ExecutionTarget{
				{Deployment: "primary", Target: "testapp-001"},
				{Deployment: "primary", Target: "testapp-002"},
				{Deployment: "primary"},
			},
			want: []string{"primary/testapp-001", "primary/testapp-002", "primary/"},
		},
		{
			name:    "no members",
			members: nil,
			want:    []string{},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, DisplayNames(tt.members))
		})
	}
}

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
