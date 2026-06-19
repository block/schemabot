package tern

import (
	"testing"

	"github.com/block/schemabot/pkg/engine"
	"github.com/stretchr/testify/assert"
)

// authoritativeEngine models an engine whose progress is read from authoritative
// external state (e.g. a remote online-DDL service), so it is correct on any
// instance.
type authoritativeEngine struct {
	engine.Engine
	authoritative bool
}

func (e *authoritativeEngine) ProgressIsExternallyAuthoritative() bool {
	return e.authoritative
}

// instanceLocalEngine models an engine whose progress comes from instance-local
// memory and does not declare the capability at all.
type instanceLocalEngine struct {
	engine.Engine
}

// The progress read path may query an engine directly only when that engine's
// progress is authoritative regardless of which instance answers. Instance-local
// engines must be served from shared storage instead, and an engine that does
// not declare the capability must default to storage (fail closed).
func TestEngineProgressIsExternallyAuthoritative(t *testing.T) {
	tests := []struct {
		name string
		eng  engine.Engine
		want bool
	}{
		{
			name: "engine declaring authoritative progress is queried directly",
			eng:  &authoritativeEngine{authoritative: true},
			want: true,
		},
		{
			name: "engine declaring non-authoritative progress is served from storage",
			eng:  &authoritativeEngine{authoritative: false},
			want: false,
		},
		{
			name: "engine without the capability defaults to storage",
			eng:  &instanceLocalEngine{},
			want: false,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.want, engineProgressIsExternallyAuthoritative(tc.eng))
		})
	}
}
