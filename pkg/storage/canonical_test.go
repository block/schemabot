package storage

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestCanonicalKey(t *testing.T) {
	tests := []struct {
		name string
		key  string
		want string
	}{
		{name: "mixed case", key: "MixedCase/Sample-Repo", want: "mixedcase/sample-repo"},
		{name: "lowercase", key: "mixedcase/sample-repo", want: "mixedcase/sample-repo"},
		{name: "empty", key: "", want: ""},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, CanonicalKey(tt.key))
		})
	}
}
