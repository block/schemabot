package commands

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestValidateSettingKey(t *testing.T) {
	tests := []struct {
		name    string
		key     string
		wantErr bool
	}{
		{name: "known setting", key: "require_review", wantErr: false},
		{name: "known setting spirit", key: "spirit_debug_logs", wantErr: false},
		{name: "scoped setting", key: "require_review:octocat/hello-world", wantErr: false},
		{name: "unknown setting", key: "nonexistent", wantErr: true},
		{name: "unknown scoped setting", key: "nonexistent:octocat/repo", wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := validateSettingKey(tt.key)
			if tt.wantErr {
				require.Error(t, err)
				assert.Contains(t, err.Error(), "unknown setting")
			} else {
				require.NoError(t, err)
			}
		})
	}
}
