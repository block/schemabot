package commands

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestBranchFlagValidation(t *testing.T) {
	tests := []struct {
		name    string
		branch  string
		wantErr string
	}{
		{
			name:   "empty branch is allowed",
			branch: "",
		},
		{
			name:   "development branch is allowed",
			branch: "my-feature-branch",
		},
		{
			name:    "main branch is rejected",
			branch:  "main",
			wantErr: "cannot reuse the main branch",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := validateBranchFlag(tt.branch)
			if tt.wantErr != "" {
				assert.ErrorContains(t, err, tt.wantErr)
			} else {
				assert.NoError(t, err)
			}
		})
	}
}
