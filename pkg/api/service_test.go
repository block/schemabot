package api

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestTernConfig_Endpoint_SingleDeployment(t *testing.T) {
	config := TernConfig{
		"default": {
			"staging":    "http://staging:8080",
			"production": "http://production:8080",
		},
	}

	tests := []struct {
		name        string
		deployment  string
		environment string
		want        string
		wantErr     bool
	}{
		{
			name:        "staging endpoint with empty deployment",
			deployment:  "",
			environment: "staging",
			want:        "http://staging:8080",
		},
		{
			name:        "production endpoint with empty deployment",
			deployment:  "",
			environment: "production",
			want:        "http://production:8080",
		},
		{
			name:        "staging endpoint with explicit default deployment",
			deployment:  "default",
			environment: "staging",
			want:        "http://staging:8080",
		},
		{
			name:        "unknown environment",
			deployment:  "",
			environment: "dev",
			wantErr:     true,
		},
		{
			name:        "unknown deployment",
			deployment:  "unknown",
			environment: "staging",
			wantErr:     true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := config.Endpoint(tt.deployment, tt.environment)
			if tt.wantErr {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
				assert.Equal(t, tt.want, got)
			}
		})
	}
}

func TestTernConfig_Endpoint_MultiDeployment(t *testing.T) {
	config := TernConfig{
		"a": {
			"staging":    "http://tern-a-staging:8080",
			"production": "http://tern-a-production:8080",
		},
		"b": {
			"staging":    "http://tern-b-staging:8080",
			"production": "http://tern-b-production:8080",
		},
	}

	tests := []struct {
		name        string
		deployment  string
		environment string
		want        string
		wantErr     bool
	}{
		{
			name:        "deployment a staging",
			deployment:  "a",
			environment: "staging",
			want:        "http://tern-a-staging:8080",
		},
		{
			name:        "deployment a production",
			deployment:  "a",
			environment: "production",
			want:        "http://tern-a-production:8080",
		},
		{
			name:        "deployment b staging",
			deployment:  "b",
			environment: "staging",
			want:        "http://tern-b-staging:8080",
		},
		{
			name:        "deployment b production",
			deployment:  "b",
			environment: "production",
			want:        "http://tern-b-production:8080",
		},
		{
			name:        "unknown deployment",
			deployment:  "unknown",
			environment: "staging",
			wantErr:     true,
		},
		{
			name:        "unknown environment for deployment",
			deployment:  "a",
			environment: "dev",
			wantErr:     true,
		},
		{
			name:        "empty deployment falls back to default (not found)",
			deployment:  "",
			environment: "staging",
			wantErr:     true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := config.Endpoint(tt.deployment, tt.environment)
			if tt.wantErr {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
				assert.Equal(t, tt.want, got)
			}
		})
	}
}

func TestTernConfig_Endpoint_EmptyEndpoint(t *testing.T) {
	config := TernConfig{
		"default": {
			"staging":    "http://staging:8080",
			"production": "", // empty endpoint
		},
	}

	_, err := config.Endpoint("", "production")
	assert.Error(t, err)
}
