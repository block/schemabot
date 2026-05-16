package api

import (
	"log/slog"
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

// TestDeploymentResolution_ConsistentAcrossHandlers verifies that the recovery
// worker and webhook handlers resolve to the same TernClient cache key for a
// given database. This prevents the bug where different code paths created
// separate LocalClient instances with separate Spirit engine instances, causing
// "no active schema change to cutover" after crash recovery.
func TestDeploymentResolution_ConsistentAcrossHandlers(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(nil, nil))

	// Local mode: database registered in config, no TernDeployments
	svc := New(nil, &ServerConfig{
		Databases: map[string]DatabaseConfig{
			"orders": {Type: "mysql"},
		},
	}, nil, logger)

	// Recovery worker path: ResolveDeployment(database, "")
	recoveryDeployment := svc.ResolveDeployment("orders", "")

	// Webhook cutover path: TernDeployment(repo) → "" → ResolveDeployment(database, "")
	webhookDeployment := svc.TernDeployment("myorg/myrepo")
	cutoverDeployment := svc.ResolveDeployment("orders", webhookDeployment)

	assert.Equal(t, recoveryDeployment, cutoverDeployment,
		"recovery worker and cutover handler must resolve to the same deployment key; "+
			"different keys create separate Spirit engine instances that don't share runningMigration state")

	// Also verify the actual key value
	assert.Equal(t, "orders", recoveryDeployment,
		"in local mode, deployment should be the database name")
}

// TestDeploymentResolution_WithExplicitDeployment verifies that when an apply
// has an explicit deployment stored, both paths use it directly.
func TestDeploymentResolution_WithExplicitDeployment(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(nil, nil))

	svc := New(nil, &ServerConfig{
		Databases: map[string]DatabaseConfig{
			"orders": {Type: "mysql"},
		},
	}, nil, logger)

	// Both paths should use the explicit deployment directly
	assert.Equal(t,
		svc.ResolveDeployment("orders", "my-cluster"),
		svc.ResolveDeployment("orders", "my-cluster"),
	)
	assert.Equal(t, "my-cluster", svc.ResolveDeployment("orders", "my-cluster"))
}

// TestDeploymentResolution_GRPCMode verifies deployment resolution when
// TernDeployments is configured (gRPC mode).
func TestDeploymentResolution_GRPCMode(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(nil, nil))

	svc := New(nil, &ServerConfig{
		TernDeployments: TernConfig{
			"tern-prod": {
				"staging": "http://tern:8080",
			},
		},
		Repos: map[string]RepoConfig{
			"myorg/myrepo": {DefaultTernDeployment: "tern-prod"},
		},
	}, nil, logger)

	// In gRPC mode, TernDeployment uses the repo mapping
	webhookDeployment := svc.TernDeployment("myorg/myrepo")
	assert.Equal(t, "tern-prod", webhookDeployment)

	// Recovery worker uses ResolveDeployment with the stored deployment from the apply
	recoveryDeployment := svc.ResolveDeployment("orders", "tern-prod")
	assert.Equal(t, "tern-prod", recoveryDeployment)

	assert.Equal(t, webhookDeployment, recoveryDeployment,
		"gRPC mode: recovery and webhook should resolve to the same deployment")
}
