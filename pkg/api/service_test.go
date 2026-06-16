package api

import (
	"io"
	"log/slog"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/block/schemabot/pkg/engine"
	ternv1 "github.com/block/schemabot/pkg/proto/ternv1"
	"github.com/block/schemabot/pkg/storage"
	"github.com/block/schemabot/pkg/tern"
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
			name:        "empty deployment resolves to missing default key",
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

func TestService_RoutingTernClientRoutesPlanThroughConfiguredDeployment(t *testing.T) {
	deploymentClient := &mockTernClient{planResp: &ternv1.PlanResponse{PlanId: "plan-1"}}
	service := New(&mockStorageWithApplyStores{
		plans:   &staticPlanStore{},
		applies: &staticApplyStore{},
	}, &ServerConfig{
		Databases: map[string]DatabaseConfig{
			"appdb": {
				Type: storage.DatabaseTypeMySQL,
				Environments: map[string]EnvironmentConfig{
					"staging": {
						Deployment: "primary",
						Target:     "appdb-target",
					},
				},
			},
		},
	}, map[string]tern.Client{
		"primary/staging": deploymentClient,
	}, slog.New(slog.NewTextHandler(io.Discard, nil)))

	client, err := service.RoutingTernClient()
	require.NoError(t, err)
	again, err := service.RoutingTernClient()
	require.NoError(t, err)
	assert.Same(t, client, again)

	resp, err := client.Plan(t.Context(), &ternv1.PlanRequest{
		Database:    "appdb",
		Environment: "staging",
	})
	require.NoError(t, err)
	assert.Equal(t, "plan-1", resp.PlanId)
	require.NotNil(t, deploymentClient.planReq)
	assert.Equal(t, storage.DatabaseTypeMySQL, deploymentClient.planReq.Type)
	assert.Equal(t, "appdb-target", deploymentClient.planReq.Target)
	assert.Equal(t, "appdb", deploymentClient.planReq.Database)
	assert.Equal(t, "staging", deploymentClient.planReq.Environment)
}

func TestService_RoutingTernClientRoutesApplyThroughStoredPlanTarget(t *testing.T) {
	deploymentClient := &mockTernClient{applyResp: &ternv1.ApplyResponse{Accepted: true, ApplyId: "apply-routed"}}
	service := New(&mockStorageWithApplyStores{
		plans: &staticPlanStore{plan: &storage.Plan{
			PlanIdentifier: "plan-1",
			Database:       "appdb",
			DatabaseType:   storage.DatabaseTypeMySQL,
			Deployment:     "primary",
			Target:         "appdb-target",
			Environment:    "staging",
		}},
		applies: &staticApplyStore{},
	}, &ServerConfig{}, map[string]tern.Client{
		"primary/staging": deploymentClient,
	}, slog.New(slog.NewTextHandler(io.Discard, nil)))

	client, err := service.RoutingTernClient()
	require.NoError(t, err)
	resp, err := client.Apply(t.Context(), &ternv1.ApplyRequest{
		PlanId:      "plan-1",
		Environment: "staging",
	})

	require.NoError(t, err)
	assert.True(t, resp.Accepted)
	assert.Equal(t, "apply-routed", resp.ApplyId)
	require.NotNil(t, deploymentClient.applyReq)
	assert.Equal(t, "appdb", deploymentClient.applyReq.Database)
	assert.Equal(t, storage.DatabaseTypeMySQL, deploymentClient.applyReq.Type)
	assert.Equal(t, "appdb-target", deploymentClient.applyReq.Target)
	assert.Equal(t, "staging", deploymentClient.applyReq.Environment)
	assert.Equal(t, "appdb-target", deploymentClient.applyReq.Options["target"])
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

func TestServiceDeploymentForDatabaseEnvironment(t *testing.T) {
	cfg := &ServerConfig{
		Databases: map[string]DatabaseConfig{
			"localdb": {
				Type: "mysql",
				Environments: map[string]EnvironmentConfig{
					"staging": {DSN: "root@tcp(localhost:3306)/localdb"},
				},
			},
			"remotedb": {
				Type: "mysql",
				Environments: map[string]EnvironmentConfig{
					"staging": {Target: "remote-target-001", Deployment: "tenant-a"},
				},
			},
		},
		TernDeployments: TernConfig{
			"tenant-a": {"staging": "localhost:9090"},
		},
	}
	service := New(nil, cfg, nil, slog.New(slog.NewTextHandler(io.Discard, nil)))

	localDeployment, err := service.deploymentForDatabaseEnvironment("localdb", "", "staging")
	require.NoError(t, err)
	assert.Equal(t, "localdb", localDeployment)

	remoteDeployment, err := service.deploymentForDatabaseEnvironment("remotedb", "", "staging")
	require.NoError(t, err)
	assert.Equal(t, "tenant-a", remoteDeployment)

	storedDeployment, err := service.deploymentForDatabaseEnvironment("remotedb", "stored-route", "staging")
	require.NoError(t, err)
	assert.Equal(t, "stored-route", storedDeployment)

	_, err = service.deploymentForDatabaseEnvironment("missing", "", "staging")
	require.Error(t, err)
}

type fakeRegisteredEngine struct{ engine.Engine }

// A database type with no built-in engine fails closed until an embedding
// service registers one; once registered, the service builds the local client
// with the supplied engine.
func TestServiceRegisterEngineSuppliesLocalClientEngine(t *testing.T) {
	cfg := &ServerConfig{
		Databases: map[string]DatabaseConfig{
			"customdb": {
				Type: "customengine",
				Environments: map[string]EnvironmentConfig{
					"staging": {DSN: "root@tcp(localhost:3306)/customdb"},
				},
			},
		},
	}
	service := New(nil, cfg, nil, slog.New(slog.NewTextHandler(io.Discard, nil)))

	_, err := service.TernClient("customdb", "staging")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "no engine registered")

	require.NoError(t, service.RegisterEngine("customengine", func(tern.LocalConfig, *slog.Logger) (engine.Engine, error) {
		return &fakeRegisteredEngine{}, nil
	}))
	client, err := service.TernClient("customdb", "staging")
	require.NoError(t, err)
	require.NotNil(t, client)
}

// RegisterEngine validates its inputs so a misconfiguration fails fast at setup.
func TestServiceRegisterEngineValidates(t *testing.T) {
	service := New(nil, &ServerConfig{}, nil, slog.New(slog.NewTextHandler(io.Discard, nil)))

	require.Error(t, service.RegisterEngine("", func(tern.LocalConfig, *slog.Logger) (engine.Engine, error) {
		return &fakeRegisteredEngine{}, nil
	}))
	require.Error(t, service.RegisterEngine("customengine", nil))
}

// A PlanetScale token configured as a literal (the secrets resolver returns
// unprefixed values as-is) that lacks the name:value separator is rejected when
// building the local client. The literal is the raw credential, so the error
// redacts it and identifies the environment by its config key instead.
func TestNewLocalTernClient_RejectsMalformedLiteralTokenWithoutLeakingValue(t *testing.T) {
	cfg := &ServerConfig{}
	service := New(nil, cfg, nil, slog.New(slog.NewTextHandler(io.Discard, nil)))

	const secretLiteral = "pscale_tkn_supersecretvalue"
	envConfig := EnvironmentConfig{
		DSN:            "root@tcp(localhost:3306)/mydb",
		Organization:   "acme",
		TokenSecretRef: secretLiteral,
	}

	_, err := service.newLocalTernClient("vitessdb-staging", "vitessdb", "vitess", envConfig)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "vitessdb-staging")
	assert.Contains(t, err.Error(), "literal value redacted")
	assert.Contains(t, err.Error(), "name:value format")
	assert.NotContains(t, err.Error(), secretLiteral)
}

// A literal PlanetScale token with the name:value separator present but an empty
// name or value is also redacted: the whole literal is the credential, so the
// error never echoes it. Each case carries a distinctive secret fragment that
// must be absent from the error.
func TestNewLocalTernClient_RejectsLiteralTokenWithEmptyPartsWithoutLeakingValue(t *testing.T) {
	cfg := &ServerConfig{}
	service := New(nil, cfg, nil, slog.New(slog.NewTextHandler(io.Discard, nil)))

	cases := []struct {
		ref      string
		fragment string // distinctive credential text that must not appear in the error
	}{
		{ref: ":supersecretvalue", fragment: "supersecretvalue"},
		{ref: "supersecretname:", fragment: "supersecretname"},
		{ref: "  :  ", fragment: "  :  "},
	}
	for _, tc := range cases {
		t.Run(tc.ref, func(t *testing.T) {
			envConfig := EnvironmentConfig{
				DSN:            "root@tcp(localhost:3306)/mydb",
				Organization:   "acme",
				TokenSecretRef: tc.ref,
			}

			_, err := service.newLocalTernClient("vitessdb-staging", "vitessdb", "vitess", envConfig)
			require.Error(t, err)
			assert.Contains(t, err.Error(), "vitessdb-staging")
			assert.Contains(t, err.Error(), "literal value redacted")
			assert.Contains(t, err.Error(), "non-empty name and value")
			assert.NotContains(t, err.Error(), tc.fragment)
		})
	}
}

// A token configured as a real reference indirection (e.g. env:VAR) that
// resolves to a malformed value is rejected with the reference echoed: the
// reference names where the credential lives, not the credential itself, so it
// is safe to surface for triage while the resolved secret value stays hidden.
func TestNewLocalTernClient_RejectsMalformedTokenReferenceEchoingTheReference(t *testing.T) {
	cfg := &ServerConfig{}
	service := New(nil, cfg, nil, slog.New(slog.NewTextHandler(io.Discard, nil)))

	const secretValue = "no-separator-secret-value"
	t.Setenv("SCHEMABOT_TEST_TOKEN", secretValue)

	envConfig := EnvironmentConfig{
		DSN:            "root@tcp(localhost:3306)/mydb",
		Organization:   "acme",
		TokenSecretRef: "env:SCHEMABOT_TEST_TOKEN",
	}

	_, err := service.newLocalTernClient("vitessdb-staging", "vitessdb", "vitess", envConfig)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "vitessdb-staging")
	assert.Contains(t, err.Error(), `"env:SCHEMABOT_TEST_TOKEN"`)
	assert.Contains(t, err.Error(), "name:value format")
	assert.NotContains(t, err.Error(), secretValue)
}

// A PlanetScale token reference that resolves to a well-formed name:value pair
// is accepted when building the local client.
func TestNewLocalTernClient_AcceptsWellFormedTokenReference(t *testing.T) {
	cfg := &ServerConfig{}
	service := New(nil, cfg, nil, slog.New(slog.NewTextHandler(io.Discard, nil)))

	envConfig := EnvironmentConfig{
		DSN:            "root@tcp(localhost:3306)/mydb",
		Organization:   "acme",
		TokenSecretRef: "name:value",
	}

	client, err := service.newLocalTernClient("vitessdb-staging", "vitessdb", "vitess", envConfig)
	require.NoError(t, err)
	assert.NotNil(t, client)
}
