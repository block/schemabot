package api

import (
	"context"
	"testing"
	"time"

	"github.com/block/schemabot/pkg/checkstate"
	ghclient "github.com/block/schemabot/pkg/github"
	"github.com/block/schemabot/pkg/storage"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type coverageDiscovery struct {
	result *ghclient.FindAllConfigsResult
	err    error
}

func (d coverageDiscovery) FindAllConfigs(context.Context, string, string) (*ghclient.FindAllConfigsResult, error) {
	return d.result, d.err
}

func TestCheckEnvironmentRepositoryCoverage(t *testing.T) {
	cfg := &ServerConfig{AllowedEnvironments: []string{"sandbox", "production"}, Databases: map[string]DatabaseConfig{
		"orders":   {AllowedRepos: []string{"org/orders"}, Environments: map[string]EnvironmentConfig{"sandbox": {}, "production": {}}},
		"payments": {AllowedRepos: []string{"org/payments"}, Environments: map[string]EnvironmentConfig{"production": {}}},
	}}
	for repo, want := range map[string][]string{"org/orders": {"sandbox", "production"}, "org/payments": {"production"}} {
		got, err := cfg.CheckEnvironmentsForRepo(t.Context(), nil, repo, "head")
		require.NoError(t, err)
		assert.Equal(t, want, got)
	}
	// Without repo binding, a complete repository discovery decides which of
	// these registrations belongs to this repository, including documentation PRs.
	for name, db := range cfg.Databases {
		db.AllowedRepos = nil
		cfg.Databases[name] = db
	}
	discovery := coverageDiscovery{result: &ghclient.FindAllConfigsResult{ValidConfigs: []ghclient.DiscoveredConfig{{Config: &ghclient.SchemabotConfig{Database: "payments"}, SchemaDir: "."}}}}
	got, err := cfg.CheckEnvironmentsForRepo(t.Context(), discovery, "org/payments", "head")
	require.NoError(t, err)
	assert.Equal(t, []string{"production"}, got)
	_, err = cfg.CheckEnvironmentsForRepo(t.Context(), coverageDiscovery{err: assert.AnError}, "org/payments", "head")
	require.ErrorIs(t, err, assert.AnError)
	_, err = cfg.CheckEnvironmentsForRepo(t.Context(), coverageDiscovery{result: &ghclient.FindAllConfigsResult{InvalidConfigs: []ghclient.InvalidConfigInfo{{Path: "schemabot.yaml"}}}}, "org/payments", "head")
	require.ErrorContains(t, err, "invalid configs")
}

func TestEnvironmentInspectionIncludesMissingPrerequisite(t *testing.T) {
	cfg := &ServerConfig{AllowedEnvironments: []string{"sandbox", "production"}, EnvironmentOrder: []string{"staging", "sandbox", "production"}, Databases: map[string]DatabaseConfig{"orders": {Environments: map[string]EnvironmentConfig{"sandbox": {}, "production": {}}}}}
	checks := []*storage.Check{{Environment: "production", DatabaseType: "mysql", DatabaseName: "orders", HeadSHA: "head", Status: checkstate.StatusCompleted, Conclusion: checkstate.ConclusionSuccess}}
	rows := cfg.CheckRowsForEnvironment(checks, "production", "org/orders", 1, "head")
	require.Len(t, rows, 2)
	assert.Equal(t, "sandbox", rows[1].Environment)
	assert.Equal(t, checkstate.StatusInProgress, rows[1].Status)
	assert.Empty(t, rows[1].Conclusion)
	assert.Equal(t, checkstate.ConclusionSuccess, checks[0].Conclusion)
}

func TestScanUsesApplicableNamesAndPreservesExplicitOverrides(t *testing.T) {
	cfg := &ServerConfig{AllowedEnvironments: []string{"sandbox", "production"}, Databases: map[string]DatabaseConfig{"payments": {AllowedRepos: []string{"org/payments"}, Environments: map[string]EnvironmentConfig{"production": {}}}}}
	client := fakeWebhookMissingCheckScanClient{prs: []ghclient.OpenPullRequest{{Number: 1, HeadSHA: "head"}}}
	store := &inspectStorage{checks: &inspectCheckStore{}}
	for _, override := range []string{"", "Custom Gate"} {
		t.Run(override, func(t *testing.T) {
			expected := webhookExpectedCheckNames(cfg, "org/payments", "", override)
			result, err := scanWebhookMissingChecks(t.Context(), client, store, "org/payments", expected, 0, time.Time{}, 0, discardLogger(), checkScanScope{config: cfg, override: override})
			require.NoError(t, err)
			require.Len(t, result.Missing, 1)
			want := "SchemaBot (production)"
			if override != "" {
				want = override
			}
			assert.Equal(t, []string{want}, result.Missing[0].MissingNames)
		})
	}
}
