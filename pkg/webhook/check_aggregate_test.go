package webhook

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/block/spirit/pkg/utils"

	"github.com/block/schemabot/pkg/api"
	"github.com/block/schemabot/pkg/storage"
)

func TestComputeAggregate(t *testing.T) {
	tests := []struct {
		name           string
		checks         []*storage.Check
		wantConclusion string
		wantStatus     string
	}{
		{
			name: "all success",
			checks: []*storage.Check{
				{Status: checkStatusCompleted, Conclusion: checkConclusionSuccess},
				{Status: checkStatusCompleted, Conclusion: checkConclusionSuccess},
			},
			wantConclusion: checkConclusionSuccess,
			wantStatus:     checkStatusCompleted,
		},
		{
			name: "any failure dominates",
			checks: []*storage.Check{
				{Status: checkStatusCompleted, Conclusion: checkConclusionSuccess},
				{Status: checkStatusCompleted, Conclusion: checkConclusionFailure},
				{Status: checkStatusCompleted, Conclusion: checkConclusionActionRequired},
			},
			wantConclusion: checkConclusionFailure,
			wantStatus:     checkStatusCompleted,
		},
		{
			name: "action_required when no failure",
			checks: []*storage.Check{
				{Status: checkStatusCompleted, Conclusion: checkConclusionSuccess},
				{Status: checkStatusCompleted, Conclusion: checkConclusionActionRequired},
			},
			wantConclusion: checkConclusionActionRequired,
			wantStatus:     checkStatusCompleted,
		},
		{
			name: "in_progress takes priority over conclusions",
			checks: []*storage.Check{
				{Status: checkStatusCompleted, Conclusion: checkConclusionSuccess},
				{Status: checkStatusInProgress, Conclusion: ""},
				{Status: checkStatusCompleted, Conclusion: checkConclusionFailure},
			},
			wantConclusion: "", // in_progress has no conclusion
			wantStatus:     checkStatusInProgress,
		},
		{
			name: "single check success",
			checks: []*storage.Check{
				{Status: checkStatusCompleted, Conclusion: checkConclusionSuccess},
			},
			wantConclusion: checkConclusionSuccess,
			wantStatus:     checkStatusCompleted,
		},
		{
			name: "single check action_required",
			checks: []*storage.Check{
				{Status: checkStatusCompleted, Conclusion: checkConclusionActionRequired},
			},
			wantConclusion: checkConclusionActionRequired,
			wantStatus:     checkStatusCompleted,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			conclusion, status := computeAggregate(tt.checks)
			assert.Equal(t, tt.wantConclusion, conclusion)
			assert.Equal(t, tt.wantStatus, status)
		})
	}
}

func TestValidateRequestedDatabaseEnvironmentUsesServerConfig(t *testing.T) {
	service := api.New(&emptyStorage{}, &api.ServerConfig{
		Databases: map[string]api.DatabaseConfig{
			"orders": {
				Environments: map[string]api.EnvironmentConfig{
					"production": {Deployment: "default", Target: "orders"},
				},
			},
		},
	}, nil, testLogger())
	t.Cleanup(func() { utils.CloseAndLog(service) })
	h := &Handler{service: service}

	assert.NoError(t, h.validateRequestedDatabaseEnvironment("orders", "production"))
	assert.NoError(t, h.validateRequestedDatabaseEnvironment("orders", ""))

	err := h.validateRequestedDatabaseEnvironment("orders", "staging")
	require.Error(t, err)
	assert.Contains(t, err.Error(), `database "orders" environment "staging" is not configured on this server`)

	err = h.validateRequestedDatabaseEnvironment("payments", "production")
	require.Error(t, err)
	assert.Contains(t, err.Error(), `database "payments" is not configured on this server`)
}

func TestIsAggregateCheck(t *testing.T) {
	t.Run("global aggregate", func(t *testing.T) {
		aggregate := &storage.Check{
			Environment:  aggregateSentinel,
			DatabaseType: aggregateSentinel,
			DatabaseName: aggregateSentinel,
		}
		require.True(t, isAggregateCheck(aggregate))
	})

	t.Run("per-environment aggregate", func(t *testing.T) {
		aggregate := &storage.Check{
			Environment:  "staging",
			DatabaseType: aggregateSentinel,
			DatabaseName: aggregateSentinel,
		}
		require.True(t, isAggregateCheck(aggregate))
	})

	t.Run("per-database check", func(t *testing.T) {
		perDB := &storage.Check{
			Environment:  "staging",
			DatabaseType: "mysql",
			DatabaseName: "orders",
		}
		require.False(t, isAggregateCheck(perDB))
	})
}

func TestAggregateSummary(t *testing.T) {
	checks := []*storage.Check{
		{DatabaseName: "orders", Environment: "staging", Status: checkStatusCompleted, Conclusion: checkConclusionSuccess, HasChanges: true},
		{DatabaseName: "orders", Environment: "production", Status: checkStatusCompleted, Conclusion: checkConclusionActionRequired},
	}

	title, summary := aggregateSummary(checks, checkConclusionActionRequired)

	assert.Contains(t, title, "1 apply pending")
	assert.Contains(t, summary, "`orders`")
	assert.Contains(t, summary, "staging")
	assert.Contains(t, summary, "production")
	assert.Contains(t, summary, "Applied")
	assert.Contains(t, summary, "Pending")
}

// When the leader gates on participant deployments, their folded outcomes render
// in a separate "Tenant deployments" section, keyed by tenant, distinct from the
// leader's own per-database rows.
func TestAggregateSummary_WithParticipants(t *testing.T) {
	checks := []*storage.Check{
		{DatabaseType: "mysql", DatabaseName: "orders", Environment: "production", Status: checkStatusCompleted, Conclusion: checkConclusionSuccess, HasChanges: true},
		{DatabaseType: aggregateSentinel, DatabaseName: "tenant-b", Environment: "production", Status: checkStatusCompleted, Conclusion: checkConclusionActionRequired},
		{DatabaseType: aggregateSentinel, DatabaseName: "tenant-c", Environment: "production", Status: checkStatusInProgress},
	}

	_, summary := aggregateSummary(checks, checkConclusionActionRequired)

	dbSection, tenantSection, found := strings.Cut(summary, "**Tenant deployments**")
	require.True(t, found, "summary has a Tenant deployments section")

	// The leader's own database renders in the Database section; participants do not.
	assert.Contains(t, dbSection, "| Database | Environment | Status |")
	assert.Contains(t, dbSection, "`orders`")
	assert.NotContains(t, dbSection, "tenant-b", "participants must not appear in the Database section")
	assert.NotContains(t, dbSection, "tenant-c")

	// Participants render in their own tenant-keyed section.
	assert.Contains(t, tenantSection, "| Tenant | Status |")
	assert.Contains(t, tenantSection, "`tenant-b`")
	assert.Contains(t, tenantSection, "`tenant-c`")
}

// Participant gating is the key information, so the Tenant deployments section
// must survive even when the leader has so many per-database checks that the
// Database section truncates.
func TestAggregateSummary_TenantSectionSurvivesDatabaseTruncation(t *testing.T) {
	var checks []*storage.Check
	for range 6000 {
		checks = append(checks, &storage.Check{
			DatabaseType: "mysql", DatabaseName: "orders", Environment: "production",
			Status: checkStatusCompleted, Conclusion: checkConclusionActionRequired,
		})
	}
	checks = append(checks, &storage.Check{
		DatabaseType: aggregateSentinel, DatabaseName: "tenant-b", Environment: "production",
		Status: checkStatusInProgress,
	})

	_, summary := aggregateSummary(checks, checkStatusInProgress)

	assert.Less(t, len(summary), maxCheckRunTextLength, "summary stays under the Check Run limit")
	assert.Contains(t, summary, "more check(s)", "the Database section truncates")
	assert.Contains(t, summary, "**Tenant deployments**", "the tenant section is not dropped")
	assert.Contains(t, summary, "`tenant-b`", "the participant still appears despite database truncation")
}

func TestAggregateSummary_AllSuccess(t *testing.T) {
	checks := []*storage.Check{
		{DatabaseName: "orders", Environment: "staging", Status: checkStatusCompleted, Conclusion: checkConclusionSuccess, HasChanges: true},
		{DatabaseName: "orders", Environment: "production", Status: checkStatusCompleted, Conclusion: checkConclusionSuccess, HasChanges: true},
	}

	title, _ := aggregateSummary(checks, checkConclusionSuccess)
	assert.Equal(t, "All applies complete", title)
}

func TestAggregateSummary_AllUpToDate(t *testing.T) {
	checks := []*storage.Check{
		{DatabaseName: "orders", Environment: "staging", Status: checkStatusCompleted, Conclusion: checkConclusionSuccess},
		{DatabaseName: "users", Environment: "staging", Status: checkStatusCompleted, Conclusion: checkConclusionSuccess},
	}

	title, summary := aggregateSummary(checks, checkConclusionSuccess)

	assert.Equal(t, "Schema up to date", title)
	assert.Contains(t, summary, "Up to date")
	assert.NotContains(t, summary, "Applied")
}

func TestConclusionEmoji(t *testing.T) {
	assert.Equal(t, "Applied", conclusionEmoji(checkStatusCompleted, checkConclusionSuccess))
	assert.Equal(t, "Failed", conclusionEmoji(checkStatusCompleted, checkConclusionFailure))
	assert.Equal(t, "Pending", conclusionEmoji(checkStatusCompleted, checkConclusionActionRequired))
	assert.Equal(t, "In progress", conclusionEmoji(checkStatusInProgress, ""))
	assert.Equal(t, "Cancelled", conclusionEmoji(checkStatusCompleted, checkConclusionNeutral))
}

func TestAggregateCheckNameForEnv(t *testing.T) {
	assert.Equal(t, "SchemaBot (staging)", aggregateCheckNameForEnv(aggregateCheckName, "staging"))
	assert.Equal(t, "SchemaBot (production)", aggregateCheckNameForEnv(aggregateCheckName, "production"))
	assert.Equal(t, "SchemaBot X (sandbox)", aggregateCheckNameForEnv("SchemaBot X", "sandbox"))
}

func TestAggregateCheckNameForRepo(t *testing.T) {
	t.Run("defaults without service config", func(t *testing.T) {
		h := &Handler{}
		assert.Equal(t, aggregateCheckName, h.aggregateCheckNameForRepo("octocat/hello-world"))
	})

	t.Run("uses single app custom name", func(t *testing.T) {
		service := api.New(&emptyStorage{}, &api.ServerConfig{GitHub: api.GitHubConfig{CheckName: "SchemaBot X"}}, nil, testLogger())
		t.Cleanup(func() { utils.CloseAndLog(service) })
		h := &Handler{service: service}
		assert.Equal(t, "SchemaBot X", h.aggregateCheckNameForRepo("octocat/hello-world"))
	})
}

func TestFilterChecksByEnvironment(t *testing.T) {
	checks := []*storage.Check{
		{Environment: "staging", DatabaseName: "orders", DatabaseType: "mysql"},
		{Environment: "production", DatabaseName: "orders", DatabaseType: "mysql"},
		{Environment: "staging", DatabaseName: "users", DatabaseType: "mysql"},
		// Global aggregate (no allowed_environments)
		{Environment: aggregateSentinel, DatabaseType: aggregateSentinel, DatabaseName: aggregateSentinel},
		// Per-environment aggregate (with allowed_environments)
		{Environment: "staging", DatabaseType: aggregateSentinel, DatabaseName: aggregateSentinel},
	}

	t.Run("filters to staging only and excludes per-env aggregate", func(t *testing.T) {
		result := filterChecksByEnvironment(checks, "staging")
		require.Len(t, result, 2)
		assert.Equal(t, "orders", result[0].DatabaseName)
		assert.Equal(t, "users", result[1].DatabaseName)
	})

	t.Run("filters to production only", func(t *testing.T) {
		result := filterChecksByEnvironment(checks, "production")
		require.Len(t, result, 1)
		assert.Equal(t, "orders", result[0].DatabaseName)
	})

	t.Run("returns empty for unknown environment", func(t *testing.T) {
		result := filterChecksByEnvironment(checks, "sandbox")
		assert.Empty(t, result)
	})

	t.Run("excludes global aggregate checks", func(t *testing.T) {
		result := filterChecksByEnvironment(checks, aggregateSentinel)
		assert.Empty(t, result)
	})
}
