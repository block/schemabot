package storage

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestEnvironmentFreezeInEffectAt(t *testing.T) {
	start := time.Date(2026, 11, 25, 8, 0, 0, 0, time.UTC)
	end := start.Add(5 * 24 * time.Hour)
	lifted := start.Add(time.Hour)

	tests := []struct {
		name   string
		freeze *EnvironmentFreeze
		at     time.Time
		want   bool
	}{
		{name: "nil", freeze: nil, at: start, want: false},
		{name: "before start", freeze: &EnvironmentFreeze{StartsAt: start, EndsAt: &end}, at: start.Add(-time.Second), want: false},
		{name: "at start", freeze: &EnvironmentFreeze{StartsAt: start, EndsAt: &end}, at: start, want: true},
		{name: "inside window", freeze: &EnvironmentFreeze{StartsAt: start, EndsAt: &end}, at: start.Add(time.Hour), want: true},
		{name: "at end", freeze: &EnvironmentFreeze{StartsAt: start, EndsAt: &end}, at: end, want: false},
		{name: "open-ended", freeze: &EnvironmentFreeze{StartsAt: start}, at: start.Add(365 * 24 * time.Hour), want: true},
		{name: "lifted", freeze: &EnvironmentFreeze{StartsAt: start, LiftedAt: &lifted}, at: start.Add(time.Minute), want: false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, tt.freeze.InEffectAt(tt.at))
		})
	}
}

func TestEnvironmentFreezeCovers(t *testing.T) {
	whole := &EnvironmentFreeze{}
	assert.True(t, whole.Covers("cash", "orders"), "an empty scope covers the whole environment")

	narrowed := &EnvironmentFreeze{Deployments: []string{"cash"}, Databases: []string{"orders"}}
	assert.True(t, narrowed.Covers("Cash", "ORDERS"), "scope matches in canonical form")
	assert.False(t, narrowed.Covers("square", "orders"))
	assert.False(t, narrowed.Covers("cash", "payments"))

	byDeployment := &EnvironmentFreeze{Deployments: []string{"cash", "square"}}
	assert.True(t, byDeployment.Covers("square", "anything"))

	var missing *EnvironmentFreeze
	assert.False(t, missing.Covers("cash", "orders"))
}

func TestEnvironmentFreezeValidate(t *testing.T) {
	start := time.Date(2026, 11, 25, 8, 0, 0, 0, time.UTC)
	end := start.Add(time.Hour)
	before := start.Add(-time.Hour)
	valid := func() *EnvironmentFreeze {
		return &EnvironmentFreeze{
			Identifier:  "freeze-0123456789abcdef",
			Environment: "production",
			Kind:        FreezeKindOnDemand,
			Running:     FreezeRunningStop,
			StartsAt:    start,
			Reason:      "incident",
			CreatedBy:   "alice",
			Source:      FreezeSourceManual,
		}
	}
	require.NoError(t, valid().Validate())

	planned := valid()
	planned.Kind = FreezeKindPlanned
	planned.Running = FreezeRunningContinue
	planned.EndsAt = &end
	planned.Source = "calendar"
	planned.ExternalID = "event-1"
	require.NoError(t, planned.Validate())

	tests := []struct {
		name   string
		mutate func(*EnvironmentFreeze)
		want   string
	}{
		{name: "identifier", mutate: func(f *EnvironmentFreeze) { f.Identifier = " " }, want: "identifier is required"},
		{name: "environment", mutate: func(f *EnvironmentFreeze) { f.Environment = "" }, want: "environment is required"},
		{name: "blank environment", mutate: func(f *EnvironmentFreeze) { f.Environment = "  " }, want: "environment is required"},
		{name: "kind", mutate: func(f *EnvironmentFreeze) { f.Kind = "sometimes" }, want: "freeze kind"},
		{name: "running", mutate: func(f *EnvironmentFreeze) { f.Running = "throttle" }, want: "freeze running policy"},
		{name: "start", mutate: func(f *EnvironmentFreeze) { f.StartsAt = time.Time{} }, want: "start time is required"},
		{name: "end before start", mutate: func(f *EnvironmentFreeze) { f.EndsAt = &before }, want: "end time must be after"},
		{name: "planned without end", mutate: func(f *EnvironmentFreeze) { f.Kind = FreezeKindPlanned }, want: "planned freeze needs an end time"},
		{name: "blank deployment", mutate: func(f *EnvironmentFreeze) { f.Deployments = []string{"cash", " "} }, want: "deployments must not contain an empty name"},
		{name: "blank database", mutate: func(f *EnvironmentFreeze) { f.Databases = []string{""} }, want: "databases must not contain an empty name"},
		{name: "reason", mutate: func(f *EnvironmentFreeze) { f.Reason = "  " }, want: "reason is required"},
		{name: "creator", mutate: func(f *EnvironmentFreeze) { f.CreatedBy = "" }, want: "creator is required"},
		{name: "source", mutate: func(f *EnvironmentFreeze) { f.Source = "" }, want: "source is required"},
		{name: "manual with external id", mutate: func(f *EnvironmentFreeze) { f.ExternalID = "x" }, want: "manual freeze has no external id"},
		{name: "synced without external id", mutate: func(f *EnvironmentFreeze) { f.Source = "calendar" }, want: "needs an external id"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			freeze := valid()
			tt.mutate(freeze)
			require.ErrorContains(t, freeze.Validate(), tt.want)
		})
	}

	var missing *EnvironmentFreeze
	require.ErrorContains(t, missing.Validate(), "freeze is required")
}
