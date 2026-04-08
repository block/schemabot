package github

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gopkg.in/yaml.v3"
)

func TestEnvironmentList_SimpleForm(t *testing.T) {
	yamlData := `
database: testdb
type: mysql
environments:
  - staging
  - production
`
	var config SchemabotConfig
	require.NoError(t, yaml.Unmarshal([]byte(yamlData), &config))
	assert.Equal(t, []string{"staging", "production"}, config.GetEnvironments())
}

func TestEnvironmentList_MapForm(t *testing.T) {
	yamlData := `
database: testdb
type: mysql
environments:
  staging:
    target: cluster-staging-001
  production:
    target: cluster-production-001
`
	var config SchemabotConfig
	require.NoError(t, yaml.Unmarshal([]byte(yamlData), &config))
	// Map form preserves YAML declaration order
	assert.Equal(t, []string{"staging", "production"}, config.GetEnvironments())
}

func TestEnvironmentList_MapFormEmptyTarget(t *testing.T) {
	yamlData := `
database: testdb
type: mysql
environments:
  staging:
    target: cluster-01
  production: {}
`
	var config SchemabotConfig
	require.NoError(t, yaml.Unmarshal([]byte(yamlData), &config))
	assert.Equal(t, []string{"staging", "production"}, config.GetEnvironments())
	assert.Equal(t, "cluster-01", config.GetTarget("staging"))
	assert.Equal(t, "testdb", config.GetTarget("production"))
}

func TestGetTarget_ExplicitTarget(t *testing.T) {
	config := SchemabotConfig{
		Database: "mydb",
		Environments: EnvironmentList{
			{Name: "staging", Target: "cluster-001"},
			{Name: "production", Target: "cluster-002"},
		},
	}
	assert.Equal(t, "cluster-001", config.GetTarget("staging"))
	assert.Equal(t, "cluster-002", config.GetTarget("production"))
}

func TestGetTarget_FallsBackToDatabase(t *testing.T) {
	config := SchemabotConfig{
		Database: "mydb",
		Environments: EnvironmentList{
			{Name: "staging"},
			{Name: "production"},
		},
	}
	assert.Equal(t, "mydb", config.GetTarget("staging"))
	assert.Equal(t, "mydb", config.GetTarget("production"))
	assert.Equal(t, "mydb", config.GetTarget("unknown"))
}

func TestGetTarget_EmptyEnvironments(t *testing.T) {
	config := SchemabotConfig{Database: "mydb"}
	assert.Equal(t, "mydb", config.GetTarget("staging"))
}

func TestGetEnvironments_Default(t *testing.T) {
	config := SchemabotConfig{Database: "mydb"}
	assert.Equal(t, []string{"staging"}, config.GetEnvironments())
}

func TestHasEnvironment_SimpleForm(t *testing.T) {
	config := SchemabotConfig{
		Database: "mydb",
		Environments: EnvironmentList{
			{Name: "staging"},
			{Name: "production"},
		},
	}
	assert.True(t, config.HasEnvironment("staging"))
	assert.True(t, config.HasEnvironment("production"))
	assert.False(t, config.HasEnvironment("unknown"))
}

func TestHasEnvironment_MapForm(t *testing.T) {
	yamlData := `
database: testdb
type: mysql
environments:
  staging:
    target: cluster-001
  production:
    target: cluster-002
`
	var config SchemabotConfig
	require.NoError(t, yaml.Unmarshal([]byte(yamlData), &config))
	assert.True(t, config.HasEnvironment("staging"))
	assert.True(t, config.HasEnvironment("production"))
	assert.False(t, config.HasEnvironment("dev"))
}
