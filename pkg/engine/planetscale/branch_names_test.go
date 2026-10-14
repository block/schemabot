package planetscale

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Only branches with the shape SchemaBot generates for the database are its
// own: a branch for another database, an operator's branch that merely starts
// with "schemabot-", and production branches are not.
func TestIsGeneratedBranchName(t *testing.T) {
	assert.True(t, IsGeneratedBranchName("order_events", generateBranchName("order_events", "plan-0123456789abcdef")))
	long := "a_very_long_database_name_indeed"
	assert.True(t, IsGeneratedBranchName(long, generateBranchName(long, "plan-0123456789abcdef")))
	assert.True(t, IsGeneratedBranchName("orders", generateBranchName("orders", "p-1")), "a short plan identifier is kept whole")

	assert.False(t, IsGeneratedBranchName("orders", "main"))
	assert.False(t, IsGeneratedBranchName("orders", "schemabot-payments-89abcdef"), "another database's branch")
	assert.False(t, IsGeneratedBranchName("orders", "schemabot-orders-my-feature-branch"), "an operator's branch with the prefix")
	assert.False(t, IsGeneratedBranchName("orders", "schemabot-orders-"))
}

func TestBranchNameFromResumeMetadata(t *testing.T) {
	encoded, err := encodePSMetadata(&psMetadata{BranchName: "schemabot-orders-89abcdef"})
	require.NoError(t, err)

	branch, err := BranchNameFromResumeMetadata(encoded)
	require.NoError(t, err)
	assert.Equal(t, "schemabot-orders-89abcdef", branch)

	branch, err = BranchNameFromResumeMetadata("")
	require.NoError(t, err)
	assert.Empty(t, branch)

	_, err = BranchNameFromResumeMetadata("{not json")
	require.Error(t, err, "an unreadable record is never read as no branch")
}
