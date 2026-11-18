package planetscale

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/block/schemabot/pkg/engine"
)

// Only branches with the shape SchemaBot generates for the database are its
// own: the prefix for the database and exactly the tail a minted plan
// identifier leaves. A branch for another database, an operator's branch that
// merely starts with "schemabot-", one with a readable suffix of the same
// length, and production branches are not.
func TestIsGeneratedBranchName(t *testing.T) {
	assert.True(t, IsGeneratedBranchName("order_events", GenerateBranchName("order_events", "plan-0123456789abcdef")))
	long := "a_very_long_database_name_indeed"
	assert.True(t, IsGeneratedBranchName(long, GenerateBranchName(long, "plan-0123456789abcdef")))
	for range 20 {
		planID := engine.NewPlanID()
		assert.True(t, IsGeneratedBranchName("orders", GenerateBranchName("orders", planID)), "a branch named from a minted plan identifier %s", planID)
	}

	assert.False(t, IsGeneratedBranchName("orders", "main"))
	assert.False(t, IsGeneratedBranchName("orders", "schemabot-payments-89abcdef"), "another database's branch")
	assert.False(t, IsGeneratedBranchName("orders", "schemabot-orders-my-feature-branch"), "an operator's branch with the prefix")
	assert.False(t, IsGeneratedBranchName("orders", "schemabot-orders-"))
	assert.False(t, IsGeneratedBranchName("orders", "schemabot-orders-hotfix"), "a hand-written suffix shorter than a generated one")
	assert.False(t, IsGeneratedBranchName("orders", "schemabot-orders-v2"))
	assert.False(t, IsGeneratedBranchName("orders", "schemabot-orders-testing1"), "a hand-written suffix of the generated length, outside its alphabet")
	assert.False(t, IsGeneratedBranchName("orders", "schemabot-orders-89ABCDEF"), "uppercase is outside the alphabet a plan identifier uses")
	assert.False(t, IsGeneratedBranchName("orders", "schemabot-orders-89abcde"), "one character short")
	assert.False(t, IsGeneratedBranchName("orders", "schemabot-orders-89abcdef0"), "one character long")
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
