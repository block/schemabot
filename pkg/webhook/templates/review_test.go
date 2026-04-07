package templates

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestRenderReviewRequired(t *testing.T) {
	data := ReviewGateData{
		Database:    "payments",
		Environment: "staging",
		RequestedBy: "alice",
		Owners:      []string{"bob", "org/dba-team"},
		PRAuthor:    "alice",
	}

	result := RenderReviewRequired(data)

	assert.Contains(t, result, "## Review Required")
	assert.Contains(t, result, "`payments`")
	assert.Contains(t, result, "`staging`")
	assert.Contains(t, result, "@alice")
	assert.Contains(t, result, "approval from a code owner")
	assert.Contains(t, result, "@bob")
	assert.Contains(t, result, "@org/dba-team")
	assert.Contains(t, result, "code owner above")
	assert.Contains(t, result, "schemabot apply -e staging")
}

func TestRenderReviewRequired_NoOwners(t *testing.T) {
	data := ReviewGateData{
		Database:    "payments",
		Environment: "production",
		RequestedBy: "alice",
		PRAuthor:    "alice",
	}

	result := RenderReviewRequired(data)

	assert.Contains(t, result, "## Review Required")
	assert.Contains(t, result, "at least one approval")
	assert.Contains(t, result, "Request a review from a teammate")
	assert.NotContains(t, result, "Code owners")
}

func TestRenderReviewRequired_NoAuthor(t *testing.T) {
	data := ReviewGateData{
		Database:    "payments",
		Environment: "staging",
		RequestedBy: "alice",
		Owners:      []string{"bob"},
	}

	result := RenderReviewRequired(data)

	assert.Contains(t, result, "@bob")
}
