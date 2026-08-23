package templates

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

// The review-required comment leads with the database's own operators in
// their own section, then lists the broader authorized principals in a
// separate fallback section, so the author knows who to ping first.
func TestRenderReviewRequired(t *testing.T) {
	data := ReviewGateData{
		Database:          "payments",
		Environment:       "staging",
		RequestedBy:       "alice",
		OperatorReviewers: []string{"org/payments-operators"},
		OtherReviewers:    []string{"bob", "org/dba-team"},
		PRAuthor:          "alice",
	}

	result := RenderReviewRequired(data)

	assert.Contains(t, result, "## Review Required")
	assert.Contains(t, result, "`payments`")
	assert.Contains(t, result, "`staging`")
	assert.Contains(t, result, "@alice")
	assert.Contains(t, result, "approval from an authorized reviewer")
	assert.Contains(t, result, "**Operators of `payments`**:\n- @org/payments-operators")
	assert.Contains(t, result, "**Other authorized reviewers**:\n- @bob\n- @org/dba-team")
	assert.Contains(t, result, "Request a review from anyone listed above")
	assert.Contains(t, result, "schemabot apply -e staging")
}

// A database with no operator principals falls back to a single flat list —
// no empty operators section and no "other" framing.
func TestRenderReviewRequired_NoOperators(t *testing.T) {
	data := ReviewGateData{
		Database:       "payments",
		Environment:    "staging",
		RequestedBy:    "alice",
		OtherReviewers: []string{"bob", "org/dba-team"},
		PRAuthor:       "alice",
	}

	result := RenderReviewRequired(data)

	assert.Contains(t, result, "**Authorized reviewers**:\n- @bob\n- @org/dba-team")
	assert.NotContains(t, result, "Operators of")
	assert.NotContains(t, result, "Other authorized reviewers")
	assert.Contains(t, result, "Request a review from anyone listed above")
}

// Operators without any broader fallback principals render only the
// operators section.
func TestRenderReviewRequired_OperatorsOnly(t *testing.T) {
	data := ReviewGateData{
		Database:          "payments",
		Environment:       "staging",
		RequestedBy:       "alice",
		OperatorReviewers: []string{"org/payments-operators"},
		PRAuthor:          "alice",
	}

	result := RenderReviewRequired(data)

	assert.Contains(t, result, "**Operators of `payments`**:\n- @org/payments-operators")
	assert.NotContains(t, result, "Other authorized reviewers")
	assert.NotContains(t, result, "**Authorized reviewers**:")
	assert.Contains(t, result, "Request a review from anyone listed above")
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
	assert.Contains(t, result, "approval from an authorized reviewer")
	assert.Contains(t, result, "Request a review from a database operator or admin")
	assert.NotContains(t, result, "Authorized reviewers")
}

func TestRenderReviewRequired_NoAuthor(t *testing.T) {
	data := ReviewGateData{
		Database:          "payments",
		Environment:       "staging",
		RequestedBy:       "alice",
		OperatorReviewers: []string{"bob"},
	}

	result := RenderReviewRequired(data)

	assert.Contains(t, result, "@bob")
}
