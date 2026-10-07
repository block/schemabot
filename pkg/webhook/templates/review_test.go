package templates

import (
	"strings"
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
	assert.NotContains(t, result, "earlier commit", "no approval on an earlier commit means no explanation line")
}

// An authorized reviewer whose approval was given on an earlier commit sees
// why that approval, still visible on the PR, does not satisfy the gate: the
// PR's schema change differs now, or the earlier commit cannot be compared.
func TestRenderReviewRequired_EarlierCommitApprovers(t *testing.T) {
	tests := []struct {
		name         string
		changed      []string
		uncomparable []string
		want         []string
		notWant      []string
	}{
		{
			name:    "schema change differs",
			changed: []string{"bob", "carol"},
			want: []string{
				"\nApprovals on an earlier commit no longer count because this PR's schema change is different now: @bob, @carol.\nAsk for an approval of the latest commit.\n",
			},
			notWant: []string{"can't compare"},
		},
		{
			name:         "earlier commit cannot be compared",
			uncomparable: []string{"bob"},
			want: []string{
				"\nApprovals on an earlier commit can't carry over because SchemaBot can't compare that commit with the latest one: @bob.\nAsk for an approval of the latest commit.\n",
			},
			notWant: []string{"is different now"},
		},
		{
			name:         "both reasons",
			changed:      []string{"bob"},
			uncomparable: []string{"carol"},
			want: []string{
				"is different now: @bob.\n",
				"can't compare that commit with the latest one: @carol.\nAsk for an approval of the latest commit.\n",
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := RenderReviewRequired(ReviewGateData{
				Database:              "payments",
				Environment:           "staging",
				RequestedBy:           "alice",
				OperatorReviewers:     []string{"org/payments-operators"},
				OtherReviewers:        []string{"bob", "carol"},
				PRAuthor:              "alice",
				ChangedApprovers:      tt.changed,
				UncomparableApprovers: tt.uncomparable,
			})
			for _, w := range tt.want {
				assert.Contains(t, result, w)
			}
			for _, w := range tt.notWant {
				assert.NotContains(t, result, w)
			}
			assert.Equal(t, 1, strings.Count(result, "Ask for an approval of the latest commit."))
			assert.Less(t, strings.Index(result, "Approvals on an earlier commit"), strings.Index(result, "**Operators of `payments`**"),
				"the explanation precedes the reviewer lists")
		})
	}
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
