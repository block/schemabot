package presentation

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestGroupFailureReasons(t *testing.T) {
	groups := GroupFailureReasons([]Failure{
		{Name: "-40", Reason: "timeout"},
		{Name: "40-80", Reason: "duplicates"},
		{Name: "80-c0", Reason: ""},
		{Name: "c0-", Reason: "duplicates"},
		{Name: "e0-", Reason: "lost connection"},
	})

	assert.Equal(t, []FailureReason{
		{Reason: "duplicates", Names: []string{"40-80", "c0-"}},
		{Reason: "timeout", Names: []string{"-40"}},
		{Reason: "", Names: []string{"80-c0"}},
		{Reason: "lost connection", Names: []string{"e0-"}},
	}, groups, "the most common reason leads; equal counts keep first-seen order")
	assert.Empty(t, GroupFailureReasons(nil))
}
