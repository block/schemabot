package engine

import (
	"regexp"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestIdentifierMintsPrefixAndRandomSuffix(t *testing.T) {
	suffix := regexp.MustCompile(`^[0-9a-f]{16}$`)
	mints := map[string]func() string{
		"plan-":  NewPlanID,
		"apply-": NewApplyID,
		"task-":  NewTaskID,
	}
	for prefix, mint := range mints {
		seen := map[string]bool{}
		for range 100 {
			id := mint()
			assert.Regexp(t, suffix, id[len(prefix):], "identifier %q must carry a hex suffix after %q", id, prefix)
			assert.Equal(t, prefix, id[:len(prefix)])
			assert.False(t, seen[id], "identifier %q minted twice", id)
			seen[id] = true
		}
	}
}
