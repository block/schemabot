package webhook

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/block/schemabot/pkg/state"
	"github.com/block/schemabot/pkg/storage"
)

// An active apply of the PR's latest commit leaves work for a later apply when
// it undoes the change or runs on only one target, so the refusal of a second
// apply keeps offering one. An apply of the whole change leaves none.
func TestActiveApplyLeavesWork(t *testing.T) {
	withOptions := func(opts storage.ApplyOptions) *storage.Apply {
		a := &storage.Apply{State: state.Apply.Running}
		a.SetOptions(opts)
		return a
	}
	assert.Equal(t, "rollback", activeApplyLeavesWork(withOptions(storage.ApplyOptions{Rollback: true})))
	assert.Equal(t, "reverting", activeApplyLeavesWork(&storage.Apply{State: state.Apply.Reverting}))
	assert.Equal(t, "narrowed to one target", activeApplyLeavesWork(withOptions(storage.ApplyOptions{NarrowedTo: "staging/us"})))
	assert.Empty(t, activeApplyLeavesWork(&storage.Apply{State: state.Apply.Running}))
	assert.Empty(t, activeApplyLeavesWork(&storage.Apply{State: state.Apply.WaitingForCutover}))
}
