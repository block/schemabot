package webhook

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/block/schemabot/pkg/api"
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

// latestCommitPlans serves one stored plan, or fails every read.
type latestCommitPlans struct {
	storage.PlanStore
	plan *storage.Plan
	err  error
}

func (p latestCommitPlans) GetByID(_ context.Context, id int64) (*storage.Plan, error) {
	if p.err != nil {
		return nil, p.err
	}
	if p.plan == nil || p.plan.ID != id {
		return nil, nil
	}
	return p.plan, nil
}

type latestCommitStorage struct {
	emptyStorage
	plans latestCommitPlans
}

func (s *latestCommitStorage) Plans() storage.PlanStore { return s.plans }

// An active apply counts as applying the PR's latest commit only when its plan
// loads and was planned from that commit. Every case where the plan cannot
// show it (no plan recorded, a failed read, a deleted plan, a plan with no
// commit) keeps offering the apply again, so the operator is never left
// without a way forward.
func TestActiveApplyAppliesLatestCommit(t *testing.T) {
	const head = "abc123"
	run := func(t *testing.T, planID int64, plans latestCommitPlans) bool {
		t.Helper()
		h := &Handler{
			service: api.New(&latestCommitStorage{plans: plans}, &api.ServerConfig{}, nil, testLogger()),
			logger:  testLogger(),
		}
		active := &storage.Apply{ApplyIdentifier: "apply-abc", Environment: "staging", State: state.Apply.Running, PlanID: planID}
		return h.activeApplyAppliesLatestCommit(t.Context(), "staging", head, active)
	}

	t.Run("a plan of the latest commit", func(t *testing.T) {
		assert.True(t, run(t, 7, latestCommitPlans{plan: &storage.Plan{ID: 7, HeadSHA: head}}))
	})
	t.Run("a plan of an earlier commit", func(t *testing.T) {
		assert.False(t, run(t, 7, latestCommitPlans{plan: &storage.Plan{ID: 7, HeadSHA: "earlier"}}))
	})
	t.Run("no plan recorded on the apply", func(t *testing.T) {
		assert.False(t, run(t, 0, latestCommitPlans{plan: &storage.Plan{ID: 0, HeadSHA: head}}))
	})
	t.Run("the plan read fails", func(t *testing.T) {
		assert.False(t, run(t, 7, latestCommitPlans{err: errors.New("connection refused")}))
	})
	t.Run("the plan no longer exists", func(t *testing.T) {
		assert.False(t, run(t, 7, latestCommitPlans{}))
	})
	t.Run("the plan records no commit", func(t *testing.T) {
		assert.False(t, run(t, 7, latestCommitPlans{plan: &storage.Plan{ID: 7}}))
	})
}
