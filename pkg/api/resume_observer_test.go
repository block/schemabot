package api

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestResumeObserverRecord(t *testing.T) {
	t.Run("a claim against the same stop attaches once", func(t *testing.T) {
		s := &Service{}
		assert.True(t, s.claimResumeObserver(7, 0))
		assert.False(t, s.claimResumeObserver(7, 0))
	})

	t.Run("a later stop attaches again", func(t *testing.T) {
		s := &Service{}
		assert.True(t, s.claimResumeObserver(7, 0))
		assert.True(t, s.claimResumeObserver(7, 42))
		assert.False(t, s.claimResumeObserver(7, 42))
	})

	t.Run("a drive forgets only the record it saw", func(t *testing.T) {
		s := &Service{}
		assert.True(t, s.claimResumeObserver(7, 0))
		seen := s.resumeObserverGeneration(7)

		// Another drive in this process attached to the rollout's next resume.
		assert.True(t, s.claimResumeObserver(7, 42))
		s.forgetResumeObserverGeneration(7, seen)
		assert.False(t, s.claimResumeObserver(7, 42),
			"the newer record survives the older drive's forget")

		s.forgetResumeObserverGeneration(7, s.resumeObserverGeneration(7))
		assert.True(t, s.claimResumeObserver(7, 42),
			"the drive that saw the newer record forgets it")
	})
}
