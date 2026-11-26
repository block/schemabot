package tern

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"testing/synctest"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/block/schemabot/pkg/engine"
	"github.com/block/schemabot/pkg/storage"
)

// noProgressPollJitter takes nothing off a backed-off wait, so a test sees
// the full doubling schedule.
func noProgressPollJitter(int64) int64 { return 0 }

// A failing drive waits the plain interval after its first failure, then
// doubles the wait with each further failure until it reaches the cap, where
// it stays however long the failures last. An answered poll, which leaves no
// failures counted, waits the plain interval.
func TestProgressPollWait_DoublesWhileFailingUpToTheCap(t *testing.T) {
	client := &LocalClient{progressPollJitter: noProgressPollJitter}
	const interval = defaultTaskPollInterval
	require.Equal(t, 8*time.Second, interval*progressPollBackoffCapMultiple, "the default cap")

	cases := []struct {
		consecutiveErrors int
		want              time.Duration
	}{
		{0, interval},
		{1, interval},
		{2, 2 * interval},
		{3, 4 * interval},
		{4, 8 * interval},
		{5, 16 * interval},
		{6, 16 * interval},
		{maxConsecutiveProgressPollErrors, 16 * interval},
		{1 << 40, 16 * interval},
	}
	for _, tc := range cases {
		assert.Equal(t, tc.want, client.progressPollWait(tc.consecutiveErrors), "after %d failed polls", tc.consecutiveErrors)
	}
}

// Jitter only ever shortens a backed-off wait, by at most a quarter, so it
// never lengthens one past the cap and never shortens one below the plain
// interval. A wait at the plain interval is not jittered at all.
func TestProgressPollWait_JitterShortensABackedOffWaitByAtMostAQuarter(t *testing.T) {
	const interval = time.Second
	var asked []int64
	client := &LocalClient{
		taskPollIntervalOverride: interval,
		progressPollJitter: func(n int64) int64 {
			asked = append(asked, n)
			return n - 1
		},
	}

	assert.Equal(t, interval, client.progressPollWait(0))
	assert.Equal(t, interval, client.progressPollWait(1))
	assert.Empty(t, asked, "a wait at the plain interval draws no jitter")

	assert.Equal(t, 1500*time.Millisecond, client.progressPollWait(2))
	assert.Equal(t, 3*time.Second, client.progressPollWait(3))
	assert.Equal(t, 12*time.Second, client.progressPollWait(9))
	assert.Equal(t, []int64{int64(500*time.Millisecond) + 1, int64(time.Second) + 1, int64(4*time.Second) + 1}, asked,
		"the jitter is drawn from [0, a quarter of the backed-off wait]")
}

// With the real random source every wait lands between three quarters of its
// backed-off ceiling and the ceiling itself, and never past the cap.
func TestProgressPollWait_RandomJitterStaysWithinBounds(t *testing.T) {
	client := &LocalClient{}
	const interval = defaultTaskPollInterval
	ceiling := interval
	for consecutiveErrors := 1; consecutiveErrors <= maxConsecutiveProgressPollErrors; consecutiveErrors++ {
		if consecutiveErrors > 1 {
			ceiling = min(2*ceiling, interval*progressPollBackoffCapMultiple)
		}
		for range 200 {
			wait := client.progressPollWait(consecutiveErrors)
			assert.LessOrEqual(t, wait, ceiling, "after %d failed polls", consecutiveErrors)
			assert.GreaterOrEqual(t, wait, max(interval, ceiling-ceiling/4), "after %d failed polls", consecutiveErrors)
		}
	}
}

// scheduleRecordingEngine answers progress polls from a script, one step per
// call, repeating the last step once the script runs out, and records when
// each poll arrived. When cancelOnCall is set, the poll with that number
// cancels the drive's context before answering. Each poll takes work to
// answer.
type scheduleRecordingEngine struct {
	engine.Engine
	steps        []finalizerPollStep
	cancelOnCall int
	cancel       context.CancelFunc
	work         time.Duration

	mu       sync.Mutex
	polledAt []time.Time
}

func (e *scheduleRecordingEngine) Name() string { return "planetscale" }

func (e *scheduleRecordingEngine) ProgressIsExternallyAuthoritative() bool { return true }

func (e *scheduleRecordingEngine) Progress(context.Context, *engine.ProgressRequest) (*engine.ProgressResult, error) {
	e.mu.Lock()
	e.polledAt = append(e.polledAt, time.Now())
	if len(e.polledAt) == e.cancelOnCall {
		e.cancel()
	}
	step := e.steps[min(len(e.polledAt)-1, len(e.steps)-1)]
	e.mu.Unlock()
	time.Sleep(e.work)
	return step.result, step.err
}

// polls returns when each poll arrived.
func (e *scheduleRecordingEngine) polls() []time.Time {
	e.mu.Lock()
	defer e.mu.Unlock()
	return append([]time.Time(nil), e.polledAt...)
}

// gaps returns the time between each poll and the one before it.
func (e *scheduleRecordingEngine) gaps() []time.Duration {
	polls := e.polls()
	var gaps []time.Duration
	for i := 1; i < len(polls); i++ {
		gaps = append(gaps, polls[i].Sub(polls[i-1]))
	}
	return gaps
}

// progressDrive is one of the three progress drives, set up to poll eng with
// interval between healthy polls and no jitter.
type progressDrive struct {
	name string
	// firstPollAfter is how long the drive waits before its first poll.
	firstPollAfter time.Duration
	// run drives eng until the drive returns, and reports how it ended: nil
	// for a hand-back, or the error the drive returned.
	run func(ctx context.Context, eng engine.Engine, interval time.Duration) error
	// cancelledErr is what run reports when the drive's context is cancelled.
	cancelledErr error
}

const progressDriveTestInterval = time.Second

func progressDrives() []progressDrive {
	return []progressDrive{
		{
			name:           "sequential task drive",
			firstPollAfter: progressDriveTestInterval,
			run: func(ctx context.Context, eng engine.Engine, interval time.Duration) error {
				client, apply, task, _ := lostWorkPollFixture(eng, lostWorkTrustBudgetAmple)
				client.logger = discardLogger()
				client.taskPollIntervalOverride = interval
				client.progressPollJitter = noProgressPollJitter
				if action := client.pollTaskToCompletion(ctx, apply, task, nil, nil); action != taskHandover {
					return fmt.Errorf("sequential drive ended with action %v rather than handing the apply back", action)
				}
				return nil
			},
		},
		{
			name:           "grouped task drive",
			firstPollAfter: progressDriveTestInterval,
			run: func(ctx context.Context, eng engine.Engine, interval time.Duration) error {
				client, apply, tasks, _ := lostWorkAtomicPollFixture(eng, lostWorkTrustBudgetAmple)
				client.logger = discardLogger()
				client.taskPollIntervalOverride = interval
				client.progressPollJitter = noProgressPollJitter
				return client.pollForCompletionAtomic(ctx, apply, tasks, nil, nil, map[string]string{}, false)
			},
		},
		{
			name:           "group_finalizer drive",
			firstPollAfter: 0,
			run: func(ctx context.Context, eng engine.Engine, interval time.Duration) error {
				client := finalizerPollClient(discardLogger(), interval)
				client.progressPollJitter = noProgressPollJitter
				apply := &storage.Apply{ApplyIdentifier: "apply-finalizer-poll", Database: "appdb"}
				_, err := client.driveFinalizerToTerminal(ctx, eng, apply, nil, &engine.ResumeState{MigrationContext: "deploy-1"}, func(*engine.ResumeState) {})
				return err
			},
			cancelledErr: context.Canceled,
		},
	}
}

// Against a provider that keeps refusing progress polls, every progress drive
// waits the plain interval after the first failure and then doubles the wait,
// up to the cap, instead of spending its whole error budget in a few seconds.
func TestProgressDrives_FailingPollsBackOffToTheCap(t *testing.T) {
	for _, drive := range progressDrives() {
		t.Run(drive.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				eng := &scheduleRecordingEngine{steps: []finalizerPollStep{{err: errors.New("get deploy request: rate limited")}}}

				start := time.Now()
				_ = drive.run(t.Context(), eng, progressDriveTestInterval)

				require.Len(t, eng.polls(), maxConsecutiveProgressPollErrors, "the drive still spends exactly its error budget")
				assert.Equal(t, drive.firstPollAfter, eng.polls()[0].Sub(start))
				s := progressDriveTestInterval
				assert.Equal(t, []time.Duration{s, 2 * s, 4 * s, 8 * s, 16 * s, 16 * s, 16 * s, 16 * s, 16 * s}, eng.gaps())
			})
		})
	}
}

// A poll that answers ends the backoff: the drive goes straight back to the
// plain interval, and a later failure starts the doubling again from there.
func TestProgressDrives_AnsweredPollReturnsToThePlainInterval(t *testing.T) {
	pollErr := errors.New("get deploy request: rate limited")
	for _, drive := range progressDrives() {
		t.Run(drive.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				ctx, cancel := context.WithCancel(t.Context())
				defer cancel()
				eng := &scheduleRecordingEngine{
					steps: []finalizerPollStep{
						{err: pollErr},
						{err: pollErr},
						{err: pollErr},
						{result: &engine.ProgressResult{State: engine.StateRunning}},
						{err: pollErr},
						{err: pollErr},
					},
					cancelOnCall: 6,
					cancel:       cancel,
				}

				_ = drive.run(ctx, eng, progressDriveTestInterval)

				s := progressDriveTestInterval
				assert.Equal(t, []time.Duration{s, 2 * s, 4 * s, s, s}, eng.gaps())
			})
		})
	}
}

// The time a poll takes to answer does not stretch the drive's cadence: each
// wait runs from when the last poll began, so a healthy drive polls on the
// plain interval and a failing one on its backoff schedule, however slowly the
// provider answers.
func TestProgressDrives_PollTimeDoesNotStretchTheInterval(t *testing.T) {
	pollErr := errors.New("get deploy request: rate limited")
	running := &engine.ProgressResult{State: engine.StateRunning}
	for _, drive := range progressDrives() {
		t.Run(drive.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				ctx, cancel := context.WithCancel(t.Context())
				defer cancel()
				eng := &scheduleRecordingEngine{
					steps: []finalizerPollStep{
						{result: running},
						{result: running},
						{err: pollErr},
						{err: pollErr},
						{result: running},
					},
					cancelOnCall: 6,
					cancel:       cancel,
					work:         progressDriveTestInterval * 3 / 10,
				}

				_ = drive.run(ctx, eng, progressDriveTestInterval)

				s := progressDriveTestInterval
				assert.Equal(t, []time.Duration{s, s, s, 2 * s, s}, eng.gaps())
			})
		})
	}
}

// A drive cancelled while it waits out a long backoff returns at once rather
// than at the end of the wait, so a stop, a lost lease, or a shutdown reaches
// a failing drive as quickly as a healthy one.
func TestProgressDrives_CancelDuringABackedOffWaitReturnsAtOnce(t *testing.T) {
	for _, drive := range progressDrives() {
		t.Run(drive.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				ctx, cancel := context.WithCancel(t.Context())
				defer cancel()
				eng := &scheduleRecordingEngine{steps: []finalizerPollStep{{err: errors.New("get deploy request: rate limited")}}}

				start := time.Now()
				done := make(chan error, 1)
				go func() { done <- drive.run(ctx, eng, progressDriveTestInterval) }()

				// Five failed polls leave the drive in a wait of sixteen
				// intervals. Cancel a few intervals into it.
				fifthPoll := drive.firstPollAfter + 15*progressDriveTestInterval
				time.Sleep(fifthPoll + 3*progressDriveTestInterval)
				require.Len(t, eng.polls(), 5)
				cancel()
				err := <-done

				assert.Equal(t, fifthPoll+3*progressDriveTestInterval, time.Since(start), "the drive returns the moment it is cancelled")
				assert.Len(t, eng.polls(), 5, "no poll after the cancel")
				if drive.cancelledErr != nil {
					assert.ErrorIs(t, err, drive.cancelledErr)
				} else {
					assert.NoError(t, err)
				}
			})
		})
	}
}
