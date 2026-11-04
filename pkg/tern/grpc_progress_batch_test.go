package tern

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	ternv1 "github.com/block/schemabot/pkg/proto/ternv1"
)

// batchProgressFakeClient answers BatchProgress with answer and records each
// request. Every other RPC is left to the embedded nil client.
type batchProgressFakeClient struct {
	ternv1.TernClient
	answer func(req *ternv1.BatchProgressRequest) (*ternv1.BatchProgressResponse, error)

	mu       sync.Mutex
	requests []*ternv1.BatchProgressRequest
}

func (c *batchProgressFakeClient) BatchProgress(_ context.Context, req *ternv1.BatchProgressRequest, _ ...grpc.CallOption) (*ternv1.BatchProgressResponse, error) {
	c.mu.Lock()
	c.requests = append(c.requests, req)
	c.mu.Unlock()
	return c.answer(req)
}

func (c *batchProgressFakeClient) sent() []*ternv1.BatchProgressRequest {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]*ternv1.BatchProgressRequest(nil), c.requests...)
}

// answerEachOperationRunning answers every operation it is asked about as a
// running scoped progress answer for that operation.
func answerEachOperationRunning(req *ternv1.BatchProgressRequest) (*ternv1.BatchProgressResponse, error) {
	resp := &ternv1.BatchProgressResponse{ApplyId: req.GetApplyId()}
	for _, id := range req.GetApplyOperationIds() {
		resp.Operations = append(resp.Operations, &ternv1.OperationProgress{
			ApplyOperationId: id,
			Progress:         &ternv1.ProgressResponse{ApplyOperationId: id, State: ternv1.State_STATE_RUNNING},
		})
	}
	return resp, nil
}

// manualRounds holds the rounds a batcher schedules until the test sends them.
type manualRounds struct {
	mu     sync.Mutex
	rounds []func()
}

func (m *manualRounds) schedule(_ time.Duration, f func()) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.rounds = append(m.rounds, f)
}

func (m *manualRounds) scheduled() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return len(m.rounds)
}

// takeNext removes the oldest round the batcher has scheduled and returns it
// unsent.
func (m *manualRounds) takeNext(t *testing.T) func() {
	t.Helper()
	m.mu.Lock()
	defer m.mu.Unlock()
	require.NotEmpty(t, m.rounds, "no round is scheduled")
	round := m.rounds[0]
	m.rounds = m.rounds[1:]
	return round
}

// sendNext sends the oldest round the batcher has scheduled.
func (m *manualRounds) sendNext(t *testing.T) {
	t.Helper()
	m.takeNext(t)()
}

var testBatchKey = progressBatchKey{remoteApplyID: "remote-apply-shared", environment: "staging"}

type batchPoll struct {
	resp *ternv1.ProgressResponse
	err  error
}

// startBatchPolls starts one poll per operation id and returns once every
// poll has joined the pending round.
func startBatchPolls(t *testing.T, b *remoteProgressBatcher, rounds *manualRounds, client ternv1.TernClient, operationIDs ...string) []chan batchPoll {
	t.Helper()
	results := make([]chan batchPoll, len(operationIDs))
	for i, id := range operationIDs {
		results[i] = make(chan batchPoll, 1)
		go func() {
			resp, err := b.progress(t.Context(), client, slog.Default(), testBatchKey, id)
			results[i] <- batchPoll{resp: resp, err: err}
		}()
	}
	require.Eventually(t, func() bool { return pendingWaiters(b, testBatchKey) == len(operationIDs) }, 5*time.Second, time.Millisecond,
		"every poll joins the pending round")
	require.Equal(t, 1, rounds.scheduled(), "the polls share one round")
	return results
}

func pendingWaiters(b *remoteProgressBatcher, key progressBatchKey) int {
	b.mu.Lock()
	defer b.mu.Unlock()
	group := b.groups[key]
	if group == nil || group.pending == nil {
		return 0
	}
	return len(group.pending.waiters)
}

func receivePoll(t *testing.T, result chan batchPoll) batchPoll {
	t.Helper()
	select {
	case poll := <-result:
		return poll
	case <-time.After(5 * time.Second):
		require.FailNow(t, "poll was not answered")
		return batchPoll{}
	}
}

// Three member targets of one deployment share a remote apply, and their
// drives on one pod poll at about the same time. The polls join one round,
// the round asks the data plane once for all three operations, and each
// drive gets the answer for its own operation.
func TestRemoteProgressBatcher_PollsOfOneRemoteApplyShareOneCall(t *testing.T) {
	rounds := &manualRounds{}
	b := &remoteProgressBatcher{afterFunc: rounds.schedule}
	client := &batchProgressFakeClient{answer: answerEachOperationRunning}

	results := startBatchPolls(t, b, rounds, client, "op-1", "op-2", "op-3")
	rounds.sendNext(t)

	for i, id := range []string{"op-1", "op-2", "op-3"} {
		poll := receivePoll(t, results[i])
		require.NoError(t, poll.err)
		assert.Equal(t, id, poll.resp.GetApplyOperationId(), "each drive gets its own operation's answer")
		assert.Equal(t, ternv1.State_STATE_RUNNING, poll.resp.GetState())
	}
	requests := client.sent()
	require.Len(t, requests, 1, "one call answers the round")
	assert.Equal(t, "remote-apply-shared", requests[0].GetApplyId())
	assert.Equal(t, "staging", requests[0].GetEnvironment())
	assert.ElementsMatch(t, []string{"op-1", "op-2", "op-3"}, requests[0].GetApplyOperationIds())
}

// A poll made while a round is in flight is not answered from that round,
// whose answer may have been read before the poll was made. It waits for the
// next round, which is scheduled one poll interval after the last one.
func TestRemoteProgressBatcher_PollDuringARoundWaitsForTheNextOne(t *testing.T) {
	var delays []time.Duration
	var mu sync.Mutex
	rounds := &manualRounds{}
	b := &remoteProgressBatcher{afterFunc: func(d time.Duration, f func()) {
		mu.Lock()
		delays = append(delays, d)
		mu.Unlock()
		rounds.schedule(d, f)
	}}
	inFlight := make(chan struct{})
	release := make(chan struct{})
	var calls int
	client := &batchProgressFakeClient{answer: func(req *ternv1.BatchProgressRequest) (*ternv1.BatchProgressResponse, error) {
		calls++
		if calls == 1 {
			close(inFlight)
			<-release
		}
		return answerEachOperationRunning(req)
	}}

	first := startBatchPolls(t, b, rounds, client, "op-1")
	go rounds.takeNext(t)()
	<-inFlight

	second := startBatchPolls(t, b, rounds, client, "op-2")
	close(release)
	firstPoll := receivePoll(t, first[0])
	require.NoError(t, firstPoll.err)
	assert.Equal(t, "op-1", firstPoll.resp.GetApplyOperationId())

	rounds.sendNext(t)
	secondPoll := receivePoll(t, second[0])
	require.NoError(t, secondPoll.err)
	assert.Equal(t, "op-2", secondPoll.resp.GetApplyOperationId())

	requests := client.sent()
	require.Len(t, requests, 2)
	assert.Equal(t, []string{"op-1"}, requests[0].GetApplyOperationIds())
	assert.Equal(t, []string{"op-2"}, requests[1].GetApplyOperationIds(), "the in-flight round did not answer the later poll")

	mu.Lock()
	defer mu.Unlock()
	require.Len(t, delays, 2)
	assert.Equal(t, time.Duration(0), delays[0], "the first round for a remote apply is sent at once")
	assert.Greater(t, delays[1], time.Duration(0), "the next round waits out the poll interval")
	assert.LessOrEqual(t, delays[1], grpcProgressPollInterval)
}

// An entry the data plane answers with an error reaches that drive as the
// gRPC status a direct Progress call would have failed with, while the other
// drives in the round get their answers.
func TestRemoteProgressBatcher_EntryErrorIsThatOperationsOwnStatus(t *testing.T) {
	rounds := &manualRounds{}
	b := &remoteProgressBatcher{afterFunc: rounds.schedule}
	client := &batchProgressFakeClient{answer: func(req *ternv1.BatchProgressRequest) (*ternv1.BatchProgressResponse, error) {
		return &ternv1.BatchProgressResponse{ApplyId: req.GetApplyId(), Operations: []*ternv1.OperationProgress{
			{ApplyOperationId: "op-1", Progress: &ternv1.ProgressResponse{ApplyOperationId: "op-1", State: ternv1.State_STATE_RUNNING}},
			{ApplyOperationId: "op-2", ErrorCode: int32(codes.Internal), ErrorMessage: "scope progress of apply remote-apply-shared to its operation: apply_operation 2 does not exist"},
		}}, nil
	}}

	results := startBatchPolls(t, b, rounds, client, "op-1", "op-2")
	rounds.sendNext(t)

	ok := receivePoll(t, results[0])
	require.NoError(t, ok.err)
	assert.Equal(t, "op-1", ok.resp.GetApplyOperationId())

	failed := receivePoll(t, results[1])
	require.Error(t, failed.err)
	assert.Nil(t, failed.resp)
	assert.Equal(t, codes.Internal, status.Code(failed.err))
	assert.Contains(t, failed.err.Error(), "apply_operation 2 does not exist")
}

// A data plane that predates BatchProgress answers Unimplemented. Every poll
// in the round is handed back to ask Progress directly, never handed the
// Unimplemented a drive would end on, and later polls of that remote apply
// ask directly without scheduling a round until batching is retried.
func TestRemoteProgressBatcher_UnimplementedHandsPollsBackAndPausesBatching(t *testing.T) {
	rounds := &manualRounds{}
	b := &remoteProgressBatcher{afterFunc: rounds.schedule}
	client := &batchProgressFakeClient{answer: func(*ternv1.BatchProgressRequest) (*ternv1.BatchProgressResponse, error) {
		return nil, status.Error(codes.Unimplemented, "unknown method BatchProgress for service tern.v1.Tern")
	}}

	results := startBatchPolls(t, b, rounds, client, "op-1", "op-2")
	rounds.sendNext(t)
	for _, result := range results {
		assert.ErrorIs(t, receivePoll(t, result).err, errProgressBatchFallback)
	}

	_, err := b.progress(t.Context(), client, slog.Default(), testBatchKey, "op-1")
	require.ErrorIs(t, err, errProgressBatchFallback)
	assert.Zero(t, rounds.scheduled(), "no round is scheduled while batching is paused")
	assert.Len(t, client.sent(), 1, "the data plane is asked once")

	b.mu.Lock()
	b.groups[testBatchKey].unsupportedUntil = time.Now().Add(-time.Second)
	b.mu.Unlock()
	retried := startBatchPolls(t, b, rounds, client, "op-1")
	rounds.sendNext(t)
	assert.ErrorIs(t, receivePoll(t, retried[0]).err, errProgressBatchFallback)
	assert.Len(t, client.sent(), 2, "batching is tried again once the pause ends")
}

// An answer the batch cannot stand in for hands that poll back to ask
// directly: an entry left out, an empty entry, an entry answered
// Unimplemented, an answer for another remote apply, or an answer too large
// to receive.
func TestRemoteProgressBatcher_UnusableAnswersHandPollsBack(t *testing.T) {
	testCases := []struct {
		name   string
		answer func(req *ternv1.BatchProgressRequest) (*ternv1.BatchProgressResponse, error)
	}{
		{
			name: "entry left out",
			answer: func(req *ternv1.BatchProgressRequest) (*ternv1.BatchProgressResponse, error) {
				return &ternv1.BatchProgressResponse{ApplyId: req.GetApplyId()}, nil
			},
		},
		{
			name: "entry with neither progress nor error",
			answer: func(req *ternv1.BatchProgressRequest) (*ternv1.BatchProgressResponse, error) {
				return &ternv1.BatchProgressResponse{ApplyId: req.GetApplyId(), Operations: []*ternv1.OperationProgress{{ApplyOperationId: "op-1"}}}, nil
			},
		},
		{
			name: "entry answered unimplemented",
			answer: func(req *ternv1.BatchProgressRequest) (*ternv1.BatchProgressResponse, error) {
				return &ternv1.BatchProgressResponse{ApplyId: req.GetApplyId(), Operations: []*ternv1.OperationProgress{
					{ApplyOperationId: "op-1", ErrorCode: int32(codes.Unimplemented), ErrorMessage: "not here"},
				}}, nil
			},
		},
		{
			name: "answer for another remote apply",
			answer: func(*ternv1.BatchProgressRequest) (*ternv1.BatchProgressResponse, error) {
				return answerEachOperationRunning(&ternv1.BatchProgressRequest{ApplyId: "remote-apply-other", ApplyOperationIds: []string{"op-1"}})
			},
		},
		{
			name: "answer too large to receive",
			answer: func(*ternv1.BatchProgressRequest) (*ternv1.BatchProgressResponse, error) {
				return nil, status.Error(codes.ResourceExhausted, "grpc: received message larger than max")
			},
		},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			rounds := &manualRounds{}
			b := &remoteProgressBatcher{afterFunc: rounds.schedule}
			client := &batchProgressFakeClient{answer: tc.answer}

			results := startBatchPolls(t, b, rounds, client, "op-1")
			rounds.sendNext(t)
			poll := receivePoll(t, results[0])
			assert.ErrorIs(t, poll.err, errProgressBatchFallback)
			assert.Nil(t, poll.resp)
		})
	}
}

// A failure of the whole call, such as the data plane not knowing the remote
// apply, reaches every drive in the round as that drive's own poll error, so
// each takes the path a direct poll would have.
func TestRemoteProgressBatcher_CallErrorReachesEveryPoll(t *testing.T) {
	rounds := &manualRounds{}
	b := &remoteProgressBatcher{afterFunc: rounds.schedule}
	client := &batchProgressFakeClient{answer: func(*ternv1.BatchProgressRequest) (*ternv1.BatchProgressResponse, error) {
		return nil, status.Error(codes.NotFound, "get apply remote-apply-shared: apply not found")
	}}

	results := startBatchPolls(t, b, rounds, client, "op-1", "op-2")
	rounds.sendNext(t)
	for _, result := range results {
		assert.Equal(t, codes.NotFound, status.Code(receivePoll(t, result).err))
	}
}

// A round with more operations than one call may name is answered by several
// calls, each within the bound, and every poll still gets its answer.
func TestRemoteProgressBatcher_LargeRoundIsSplitIntoBoundedCalls(t *testing.T) {
	rounds := &manualRounds{}
	b := &remoteProgressBatcher{afterFunc: rounds.schedule}
	client := &batchProgressFakeClient{answer: answerEachOperationRunning}

	ids := make([]string, maxProgressBatchOperations+5)
	for i := range ids {
		ids[i] = fmt.Sprintf("op-%d", i)
	}
	results := startBatchPolls(t, b, rounds, client, ids...)
	rounds.sendNext(t)

	for i, result := range results {
		poll := receivePoll(t, result)
		require.NoError(t, poll.err)
		assert.Equal(t, ids[i], poll.resp.GetApplyOperationId())
	}
	requests := client.sent()
	require.Len(t, requests, 2)
	named := 0
	for _, req := range requests {
		assert.LessOrEqual(t, len(req.GetApplyOperationIds()), maxProgressBatchOperations)
		named += len(req.GetApplyOperationIds())
	}
	assert.Equal(t, len(ids), named, "every operation is named once")
}

// Two polls for the same operation in one round, such as a drive and a
// request-time read, name it once and each get their own copy of its answer.
func TestRemoteProgressBatcher_SameOperationTwiceIsAskedOnce(t *testing.T) {
	rounds := &manualRounds{}
	b := &remoteProgressBatcher{afterFunc: rounds.schedule}
	client := &batchProgressFakeClient{answer: answerEachOperationRunning}

	results := startBatchPolls(t, b, rounds, client, "op-1", "op-1")
	rounds.sendNext(t)

	first := receivePoll(t, results[0])
	second := receivePoll(t, results[1])
	require.NoError(t, first.err)
	require.NoError(t, second.err)
	assert.Equal(t, "op-1", first.resp.GetApplyOperationId())
	assert.Equal(t, "op-1", second.resp.GetApplyOperationId())
	assert.NotSame(t, first.resp, second.resp, "each poll owns its answer")
	require.Len(t, client.sent(), 1)
	assert.Equal(t, []string{"op-1"}, client.sent()[0].GetApplyOperationIds())
}

// A drive that stops waiting returns its context's error without waiting for
// the round, and a closed batcher hands every later poll back to ask
// directly.
func TestRemoteProgressBatcher_CancelledPollAndClosedBatcher(t *testing.T) {
	rounds := &manualRounds{}
	b := &remoteProgressBatcher{afterFunc: rounds.schedule}
	client := &batchProgressFakeClient{answer: answerEachOperationRunning}

	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	_, err := b.progress(ctx, client, slog.Default(), testBatchKey, "op-1")
	require.ErrorIs(t, err, context.Canceled)

	b.close()
	_, err = b.progress(t.Context(), client, slog.Default(), testBatchKey, "op-2")
	require.ErrorIs(t, err, errProgressBatchFallback)
}

// batchingTernServer answers both progress RPCs, counting each, so a test can
// see which one a client's poll used.
type batchingTernServer struct {
	ternv1.UnimplementedTernServer
	batches bool

	mu              sync.Mutex
	progressCalls   int
	batchCalls      int
	batchedRequests [][]string
}

func (s *batchingTernServer) Progress(_ context.Context, req *ternv1.ProgressRequest) (*ternv1.ProgressResponse, error) {
	s.mu.Lock()
	s.progressCalls++
	s.mu.Unlock()
	return &ternv1.ProgressResponse{ApplyOperationId: req.GetApplyOperationId(), State: ternv1.State_STATE_RUNNING}, nil
}

func (s *batchingTernServer) BatchProgress(ctx context.Context, req *ternv1.BatchProgressRequest) (*ternv1.BatchProgressResponse, error) {
	if !s.batches {
		return s.UnimplementedTernServer.BatchProgress(ctx, req)
	}
	s.mu.Lock()
	s.batchCalls++
	s.batchedRequests = append(s.batchedRequests, req.GetApplyOperationIds())
	s.mu.Unlock()
	return answerEachOperationRunning(req)
}

func (s *batchingTernServer) counts() (progress, batch int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.progressCalls, s.batchCalls
}

// A drive's scoped poll is answered through BatchProgress by a data plane
// that serves it, and through Progress by one that predates it, with the
// same answer either way. An unscoped poll always asks Progress.
func TestGRPCClient_BatchedProgressUsesBatchProgressWhenTheDataPlaneServesIt(t *testing.T) {
	testCases := []struct {
		name         string
		batches      bool
		wantProgress int
		wantBatch    int
	}{
		{name: "data plane serves batch progress", batches: true, wantProgress: 0, wantBatch: 1},
		{name: "data plane predates batch progress", batches: false, wantProgress: 1, wantBatch: 0},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			server := &batchingTernServer{batches: tc.batches}
			client := newRetryTestClient(t, server)

			resp, err := client.batchedProgress(t.Context(), &ternv1.ProgressRequest{ApplyId: "remote-apply-shared", Environment: "staging", ApplyOperationId: "op-7"})
			require.NoError(t, err)
			assert.Equal(t, "op-7", resp.GetApplyOperationId())
			assert.Equal(t, ternv1.State_STATE_RUNNING, resp.GetState())

			progress, batch := server.counts()
			assert.Equal(t, tc.wantProgress, progress)
			assert.Equal(t, tc.wantBatch, batch)
		})
	}

	t.Run("unscoped poll", func(t *testing.T) {
		server := &batchingTernServer{batches: true}
		client := newRetryTestClient(t, server)

		_, err := client.batchedProgress(t.Context(), &ternv1.ProgressRequest{ApplyId: "remote-apply-whole", Environment: "staging"})
		require.NoError(t, err)
		progress, batch := server.counts()
		assert.Equal(t, 1, progress)
		assert.Equal(t, 0, batch)
	})
}
